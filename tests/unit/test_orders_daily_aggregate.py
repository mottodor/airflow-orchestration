from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import importlib.util
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import sys
from typing import Any

import pendulum
import pytest
from airflow.exceptions import AirflowTaskTimeout
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.types import DagRunType


DAG_PATH = Path(__file__).parents[2] / "dags" / "orders_daily_aggregate.py"
SPEC = importlib.util.spec_from_file_location("orders_daily_aggregate", DAG_PATH)
assert SPEC is not None and SPEC.loader is not None
orders_daily_aggregate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = orders_daily_aggregate
SPEC.loader.exec_module(orders_daily_aggregate)

SUMMARY_KEYS = {
    "affected_date",
    "trigger_reason",
    "initial_trigger_reason",
    "records_read",
    "unique_orders",
    "old_aggregate",
    "new_aggregate",
    "publication_state",
    "lock_waited",
    "lock_wait_ms",
    "try_number",
    "error",
}


class FakeCursor:
    def __init__(
        self,
        results: list[tuple[Any, ...] | None | BaseException],
    ) -> None:
        self.results = list(results)
        self.current_row: tuple[Any, ...] | None = None
        self.executions: list[tuple[str, tuple[Any, ...]]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: Any,
    ) -> None:
        return None

    def execute(self, sql: str, parameters: tuple[Any, ...]) -> None:
        self.executions.append((sql, parameters))
        if not self.results:
            raise AssertionError("No fake database result configured for execute")
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        self.current_row = result

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.current_row


class FakeConnection:
    def __init__(
        self,
        results: list[tuple[Any, ...] | None | BaseException],
    ) -> None:
        self.cursor_instance = FakeCursor(results)
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1


class FakeHook:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def get_conn(self) -> FakeConnection:
        return self.connection


def test_manual_affected_date_overrides_the_data_interval() -> None:
    assert orders_daily_aggregate._resolve_affected_date(
        {
            "params": {"affected_date": "2025-03-01"},
            "data_interval_start": datetime(2025, 3, 2, tzinfo=timezone.utc),
            "data_interval_end": datetime(2025, 3, 3, tzinfo=timezone.utc),
        }
    ) == date(2025, 3, 1)


def test_scheduled_run_uses_one_complete_utc_data_interval() -> None:
    assert orders_daily_aggregate._resolve_affected_date(
        {
            "params": {"affected_date": None},
            "dag_run": SimpleNamespace(run_type=DagRunType.SCHEDULED),
            "data_interval_start": datetime(2025, 3, 1, tzinfo=timezone.utc),
            "data_interval_end": datetime(2025, 3, 2, tzinfo=timezone.utc),
        }
    ) == date(2025, 3, 1)


def test_scheduled_run_accepts_airflow_pendulum_data_interval() -> None:
    assert orders_daily_aggregate._resolve_affected_date(
        {
            "params": {"affected_date": None},
            "dag_run": SimpleNamespace(run_type=DagRunType.SCHEDULED),
            "data_interval_start": pendulum.datetime(2025, 3, 1, tz="UTC"),
            "data_interval_end": pendulum.datetime(2025, 3, 2, tz="UTC"),
        }
    ) == date(2025, 3, 1)


def test_manual_run_requires_an_explicit_affected_date() -> None:
    with pytest.raises(ValueError, match="required for a manual DAG run"):
        orders_daily_aggregate._resolve_affected_date(
            {
                "params": {"affected_date": None},
                "dag_run": SimpleNamespace(run_type=DagRunType.MANUAL),
                "data_interval_start": datetime(2025, 3, 1, tzinfo=timezone.utc),
                "data_interval_end": datetime(2025, 3, 2, tzinfo=timezone.utc),
            }
        )


@pytest.mark.parametrize(
    "context",
    [
        {"params": {"affected_date": None}},
        {
            "params": {"affected_date": None},
            "data_interval_start": datetime(2025, 3, 1, 1, tzinfo=timezone.utc),
            "data_interval_end": datetime(2025, 3, 2, 1, tzinfo=timezone.utc),
        },
        {
            "params": {"affected_date": None},
            "data_interval_start": datetime(2025, 3, 1, tzinfo=timezone.utc),
            "data_interval_end": datetime(2025, 3, 3, tzinfo=timezone.utc),
        },
    ],
)
def test_run_without_an_exact_target_utc_day_is_rejected(
    context: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="affected_date|required|complete UTC day"):
        orders_daily_aggregate._resolve_affected_date(context)


def test_publish_captures_old_and_new_values_in_one_transaction() -> None:
    connection = FakeConnection(
        [
            (True,),
            (
                1,
                Decimal("150.00"),
                datetime(2025, 3, 1, 13, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 1, 11, 0, tzinfo=timezone.utc),
            ),
            (
                2,
                2,
                Decimal("170.00"),
                datetime(2025, 3, 2, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 1, 12, 0, tzinfo=timezone.utc),
            ),
        ]
    )

    result = orders_daily_aggregate._publish_daily_aggregate(
        date(2025, 3, 1),
        hook=FakeHook(connection),
    )

    assert len(connection.cursor_instance.executions) == 3
    assert connection.cursor_instance.executions[0][1] == (
        orders_daily_aggregate.ADVISORY_LOCK_NAMESPACE,
        739311,
    )
    sql, parameters = connection.cursor_instance.executions[2]
    assert "orders_demo.publish_daily_order_aggregate(%s, %s)" in sql
    assert parameters == (date(2025, 3, 1), 0)
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closes == 1
    assert result == {
        "records_read": 2,
        "unique_orders": 2,
        "old_aggregate": {
            "order_count": 1,
            "sales_amount": "150.00",
            "published_at": "2025-03-01T13:00:00Z",
            "source_max_updated_at": "2025-03-01T11:00:00Z",
        },
        "new_aggregate": {
            "order_count": 2,
            "sales_amount": "170.00",
            "published_at": "2025-03-02T00:00:00Z",
            "source_max_updated_at": "2025-03-01T12:00:00Z",
        },
        "publication_state": "published",
        "lock_waited": False,
        "lock_wait_ms": 0.0,
    }


def test_demo_delay_is_rejected_unless_local_fault_injection_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DEMO_FAULT_INJECTION_ENABLED", raising=False)

    with pytest.raises(ValueError, match="requires DEMO_FAULT_INJECTION_ENABLED"):
        orders_daily_aggregate._resolve_demo_delay_seconds(
            {"params": {"demo_delay_seconds": 1}}
        )


def test_demo_delay_is_available_for_explicit_local_probes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEMO_FAULT_INJECTION_ENABLED", "true")

    assert orders_daily_aggregate._resolve_demo_delay_seconds(
        {"params": {"demo_delay_seconds": 1.5}}
    ) == 1.5


@pytest.mark.parametrize("value", [True, "1", -1, 61])
def test_invalid_demo_delay_is_rejected(
    value: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEMO_FAULT_INJECTION_ENABLED", "true")

    with pytest.raises(ValueError, match="demo_delay_seconds"):
        orders_daily_aggregate._resolve_demo_delay_seconds(
            {"params": {"demo_delay_seconds": value}}
        )


def test_publish_passes_the_probe_delay_to_the_transactional_function() -> None:
    connection = FakeConnection(
        [
            (True,),
            None,
            (
                0,
                0,
                Decimal("0.00"),
                datetime(2025, 3, 2, 0, 0, tzinfo=timezone.utc),
                None,
            ),
        ]
    )

    orders_daily_aggregate._publish_daily_aggregate(
        date(2025, 3, 1),
        demo_delay_seconds=2.5,
        hook=FakeHook(connection),
    )

    assert connection.cursor_instance.executions[2][1] == (
        date(2025, 3, 1),
        2.5,
    )


def test_publish_measures_only_contended_lock_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(
        [
            (False,),
            (None,),
            None,
            (
                0,
                0,
                Decimal("0.00"),
                datetime(2025, 3, 2, 0, 0, tzinfo=timezone.utc),
                None,
            ),
        ]
    )
    times = iter([10.0, 10.25])
    monkeypatch.setattr(orders_daily_aggregate, "monotonic", lambda: next(times))

    result = orders_daily_aggregate._publish_daily_aggregate(
        date(2025, 3, 1),
        hook=FakeHook(connection),
    )

    assert "pg_advisory_xact_lock" in connection.cursor_instance.executions[1][0]
    assert result["lock_waited"] is True
    assert result["lock_wait_ms"] == 250.0


def test_timeout_during_lock_wait_is_reported(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timeout = AirflowTaskTimeout("execution exceeded 10 seconds")
    connection = FakeConnection([(False,), timeout])
    times = iter([10.0, 10.5])
    monkeypatch.setattr(orders_daily_aggregate, "monotonic", lambda: next(times))
    context = {
        "params": {"affected_date": "2025-03-01", "demo_delay_seconds": 0},
        "dag_run": SimpleNamespace(run_type=DagRunType.SCHEDULED),
        "ti": SimpleNamespace(try_number=1),
    }

    with caplog.at_level(logging.INFO, logger=orders_daily_aggregate.LOGGER.name):
        with pytest.raises(AirflowTaskTimeout):
            orders_daily_aggregate._run_daily_aggregate(
                context,
                hook=FakeHook(connection),
            )

    summary = json.loads(caplog.records[0].message)
    assert set(summary) == SUMMARY_KEYS
    assert summary["lock_waited"] is True
    assert summary["lock_wait_ms"] == 500.0
    assert summary["publication_state"] == "not_started"
    assert connection.rollbacks == 1
    assert connection.closes == 1


def test_publish_rolls_back_and_closes_on_database_failure() -> None:
    connection = FakeConnection(
        [(True,), None, RuntimeError("database unavailable")]
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        orders_daily_aggregate._publish_daily_aggregate(
            date(2025, 3, 1),
            hook=FakeHook(connection),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closes == 1


def test_publish_rejects_an_empty_database_result_before_commit() -> None:
    connection = FakeConnection([(True,), None, None])

    with pytest.raises(RuntimeError, match="returned no result"):
        orders_daily_aggregate._publish_daily_aggregate(
            date(2025, 3, 1),
            hook=FakeHook(connection),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closes == 1


def test_success_summary_is_logged_as_json_and_returned_for_xcom(
    caplog: pytest.LogCaptureFixture,
) -> None:
    connection = FakeConnection(
        [
            (True,),
            None,
            (
                4,
                2,
                Decimal("170.00"),
                datetime(2025, 3, 2, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 1, 12, 0, tzinfo=timezone.utc),
            ),
        ]
    )
    context = {
        "params": {
            "affected_date": "2025-03-01",
            "reason": "late_data",
            "demo_delay_seconds": 0,
        },
        "dag_run": SimpleNamespace(run_type=DagRunType.MANUAL),
        "ti": SimpleNamespace(try_number=1),
    }

    with caplog.at_level(logging.INFO, logger=orders_daily_aggregate.LOGGER.name):
        summary = orders_daily_aggregate._run_daily_aggregate(
            context,
            hook=FakeHook(connection),
        )

    assert len(caplog.records) == 1
    assert json.loads(caplog.records[0].message) == summary
    assert set(summary) == SUMMARY_KEYS
    assert summary == {
        "affected_date": "2025-03-01",
        "trigger_reason": "late_data",
        "initial_trigger_reason": "late_data",
        "records_read": 4,
        "unique_orders": 2,
        "old_aggregate": None,
        "new_aggregate": {
            "order_count": 2,
            "sales_amount": "170.00",
            "published_at": "2025-03-02T00:00:00Z",
            "source_max_updated_at": "2025-03-01T12:00:00Z",
        },
        "publication_state": "published",
        "lock_waited": False,
        "lock_wait_ms": 0.0,
        "try_number": 1,
        "error": None,
    }


def test_timeout_summary_preserves_old_result_and_reraises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    timeout = AirflowTaskTimeout("execution exceeded 10 seconds")
    connection = FakeConnection(
        [
            (True,),
            (
                2,
                Decimal("170.00"),
                datetime(2025, 3, 1, 13, 0, tzinfo=timezone.utc),
                datetime(2025, 3, 1, 12, 0, tzinfo=timezone.utc),
            ),
            timeout,
        ]
    )
    context = {
        "params": {"affected_date": "2025-03-01", "demo_delay_seconds": 0},
        "dag_run": SimpleNamespace(run_type=DagRunType.SCHEDULED),
        "ti": SimpleNamespace(try_number=2),
    }

    with caplog.at_level(logging.INFO, logger=orders_daily_aggregate.LOGGER.name):
        with pytest.raises(AirflowTaskTimeout) as raised:
            orders_daily_aggregate._run_daily_aggregate(
                context,
                hook=FakeHook(connection),
            )

    assert raised.value is timeout
    assert connection.commits == 0
    assert connection.rollbacks == 1
    summary = json.loads(caplog.records[0].message)
    assert set(summary) == SUMMARY_KEYS
    assert summary["trigger_reason"] == "retry"
    assert summary["initial_trigger_reason"] == "schedule"
    assert summary["records_read"] is None
    assert summary["unique_orders"] is None
    assert summary["old_aggregate"]["sales_amount"] == "170.00"
    assert summary["new_aggregate"] is None
    assert summary["publication_state"] == "old_result_preserved"
    assert summary["try_number"] == 2
    assert summary["error"] == (
        "AirflowTaskTimeout: execution exceeded 10 seconds"
    )
    assert getattr(timeout, "_orders_summary_logged") is True


def test_failure_callback_logs_a_fallback_summary_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    context = {
        "params": {"affected_date": "2025-03-01"},
        "dag_run": SimpleNamespace(run_type=DagRunType.BACKFILL_JOB),
        "ti": SimpleNamespace(try_number=1),
        "exception": RuntimeError("worker failure"),
    }

    with caplog.at_level(logging.INFO, logger=orders_daily_aggregate.LOGGER.name):
        orders_daily_aggregate._failure_summary_callback(context)

    summary = json.loads(caplog.records[0].message)
    assert set(summary) == SUMMARY_KEYS
    assert summary["affected_date"] == "2025-03-01"
    assert summary["trigger_reason"] == "backfill"
    assert summary["initial_trigger_reason"] == "backfill"
    assert summary["publication_state"] == "not_started"
    assert summary["lock_waited"] is None
    assert summary["lock_wait_ms"] is None
    assert summary["error"] == "RuntimeError: worker failure"


def test_dag_has_the_daily_utc_data_interval_contract() -> None:
    dag = orders_daily_aggregate.dag

    assert dag.dag_id == "orders_daily_aggregate"
    assert dag.catchup is True
    assert dag.max_active_runs == 4
    assert dag.start_date == datetime(2025, 3, 1, tzinfo=timezone.utc)
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.timetable.summary == "0 0 * * *"

    task = dag.task_dict["calculate_daily_aggregate"]
    assert task.pool == "orders_compute"
    assert task.pool_slots == 1
    assert task.execution_timeout.total_seconds() == 10
    assert task.retries == 1
    assert task.retry_delay.total_seconds() == 5
    assert task.on_retry_callback == [
        orders_daily_aggregate._failure_summary_callback
    ]
    assert task.on_failure_callback == [
        orders_daily_aggregate._failure_summary_callback
    ]

    first_run = dag.timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(
            earliest=dag.start_date,
            latest=None,
            catchup=dag.catchup,
        ),
    )
    assert first_run is not None
    assert first_run.data_interval.start == datetime(
        2025, 3, 1, tzinfo=timezone.utc
    )
    assert first_run.data_interval.end == datetime(
        2025, 3, 2, tzinfo=timezone.utc
    )
