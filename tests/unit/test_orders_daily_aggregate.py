from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
from typing import Any

import pytest
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.types import DagRunType


DAG_PATH = Path(__file__).parents[2] / "dags" / "orders_daily_aggregate.py"
SPEC = importlib.util.spec_from_file_location("orders_daily_aggregate", DAG_PATH)
assert SPEC is not None and SPEC.loader is not None
orders_daily_aggregate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = orders_daily_aggregate
SPEC.loader.exec_module(orders_daily_aggregate)


class FakeCursor:
    def __init__(
        self,
        row: tuple[Any, ...] | None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.row = row
        self.error = error
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
        if self.error is not None:
            raise self.error

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class FakeConnection:
    def __init__(
        self,
        row: tuple[Any, ...] | None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.cursor_instance = FakeCursor(row, error=error)
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


def test_publish_commits_one_parameterized_function_call() -> None:
    connection = FakeConnection(
        (
            2,
            2,
            Decimal("170.00"),
            datetime(2025, 3, 2, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 1, 12, 0, tzinfo=timezone.utc),
        )
    )

    result = orders_daily_aggregate._publish_daily_aggregate(
        date(2025, 3, 1),
        hook=FakeHook(connection),
    )

    assert len(connection.cursor_instance.executions) == 1
    sql, parameters = connection.cursor_instance.executions[0]
    assert "orders_demo.publish_daily_order_aggregate(%s, %s)" in sql
    assert parameters == (date(2025, 3, 1), 0)
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closes == 1
    assert result == {
        "affected_date": "2025-03-01",
        "records_read": 2,
        "order_count": 2,
        "sales_amount": "170.00",
        "published_at": "2025-03-02T00:00:00Z",
        "source_max_updated_at": "2025-03-01T12:00:00Z",
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
        (
            0,
            0,
            Decimal("0.00"),
            datetime(2025, 3, 2, 0, 0, tzinfo=timezone.utc),
            None,
        )
    )

    orders_daily_aggregate._publish_daily_aggregate(
        date(2025, 3, 1),
        demo_delay_seconds=2.5,
        hook=FakeHook(connection),
    )

    assert connection.cursor_instance.executions[0][1] == (
        date(2025, 3, 1),
        2.5,
    )


def test_publish_rolls_back_and_closes_on_database_failure() -> None:
    connection = FakeConnection(None, error=RuntimeError("database unavailable"))

    with pytest.raises(RuntimeError, match="database unavailable"):
        orders_daily_aggregate._publish_daily_aggregate(
            date(2025, 3, 1),
            hook=FakeHook(connection),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closes == 1


def test_publish_rejects_an_empty_database_result_before_commit() -> None:
    connection = FakeConnection(None)

    with pytest.raises(RuntimeError, match="returned no result"):
        orders_daily_aggregate._publish_daily_aggregate(
            date(2025, 3, 1),
            hook=FakeHook(connection),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closes == 1


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
