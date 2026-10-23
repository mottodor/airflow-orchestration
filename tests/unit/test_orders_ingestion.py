from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import importlib.util
from pathlib import Path
import sys
from typing import Any

import pytest


DAG_PATH = Path(__file__).parents[2] / "dags" / "orders_ingestion.py"
SPEC = importlib.util.spec_from_file_location("orders_ingestion", DAG_PATH)
assert SPEC is not None and SPEC.loader is not None
orders_ingestion = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = orders_ingestion
SPEC.loader.exec_module(orders_ingestion)


VALID_PARAMS = {
    "change_id": "change-2025-03-03-c",
    "reason": "late_order",
    "events": [
        {
            "order_id": "C",
            "order_ts": "2025-03-01T15:00:00Z",
            "amount": 30.00,
            "updated_at": "2025-03-03T09:00:00Z",
            "ingested_at": "2025-03-03T09:01:00Z",
        }
    ],
}


class FakeCursor:
    def __init__(
        self,
        *,
        fetchone_results: list[tuple[Any, ...] | None] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.error = error
        self.fetchone_results = list(
            fetchone_results if fetchone_results is not None else [("registered",)]
        )
        self.current_result: tuple[Any, ...] | None = None
        self.executions: list[tuple[str, str, Any]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: Any,
    ) -> None:
        return None

    def execute(self, sql: str, values: tuple[Any, ...]) -> None:
        self.executions.append(("execute", sql, values))
        if self.error is not None:
            raise self.error
        self.current_result = self.fetchone_results.pop(0)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.current_result

    def executemany(self, sql: str, values: list[tuple[Any, ...]]) -> None:
        self.executions.append(("executemany", sql, values))
        if self.error is not None:
            raise self.error


class FakeConnection:
    def __init__(
        self,
        *,
        fetchone_results: list[tuple[Any, ...] | None] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.cursor_instance = FakeCursor(
            fetchone_results=fetchone_results,
            error=error,
        )
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


class FakeOutletEvent:
    def __init__(self) -> None:
        self.extra: dict[str, str] | None = None


class FakeOutletEvents:
    def __init__(self) -> None:
        self.event = FakeOutletEvent()

    def __getitem__(self, asset: Any) -> FakeOutletEvent:
        assert asset is orders_ingestion.RAW_ORDER_EVENTS_ASSET
        return self.event


class FailingOutletEvents:
    def __getitem__(self, asset: Any) -> FakeOutletEvent:
        assert asset is orders_ingestion.RAW_ORDER_EVENTS_ASSET
        raise RuntimeError("outlet unavailable")


def test_asset_uri_is_normalized_to_the_postgres_default_port() -> None:
    assert (
        orders_ingestion.RAW_ORDER_EVENTS_ASSET.uri
        == "postgres://postgres:5432/orders_demo/orders_demo/raw_order_events"
    )


def test_ingest_change_inserts_input_and_sets_exact_asset_metadata() -> None:
    connection = FakeConnection()
    outlet_events = FakeOutletEvents()

    result = orders_ingestion._ingest_change(
        VALID_PARAMS,
        outlet_events,
        hook=FakeHook(connection),
    )

    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closes == 1
    assert len(connection.cursor_instance.executions) == 2

    operation, sql, values = connection.cursor_instance.executions[0]
    assert operation == "execute"
    assert "INSERT INTO orders_demo.ingestion_changes" in sql
    assert values == (
        "change-2025-03-03-c",
        "late_order",
        date(2025, 3, 1),
        orders_ingestion._change_payload_sha256(
            orders_ingestion._normalize_change(VALID_PARAMS)
        ),
        1,
    )

    operation, sql, values = connection.cursor_instance.executions[1]
    assert operation == "executemany"
    assert "INSERT INTO orders_demo.raw_order_events" in sql
    assert values == [
        (
            "C",
            datetime(2025, 3, 1, 15, 0, tzinfo=timezone.utc),
            Decimal("30.0"),
            datetime(2025, 3, 3, 9, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 3, 9, 1, tzinfo=timezone.utc),
        )
    ]
    assert outlet_events.event.extra == {
        "affected_date": "2025-03-01",
        "reason": "late_order",
        "change_id": "change-2025-03-03-c",
    }
    assert result == {
        "affected_date": "2025-03-01",
        "reason": "late_order",
        "change_id": "change-2025-03-03-c",
        "inserted_event_count": 1,
        "change_replayed": False,
    }


def test_same_change_id_and_payload_replays_without_duplicate_insert() -> None:
    change = orders_ingestion._normalize_change(VALID_PARAMS)
    connection = FakeConnection(
        fetchone_results=[
            None,
            (
                change.reason,
                change.affected_date,
                orders_ingestion._change_payload_sha256(change),
                len(change.events),
            ),
        ]
    )
    outlet_events = FakeOutletEvents()

    result = orders_ingestion._ingest_change(
        VALID_PARAMS,
        outlet_events,
        hook=FakeHook(connection),
    )

    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert [execution[0] for execution in connection.cursor_instance.executions] == [
        "execute",
        "execute",
    ]
    assert result["inserted_event_count"] == 0
    assert result["change_replayed"] is True
    assert outlet_events.event.extra == {
        "affected_date": "2025-03-01",
        "reason": "late_order",
        "change_id": "change-2025-03-03-c",
    }


def test_same_change_id_with_different_payload_is_rejected() -> None:
    change = orders_ingestion._normalize_change(VALID_PARAMS)
    connection = FakeConnection(
        fetchone_results=[
            None,
            (
                change.reason,
                change.affected_date,
                "0" * 64,
                len(change.events),
            ),
        ]
    )
    outlet_events = FakeOutletEvents()

    with pytest.raises(ValueError, match="already exists with different content"):
        orders_ingestion._ingest_change(
            VALID_PARAMS,
            outlet_events,
            hook=FakeHook(connection),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert outlet_events.event.extra is None


def test_failure_after_commit_can_replay_and_emit_asset_metadata() -> None:
    first_connection = FakeConnection()
    first_outlet_events = FakeOutletEvents()

    with pytest.raises(RuntimeError, match="after ingestion commit"):
        orders_ingestion._ingest_change(
            VALID_PARAMS,
            first_outlet_events,
            hook=FakeHook(first_connection),
            fail_after_commit=True,
        )

    assert first_connection.commits == 1
    assert first_outlet_events.event.extra is None

    change = orders_ingestion._normalize_change(VALID_PARAMS)
    retry_connection = FakeConnection(
        fetchone_results=[
            None,
            (
                change.reason,
                change.affected_date,
                orders_ingestion._change_payload_sha256(change),
                len(change.events),
            ),
        ]
    )
    retry_outlet_events = FakeOutletEvents()

    result = orders_ingestion._ingest_change(
        VALID_PARAMS,
        retry_outlet_events,
        hook=FakeHook(retry_connection),
    )

    assert result["inserted_event_count"] == 0
    assert result["change_replayed"] is True
    assert retry_outlet_events.event.extra is not None


def test_change_with_multiple_utc_dates_is_rejected_before_insert() -> None:
    params = {
        **VALID_PARAMS,
        "events": [
            VALID_PARAMS["events"][0],
            {
                **VALID_PARAMS["events"][0],
                "order_id": "D",
                "order_ts": "2025-03-02T00:00:00Z",
            },
        ],
    }
    connection = FakeConnection()
    outlet_events = FakeOutletEvents()

    with pytest.raises(ValueError, match="exactly one UTC date"):
        orders_ingestion._ingest_change(
            params,
            outlet_events,
            hook=FakeHook(connection),
        )

    assert connection.cursor_instance.executions == []
    assert connection.commits == 0
    assert outlet_events.event.extra is None


def test_outlet_lookup_failure_happens_before_database_commit() -> None:
    connection = FakeConnection()

    with pytest.raises(RuntimeError, match="outlet unavailable"):
        orders_ingestion._ingest_change(
            VALID_PARAMS,
            FailingOutletEvents(),
            hook=FakeHook(connection),
        )

    assert connection.cursor_instance.executions == []
    assert connection.commits == 0
    assert connection.rollbacks == 0
    assert connection.closes == 0


def test_database_failure_rolls_back_and_does_not_set_asset_metadata() -> None:
    connection = FakeConnection(error=RuntimeError("database unavailable"))
    outlet_events = FakeOutletEvents()

    with pytest.raises(RuntimeError, match="database unavailable"):
        orders_ingestion._ingest_change(
            VALID_PARAMS,
            outlet_events,
            hook=FakeHook(connection),
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closes == 1
    assert outlet_events.event.extra is None


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("order_ts", "2025-03-01T15:00:00", "UTC offset"),
        ("updated_at", "2025-03-03T12:00:00+03:00", "UTC offset"),
        ("amount", -0.01, "nonnegative"),
        ("amount", 1.001, "at most two decimal places"),
    ],
)
def test_invalid_event_values_are_rejected(field: str, value: Any, message: str) -> None:
    params = {
        **VALID_PARAMS,
        "events": [{**VALID_PARAMS["events"][0], field: value}],
    }

    with pytest.raises(ValueError, match=message):
        orders_ingestion._normalize_change(params)


def test_normalized_change_uses_the_order_timestamp_utc_date() -> None:
    change = orders_ingestion._normalize_change(VALID_PARAMS)

    assert change.affected_date == date(2025, 3, 1)


def test_positive_and_negative_zero_have_the_same_canonical_payload() -> None:
    negative_zero = orders_ingestion._normalize_change(
        {
            **VALID_PARAMS,
            "events": [{**VALID_PARAMS["events"][0], "amount": -0.0}],
        }
    )
    positive_zero = orders_ingestion._normalize_change(
        {
            **VALID_PARAMS,
            "events": [{**VALID_PARAMS["events"][0], "amount": 0.0}],
        }
    )

    assert negative_zero.events[0].amount == Decimal("0.00")
    assert negative_zero.events[0].amount.as_tuple().sign == 0
    assert orders_ingestion._change_payload_sha256(
        negative_zero
    ) == orders_ingestion._change_payload_sha256(positive_zero)


def test_demo_failure_after_commit_is_rejected_without_fault_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DEMO_FAULT_INJECTION_ENABLED", raising=False)

    with pytest.raises(ValueError, match="requires DEMO_FAULT_INJECTION_ENABLED"):
        orders_ingestion._resolve_demo_fail_after_commit(
            {"demo_fail_after_commit_once": True},
            1,
        )


def test_demo_failure_after_commit_only_applies_to_first_try(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEMO_FAULT_INJECTION_ENABLED", "true")
    params = {"demo_fail_after_commit_once": True}

    assert orders_ingestion._resolve_demo_fail_after_commit(params, 1) is True
    assert orders_ingestion._resolve_demo_fail_after_commit(params, 2) is False


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_demo_failure_after_commit_requires_boolean(
    value: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEMO_FAULT_INJECTION_ENABLED", "true")

    with pytest.raises(ValueError, match="must be a boolean"):
        orders_ingestion._resolve_demo_fail_after_commit(
            {"demo_fail_after_commit_once": value},
            1,
        )


def test_ingestion_task_has_bounded_retry_and_safe_default_probe() -> None:
    task = orders_ingestion.dag.task_dict["ingest_order_events"]

    assert task.retries == 1
    assert task.retry_delay.total_seconds() == 5
    assert orders_ingestion.dag.params["demo_fail_after_commit_once"] is False
