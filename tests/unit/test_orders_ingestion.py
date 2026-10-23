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
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.executions: list[tuple[str, list[tuple[Any, ...]]]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: Any,
    ) -> None:
        return None

    def executemany(self, sql: str, values: list[tuple[Any, ...]]) -> None:
        self.executions.append((sql, values))
        if self.error is not None:
            raise self.error


class FakeConnection:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.cursor_instance = FakeCursor(error=error)
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
    assert len(connection.cursor_instance.executions) == 1

    sql, values = connection.cursor_instance.executions[0]
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
    }


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
