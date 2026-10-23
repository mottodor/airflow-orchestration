from __future__ import annotations

from datetime import date, timedelta
import importlib.util
import os
from pathlib import Path
import sys
from typing import Any
import uuid

import psycopg2
import pytest


DAG_PATH = Path(__file__).parents[2] / "dags" / "orders_ingestion.py"
SPEC = importlib.util.spec_from_file_location("orders_ingestion_integration", DAG_PATH)
assert SPEC is not None and SPEC.loader is not None
orders_ingestion = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = orders_ingestion
SPEC.loader.exec_module(orders_ingestion)


class DsnHook:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def get_conn(self) -> Any:
        return psycopg2.connect(self.dsn)


class FakeOutletEvent:
    def __init__(self) -> None:
        self.extra: dict[str, str] | None = None


class FakeOutletEvents:
    def __init__(self) -> None:
        self.event = FakeOutletEvent()

    def __getitem__(self, asset: Any) -> FakeOutletEvent:
        assert asset is orders_ingestion.RAW_ORDER_EVENTS_ASSET
        return self.event


def _unused_date(dsn: str) -> date:
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT candidate::date
                FROM generate_series(
                    DATE '8000-01-01',
                    DATE '8999-12-31',
                    INTERVAL '1 day'
                ) AS candidate
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM orders_demo.raw_order_events AS raw
                    WHERE raw.order_ts >= candidate::date::timestamp AT TIME ZONE 'UTC'
                      AND raw.order_ts <
                          (candidate::date + 1)::timestamp AT TIME ZONE 'UTC'
                )
                ORDER BY candidate
                LIMIT 1
                """
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    assert row is not None
    return row[0]


def _storage_state(dsn: str, change_id: str, order_id: str) -> tuple[Any, ...]:
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    count(*) FILTER (WHERE change.change_id IS NOT NULL),
                    max(change.reason),
                    max(change.affected_date),
                    max(change.payload_sha256),
                    max(change.event_count),
                    (
                        SELECT count(*)
                        FROM orders_demo.raw_order_events AS raw
                        WHERE raw.order_id = %s
                    )
                FROM orders_demo.ingestion_changes AS change
                WHERE change.change_id = %s
                """,
                (order_id, change_id),
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    assert row is not None
    return row


def test_post_commit_replay_is_idempotent_and_conflicting_reuse_is_rejected() -> None:
    dsn = os.getenv("ORDERS_TEST_DSN")
    if not dsn:
        pytest.fail("ORDERS_TEST_DSN is required")

    target_date = _unused_date(dsn)
    token = uuid.uuid4().hex
    change_id = f"r01-integration-{token}"
    order_id = f"r01-integration-order-{token}"
    order_ts = f"{target_date.isoformat()}T12:00:00Z"
    updated_at = f"{(target_date + timedelta(days=1)).isoformat()}T09:00:00Z"
    params = {
        "change_id": change_id,
        "reason": "post_commit_retry_probe",
        "events": [
            {
                "order_id": order_id,
                "order_ts": order_ts,
                "amount": -0.0,
                "updated_at": updated_at,
                "ingested_at": updated_at,
            }
        ],
    }
    hook = DsnHook(dsn)
    failed_outlet_events = FakeOutletEvents()

    with pytest.raises(RuntimeError, match="after ingestion commit"):
        orders_ingestion._ingest_change(
            params,
            failed_outlet_events,
            hook=hook,
            fail_after_commit=True,
        )

    assert failed_outlet_events.event.extra is None
    committed_state = _storage_state(dsn, change_id, order_id)
    assert committed_state[0] == 1
    assert committed_state[1] == "post_commit_retry_probe"
    assert committed_state[2] == target_date
    assert isinstance(committed_state[3], str) and len(committed_state[3]) == 64
    assert committed_state[4] == 1
    assert committed_state[5] == 1

    retry_params = {
        **params,
        "events": [{**params["events"][0], "amount": 0.0}],
    }
    retry_outlet_events = FakeOutletEvents()
    result = orders_ingestion._ingest_change(
        retry_params,
        retry_outlet_events,
        hook=hook,
    )

    assert result == {
        "affected_date": target_date.isoformat(),
        "reason": "post_commit_retry_probe",
        "change_id": change_id,
        "inserted_event_count": 0,
        "change_replayed": True,
    }
    assert retry_outlet_events.event.extra == {
        "affected_date": target_date.isoformat(),
        "reason": "post_commit_retry_probe",
        "change_id": change_id,
    }
    assert _storage_state(dsn, change_id, order_id) == committed_state

    conflicting_params = {
        **retry_params,
        "events": [{**retry_params["events"][0], "amount": 43.00}],
    }
    with pytest.raises(ValueError, match="already exists with different content"):
        orders_ingestion._ingest_change(
            conflicting_params,
            FakeOutletEvents(),
            hook=hook,
        )

    assert _storage_state(dsn, change_id, order_id) == committed_state
