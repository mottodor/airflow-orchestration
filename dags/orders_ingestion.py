"""Manual ingestion DAG for append-only order events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, DAG, Param, get_current_context, task

ORDERS_DB_CONN_ID = "orders_db"
RAW_ORDER_EVENTS_ASSET = Asset(
    "postgres://postgres/orders_demo/orders_demo/raw_order_events"
)

_INSERT_ORDER_EVENT_SQL = """
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES (%s, %s, %s, %s, %s)
"""


@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    order_ts: datetime
    amount: Decimal
    updated_at: datetime
    ingested_at: datetime


@dataclass(frozen=True)
class IngestionChange:
    change_id: str
    reason: str
    affected_date: date
    events: tuple[OrderEvent, ...]


def _required_nonblank_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-blank string")
    return value


def _utc_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO 8601 UTC timestamp")

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO 8601 UTC timestamp") from error

    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must include the UTC offset")

    return parsed.astimezone(timezone.utc)


def _nonnegative_amount(value: Any) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("amount must be a finite decimal number") from error

    if not amount.is_finite():
        raise ValueError("amount must be a finite decimal number")
    if amount < 0:
        raise ValueError("amount must be nonnegative")
    if amount != amount.quantize(Decimal("0.01")):
        raise ValueError("amount must have at most two decimal places")

    return amount


def _normalize_change(params: Mapping[str, Any]) -> IngestionChange:
    change_id = _required_nonblank_string(params.get("change_id"), "change_id")
    reason = _required_nonblank_string(params.get("reason"), "reason")

    raw_events = params.get("events")
    if not isinstance(raw_events, Sequence) or isinstance(raw_events, (str, bytes)):
        raise ValueError("events must be a non-empty array")
    if not raw_events:
        raise ValueError("events must be a non-empty array")

    events: list[OrderEvent] = []
    affected_dates: set[date] = set()
    for index, raw_event in enumerate(raw_events):
        if not isinstance(raw_event, Mapping):
            raise ValueError(f"events[{index}] must be an object")

        order_ts = _utc_timestamp(raw_event.get("order_ts"), f"events[{index}].order_ts")
        event = OrderEvent(
            order_id=_required_nonblank_string(
                raw_event.get("order_id"), f"events[{index}].order_id"
            ),
            order_ts=order_ts,
            amount=_nonnegative_amount(raw_event.get("amount")),
            updated_at=_utc_timestamp(
                raw_event.get("updated_at"), f"events[{index}].updated_at"
            ),
            ingested_at=_utc_timestamp(
                raw_event.get("ingested_at"), f"events[{index}].ingested_at"
            ),
        )
        events.append(event)
        affected_dates.add(order_ts.date())

    if len(affected_dates) != 1:
        raise ValueError("one ingestion change must affect exactly one UTC date")

    return IngestionChange(
        change_id=change_id,
        reason=reason,
        affected_date=affected_dates.pop(),
        events=tuple(events),
    )


def _insert_events(change: IngestionChange, hook: PostgresHook) -> None:
    connection = hook.get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.executemany(
                _INSERT_ORDER_EVENT_SQL,
                [
                    (
                        event.order_id,
                        event.order_ts,
                        event.amount,
                        event.updated_at,
                        event.ingested_at,
                    )
                    for event in change.events
                ],
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _ingest_change(
    params: Mapping[str, Any],
    outlet_events: Any,
    *,
    hook: PostgresHook | None = None,
) -> dict[str, Any]:
    change = _normalize_change(params)
    outlet_event = outlet_events[RAW_ORDER_EVENTS_ASSET]
    _insert_events(change, hook or PostgresHook(postgres_conn_id=ORDERS_DB_CONN_ID))

    event_extra = {
        "affected_date": change.affected_date.isoformat(),
        "reason": change.reason,
        "change_id": change.change_id,
    }
    outlet_event.extra = event_extra

    return {**event_extra, "inserted_event_count": len(change.events)}


with DAG(
    dag_id="orders_ingestion",
    description="Append one UTC date of order changes and emit its Asset event.",
    schedule=None,
    catchup=False,
    params={
        "change_id": Param(
            type="string",
            minLength=1,
            description="Identifier of this source change.",
        ),
        "reason": Param(
            type="string",
            minLength=1,
            description="Why this order change was ingested.",
        ),
        "events": Param(
            type="array",
            minItems=1,
            description="Append-only order versions for exactly one UTC date.",
            items={
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "order_id",
                    "order_ts",
                    "amount",
                    "updated_at",
                    "ingested_at",
                ],
                "properties": {
                    "order_id": {"type": "string", "minLength": 1},
                    "order_ts": {"type": "string", "format": "date-time"},
                    "amount": {"type": "number", "minimum": 0},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "ingested_at": {"type": "string", "format": "date-time"},
                },
            },
        ),
    },
    tags=["orders", "ingestion"],
) as dag:

    @task(outlets=[RAW_ORDER_EVENTS_ASSET])
    def ingest_order_events(*, outlet_events: Any = None) -> dict[str, Any]:
        if outlet_events is None:
            raise RuntimeError("Airflow did not provide the outlet_events context")
        return _ingest_change(get_current_context()["params"], outlet_events)

    ingest_order_events()
