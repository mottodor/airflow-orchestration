"""Daily UTC order aggregation DAG with atomic PostgreSQL publication."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import os
from typing import Any, Mapping

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.types import DagRunType

ORDERS_DB_CONN_ID = "orders_db"
ORDERS_COMPUTE_POOL = "orders_compute"
EXECUTION_TIMEOUT = timedelta(seconds=10)
RETRY_DELAY = timedelta(seconds=5)

_PUBLISH_DAILY_AGGREGATE_SQL = """
SELECT
    records_read,
    order_count,
    sales_amount,
    published_at,
    source_max_updated_at
FROM orders_demo.publish_daily_order_aggregate(%s, %s)
"""


def _parse_affected_date(value: Any) -> date:
    if not isinstance(value, str):
        raise ValueError("affected_date must be a YYYY-MM-DD string")

    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("affected_date must be a valid YYYY-MM-DD date") from error

    if parsed.isoformat() != value:
        raise ValueError("affected_date must use the canonical YYYY-MM-DD format")

    return parsed


def _resolve_affected_date(context: Mapping[str, Any]) -> date:
    params = context.get("params")
    if isinstance(params, Mapping) and params.get("affected_date") is not None:
        return _parse_affected_date(params["affected_date"])

    dag_run = context.get("dag_run")
    if dag_run is not None and dag_run.run_type == DagRunType.MANUAL:
        raise ValueError("affected_date is required for a manual DAG run")

    try:
        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]
    except KeyError as error:
        raise ValueError(
            "affected_date is required when the DAG run has no data interval"
        ) from error

    if not isinstance(interval_start, datetime) or not isinstance(interval_end, datetime):
        raise ValueError("the Airflow data interval must contain timestamps")
    if interval_start.tzinfo is None or interval_end.tzinfo is None:
        raise ValueError("the Airflow data interval must be timezone-aware")

    utc_start = interval_start.astimezone(timezone.utc)
    utc_end = interval_end.astimezone(timezone.utc)
    expected_end = utc_start + timedelta(days=1)
    if utc_start.timetz() != time(0, tzinfo=timezone.utc) or utc_end != expected_end:
        raise ValueError("the Airflow data interval must be one complete UTC day")

    return utc_start.date()


def _resolve_demo_delay_seconds(context: Mapping[str, Any]) -> float:
    params = context.get("params")
    raw_value = params.get("demo_delay_seconds", 0) if isinstance(params, Mapping) else 0
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
        raise ValueError("demo_delay_seconds must be a number")

    delay_seconds = float(raw_value)
    if not 0 <= delay_seconds <= 60:
        raise ValueError("demo_delay_seconds must be between 0 and 60")

    fault_injection_enabled = os.getenv(
        "DEMO_FAULT_INJECTION_ENABLED", "false"
    ).lower() in {"1", "true", "yes"}
    if delay_seconds > 0 and not fault_injection_enabled:
        raise ValueError(
            "demo_delay_seconds requires DEMO_FAULT_INJECTION_ENABLED=true"
        )

    return delay_seconds


def _utc_isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("database timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _publish_daily_aggregate(
    affected_date: date,
    *,
    demo_delay_seconds: float = 0,
    hook: PostgresHook | None = None,
) -> dict[str, Any]:
    connection = (hook or PostgresHook(postgres_conn_id=ORDERS_DB_CONN_ID)).get_conn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                _PUBLISH_DAILY_AGGREGATE_SQL,
                (affected_date, demo_delay_seconds),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("daily aggregate publication returned no result")

        records_read, order_count, sales_amount, published_at, source_max_updated_at = row
        if not isinstance(sales_amount, Decimal):
            sales_amount = Decimal(str(sales_amount))

        result = {
            "affected_date": affected_date.isoformat(),
            "records_read": int(records_read),
            "order_count": int(order_count),
            "sales_amount": format(sales_amount, ".2f"),
            "published_at": _utc_isoformat(published_at),
            "source_max_updated_at": _utc_isoformat(source_max_updated_at),
        }
        connection.commit()
        return result
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


with DAG(
    dag_id="orders_daily_aggregate",
    description="Atomically publish the aggregate for one UTC calendar date.",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2025, 3, 1, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=4,
    params={
        "affected_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description=(
                "Optional target UTC date for a manual or delegated run. "
                "Scheduled and backfill runs use their data interval by default."
            ),
        ),
        "demo_delay_seconds": Param(
            0,
            type="number",
            minimum=0,
            maximum=60,
            description=(
                "Local fault/concurrency probe delay before publication. "
                "Rejected unless DEMO_FAULT_INJECTION_ENABLED=true."
            ),
        ),
    },
    tags=["orders", "daily-aggregate"],
) as dag:

    @task(
        pool=ORDERS_COMPUTE_POOL,
        pool_slots=1,
        execution_timeout=EXECUTION_TIMEOUT,
        retries=1,
        retry_delay=RETRY_DELAY,
    )
    def calculate_daily_aggregate() -> dict[str, Any]:
        context = get_current_context()
        affected_date = _resolve_affected_date(context)
        demo_delay_seconds = _resolve_demo_delay_seconds(context)
        return _publish_daily_aggregate(
            affected_date,
            demo_delay_seconds=demo_delay_seconds,
        )

    calculate_daily_aggregate()
