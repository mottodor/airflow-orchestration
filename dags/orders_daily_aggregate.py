"""Daily UTC order aggregation DAG with atomic PostgreSQL publication."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import json
import logging
import os
from time import monotonic
from typing import Any, Callable, Mapping

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.types import DagRunType

ORDERS_DB_CONN_ID = "orders_db"
ORDERS_COMPUTE_POOL = "orders_compute"
EXECUTION_TIMEOUT = timedelta(seconds=10)
RETRY_DELAY = timedelta(seconds=5)
ADVISORY_LOCK_NAMESPACE = 20250301

LOGGER = logging.getLogger(__name__)

_TRY_DATE_LOCK_SQL = "SELECT pg_try_advisory_xact_lock(%s, %s)"
_WAIT_FOR_DATE_LOCK_SQL = "SELECT pg_advisory_xact_lock(%s, %s)"
_SELECT_OLD_AGGREGATE_SQL = """
SELECT
    order_count,
    sales_amount,
    published_at,
    source_max_updated_at
FROM orders_demo.daily_order_aggregates
WHERE aggregate_date = %s
"""
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

    utc_start = datetime.fromtimestamp(interval_start.timestamp(), tz=timezone.utc)
    utc_end = datetime.fromtimestamp(interval_end.timestamp(), tz=timezone.utc)
    expected_end = datetime.combine(
        utc_start.date() + timedelta(days=1), time.min, tzinfo=timezone.utc
    )
    if utc_start.time() != time.min or utc_end != expected_end:
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


def _serialize_aggregate(
    row: tuple[Any, ...] | None,
) -> dict[str, Any] | None:
    if row is None:
        return None

    order_count, sales_amount, published_at, source_max_updated_at = row
    if not isinstance(sales_amount, Decimal):
        sales_amount = Decimal(str(sales_amount))
    return {
        "order_count": int(order_count),
        "sales_amount": format(sales_amount, ".2f"),
        "published_at": _utc_isoformat(published_at),
        "source_max_updated_at": _utc_isoformat(source_max_updated_at),
    }


def _date_lock_key(affected_date: date) -> int:
    return (affected_date - date.min).days + 1


def _acquire_date_lock(
    cursor: Any,
    affected_date: date,
    progress: dict[str, Any],
) -> tuple[bool, float]:
    lock_parameters = (ADVISORY_LOCK_NAMESPACE, _date_lock_key(affected_date))
    cursor.execute(_TRY_DATE_LOCK_SQL, lock_parameters)
    lock_result = cursor.fetchone()
    if lock_result is None or not isinstance(lock_result[0], bool):
        raise RuntimeError("date lock acquisition returned no boolean result")
    if lock_result[0]:
        progress.update({"lock_waited": False, "lock_wait_ms": 0.0})
        return False, 0.0

    progress.update({"lock_waited": True, "lock_wait_ms": 0.0})
    wait_started = monotonic()
    try:
        cursor.execute(_WAIT_FOR_DATE_LOCK_SQL, lock_parameters)
        cursor.fetchone()
    finally:
        progress["lock_wait_ms"] = round(
            max(0.0, monotonic() - wait_started) * 1000,
            3,
        )
    return True, progress["lock_wait_ms"]


def _publish_daily_aggregate(
    affected_date: date,
    *,
    demo_delay_seconds: float = 0,
    hook: PostgresHook | None = None,
    progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attempt_progress = progress if progress is not None else {}
    connection = (hook or PostgresHook(postgres_conn_id=ORDERS_DB_CONN_ID)).get_conn()
    try:
        with connection.cursor() as cursor:
            lock_waited, lock_wait_ms = _acquire_date_lock(
                cursor,
                affected_date,
                attempt_progress,
            )

            cursor.execute(_SELECT_OLD_AGGREGATE_SQL, (affected_date,))
            old_aggregate = _serialize_aggregate(cursor.fetchone())
            attempt_progress["old_aggregate"] = old_aggregate

            cursor.execute(
                _PUBLISH_DAILY_AGGREGATE_SQL,
                (affected_date, demo_delay_seconds),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("daily aggregate publication returned no result")

        records_read, order_count, sales_amount, published_at, source_max_updated_at = row
        new_aggregate = _serialize_aggregate(
            (order_count, sales_amount, published_at, source_max_updated_at)
        )
        connection.commit()
        return {
            "records_read": int(records_read),
            "unique_orders": int(order_count),
            "old_aggregate": old_aggregate,
            "new_aggregate": new_aggregate,
            "publication_state": "published",
            "lock_waited": lock_waited,
            "lock_wait_ms": lock_wait_ms,
        }
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _attempt_number(context: Mapping[str, Any]) -> int:
    task_instance = context.get("ti") or context.get("task_instance")
    try_number = getattr(task_instance, "try_number", 1)
    if (
        not isinstance(try_number, int)
        or isinstance(try_number, bool)
        or try_number < 1
    ):
        raise ValueError("Airflow task try_number must be a positive integer")
    return try_number


def _resolve_initial_trigger_reason(context: Mapping[str, Any]) -> str:
    params = context.get("params")
    if isinstance(params, Mapping):
        configured_reason = params.get("reason")
        if configured_reason in {"schedule", "backfill", "late_data"}:
            return configured_reason

    dag_run = context.get("dag_run")
    run_type = getattr(dag_run, "run_type", None)
    if run_type in {DagRunType.BACKFILL_JOB, DagRunType.MANUAL}:
        return "backfill"
    if run_type == DagRunType.ASSET_TRIGGERED:
        return "late_data"
    return "schedule"


def _resolve_trigger_reason(initial_trigger_reason: str, try_number: int) -> str:
    return "retry" if try_number > 1 else initial_trigger_reason


def _new_summary(
    *,
    affected_date: date | None,
    trigger_reason: str,
    initial_trigger_reason: str,
    try_number: int,
) -> dict[str, Any]:
    return {
        "affected_date": affected_date.isoformat() if affected_date is not None else None,
        "trigger_reason": trigger_reason,
        "initial_trigger_reason": initial_trigger_reason,
        "records_read": None,
        "unique_orders": None,
        "old_aggregate": None,
        "new_aggregate": None,
        "publication_state": "not_started",
        "lock_waited": None,
        "lock_wait_ms": None,
        "try_number": try_number,
        "error": None,
    }


def _error_reason(error: BaseException) -> str:
    message = str(error).strip()
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


def _log_summary(summary: Mapping[str, Any]) -> None:
    LOGGER.info(json.dumps(summary, sort_keys=True, separators=(",", ":")))


def _run_daily_aggregate(
    context: Mapping[str, Any],
    *,
    hook: PostgresHook | None = None,
    summary_logger: Callable[[Mapping[str, Any]], None] = _log_summary,
) -> dict[str, Any]:
    try_number = _attempt_number(context)
    initial_trigger_reason = _resolve_initial_trigger_reason(context)
    summary = _new_summary(
        affected_date=None,
        trigger_reason=_resolve_trigger_reason(initial_trigger_reason, try_number),
        initial_trigger_reason=initial_trigger_reason,
        try_number=try_number,
    )
    progress: dict[str, Any] = {}

    try:
        affected_date = _resolve_affected_date(context)
        summary["affected_date"] = affected_date.isoformat()
        demo_delay_seconds = _resolve_demo_delay_seconds(context)
        summary.update(
            _publish_daily_aggregate(
                affected_date,
                demo_delay_seconds=demo_delay_seconds,
                hook=hook,
                progress=progress,
            )
        )
    except BaseException as error:
        summary.update(progress)
        summary["publication_state"] = (
            "old_result_preserved"
            if summary["old_aggregate"] is not None
            else "not_started"
        )
        summary["error"] = _error_reason(error)
        summary_logger(summary)
        try:
            setattr(error, "_orders_summary_logged", True)
        except (AttributeError, TypeError):
            pass
        raise

    summary_logger(summary)
    return summary


def _failure_summary_callback(context: Mapping[str, Any]) -> None:
    error = context.get("exception")
    if isinstance(error, BaseException) and getattr(
        error, "_orders_summary_logged", False
    ):
        return

    try_number = _attempt_number(context)
    initial_trigger_reason = _resolve_initial_trigger_reason(context)
    try:
        affected_date = _resolve_affected_date(context)
    except (KeyError, TypeError, ValueError):
        affected_date = None
    summary = _new_summary(
        affected_date=affected_date,
        trigger_reason=_resolve_trigger_reason(initial_trigger_reason, try_number),
        initial_trigger_reason=initial_trigger_reason,
        try_number=try_number,
    )
    summary["error"] = (
        _error_reason(error)
        if isinstance(error, BaseException)
        else "Unknown task failure"
    )
    _log_summary(summary)


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
        on_retry_callback=_failure_summary_callback,
        on_failure_callback=_failure_summary_callback,
    )
    def calculate_daily_aggregate() -> dict[str, Any]:
        return _run_daily_aggregate(get_current_context())

    calculate_daily_aggregate()
