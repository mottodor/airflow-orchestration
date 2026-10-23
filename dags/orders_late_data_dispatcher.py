"""Asset-triggered dispatcher for idempotent late-data recalculations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
import hashlib
from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, DAG, get_current_context, task

RAW_ORDER_EVENTS_ASSET = Asset(
    "postgres://postgres/orders_demo/orders_demo/raw_order_events"
)
TARGET_DAG_ID = "orders_daily_aggregate"
TARGET_RUN_ID_PREFIX = "late_data"


def _required_event_string(extra: Mapping[str, Any], field_name: str) -> str:
    value = extra.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Asset event {field_name} must be a non-blank string")
    return value


def _event_affected_date(extra: Mapping[str, Any]) -> date:
    raw_value = _required_event_string(extra, "affected_date")
    try:
        affected_date = date.fromisoformat(raw_value)
    except ValueError as error:
        raise ValueError(
            "Asset event affected_date must be a valid YYYY-MM-DD date"
        ) from error

    if affected_date.isoformat() != raw_value:
        raise ValueError(
            "Asset event affected_date must use the canonical YYYY-MM-DD format"
        )
    return affected_date


def _target_run_id(affected_date: date, dispatcher_run_id: str) -> str:
    source_token = hashlib.sha256(dispatcher_run_id.encode("utf-8")).hexdigest()[:16]
    return f"{TARGET_RUN_ID_PREFIX}__{affected_date.isoformat()}__{source_token}"


def _build_target_run_requests(
    triggering_asset_events: Mapping[Any, Sequence[Any]],
    dispatcher_run_id: str,
) -> list[dict[str, Any]]:
    if not isinstance(dispatcher_run_id, str) or not dispatcher_run_id:
        raise ValueError("dispatcher_run_id must be a non-empty string")
    if not triggering_asset_events:
        raise ValueError("The dispatcher run has no triggering Asset events")

    events_by_date: dict[date, dict[str, set[str]]] = {}
    event_count = 0
    for asset, asset_events in triggering_asset_events.items():
        if getattr(asset, "uri", None) != RAW_ORDER_EVENTS_ASSET.uri:
            raise ValueError(f"Unexpected triggering Asset: {asset!r}")

        for event in asset_events:
            event_count += 1
            extra = getattr(event, "extra", None)
            if not isinstance(extra, Mapping):
                raise ValueError("Triggering Asset event extra must be an object")

            affected_date = _event_affected_date(extra)
            reason = _required_event_string(extra, "reason")
            change_id = _required_event_string(extra, "change_id")
            metadata = events_by_date.setdefault(
                affected_date,
                {"change_ids": set(), "source_reasons": set()},
            )
            metadata["change_ids"].add(change_id)
            metadata["source_reasons"].add(reason)

    if event_count == 0:
        raise ValueError("The dispatcher run has no triggering Asset events")

    return [
        {
            "trigger_run_id": _target_run_id(affected_date, dispatcher_run_id),
            "conf": {
                "affected_date": affected_date.isoformat(),
                "reason": "late_data",
                "source_change_ids": sorted(metadata["change_ids"]),
                "source_reasons": sorted(metadata["source_reasons"]),
            },
        }
        for affected_date, metadata in sorted(events_by_date.items())
    ]


with DAG(
    dag_id="orders_late_data_dispatcher",
    description="Dispatch one idempotent daily recalculation per affected UTC date.",
    schedule=[RAW_ORDER_EVENTS_ASSET],
    catchup=False,
    max_active_runs=1,
    tags=["orders", "assets", "late-data"],
) as dag:

    @task
    def build_target_run_requests() -> list[dict[str, Any]]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        if dag_run is None:
            raise RuntimeError("Airflow did not provide the dispatcher DAG run")

        return _build_target_run_requests(
            context.get("triggering_asset_events", {}),
            dag_run.run_id,
        )

    TriggerDagRunOperator.partial(
        task_id="trigger_daily_aggregate",
        trigger_dag_id=TARGET_DAG_ID,
        reset_dag_run=False,
        skip_when_already_exists=True,
        wait_for_completion=False,
    ).expand_kwargs(build_target_run_requests())
