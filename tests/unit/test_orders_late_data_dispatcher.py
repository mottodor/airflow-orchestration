from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import importlib.util
from pathlib import Path
import sys
from typing import Any

import pytest
from airflow.sdk import Asset


DAG_PATH = Path(__file__).parents[2] / "dags" / "orders_late_data_dispatcher.py"
SPEC = importlib.util.spec_from_file_location("orders_late_data_dispatcher", DAG_PATH)
assert SPEC is not None and SPEC.loader is not None
orders_late_data_dispatcher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = orders_late_data_dispatcher
SPEC.loader.exec_module(orders_late_data_dispatcher)


@dataclass(frozen=True)
class FakeAssetEvent:
    extra: Any


class FakeTriggeringAssetEvents:
    def __init__(self, *entries: tuple[Asset, list[FakeAssetEvent]]) -> None:
        self.entries = entries

    def __bool__(self) -> bool:
        return bool(self.entries)

    def items(self) -> tuple[tuple[Asset, list[FakeAssetEvent]], ...]:
        return self.entries


def _event(affected_date: str, reason: str, change_id: str) -> FakeAssetEvent:
    return FakeAssetEvent(
        extra={
            "affected_date": affected_date,
            "reason": reason,
            "change_id": change_id,
        }
    )


def test_duplicate_dates_create_one_deterministic_target_request() -> None:
    events = FakeTriggeringAssetEvents(
        (
            orders_late_data_dispatcher.RAW_ORDER_EVENTS_ASSET,
            [
                _event("2025-03-02", "late_version", "change-d"),
                _event("2025-03-01", "late_order", "change-c"),
                _event("2025-03-01", "source_replay", "change-c-replay"),
            ],
        )
    )

    requests = orders_late_data_dispatcher._build_target_run_requests(
        events,
        "asset_triggered__2025-03-03T10:00:00+00:00",
    )

    assert requests == [
        {
            "trigger_run_id": "late_data__2025-03-01__f29083eec85f3373",
            "conf": {
                "affected_date": "2025-03-01",
                "reason": "late_data",
                "source_change_ids": ["change-c", "change-c-replay"],
                "source_reasons": ["late_order", "source_replay"],
            },
        },
        {
            "trigger_run_id": "late_data__2025-03-02__f29083eec85f3373",
            "conf": {
                "affected_date": "2025-03-02",
                "reason": "late_data",
                "source_change_ids": ["change-d"],
                "source_reasons": ["late_version"],
            },
        },
    ]


def test_target_run_id_is_stable_for_a_retry_but_changes_for_a_new_dispatcher_run() -> None:
    affected_date = date(2025, 3, 1)

    first = orders_late_data_dispatcher._target_run_id(
        affected_date,
        "asset_triggered__first",
    )

    assert first == orders_late_data_dispatcher._target_run_id(
        affected_date,
        "asset_triggered__first",
    )
    assert first != orders_late_data_dispatcher._target_run_id(
        affected_date,
        "asset_triggered__second",
    )


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (None, "extra must be an object"),
        (
            {"affected_date": "2025-03-01", "reason": "late_order"},
            "change_id must be a non-blank string",
        ),
        (
            {
                "affected_date": "2025-03-01T00:00:00Z",
                "reason": "late_order",
                "change_id": "change-c",
            },
            "valid YYYY-MM-DD date",
        ),
    ],
)
def test_invalid_event_metadata_fails_before_any_target_request(
    extra: Any,
    message: str,
) -> None:
    events = FakeTriggeringAssetEvents(
        (
            orders_late_data_dispatcher.RAW_ORDER_EVENTS_ASSET,
            [
                _event("2025-03-01", "late_order", "valid-change"),
                FakeAssetEvent(extra=extra),
            ],
        )
    )

    with pytest.raises(ValueError, match=message):
        orders_late_data_dispatcher._build_target_run_requests(
            events,
            "asset_triggered__test",
        )


def test_unexpected_asset_is_rejected() -> None:
    events = FakeTriggeringAssetEvents(
        (
            Asset("postgres://postgres/orders_demo/orders_demo/another_table"),
            [_event("2025-03-01", "late_order", "change-c")],
        )
    )

    with pytest.raises(ValueError, match="Unexpected triggering Asset"):
        orders_late_data_dispatcher._build_target_run_requests(
            events,
            "asset_triggered__test",
        )


def test_dispatcher_dag_uses_asset_schedule_and_idempotent_trigger_operator() -> None:
    dag = orders_late_data_dispatcher.dag

    assert dag.dag_id == "orders_late_data_dispatcher"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert [asset.uri for asset in dag.schedule] == [
        orders_late_data_dispatcher.RAW_ORDER_EVENTS_ASSET.uri
    ]

    trigger_task = dag.task_dict["trigger_daily_aggregate"]
    assert trigger_task.operator_class is orders_late_data_dispatcher.TriggerDagRunOperator
    assert trigger_task.partial_kwargs["trigger_dag_id"] == "orders_daily_aggregate"
    assert trigger_task.partial_kwargs["reset_dag_run"] is False
    assert trigger_task.partial_kwargs["skip_when_already_exists"] is True
    assert trigger_task.partial_kwargs["wait_for_completion"] is False
