from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import uuid

import psycopg2
import pytest


DAILY_DAG_ID = "orders_daily_aggregate"
INGESTION_DAG_ID = "orders_ingestion"
DISPATCHER_DAG_ID = "orders_late_data_dispatcher"
DAILY_TASK_ID = "calculate_daily_aggregate"
INGESTION_TASK_ID = "ingest_order_events"
POOL_NAME = "orders_compute"
LOCK_NAMESPACE = 20250301
TERMINAL_RUN_STATES = {"success", "failed"}
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


def _utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


def _utc_isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _day_timestamp(target_date: date, hour: int) -> datetime:
    return datetime.combine(
        target_date,
        datetime_time(hour=hour),
        tzinfo=timezone.utc,
    )


class AirflowApiError(AssertionError):
    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        super().__init__(f"Airflow API {method} {path} returned {status}: {body}")
        self.status = status


class AirflowApi:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        token_response = self._raw_request(
            "POST",
            "/auth/token",
            body={"username": username, "password": password},
            authenticated=False,
        )
        self.token = token_response["access_token"]

    def _raw_request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        authenticated: bool = True,
    ) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if authenticated:
            headers["Authorization"] = f"Bearer {self.token}"

        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                response_body = response.read()
        except HTTPError as error:
            error_body = error.read().decode(errors="replace")
            raise AirflowApiError(method, path, error.code, error_body) from error

        return json.loads(response_body) if response_body else None

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        return self._raw_request(method, path, body=body, params=params)

    def dag(self, dag_id: str) -> dict[str, Any]:
        return self.request("GET", f"/api/v2/dags/{quote(dag_id, safe='')}")

    def set_paused(self, dag_id: str, is_paused: bool) -> None:
        self.request(
            "PATCH",
            f"/api/v2/dags/{quote(dag_id, safe='')}",
            params={"update_mask": "is_paused"},
            body={"is_paused": is_paused},
        )

    def trigger(
        self,
        dag_id: str,
        run_id: str,
        conf: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns",
            body={
                "dag_run_id": run_id,
                "logical_date": None,
                "conf": dict(conf),
            },
        )

    def dag_run(
        self,
        dag_id: str,
        run_id: str,
        *,
        allow_missing: bool = False,
    ) -> dict[str, Any] | None:
        path = (
            f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/"
            f"{quote(run_id, safe='')}"
        )
        try:
            return self.request("GET", path)
        except AirflowApiError as error:
            if allow_missing and error.status == 404:
                return None
            raise

    def task_instance(self, dag_id: str, run_id: str) -> dict[str, Any]:
        response = self.request(
            "GET",
            (
                f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/"
                f"{quote(run_id, safe='')}/taskInstances"
            ),
        )
        task_instances = response["task_instances"]
        assert len(task_instances) == 1, task_instances
        return task_instances[0]

    def task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
    ) -> dict[str, Any]:
        return self.request(
            "GET",
            (
                f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/"
                f"{quote(run_id, safe='')}/taskInstances/"
                f"{quote(task_id, safe='')}/logs/{try_number}"
            ),
            params={"full_content": "true"},
        )

    def xcom(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str = "return_value",
    ) -> Any:
        response = self.request(
            "GET",
            (
                f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/"
                f"{quote(run_id, safe='')}/taskInstances/"
                f"{quote(task_id, safe='')}/xcomEntries/{quote(key, safe='')}"
            ),
            params={"deserialize": "true"},
        )
        return response["value"]

    def asset_events(self, source_run_id: str) -> list[dict[str, Any]]:
        response = self.request(
            "GET",
            "/api/v2/assets/events",
            params={
                "source_run_id": source_run_id,
                "limit": 50,
                "order_by": "-timestamp",
            },
        )
        return response["asset_events"]

    def upstream_asset_events(
        self,
        dag_id: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        response = self.request(
            "GET",
            (
                f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/"
                f"{quote(run_id, safe='')}/upstreamAssetEvents"
            ),
        )
        return response["asset_events"]

    def pool(self, pool_name: str) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/api/v2/pools/{quote(pool_name, safe='')}",
        )


def _wait_until(
    description: str,
    predicate: Callable[[], Any],
    *,
    timeout: float = 90,
    interval: float = 0.25,
) -> Any:
    deadline = time.monotonic() + timeout
    last_value: Any = None
    while time.monotonic() < deadline:
        last_value = predicate()
        if last_value:
            return last_value
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {description}; last value: {last_value!r}")


def _wait_for_run(
    api: AirflowApi,
    dag_id: str,
    run_id: str,
    expected_state: str,
    *,
    timeout: float = 120,
) -> dict[str, Any]:
    def completed_run() -> dict[str, Any] | None:
        run = api.dag_run(dag_id, run_id, allow_missing=True)
        if run is None or run["state"] not in TERMINAL_RUN_STATES:
            return None
        return run

    run = _wait_until(
        f"{dag_id} run {run_id} to finish",
        completed_run,
        timeout=timeout,
    )
    assert run["state"] == expected_state, run
    assert run["start_date"] is not None
    assert run["end_date"] is not None
    return run


def _wait_for_task_state(
    api: AirflowApi,
    dag_id: str,
    run_id: str,
    expected_states: set[str],
    *,
    timeout: float = 60,
) -> dict[str, Any]:
    def matching_task() -> dict[str, Any] | None:
        run = api.dag_run(dag_id, run_id, allow_missing=True)
        if run is None:
            return None
        try:
            task_instance = api.task_instance(dag_id, run_id)
        except AirflowApiError as error:
            if error.status == 404:
                return None
            raise
        return task_instance if task_instance["state"] in expected_states else None

    return _wait_until(
        f"{dag_id} run {run_id} task state in {sorted(expected_states)}",
        matching_task,
        timeout=timeout,
    )


def _summary_from_log(log_response: Mapping[str, Any]) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    for item in log_response["content"]:
        event = item if isinstance(item, str) else item.get("event")
        if not isinstance(event, str) or not event.startswith("{"):
            continue
        try:
            candidate = json.loads(event)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and set(candidate) == SUMMARY_KEYS:
            summaries.append(candidate)
    assert len(summaries) == 1, summaries
    return summaries[0]


@dataclass(frozen=True)
class Aggregate:
    row_count: int
    order_count: int
    sales_amount: Decimal


@dataclass(frozen=True)
class AggregateSnapshot:
    order_count: int
    sales_amount: Decimal
    published_at: datetime
    source_max_updated_at: datetime | None


@dataclass(frozen=True)
class IngestionStorageState:
    receipt_count: int
    reason: str
    affected_date: date
    payload_sha256: str
    event_count: int
    raw_event_count: int


def _aggregate_snapshot(
    connect: Callable[[], Any],
    target_date: date,
) -> AggregateSnapshot:
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    order_count,
                    sales_amount,
                    published_at,
                    source_max_updated_at
                FROM orders_demo.daily_order_aggregates
                WHERE aggregate_date = %s
                """,
                (target_date,),
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    assert row is not None
    return AggregateSnapshot(
        order_count=int(row[0]),
        sales_amount=Decimal(row[1]),
        published_at=row[2],
        source_max_updated_at=row[3],
    )


def _aggregate(connect: Callable[[], Any], target_date: date) -> Aggregate:
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT count(*), max(order_count), max(sales_amount)
                FROM orders_demo.daily_order_aggregates
                WHERE aggregate_date = %s
                """,
                (target_date,),
            )
            row_count, order_count, sales_amount = cursor.fetchone()
    finally:
        connection.close()
    return Aggregate(
        row_count=int(row_count),
        order_count=int(order_count) if order_count is not None else 0,
        sales_amount=Decimal(sales_amount) if sales_amount is not None else Decimal("0"),
    )


def _ingestion_storage_state(
    connect: Callable[[], Any],
    change_id: str,
    order_id: str,
) -> IngestionStorageState:
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    count(*),
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
    assert row[1] is not None
    assert row[2] is not None
    assert row[3] is not None
    assert row[4] is not None
    return IngestionStorageState(
        receipt_count=int(row[0]),
        reason=str(row[1]),
        affected_date=row[2],
        payload_sha256=str(row[3]),
        event_count=int(row[4]),
        raw_event_count=int(row[5]),
    )


def _insert_order_versions(
    connect: Callable[[], Any],
    target_date: date,
    versions: Sequence[tuple[str, Decimal, int]],
) -> None:
    connection = connect()
    try:
        with connection.cursor() as cursor:
            for index, (order_id, amount, updated_hour) in enumerate(versions, start=1):
                order_ts = _day_timestamp(target_date, min(index, 23))
                updated_at = _day_timestamp(target_date, updated_hour)
                cursor.execute(
                    """
                    INSERT INTO orders_demo.raw_order_events (
                        order_id,
                        order_ts,
                        amount,
                        updated_at,
                        ingested_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_id, order_ts, amount, updated_at, updated_at),
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _insert_canonical_r1_r4(
    connect: Callable[[], Any],
    target_date: date,
    order_prefix: str,
) -> None:
    order_a = f"{order_prefix}-A"
    order_b = f"{order_prefix}-B"
    order_a_ts = _day_timestamp(target_date, 10)
    order_b_ts = _day_timestamp(target_date, 11)
    version_a_100 = _day_timestamp(target_date, 10)
    version_b_50 = _day_timestamp(target_date, 11)
    version_a_120 = _day_timestamp(target_date, 12)
    rows = [
        (order_a, order_a_ts, Decimal("100.00"), version_a_100, version_a_100),
        (order_b, order_b_ts, Decimal("50.00"), version_b_50, version_b_50),
        (order_a, order_a_ts, Decimal("120.00"), version_a_120, version_a_120),
        (
            order_a,
            order_a_ts,
            Decimal("120.00"),
            version_a_120,
            version_a_120 + timedelta(minutes=1),
        ),
    ]

    connection = connect()
    try:
        with connection.cursor() as cursor:
            for order_id, order_ts, amount, updated_at, ingested_at in rows:
                cursor.execute(
                    """
                    INSERT INTO orders_demo.raw_order_events (
                        order_id,
                        order_ts,
                        amount,
                        updated_at,
                        ingested_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_id, order_ts, amount, updated_at, ingested_at),
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _insert_boundary_fixture(
    connect: Callable[[], Any],
    first_date: date,
    order_prefix: str,
) -> None:
    second_date = first_date + timedelta(days=1)
    first_timestamp = datetime.combine(
        first_date,
        datetime_time(23, 59, 59),
        tzinfo=timezone.utc,
    )
    second_timestamp = datetime.combine(
        second_date,
        datetime_time.min,
        tzinfo=timezone.utc,
    )
    rows = [
        (
            f"{order_prefix}-before-midnight",
            first_timestamp,
            Decimal("11.00"),
            first_timestamp,
            first_timestamp,
        ),
        (
            f"{order_prefix}-at-midnight",
            second_timestamp,
            Decimal("22.00"),
            second_timestamp,
            second_timestamp,
        ),
    ]

    connection = connect()
    try:
        with connection.cursor() as cursor:
            for order_id, order_ts, amount, updated_at, ingested_at in rows:
                cursor.execute(
                    """
                    INSERT INTO orders_demo.raw_order_events (
                        order_id,
                        order_ts,
                        amount,
                        updated_at,
                        ingested_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (order_id, order_ts, amount, updated_at, ingested_at),
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _insert_new_version(
    connect: Callable[[], Any],
    target_date: date,
    order_id: str,
    amount: Decimal,
    updated_hour: int,
) -> None:
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_ts
                FROM orders_demo.raw_order_events
                WHERE order_id = %s
                ORDER BY event_id
                LIMIT 1
                """,
                (order_id,),
            )
            row = cursor.fetchone()
            assert row is not None
            updated_at = _day_timestamp(target_date, updated_hour)
            cursor.execute(
                """
                INSERT INTO orders_demo.raw_order_events (
                    order_id,
                    order_ts,
                    amount,
                    updated_at,
                    ingested_at
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (order_id, row[0], amount, updated_at, updated_at),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def _unused_dates(connect: Callable[[], Any], count: int) -> list[date]:
    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH candidates AS (
                    SELECT day::date AS candidate_date
                    FROM generate_series(
                        DATE '2200-01-01',
                        DATE '2299-12-31' - %s,
                        INTERVAL '1 day'
                    ) AS day
                )
                SELECT candidate_date
                FROM candidates
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM orders_demo.raw_order_events AS raw
                    WHERE raw.order_ts >= candidate_date::timestamp AT TIME ZONE 'UTC'
                      AND raw.order_ts <
                          (candidate_date + %s)::timestamp AT TIME ZONE 'UTC'
                )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM orders_demo.daily_order_aggregates AS aggregate
                    WHERE aggregate.aggregate_date >= candidate_date
                      AND aggregate.aggregate_date < candidate_date + %s
                )
                ORDER BY candidate_date
                LIMIT 1
                """,
                (count, count, count),
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    assert row is not None, f"could not allocate {count} consecutive unused dates"
    return [row[0] + timedelta(days=offset) for offset in range(count)]


def _wait_for_date_calculation_sleep(
    connect: Callable[[], Any],
    target_date: date,
    *,
    timeout: float = 20,
) -> None:
    lock_key = target_date.toordinal()

    def calculation_is_sleeping() -> bool:
        connection = connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks AS lock
                        INNER JOIN pg_stat_activity AS activity
                            ON activity.pid = lock.pid
                        WHERE lock.locktype = 'advisory'
                          AND lock.classid = %s
                          AND lock.objid = %s
                          AND lock.granted
                          AND activity.datname = current_database()
                          AND activity.wait_event = 'PgSleep'
                          AND activity.query LIKE
                              '%%orders_demo.publish_daily_order_aggregate%%'
                    )
                    """,
                    (LOCK_NAMESPACE, lock_key),
                )
                return bool(cursor.fetchone()[0])
        finally:
            connection.close()

    _wait_until(
        f"calculation for {target_date} to enter pg_sleep after reading input",
        calculation_is_sleeping,
        timeout=timeout,
    )


def _wait_for_date_lock_waiter(
    connect: Callable[[], Any],
    target_date: date,
    *,
    timeout: float = 10,
) -> None:
    lock_key = target_date.toordinal()

    def calculation_is_waiting_for_lock() -> bool:
        connection = connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks AS lock
                        INNER JOIN pg_stat_activity AS activity
                            ON activity.pid = lock.pid
                        WHERE lock.locktype = 'advisory'
                          AND lock.classid = %s
                          AND lock.objid = %s
                          AND NOT lock.granted
                          AND activity.datname = current_database()
                          AND activity.query LIKE '%%pg_advisory_xact_lock%%'
                    )
                    """,
                    (LOCK_NAMESPACE, lock_key),
                )
                return bool(cursor.fetchone()[0])
        finally:
            connection.close()

    _wait_until(
        f"calculation for {target_date} to wait for the advisory lock",
        calculation_is_waiting_for_lock,
        timeout=timeout,
    )


def _manual_daily_run(
    api: AirflowApi,
    run_id: str,
    target_date: date,
    **extra_conf: Any,
) -> dict[str, Any]:
    conf = {
        "affected_date": target_date.isoformat(),
        "reason": "backfill",
        **extra_conf,
    }
    api.trigger(DAILY_DAG_ID, run_id, conf)
    return _wait_for_run(api, DAILY_DAG_ID, run_id, "success")


@pytest.fixture(scope="session")
def api() -> Iterator[AirflowApi]:
    base_url = os.getenv("AIRFLOW_API_BASE_URL")
    username = os.getenv("AIRFLOW_API_USERNAME")
    password = os.getenv("AIRFLOW_API_PASSWORD")
    if not base_url or not username or not password:
        pytest.fail(
            "AIRFLOW_API_BASE_URL, AIRFLOW_API_USERNAME, and "
            "AIRFLOW_API_PASSWORD are required"
        )

    client = AirflowApi(base_url, username, password)
    dag_ids = [DAILY_DAG_ID, INGESTION_DAG_ID, DISPATCHER_DAG_ID]
    original_pause_states = {
        dag_id: client.dag(dag_id)["is_paused"] for dag_id in dag_ids
    }

    try:
        for dag_id in dag_ids:
            if original_pause_states[dag_id]:
                client.set_paused(dag_id, False)
        pool = client.pool(POOL_NAME)
        assert pool["slots"] == 2, pool
        yield client
    finally:
        for dag_id, was_paused in original_pause_states.items():
            if client.dag(dag_id)["is_paused"] != was_paused:
                client.set_paused(dag_id, was_paused)


@pytest.fixture(scope="session")
def connect() -> Callable[[], Any]:
    dsn = os.getenv("ORDERS_TEST_DSN")
    if not dsn:
        pytest.fail("ORDERS_TEST_DSN is required")
    return lambda: psycopg2.connect(dsn)


@pytest.fixture(scope="session")
def scenario_dates(connect: Callable[[], Any]) -> dict[str, Any]:
    dates = _unused_dates(connect, 13)
    return {
        "canonical": dates[0],
        "boundary": dates[1:3],
        "retry": dates[3],
        "late_failure": dates[4],
        "same_date": dates[5],
        "parallel": dates[6:8],
        "pool": dates[8:11],
        "timeout": dates[11],
        "ingestion_retry": dates[12],
    }


@pytest.fixture(scope="session")
def run_token() -> str:
    return uuid.uuid4().hex[:12]


def test_ingestion_retry_after_commit_emits_one_asset_event_without_duplicates(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    target_date = scenario_dates["ingestion_retry"]
    run_id = f"r01-{run_token}-post-commit-retry"
    change_id = f"r01-{run_token}-post-commit-change"
    order_id = f"r01-{run_token}-post-commit-order"
    event_timestamp = _day_timestamp(target_date, 12)
    api.trigger(
        INGESTION_DAG_ID,
        run_id,
        {
            "change_id": change_id,
            "reason": "post_commit_retry_probe",
            "demo_fail_after_commit_once": True,
            "events": [
                {
                    "order_id": order_id,
                    "order_ts": _utc_isoformat(event_timestamp),
                    "amount": 42.00,
                    "updated_at": _utc_isoformat(event_timestamp),
                    "ingested_at": _utc_isoformat(event_timestamp),
                }
            ],
        },
    )

    _wait_for_task_state(
        api,
        INGESTION_DAG_ID,
        run_id,
        {"up_for_retry"},
        timeout=40,
    )
    committed_state = _ingestion_storage_state(connect, change_id, order_id)
    assert committed_state.receipt_count == 1
    assert committed_state.reason == "post_commit_retry_probe"
    assert committed_state.affected_date == target_date
    assert len(committed_state.payload_sha256) == 64
    assert committed_state.event_count == 1
    assert committed_state.raw_event_count == 1
    assert api.asset_events(run_id) == []

    _wait_for_run(api, INGESTION_DAG_ID, run_id, "success")
    task_instance = api.task_instance(INGESTION_DAG_ID, run_id)
    assert task_instance["state"] == "success"
    assert task_instance["try_number"] == 2
    assert api.xcom(INGESTION_DAG_ID, run_id, INGESTION_TASK_ID) == {
        "affected_date": target_date.isoformat(),
        "reason": "post_commit_retry_probe",
        "change_id": change_id,
        "inserted_event_count": 0,
        "change_replayed": True,
    }
    assert _ingestion_storage_state(connect, change_id, order_id) == committed_state

    def event_with_dispatcher() -> dict[str, Any] | None:
        events = api.asset_events(run_id)
        if len(events) != 1 or not events[0]["created_dagruns"]:
            return None
        return events[0]

    asset_event = _wait_until(
        "post-commit retry Asset event and dispatcher run",
        event_with_dispatcher,
        timeout=60,
    )
    assert asset_event["extra"] == {
        "affected_date": target_date.isoformat(),
        "reason": "post_commit_retry_probe",
        "change_id": change_id,
    }
    dispatcher_reference = next(
        reference
        for reference in asset_event["created_dagruns"]
        if reference["dag_id"] == DISPATCHER_DAG_ID
    )
    dispatcher_run_id = dispatcher_reference["run_id"]
    _wait_for_run(api, DISPATCHER_DAG_ID, dispatcher_run_id, "success")

    source_token = hashlib.sha256(dispatcher_run_id.encode()).hexdigest()[:16]
    target_run_id = f"late_data__{target_date.isoformat()}__{source_token}"
    _wait_for_run(api, DAILY_DAG_ID, target_run_id, "success", timeout=120)
    assert _aggregate(connect, target_date) == Aggregate(1, 1, Decimal("42.00"))


def test_canonical_fixture_repeat_boundary_and_successful_late_order(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    target_date = scenario_dates["canonical"]
    order_prefix = f"imp08-{run_token}-canonical"
    _insert_canonical_r1_r4(connect, target_date, order_prefix)

    first_run_id = f"imp08-{run_token}-canonical-first"
    _manual_daily_run(api, first_run_id, target_date)
    assert _aggregate(connect, target_date) == Aggregate(1, 2, Decimal("170.00"))
    first_summary = api.xcom(DAILY_DAG_ID, first_run_id, DAILY_TASK_ID)
    assert first_summary["records_read"] == 4
    assert first_summary["unique_orders"] == 2
    assert first_summary["new_aggregate"]["order_count"] == 2
    assert first_summary["new_aggregate"]["sales_amount"] == "170.00"

    repeat_run_id = f"imp08-{run_token}-canonical-repeat"
    _manual_daily_run(api, repeat_run_id, target_date)
    assert _aggregate(connect, target_date) == Aggregate(1, 2, Decimal("170.00"))
    baseline_snapshot = _aggregate_snapshot(connect, target_date)

    boundary_dates = scenario_dates["boundary"]
    assert boundary_dates[1] == boundary_dates[0] + timedelta(days=1)
    _insert_boundary_fixture(
        connect,
        boundary_dates[0],
        f"imp08-{run_token}-boundary",
    )
    _manual_daily_run(api, f"imp08-{run_token}-boundary-first", boundary_dates[0])
    _manual_daily_run(api, f"imp08-{run_token}-boundary-second", boundary_dates[1])
    assert _aggregate(connect, boundary_dates[0]) == Aggregate(
        1,
        1,
        Decimal("11.00"),
    )
    assert _aggregate(connect, boundary_dates[1]) == Aggregate(
        1,
        1,
        Decimal("22.00"),
    )

    ingestion_run_id = f"imp08-{run_token}-late-order-c"
    change_id = f"imp08-{run_token}-late-order-c"
    order_c_updated_at = datetime.combine(
        target_date + timedelta(days=2),
        datetime_time(hour=9),
        tzinfo=timezone.utc,
    )
    lock_connection = connect()
    try:
        with lock_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s, %s)",
                (LOCK_NAMESPACE, target_date.toordinal()),
            )

        api.trigger(
            INGESTION_DAG_ID,
            ingestion_run_id,
            {
                "change_id": change_id,
                "reason": "late_order",
                "events": [
                    {
                        "order_id": f"{order_prefix}-C",
                        "order_ts": _utc_isoformat(_day_timestamp(target_date, 15)),
                        "amount": 30.00,
                        "updated_at": _utc_isoformat(order_c_updated_at),
                        "ingested_at": _utc_isoformat(
                            order_c_updated_at + timedelta(minutes=1)
                        ),
                    }
                ],
            },
        )
        _wait_for_run(api, INGESTION_DAG_ID, ingestion_run_id, "success")

        def event_with_dispatcher() -> dict[str, Any] | None:
            events = api.asset_events(ingestion_run_id)
            if len(events) != 1 or not events[0]["created_dagruns"]:
                return None
            return events[0]

        asset_event = _wait_until(
            "canonical late-order Asset event and dispatcher run",
            event_with_dispatcher,
            timeout=60,
        )
        assert asset_event["extra"] == {
            "affected_date": target_date.isoformat(),
            "reason": "late_order",
            "change_id": change_id,
        }
        dispatcher_reference = next(
            reference
            for reference in asset_event["created_dagruns"]
            if reference["dag_id"] == DISPATCHER_DAG_ID
        )
        dispatcher_run_id = dispatcher_reference["run_id"]
        _wait_for_run(api, DISPATCHER_DAG_ID, dispatcher_run_id, "success")

        source_token = hashlib.sha256(dispatcher_run_id.encode()).hexdigest()[:16]
        target_run_id = f"late_data__{target_date.isoformat()}__{source_token}"
        _wait_for_task_state(api, DAILY_DAG_ID, target_run_id, {"running"})
        _wait_for_date_lock_waiter(connect, target_date)
        assert _aggregate_snapshot(connect, target_date) == baseline_snapshot
    finally:
        lock_connection.rollback()
        lock_connection.close()

    _wait_for_run(api, DAILY_DAG_ID, target_run_id, "success", timeout=120)
    assert _aggregate(connect, target_date) == Aggregate(1, 3, Decimal("200.00"))

    late_summary = api.xcom(DAILY_DAG_ID, target_run_id, DAILY_TASK_ID)
    assert late_summary["initial_trigger_reason"] == "late_data"
    assert late_summary["records_read"] == 5
    assert late_summary["unique_orders"] == 3
    assert late_summary["old_aggregate"] == {
        "order_count": 2,
        "sales_amount": "170.00",
        "published_at": _utc_isoformat(baseline_snapshot.published_at),
        "source_max_updated_at": _utc_isoformat(
            baseline_snapshot.source_max_updated_at
        ),
    }
    assert late_summary["new_aggregate"]["order_count"] == 3
    assert late_summary["new_aggregate"]["sales_amount"] == "200.00"
    assert late_summary["publication_state"] == "published"


def test_fail_first_retry_preserves_then_replaces_and_exposes_xcom(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    target_date = scenario_dates["retry"]
    order_a = f"v01-{run_token}-retry-a"
    order_b = f"v01-{run_token}-retry-b"
    _insert_order_versions(connect, target_date, [(order_a, Decimal("10.00"), 10)])

    baseline_run_id = f"v01-{run_token}-retry-baseline"
    _manual_daily_run(api, baseline_run_id, target_date)
    assert _aggregate(connect, target_date) == Aggregate(1, 1, Decimal("10.00"))
    baseline_snapshot = _aggregate_snapshot(connect, target_date)

    _insert_new_version(connect, target_date, order_a, Decimal("12.00"), 11)
    _insert_order_versions(connect, target_date, [(order_b, Decimal("5.00"), 12)])

    retry_run_id = f"v01-{run_token}-fail-first-retry"
    api.trigger(
        DAILY_DAG_ID,
        retry_run_id,
        {
            "affected_date": target_date.isoformat(),
            "reason": "schedule",
            "demo_fail_first_attempt": True,
        },
    )
    _wait_for_task_state(
        api,
        DAILY_DAG_ID,
        retry_run_id,
        {"up_for_retry"},
        timeout=40,
    )

    assert _aggregate_snapshot(connect, target_date) == baseline_snapshot
    failed_summary = _summary_from_log(
        api.task_log(DAILY_DAG_ID, retry_run_id, DAILY_TASK_ID, 1)
    )
    assert failed_summary == {
        "affected_date": target_date.isoformat(),
        "trigger_reason": "schedule",
        "initial_trigger_reason": "schedule",
        "records_read": None,
        "unique_orders": None,
        "old_aggregate": {
            "order_count": 1,
            "sales_amount": "10.00",
            "published_at": _utc_isoformat(baseline_snapshot.published_at),
            "source_max_updated_at": _utc_isoformat(
                baseline_snapshot.source_max_updated_at
            ),
        },
        "new_aggregate": None,
        "publication_state": "old_result_preserved",
        "lock_waited": False,
        "lock_wait_ms": 0.0,
        "try_number": 1,
        "error": "RuntimeError: demo fail-first attempt before publication",
    }

    _wait_for_run(api, DAILY_DAG_ID, retry_run_id, "success")
    task_instance = api.task_instance(DAILY_DAG_ID, retry_run_id)
    assert task_instance["state"] == "success"
    assert task_instance["try_number"] == 2
    assert _aggregate(connect, target_date) == Aggregate(1, 2, Decimal("17.00"))
    published_snapshot = _aggregate_snapshot(connect, target_date)

    success_summary = api.xcom(DAILY_DAG_ID, retry_run_id, DAILY_TASK_ID)
    assert set(success_summary) == SUMMARY_KEYS
    assert success_summary["affected_date"] == target_date.isoformat()
    assert success_summary["trigger_reason"] == "retry"
    assert success_summary["initial_trigger_reason"] == "schedule"
    assert success_summary["records_read"] == 3
    assert success_summary["unique_orders"] == 2
    assert success_summary["old_aggregate"] == {
        "order_count": 1,
        "sales_amount": "10.00",
        "published_at": _utc_isoformat(baseline_snapshot.published_at),
        "source_max_updated_at": _utc_isoformat(
            baseline_snapshot.source_max_updated_at
        ),
    }
    assert published_snapshot.source_max_updated_at == _day_timestamp(
        target_date,
        12,
    )
    assert success_summary["new_aggregate"] == {
        "order_count": published_snapshot.order_count,
        "sales_amount": format(published_snapshot.sales_amount, ".2f"),
        "published_at": _utc_isoformat(published_snapshot.published_at),
        "source_max_updated_at": _utc_isoformat(
            published_snapshot.source_max_updated_at
        ),
    }
    assert success_summary["publication_state"] == "published"
    assert success_summary["lock_waited"] is False
    assert success_summary["lock_wait_ms"] == 0.0
    assert success_summary["try_number"] == 2
    assert success_summary["error"] is None
    assert _summary_from_log(
        api.task_log(DAILY_DAG_ID, retry_run_id, DAILY_TASK_ID, 2)
    ) == success_summary


def test_failed_late_data_target_keeps_old_result_and_exposes_asset_metadata(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    target_date = scenario_dates["late_failure"]
    baseline_order = f"v01-{run_token}-late-baseline"
    conflict_order = f"v01-{run_token}-late-conflict"
    _insert_order_versions(
        connect,
        target_date,
        [(baseline_order, Decimal("20.00"), 10)],
    )
    _manual_daily_run(api, f"v01-{run_token}-late-baseline", target_date)
    assert _aggregate(connect, target_date) == Aggregate(1, 1, Decimal("20.00"))
    baseline_snapshot = _aggregate_snapshot(connect, target_date)

    ingestion_run_id = f"v01-{run_token}-conflicting-ingestion"
    change_id = f"v01-{run_token}-conflict-change"
    order_ts = _day_timestamp(target_date, 12).isoformat().replace("+00:00", "Z")
    updated_at = _day_timestamp(target_date, 13).isoformat().replace("+00:00", "Z")
    api.trigger(
        INGESTION_DAG_ID,
        ingestion_run_id,
        {
            "change_id": change_id,
            "reason": "late_conflict_probe",
            "events": [
                {
                    "order_id": conflict_order,
                    "order_ts": order_ts,
                    "amount": 30.00,
                    "updated_at": updated_at,
                    "ingested_at": updated_at,
                },
                {
                    "order_id": conflict_order,
                    "order_ts": order_ts,
                    "amount": 31.00,
                    "updated_at": updated_at,
                    "ingested_at": updated_at,
                },
            ],
        },
    )
    _wait_for_run(api, INGESTION_DAG_ID, ingestion_run_id, "success")

    def event_with_dispatcher() -> dict[str, Any] | None:
        events = api.asset_events(ingestion_run_id)
        if len(events) != 1 or not events[0]["created_dagruns"]:
            return None
        return events[0]

    asset_event = _wait_until(
        "Asset event and its dispatcher run",
        event_with_dispatcher,
        timeout=60,
    )
    assert asset_event["source_dag_id"] == INGESTION_DAG_ID
    assert asset_event["source_task_id"] == INGESTION_TASK_ID
    assert asset_event["extra"] == {
        "affected_date": target_date.isoformat(),
        "reason": "late_conflict_probe",
        "change_id": change_id,
    }
    dispatcher_reference = next(
        reference
        for reference in asset_event["created_dagruns"]
        if reference["dag_id"] == DISPATCHER_DAG_ID
    )
    dispatcher_run_id = dispatcher_reference["run_id"]
    _wait_for_run(api, DISPATCHER_DAG_ID, dispatcher_run_id, "success")

    upstream_events = api.upstream_asset_events(DISPATCHER_DAG_ID, dispatcher_run_id)
    matching_events = [
        event for event in upstream_events if event["source_run_id"] == ingestion_run_id
    ]
    assert len(matching_events) == 1
    assert matching_events[0]["extra"] == asset_event["extra"]

    source_token = hashlib.sha256(dispatcher_run_id.encode()).hexdigest()[:16]
    target_run_id = f"late_data__{target_date.isoformat()}__{source_token}"
    _wait_for_run(api, DAILY_DAG_ID, target_run_id, "failed", timeout=120)
    task_instance = api.task_instance(DAILY_DAG_ID, target_run_id)
    assert task_instance["state"] == "failed"
    assert task_instance["try_number"] == 2
    assert _aggregate_snapshot(connect, target_date) == baseline_snapshot

    failure_summary = _summary_from_log(
        api.task_log(DAILY_DAG_ID, target_run_id, DAILY_TASK_ID, 2)
    )
    assert failure_summary["affected_date"] == target_date.isoformat()
    assert failure_summary["initial_trigger_reason"] == "late_data"
    assert failure_summary["trigger_reason"] == "retry"
    assert failure_summary["records_read"] is None
    assert failure_summary["unique_orders"] is None
    assert failure_summary["old_aggregate"] == {
        "order_count": 1,
        "sales_amount": "20.00",
        "published_at": _utc_isoformat(baseline_snapshot.published_at),
        "source_max_updated_at": _utc_isoformat(
            baseline_snapshot.source_max_updated_at
        ),
    }
    assert failure_summary["new_aggregate"] is None
    assert failure_summary["publication_state"] == "old_result_preserved"
    assert failure_summary["lock_waited"] is False
    assert failure_summary["lock_wait_ms"] == 0.0
    assert failure_summary["try_number"] == 2
    assert "conflicting records" in failure_summary["error"]


def test_real_timeout_preserves_the_published_aggregate_and_reports_error(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    target_date = scenario_dates["timeout"]
    order_a = f"imp08-{run_token}-timeout-a"
    order_b = f"imp08-{run_token}-timeout-b"
    _insert_order_versions(connect, target_date, [(order_a, Decimal("10.00"), 10)])
    _manual_daily_run(api, f"imp08-{run_token}-timeout-baseline", target_date)
    baseline_snapshot = _aggregate_snapshot(connect, target_date)

    _insert_new_version(connect, target_date, order_a, Decimal("12.00"), 11)
    _insert_order_versions(connect, target_date, [(order_b, Decimal("5.00"), 12)])

    timeout_run_id = f"imp08-{run_token}-timeout"
    api.trigger(
        DAILY_DAG_ID,
        timeout_run_id,
        {
            "affected_date": target_date.isoformat(),
            "reason": "backfill",
            "demo_delay_seconds": 15,
        },
    )
    _wait_for_task_state(api, DAILY_DAG_ID, timeout_run_id, {"running"})
    _wait_for_date_calculation_sleep(connect, target_date)
    assert _aggregate_snapshot(connect, target_date) == baseline_snapshot

    _wait_for_run(api, DAILY_DAG_ID, timeout_run_id, "failed", timeout=90)
    task_instance = api.task_instance(DAILY_DAG_ID, timeout_run_id)
    assert task_instance["state"] == "failed"
    assert task_instance["try_number"] == 2
    assert _aggregate_snapshot(connect, target_date) == baseline_snapshot

    timeout_summary = _summary_from_log(
        api.task_log(DAILY_DAG_ID, timeout_run_id, DAILY_TASK_ID, 2)
    )
    assert timeout_summary["affected_date"] == target_date.isoformat()
    assert timeout_summary["trigger_reason"] == "retry"
    assert timeout_summary["initial_trigger_reason"] == "backfill"
    assert timeout_summary["old_aggregate"] == {
        "order_count": 1,
        "sales_amount": "10.00",
        "published_at": _utc_isoformat(baseline_snapshot.published_at),
        "source_max_updated_at": _utc_isoformat(
            baseline_snapshot.source_max_updated_at
        ),
    }
    assert timeout_summary["new_aggregate"] is None
    assert timeout_summary["publication_state"] == "old_result_preserved"
    assert timeout_summary["try_number"] == 2
    assert timeout_summary["error"].startswith("AirflowTaskTimeout: Timeout, PID:")


def test_same_date_airflow_runs_serialize_and_second_reads_late_order(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    target_date = scenario_dates["same_date"]
    initial_versions = [
        (f"v01-{run_token}-same-{number}", Decimal(number), 12)
        for number in range(1, 11)
    ]
    _insert_order_versions(connect, target_date, initial_versions)

    first_run_id = f"v01-{run_token}-same-date-a"
    api.trigger(
        DAILY_DAG_ID,
        first_run_id,
        {
            "affected_date": target_date.isoformat(),
            "reason": "backfill",
            "demo_delay_seconds": 6,
        },
    )
    _wait_for_task_state(api, DAILY_DAG_ID, first_run_id, {"running"})
    _wait_for_date_calculation_sleep(connect, target_date)

    _insert_order_versions(
        connect,
        target_date,
        [(f"v01-{run_token}-same-11", Decimal("11.00"), 13)],
    )
    second_run_id = f"v01-{run_token}-same-date-b"
    api.trigger(
        DAILY_DAG_ID,
        second_run_id,
        {
            "affected_date": target_date.isoformat(),
            "reason": "late_data",
        },
    )
    _wait_for_task_state(api, DAILY_DAG_ID, second_run_id, {"running"})

    _wait_for_run(api, DAILY_DAG_ID, first_run_id, "success")
    _wait_for_run(api, DAILY_DAG_ID, second_run_id, "success")
    first_task = api.task_instance(DAILY_DAG_ID, first_run_id)
    second_task = api.task_instance(DAILY_DAG_ID, second_run_id)
    assert _utc_timestamp(second_task["start_date"]) < _utc_timestamp(
        first_task["end_date"]
    )

    second_summary = api.xcom(DAILY_DAG_ID, second_run_id, DAILY_TASK_ID)
    assert second_summary["lock_waited"] is True
    assert second_summary["lock_wait_ms"] > 0
    assert second_summary["old_aggregate"]["order_count"] == 10
    assert second_summary["old_aggregate"]["sales_amount"] == "55.00"
    assert second_summary["new_aggregate"]["order_count"] == 11
    assert second_summary["new_aggregate"]["sales_amount"] == "66.00"
    assert _aggregate(connect, target_date) == Aggregate(1, 11, Decimal("66.00"))


def test_different_dates_overlap_at_airflow_level(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    dates = scenario_dates["parallel"]
    run_ids: list[str] = []
    for index, target_date in enumerate(dates, start=1):
        _insert_order_versions(
            connect,
            target_date,
            [(f"v01-{run_token}-parallel-{index}", Decimal(index * 10), 10)],
        )
        run_id = f"v01-{run_token}-parallel-{index}"
        run_ids.append(run_id)
        api.trigger(
            DAILY_DAG_ID,
            run_id,
            {
                "affected_date": target_date.isoformat(),
                "reason": "backfill",
                "demo_delay_seconds": 4,
            },
        )

    for run_id in run_ids:
        _wait_for_run(api, DAILY_DAG_ID, run_id, "success")
    tasks = [api.task_instance(DAILY_DAG_ID, run_id) for run_id in run_ids]
    starts = [_utc_timestamp(task["start_date"]) for task in tasks]
    ends = [_utc_timestamp(task["end_date"]) for task in tasks]
    assert max(starts) < min(ends)

    for index, target_date in enumerate(dates, start=1):
        assert _aggregate(connect, target_date) == Aggregate(
            1,
            1,
            Decimal(index * 10),
        )


def test_pool_limits_three_airflow_tasks_to_two_slots(
    api: AirflowApi,
    connect: Callable[[], Any],
    scenario_dates: dict[str, Any],
    run_token: str,
) -> None:
    dates = scenario_dates["pool"]
    run_ids: list[str] = []
    for index, target_date in enumerate(dates, start=1):
        _insert_order_versions(
            connect,
            target_date,
            [(f"v01-{run_token}-pool-{index}", Decimal(index), 10)],
        )
        run_id = f"v01-{run_token}-pool-{index}"
        run_ids.append(run_id)
        api.trigger(
            DAILY_DAG_ID,
            run_id,
            {
                "affected_date": target_date.isoformat(),
                "reason": "backfill",
                "demo_delay_seconds": 5,
            },
        )

    def capacity_observation() -> dict[str, Any] | None:
        tasks: list[dict[str, Any]] = []
        for run_id in run_ids:
            run = api.dag_run(DAILY_DAG_ID, run_id, allow_missing=True)
            if run is None:
                return None
            try:
                tasks.append(api.task_instance(DAILY_DAG_ID, run_id))
            except AirflowApiError as error:
                if error.status == 404:
                    return None
                raise
        pool = api.pool(POOL_NAME)
        states = [task["state"] for task in tasks]
        waiting = [state for state in states if state in {"scheduled", "queued"}]
        if (
            states.count("running") == 2
            and len(waiting) == 1
            and pool["running_slots"] == 2
            and pool["open_slots"] == 0
            and pool["scheduled_slots"] + pool["queued_slots"] >= 1
        ):
            return {"tasks": tasks, "pool": pool, "waiting_state": waiting[0]}
        return None

    observation = _wait_until(
        "two running pool tasks and one pool-blocked task",
        capacity_observation,
        timeout=45,
    )
    assert observation["pool"]["slots"] == 2
    assert observation["waiting_state"] in {"scheduled", "queued"}

    for run_id in run_ids:
        _wait_for_run(api, DAILY_DAG_ID, run_id, "success")
    tasks = [api.task_instance(DAILY_DAG_ID, run_id) for run_id in run_ids]
    intervals = sorted(
        (
            _utc_timestamp(task["start_date"]),
            _utc_timestamp(task["end_date"]),
        )
        for task in tasks
    )
    latest_start = max(start for start, _ in intervals)
    earliest_end = min(end for _, end in intervals)
    assert latest_start >= earliest_end

    for index, target_date in enumerate(dates, start=1):
        assert _aggregate(connect, target_date) == Aggregate(
            1,
            1,
            Decimal(index),
        )
