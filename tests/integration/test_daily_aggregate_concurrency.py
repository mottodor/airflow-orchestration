from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os
import threading
import time
from typing import Callable

import psycopg2
from psycopg2 import errors
import pytest


ORDERS_LOCK_NAMESPACE = 20250301
ORDERS_TEST_DSN = os.getenv("ORDERS_TEST_DSN")


@pytest.fixture
def connect() -> Callable[[], psycopg2.extensions.connection]:
    if not ORDERS_TEST_DSN:
        pytest.skip("ORDERS_TEST_DSN is required for PostgreSQL integration tests")

    def create_connection() -> psycopg2.extensions.connection:
        return psycopg2.connect(ORDERS_TEST_DSN)

    return create_connection


@dataclass
class CallTiming:
    started: float = 0
    finished: float = 0
    released: float = 0
    error: BaseException | None = None


def _date_lock_key(affected_date: date) -> int:
    return affected_date.toordinal()


def _call_aggregate(
    connect: Callable[[], psycopg2.extensions.connection],
    affected_date: date,
    delay_seconds: float,
    timing: CallTiming,
    *,
    start_event: threading.Event | None = None,
) -> None:
    connection = connect()
    try:
        if start_event is not None:
            start_event.wait(timeout=5)
        timing.started = time.monotonic()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM orders_demo.publish_daily_order_aggregate(%s, %s)",
                (affected_date, delay_seconds),
            )
            cursor.fetchone()
        timing.finished = time.monotonic()
        connection.rollback()
        timing.released = time.monotonic()
    except BaseException as error:
        timing.error = error
        connection.rollback()
        timing.released = time.monotonic()
    finally:
        connection.close()


def test_same_date_waits_for_the_transaction_lock(
    connect: Callable[[], psycopg2.extensions.connection],
) -> None:
    affected_date = date(2199, 5, 1)
    lock_acquired = threading.Event()
    first = CallTiming()
    second = CallTiming()

    def hold_first_lock() -> None:
        connection = connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s, %s)",
                    (ORDERS_LOCK_NAMESPACE, _date_lock_key(affected_date)),
                )
                lock_acquired.set()
                first.started = time.monotonic()
                cursor.execute(
                    "SELECT * FROM orders_demo.publish_daily_order_aggregate(%s, %s)",
                    (affected_date, 2),
                )
                cursor.fetchone()
                first.finished = time.monotonic()
            connection.rollback()
            first.released = time.monotonic()
        except BaseException as error:
            first.error = error
            connection.rollback()
            first.released = time.monotonic()
        finally:
            connection.close()

    first_thread = threading.Thread(target=hold_first_lock)
    second_thread = threading.Thread(
        target=_call_aggregate,
        args=(connect, affected_date, 0, second),
        kwargs={"start_event": lock_acquired},
    )

    first_thread.start()
    second_thread.start()
    first_thread.join(timeout=10)
    second_thread.join(timeout=10)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert first.error is None
    assert second.error is None
    assert second.started < first.released
    assert second.finished - second.started >= 1.5


def test_different_dates_can_calculate_in_parallel(
    connect: Callable[[], psycopg2.extensions.connection],
) -> None:
    barrier = threading.Barrier(2)
    first = CallTiming()
    second = CallTiming()

    def call_after_barrier(affected_date: date, timing: CallTiming) -> None:
        barrier.wait(timeout=5)
        _call_aggregate(connect, affected_date, 2, timing)

    first_thread = threading.Thread(
        target=call_after_barrier,
        args=(date(2199, 5, 2), first),
    )
    second_thread = threading.Thread(
        target=call_after_barrier,
        args=(date(2199, 5, 3), second),
    )

    started = time.monotonic()
    first_thread.start()
    second_thread.start()
    first_thread.join(timeout=10)
    second_thread.join(timeout=10)
    elapsed = time.monotonic() - started

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert first.error is None
    assert second.error is None
    assert max(first.started, second.started) < min(first.finished, second.finished)
    assert elapsed < 3.5


def test_cancelled_calculation_keeps_old_result_and_releases_lock(
    connect: Callable[[], psycopg2.extensions.connection],
) -> None:
    affected_date = date(2199, 5, 4)
    setup_connection = connect()
    try:
        with setup_connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM orders_demo.daily_order_aggregates WHERE aggregate_date = %s",
                (affected_date,),
            )
            cursor.execute(
                """
                INSERT INTO orders_demo.daily_order_aggregates (
                    aggregate_date,
                    order_count,
                    sales_amount,
                    published_at,
                    source_max_updated_at
                ) VALUES (%s, 1, 10.00, clock_timestamp(), clock_timestamp())
                """,
                (affected_date,),
            )
        setup_connection.commit()

        timed_connection = connect()
        try:
            with timed_connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = '1000ms'")
                with pytest.raises(errors.QueryCanceled):
                    cursor.execute(
                        "SELECT * FROM orders_demo.publish_daily_order_aggregate(%s, %s)",
                        (affected_date, 3),
                    )
            timed_connection.rollback()
        finally:
            timed_connection.close()

        with setup_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_count, sales_amount
                FROM orders_demo.daily_order_aggregates
                WHERE aggregate_date = %s
                """,
                (affected_date,),
            )
            assert cursor.fetchone() == (1, 10)

            cursor.execute(
                "SELECT * FROM orders_demo.publish_daily_order_aggregate(%s, %s)",
                (affected_date, 0),
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[1:3] == (0, 0)
        setup_connection.commit()
    finally:
        with setup_connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM orders_demo.daily_order_aggregates WHERE aggregate_date = %s",
                (affected_date,),
            )
        setup_connection.commit()
        setup_connection.close()
