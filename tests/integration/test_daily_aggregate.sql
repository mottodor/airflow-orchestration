\set ON_ERROR_STOP on

BEGIN;

-- Canonical R1-R4 behavior: latest A wins and the exact duplicate is ignored.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('IMP04-A', '2099-01-01T10:00:00Z', 100.00, '2099-01-01T10:00:00Z', '2099-01-01T10:01:00Z'),
    ('IMP04-B', '2099-01-01T11:00:00Z',  50.00, '2099-01-01T11:00:00Z', '2099-01-01T11:01:00Z'),
    ('IMP04-A', '2099-01-01T10:00:00Z', 120.00, '2099-01-01T12:00:00Z', '2099-01-01T12:01:00Z'),
    ('IMP04-A', '2099-01-01T10:00:00Z', 120.00, '2099-01-01T12:00:00Z', '2099-01-01T12:02:00Z'),
    ('IMP04-BOUNDARY', '2099-01-02T00:00:00Z', 25.00, '2099-01-02T00:00:00Z', '2099-01-02T00:00:01Z');

DO $$
DECLARE
    result record;
BEGIN
    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-01');

    IF result.records_read <> 4
       OR result.order_count <> 2
       OR result.sales_amount <> 170.00
       OR result.source_max_updated_at <> '2099-01-01T12:00:00Z'::timestamptz THEN
        RAISE EXCEPTION 'latest-version or exact-copy resolution is wrong: %',
            row_to_json(result);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-01-01'
          AND order_count = 2
          AND sales_amount = 170.00
          AND source_max_updated_at = '2099-01-01T12:00:00Z'::timestamptz
    ) THEN
        RAISE EXCEPTION 'canonical aggregate was not published';
    END IF;

    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-01');

    IF result.records_read <> 4
       OR result.order_count <> 2
       OR result.sales_amount <> 170.00
       OR (
           SELECT count(*)
           FROM orders_demo.daily_order_aggregates
           WHERE aggregate_date = DATE '2099-01-01'
       ) <> 1 THEN
        RAISE EXCEPTION 'repeat calculation is not idempotent: %', row_to_json(result);
    END IF;

    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-02');

    IF result.records_read <> 1
       OR result.order_count <> 1
       OR result.sales_amount <> 25.00 THEN
        RAISE EXCEPTION 'UTC boundary assigned an order to the wrong day: %',
            row_to_json(result);
    END IF;

    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-03');

    IF result.records_read <> 0
       OR result.order_count <> 0
       OR result.sales_amount <> 0
       OR result.source_max_updated_at IS NOT NULL THEN
        RAISE EXCEPTION 'empty UTC day produced an inconsistent aggregate: %',
            row_to_json(result);
    END IF;
END;
$$;

-- A later order is added to the same historical date without changing existing orders.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES (
    'IMP04-C', '2099-01-01T15:00:00Z', 30.00,
    '2099-01-03T09:00:00Z', '2099-01-03T09:01:00Z'
);

DO $$
DECLARE
    result record;
BEGIN
    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-01');

    IF result.records_read <> 5
       OR result.order_count <> 3
       OR result.sales_amount <> 200.00
       OR result.source_max_updated_at <> '2099-01-03T09:00:00Z'::timestamptz THEN
        RAISE EXCEPTION 'historical date recalculation is wrong: %', row_to_json(result);
    END IF;

    IF (
        SELECT count(*)
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-01-01'
    ) <> 1 THEN
        RAISE EXCEPTION 'UPSERT created more than one active daily aggregate';
    END IF;
END;
$$;

-- Different business values at the same order_id/updated_at are a conflict.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('IMP04-CONFLICT', '2099-02-01T08:00:00Z', 40.00, '2099-02-01T08:00:00Z', '2099-02-01T08:01:00Z');

SELECT *
FROM orders_demo.publish_daily_order_aggregate(DATE '2099-02-01');

INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('IMP04-CONFLICT', '2099-02-01T08:00:00Z', 41.00, '2099-02-01T08:00:00Z', '2099-02-01T08:02:00Z');

DO $$
BEGIN
    BEGIN
        PERFORM *
        FROM orders_demo.publish_daily_order_aggregate(DATE '2099-02-01');
        RAISE EXCEPTION 'conflicting records at the same updated_at were published';
    EXCEPTION
        WHEN SQLSTATE 'P0001' THEN
            IF SQLERRM NOT LIKE 'order_id IMP04-CONFLICT has conflicting records at updated_at%' THEN
                RAISE;
            END IF;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-02-01'
          AND order_count = 1
          AND sales_amount = 40.00
    ) THEN
        RAISE EXCEPTION 'version conflict changed the previously published aggregate';
    END IF;
END;
$$;

-- order_ts is immutable for one stable order_id, including moves across UTC dates.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('IMP04-MOVED', '2099-03-01T10:00:00Z', 60.00, '2099-03-01T10:00:00Z', '2099-03-01T10:01:00Z');

SELECT *
FROM orders_demo.publish_daily_order_aggregate(DATE '2099-03-01');

INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('IMP04-MOVED', '2099-03-02T10:00:00Z', 60.00, '2099-03-02T10:00:00Z', '2099-03-02T10:01:00Z');

DO $$
BEGIN
    BEGIN
        PERFORM *
        FROM orders_demo.publish_daily_order_aggregate(DATE '2099-03-01');
        RAISE EXCEPTION 'an order was moved to another order_ts';
    EXCEPTION
        WHEN SQLSTATE 'P0001' THEN
            IF SQLERRM <> 'order_id IMP04-MOVED has conflicting order_ts values' THEN
                RAISE;
            END IF;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-03-01'
          AND order_count = 1
          AND sales_amount = 60.00
    ) THEN
        RAISE EXCEPTION 'order_ts conflict changed the previously published aggregate';
    END IF;

    BEGIN
        PERFORM *
        FROM orders_demo.publish_daily_order_aggregate(DATE '2099-03-02');
        RAISE EXCEPTION 'the moved order was published on its new date';
    EXCEPTION
        WHEN SQLSTATE 'P0001' THEN
            IF SQLERRM <> 'order_id IMP04-MOVED has conflicting order_ts values' THEN
                RAISE;
            END IF;
    END;

    IF EXISTS (
        SELECT 1
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-03-02'
    ) THEN
        RAISE EXCEPTION 'order_ts conflict created an aggregate for the new date';
    END IF;
END;
$$;

ROLLBACK;
