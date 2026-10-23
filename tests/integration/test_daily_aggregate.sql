\set ON_ERROR_STOP on

BEGIN;

INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('IMP03-A', '2099-01-01T23:59:59Z', 100.00, '2099-01-01T23:59:59Z', '2099-01-02T00:00:01Z'),
    ('IMP03-B', '2099-01-01T12:00:00Z',  50.00, '2099-01-01T12:00:00Z', '2099-01-01T12:00:01Z'),
    ('IMP03-C', '2099-01-02T00:00:00Z',  25.00, '2099-01-02T00:00:00Z', '2099-01-02T00:00:01Z');

DO $$
DECLARE
    result record;
BEGIN
    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-01');

    IF result.records_read <> 2
       OR result.order_count <> 2
       OR result.sales_amount <> 150.00
       OR result.source_max_updated_at <> '2099-01-01T23:59:59Z'::timestamptz THEN
        RAISE EXCEPTION 'unexpected first-day aggregate: %', row_to_json(result);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-01-01'
          AND order_count = 2
          AND sales_amount = 150.00
          AND source_max_updated_at = '2099-01-01T23:59:59Z'::timestamptz
    ) THEN
        RAISE EXCEPTION 'first-day aggregate was not published';
    END IF;

    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-02');

    IF result.records_read <> 1
       OR result.order_count <> 1
       OR result.sales_amount <> 25.00 THEN
        RAISE EXCEPTION 'UTC boundary assigned an order to the wrong day: %', row_to_json(result);
    END IF;

    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-03');

    IF result.records_read <> 0
       OR result.order_count <> 0
       OR result.sales_amount <> 0
       OR result.source_max_updated_at IS NOT NULL THEN
        RAISE EXCEPTION 'empty UTC day produced an inconsistent aggregate: %', row_to_json(result);
    END IF;
END;
$$;

INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES (
    'IMP03-D', '2099-01-01T08:00:00Z', 20.00,
    '2099-01-01T08:00:00Z', '2099-01-01T08:00:01Z'
);

DO $$
DECLARE
    result record;
BEGIN
    SELECT *
    INTO result
    FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-01');

    IF result.records_read <> 3
       OR result.order_count <> 3
       OR result.sales_amount <> 170.00 THEN
        RAISE EXCEPTION 'repeat calculation did not replace the aggregate: %', row_to_json(result);
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

-- A conflicting repeated order must fail safely until IMP-04 resolves versions explicitly.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES (
    'IMP03-A', '2099-01-01T23:59:59Z', 101.00,
    '2099-01-01T23:59:59Z', '2099-01-02T00:00:02Z'
);

DO $$
BEGIN
    BEGIN
        PERFORM *
        FROM orders_demo.publish_daily_order_aggregate(DATE '2099-01-01');
        RAISE EXCEPTION 'ambiguous raw versions were published';
    EXCEPTION
        WHEN SQLSTATE 'P0001' THEN
            IF SQLERRM NOT LIKE 'raw input contains multiple records for order_id%' THEN
                RAISE;
            END IF;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM orders_demo.daily_order_aggregates
        WHERE aggregate_date = DATE '2099-01-01'
          AND order_count = 3
          AND sales_amount = 170.00
    ) THEN
        RAISE EXCEPTION 'failed recalculation changed the previously published aggregate';
    END IF;
END;
$$;

ROLLBACK;
