\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM (
            VALUES
                ('event_id', 'bigint', 'NO', 'YES'),
                ('order_id', 'text', 'NO', 'NO'),
                ('order_ts', 'timestamp with time zone', 'NO', 'NO'),
                ('amount', 'numeric', 'NO', 'NO'),
                ('updated_at', 'timestamp with time zone', 'NO', 'NO'),
                ('ingested_at', 'timestamp with time zone', 'NO', 'NO')
        ) AS expected(column_name, data_type, is_nullable, is_identity)
        LEFT JOIN information_schema.columns AS actual
          ON actual.table_schema = 'orders_demo'
         AND actual.table_name = 'raw_order_events'
         AND actual.column_name = expected.column_name
        WHERE actual.column_name IS NULL
           OR actual.data_type <> expected.data_type
           OR actual.is_nullable <> expected.is_nullable
           OR actual.is_identity <> expected.is_identity
    ) THEN
        RAISE EXCEPTION 'raw_order_events column contract does not match';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (
            VALUES
                ('aggregate_date', 'date', 'NO'),
                ('order_count', 'bigint', 'NO'),
                ('sales_amount', 'numeric', 'NO'),
                ('published_at', 'timestamp with time zone', 'NO'),
                ('source_max_updated_at', 'timestamp with time zone', 'YES')
        ) AS expected(column_name, data_type, is_nullable)
        LEFT JOIN information_schema.columns AS actual
          ON actual.table_schema = 'orders_demo'
         AND actual.table_name = 'daily_order_aggregates'
         AND actual.column_name = expected.column_name
        WHERE actual.column_name IS NULL
           OR actual.data_type <> expected.data_type
           OR actual.is_nullable <> expected.is_nullable
    ) THEN
        RAISE EXCEPTION 'daily_order_aggregates column contract does not match';
    END IF;
END;
$$;

-- The approved R1-R4 fixture includes two versions of A and an exact duplicate.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('A', '2025-03-01T10:00:00Z', 100.00, '2025-03-01T10:00:00Z', '2025-03-01T10:01:00Z'),
    ('B', '2025-03-01T11:00:00Z',  50.00, '2025-03-01T11:00:00Z', '2025-03-01T11:01:00Z'),
    ('A', '2025-03-01T10:00:00Z', 120.00, '2025-03-01T12:00:00Z', '2025-03-01T12:01:00Z'),
    ('A', '2025-03-01T10:00:00Z', 120.00, '2025-03-01T12:00:00Z', '2025-03-01T12:02:00Z');

DO $$
BEGIN
    IF (SELECT count(*) FROM orders_demo.raw_order_events) <> 4 THEN
        RAISE EXCEPTION 'raw table did not preserve all four append-only events';
    END IF;
END;
$$;

-- Ambiguous versions must be representable in raw storage so later validation can detect them.
INSERT INTO orders_demo.raw_order_events (
    order_id,
    order_ts,
    amount,
    updated_at,
    ingested_at
)
VALUES
    ('C', '2025-03-01T15:00:00Z', 30.00, '2025-03-03T09:00:00Z', '2025-03-03T09:01:00Z'),
    ('C', '2025-03-01T15:00:00Z', 31.00, '2025-03-03T09:00:00Z', '2025-03-03T09:02:00Z');

DO $$
BEGIN
    BEGIN
        UPDATE orders_demo.raw_order_events
        SET amount = 999.00
        WHERE order_id = 'A';
        RAISE EXCEPTION 'raw event update was accepted';
    EXCEPTION
        WHEN SQLSTATE '55000' THEN NULL;
    END;

    BEGIN
        DELETE FROM orders_demo.raw_order_events
        WHERE order_id = 'A';
        RAISE EXCEPTION 'raw event delete was accepted';
    EXCEPTION
        WHEN SQLSTATE '55000' THEN NULL;
    END;

    BEGIN
        TRUNCATE TABLE orders_demo.raw_order_events;
        RAISE EXCEPTION 'raw event truncate was accepted';
    EXCEPTION
        WHEN SQLSTATE '55000' THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            NULL, '2025-03-01T10:00:00Z', 1.00,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'NULL order_id was accepted';
    EXCEPTION
        WHEN not_null_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            '   ', '2025-03-01T10:00:00Z', 1.00,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'blank order_id was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'NEGATIVE', '2025-03-01T10:00:00Z', -0.01,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'negative amount was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'FRACTION', '2025-03-01T10:00:00Z', 1.001,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'amount with more than two decimal places was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'NAN', '2025-03-01T10:00:00Z', 'NaN'::numeric,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'NaN amount was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'INFINITE_AMOUNT', '2025-03-01T10:00:00Z', 'Infinity'::numeric,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'infinite amount was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'INFINITE_ORDER_TS', 'infinity'::timestamptz, 1.00,
            '2025-03-01T10:00:00Z', '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'infinite order_ts was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'INFINITE_UPDATED_AT', '2025-03-01T10:00:00Z', 1.00,
            'infinity'::timestamptz, '2025-03-01T10:00:01Z'
        );
        RAISE EXCEPTION 'infinite updated_at was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.raw_order_events (
            order_id, order_ts, amount, updated_at, ingested_at
        ) VALUES (
            'INFINITE_INGESTED_AT', '2025-03-01T10:00:00Z', 1.00,
            '2025-03-01T10:00:00Z', 'infinity'::timestamptz
        );
        RAISE EXCEPTION 'infinite ingested_at was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END;
$$;

INSERT INTO orders_demo.daily_order_aggregates (
    aggregate_date,
    order_count,
    sales_amount,
    published_at,
    source_max_updated_at
)
VALUES (
    DATE '2025-03-01',
    2,
    170.00,
    '2025-03-01T13:00:00Z',
    '2025-03-01T12:00:00Z'
);

DO $$
BEGIN
    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-01', 3, 200.00,
            '2025-03-03T10:00:00Z', '2025-03-03T09:00:00Z'
        );
        RAISE EXCEPTION 'a second active aggregate for one date was accepted';
    EXCEPTION
        WHEN unique_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-02', 0, 1.00, '2025-03-02T01:00:00Z', NULL
        );
        RAISE EXCEPTION 'inconsistent empty aggregate was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-02', 1, 1.001,
            '2025-03-02T01:00:00Z', '2025-03-02T00:30:00Z'
        );
        RAISE EXCEPTION 'aggregate with more than two decimal places was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-02', 1, 'NaN'::numeric,
            '2025-03-02T01:00:00Z', '2025-03-02T00:30:00Z'
        );
        RAISE EXCEPTION 'NaN aggregate amount was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-02', 1, 'Infinity'::numeric,
            '2025-03-02T01:00:00Z', '2025-03-02T00:30:00Z'
        );
        RAISE EXCEPTION 'infinite aggregate amount was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            'infinity'::date, 1, 1.00,
            '2025-03-02T01:00:00Z', '2025-03-02T00:30:00Z'
        );
        RAISE EXCEPTION 'infinite aggregate_date was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-02', 1, 1.00,
            'infinity'::timestamptz, '2025-03-02T00:30:00Z'
        );
        RAISE EXCEPTION 'infinite published_at was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO orders_demo.daily_order_aggregates (
            aggregate_date, order_count, sales_amount, published_at, source_max_updated_at
        ) VALUES (
            DATE '2025-03-02', 1, 1.00,
            '2025-03-02T01:00:00Z', 'infinity'::timestamptz
        );
        RAISE EXCEPTION 'infinite source_max_updated_at was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END;
$$;

INSERT INTO orders_demo.daily_order_aggregates (
    aggregate_date,
    order_count,
    sales_amount,
    published_at,
    source_max_updated_at
)
VALUES (
    DATE '2025-03-02',
    0,
    0,
    '2025-03-02T01:00:00Z',
    NULL
);

DO $$
BEGIN
    IF (SELECT count(*) FROM orders_demo.daily_order_aggregates) <> 2 THEN
        RAISE EXCEPTION 'unexpected active aggregate row count';
    END IF;
END;
$$;

ROLLBACK;
