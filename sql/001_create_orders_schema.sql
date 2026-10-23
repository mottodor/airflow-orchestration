BEGIN;

CREATE SCHEMA IF NOT EXISTS orders_demo;

CREATE TABLE IF NOT EXISTS orders_demo.raw_order_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id text NOT NULL,
    order_ts timestamp with time zone NOT NULL,
    amount numeric NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    ingested_at timestamp with time zone NOT NULL,
    CONSTRAINT raw_order_events_order_id_not_blank
        CHECK (btrim(order_id) <> ''),
    CONSTRAINT raw_order_events_timestamps_finite
        CHECK (isfinite(order_ts) AND isfinite(updated_at) AND isfinite(ingested_at)),
    CONSTRAINT raw_order_events_amount_finite
        CHECK (amount NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)),
    CONSTRAINT raw_order_events_amount_nonnegative
        CHECK (amount >= 0),
    CONSTRAINT raw_order_events_amount_two_decimal_places
        CHECK (amount = round(amount, 2))
);

COMMENT ON TABLE orders_demo.raw_order_events IS
    'Append-only order versions. Exact business duplicates remain stored so calculation logic can prove deduplication.';

CREATE INDEX IF NOT EXISTS raw_order_events_interval_lookup_idx
    ON orders_demo.raw_order_events (order_ts, order_id, updated_at DESC);

CREATE OR REPLACE FUNCTION orders_demo.reject_raw_order_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'orders_demo.raw_order_events is append-only; % is not allowed', TG_OP
        USING ERRCODE = '55000';
END;
$$;

DROP TRIGGER IF EXISTS raw_order_events_reject_update_delete
    ON orders_demo.raw_order_events;
CREATE TRIGGER raw_order_events_reject_update_delete
    BEFORE UPDATE OR DELETE ON orders_demo.raw_order_events
    FOR EACH ROW
    EXECUTE FUNCTION orders_demo.reject_raw_order_event_mutation();

DROP TRIGGER IF EXISTS raw_order_events_reject_truncate
    ON orders_demo.raw_order_events;
CREATE TRIGGER raw_order_events_reject_truncate
    BEFORE TRUNCATE ON orders_demo.raw_order_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION orders_demo.reject_raw_order_event_mutation();

CREATE TABLE IF NOT EXISTS orders_demo.daily_order_aggregates (
    aggregate_date date PRIMARY KEY,
    order_count bigint NOT NULL,
    sales_amount numeric NOT NULL,
    published_at timestamp with time zone NOT NULL,
    source_max_updated_at timestamp with time zone,
    CONSTRAINT daily_order_aggregates_date_finite
        CHECK (isfinite(aggregate_date)),
    CONSTRAINT daily_order_aggregates_timestamps_finite
        CHECK (
            isfinite(published_at)
            AND (source_max_updated_at IS NULL OR isfinite(source_max_updated_at))
        ),
    CONSTRAINT daily_order_aggregates_order_count_nonnegative
        CHECK (order_count >= 0),
    CONSTRAINT daily_order_aggregates_sales_amount_finite
        CHECK (sales_amount NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)),
    CONSTRAINT daily_order_aggregates_sales_amount_nonnegative
        CHECK (sales_amount >= 0),
    CONSTRAINT daily_order_aggregates_sales_amount_two_decimal_places
        CHECK (sales_amount = round(sales_amount, 2)),
    CONSTRAINT daily_order_aggregates_empty_result_consistent
        CHECK (
            (order_count = 0 AND sales_amount = 0 AND source_max_updated_at IS NULL)
            OR (order_count > 0 AND source_max_updated_at IS NOT NULL)
        )
);

COMMENT ON TABLE orders_demo.daily_order_aggregates IS
    'Currently published daily aggregates; aggregate_date is the single active result key.';

CREATE OR REPLACE FUNCTION orders_demo.publish_daily_order_aggregate(
    p_aggregate_date date
)
RETURNS TABLE (
    records_read bigint,
    order_count bigint,
    sales_amount numeric,
    published_at timestamp with time zone,
    source_max_updated_at timestamp with time zone
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_records_read bigint;
    v_order_count bigint;
    v_sales_amount numeric;
    v_published_at timestamp with time zone;
    v_source_max_updated_at timestamp with time zone;
    v_repeated_order_id text;
BEGIN
    IF p_aggregate_date IS NULL OR NOT isfinite(p_aggregate_date) THEN
        RAISE EXCEPTION 'aggregate date must be finite and non-null'
            USING ERRCODE = '22007';
    END IF;

    SELECT
        count(*),
        count(DISTINCT order_id),
        COALESCE(sum(amount), 0),
        max(updated_at)
    INTO
        v_records_read,
        v_order_count,
        v_sales_amount,
        v_source_max_updated_at
    FROM orders_demo.raw_order_events
    WHERE order_ts >= (p_aggregate_date::timestamp AT TIME ZONE 'UTC')
      AND order_ts < ((p_aggregate_date + 1)::timestamp AT TIME ZONE 'UTC');

    IF v_records_read <> v_order_count THEN
        SELECT order_id
        INTO v_repeated_order_id
        FROM orders_demo.raw_order_events
        WHERE order_ts >= (p_aggregate_date::timestamp AT TIME ZONE 'UTC')
          AND order_ts < ((p_aggregate_date + 1)::timestamp AT TIME ZONE 'UTC')
        GROUP BY order_id
        HAVING count(*) > 1
        ORDER BY order_id
        LIMIT 1;

        RAISE EXCEPTION 'raw input contains multiple records for order_id %',
            v_repeated_order_id;
    END IF;

    v_published_at := clock_timestamp();

    INSERT INTO orders_demo.daily_order_aggregates (
        aggregate_date,
        order_count,
        sales_amount,
        published_at,
        source_max_updated_at
    )
    VALUES (
        p_aggregate_date,
        v_order_count,
        v_sales_amount,
        v_published_at,
        v_source_max_updated_at
    )
    ON CONFLICT (aggregate_date) DO UPDATE
    SET
        order_count = EXCLUDED.order_count,
        sales_amount = EXCLUDED.sales_amount,
        published_at = EXCLUDED.published_at,
        source_max_updated_at = EXCLUDED.source_max_updated_at;

    RETURN QUERY
    SELECT
        v_records_read,
        v_order_count,
        v_sales_amount,
        v_published_at,
        v_source_max_updated_at;
END;
$$;

COMMENT ON FUNCTION orders_demo.publish_daily_order_aggregate(date) IS
    'Calculates and atomically upserts one UTC day. Version resolution is added separately.';

COMMIT;
