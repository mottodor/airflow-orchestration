# Airflow orchestration and safe backfills

A local Apache Airflow 3.1.6 learning project that demonstrates safe retries, backfills, late-data recalculation, timeouts, and concurrent execution.

The example workflow ingests versioned orders and publishes one daily aggregate with the order count and total sales amount for each UTC date.

> [!WARNING]
> This Docker Compose environment is intended for local development and learning only. It is not a production deployment.

## Architecture

```text
orders_ingestion
  -> raw_order_events Asset
  -> orders_late_data_dispatcher
  -> orders_daily_aggregate
  -> daily_order_aggregates
```

- `orders_ingestion` stores idempotent, append-only order changes and publishes an Airflow Asset event.
- `orders_late_data_dispatcher` groups Asset events by affected date and starts targeted recalculations.
- `orders_daily_aggregate` runs on a schedule, manually, or through backfill and atomically publishes one aggregate per date.

Manual, scheduled, backfill, and Asset-triggered workflows are implemented. The automated test suites cover acceptance criteria AC-01 through AC-13.

## Prerequisites

- Docker with Docker Compose;
- at least 4 GiB of memory available to Docker;
- port `8080` available, or a different `AIRFLOW_API_PORT` configured in `.env`.

## Quick start

Create the local environment file:

```bash
cp .env.example .env
```

On Linux or WSL, set `AIRFLOW_UID` in `.env` to the output of `id -u`.

Build and start the stack:

```bash
docker compose config --quiet
docker compose build
docker compose up airflow-init
docker compose up -d --wait --wait-timeout 300
docker compose ps
```

Open <http://localhost:8080> and sign in with the default local credentials:

```text
Username: airflow
Password: airflow
```

If `AIRFLOW_API_PORT` was changed, use `http://localhost:<AIRFLOW_API_PORT>` instead.

## Run the example

Ready-to-use configurations for the initial load and a late change are available in [`data/`](data/).

All DAGs are paused when first created. Unpause the project DAGs:

```bash
docker compose run --rm airflow-cli airflow dags unpause orders_ingestion
docker compose run --rm airflow-cli airflow dags unpause orders_late_data_dispatcher
docker compose run --rm airflow-cli airflow dags unpause orders_daily_aggregate
```

Alternatively, open the **Dags** page in the Airflow UI and switch on the toggle next to each of these three DAGs.

Trigger ingestion with one late order:

```bash
docker compose run --rm airflow-cli airflow dags trigger orders_ingestion \
  --run-id example-late-order-2026-10-03 \
  --conf '{
    "change_id": "late-order-c-2026-10-03",
    "reason": "late_order",
    "events": [{
      "order_id": "C",
      "order_ts": "2026-10-01T15:00:00Z",
      "amount": 30.00,
      "updated_at": "2026-10-03T09:00:00Z",
      "ingested_at": "2026-10-03T09:01:00Z"
    }]
  }'
```

To trigger the same run through the Airflow UI:

1. Open **Dags** and select `orders_ingestion`.
2. Click **Trigger**, then select **Single Run**.
3. Expand **Advanced Config** and paste the JSON object from `--conf` above into the DAG run configuration field.
4. Optionally set the run ID to `example-late-order-2026-10-03`, then confirm the trigger.

This starts the complete workflow: ingestion publishes an Asset event, the dispatcher creates a targeted run, and the daily DAG recalculates `2026-10-01`.

Use the Airflow UI to inspect DAG runs, task states, logs, and the successful task's XCom value.

To run the daily aggregate directly:

```bash
docker compose run --rm airflow-cli airflow dags trigger orders_daily_aggregate \
  --run-id example-daily-2026-10-01 \
  --conf '{"affected_date": "2026-10-01"}'
```

In the UI, open `orders_daily_aggregate`, click **Trigger** → **Single Run**, and enter `{"affected_date": "2026-10-01"}` in **Advanced Config**.

## Safety guarantees

- Ingestion is idempotent by `change_id`; conflicting reuse of an ID fails.
- Exact order-version duplicates are counted once, while conflicting versions fail before publication.
- Aggregate calculation and publication run in one PostgreSQL transaction.
- Failures, retries, and timeouts preserve the previously published aggregate.
- PostgreSQL advisory locks serialize calculations for the same date.
- Different dates can run in parallel.
- The `orders_compute` Airflow pool allows at most two concurrent compute tasks.
- Successful runs return the structured JSON log summary as the XCom `return_value`.

The complete behavioral contract, including UTC boundaries, retry policy, conflict rules, and late-data semantics, is defined in [`docs/spec.md`](docs/spec.md).

## Tests

Detailed commands for environment checks, schema validation, unit tests, PostgreSQL integration tests, the Airflow-level acceptance suite, and test-only fault injection are in [`docs/testing.md`](docs/testing.md).

The acceptance suite uses the real scheduler and Celery worker, Airflow API v2, Asset events, task logs, XCom, and both PostgreSQL databases.

## Documentation

- [Behavioral specification](docs/spec.md)
- [Testing guide](docs/testing.md)
- [Current implementation status and verification evidence](docs/status.md)
- [Historical implementation plan](docs/bootstrap.md)

## Stop the environment

Stop the services while preserving the PostgreSQL volume:

```bash
docker compose down
```

To explicitly remove all local database data:

```bash
docker compose down -v
```

The normal shutdown command does not remove the volume because it may contain user-created events and aggregate results.
