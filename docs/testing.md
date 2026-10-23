# Testing guide

This guide contains the detailed verification commands intentionally omitted from the project README. Run all commands from the repository root.

## Test levels

| Level | Purpose |
| --- | --- |
| Environment checks | Verify containers, Airflow, connections, and pinned package versions |
| Schema test | Verify idempotent schema application and database constraints |
| Unit tests | Validate DAG helpers, configuration rules, and failure summaries |
| PostgreSQL integration tests | Validate ingestion, aggregation, transactions, locks, and concurrency |
| Airflow acceptance suite | Validate the end-to-end behavior with real Airflow services |

The current results and acceptance evidence are tracked in [`status.md`](status.md).

## Environment checks

Start the stack as described in the [README](../README.md), then run:

```bash
docker compose ps
docker compose run --rm airflow-cli airflow version
docker compose run --rm airflow-cli airflow connections get orders_db
```

Verify the pinned versions inside the project image:

```bash
docker compose run --rm airflow-cli python -c \
  'import airflow, pytest; from importlib.metadata import version; assert airflow.__version__ == "3.1.6"; assert pytest.__version__ == "8.4.2"; assert version("apache-airflow-providers-postgres") == "6.5.1"; print("image versions: ok")'
```

## Schema validation

Reapply the schema and run the schema integration test:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < sql/001_create_orders_schema.sql

docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < tests/integration/test_schema.sql
```

Reapplying the schema is intentional: the migration must remain idempotent.

The schema test uses a uniquely prefixed fixture, selects unused aggregate dates, and rolls back its transaction. It can be repeated against a populated persistent `orders_demo` database without deleting existing data.

## Unit tests

Run the DAG unit tests inside the project image:

```bash
docker run --rm --entrypoint pytest \
  -v "$PWD/dags:/opt/airflow/dags:ro" \
  -v "$PWD/tests:/opt/airflow/tests:ro" \
  -w /opt/airflow \
  airflow-orchestration:3.1.6 \
  -q tests/unit
```

## PostgreSQL integration tests

Apply the schema before running these tests.

Run the SQL aggregate test:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < tests/integration/test_daily_aggregate.sql
```

Run the Python ingestion and concurrency tests:

```bash
docker run --rm --network airflow-orchestration_default \
  -e ORDERS_TEST_DSN=postgresql://airflow:airflow@postgres:5432/orders_demo \
  -v "$PWD/dags:/opt/airflow/dags:ro" \
  -v "$PWD/tests:/opt/airflow/tests:ro" \
  -w /opt/airflow \
  --entrypoint pytest \
  airflow-orchestration:3.1.6 \
  -q tests/integration/test_orders_ingestion.py \
     tests/integration/test_daily_aggregate_concurrency.py
```

These tests cover:

- ingestion idempotency and immutable `change_id` receipts;
- latest-version selection and exact-duplicate handling;
- conflict detection before publication;
- transactional aggregate replacement and rollback;
- same-date advisory-lock serialization;
- parallel database access for different dates;
- lock release after cancellation or failure.

## Test-only fault injection

The project includes controlled fault-injection parameters for acceptance, retry, timeout, concurrency, and recovery scenarios.

| Parameter | DAG | Behavior |
| --- | --- | --- |
| `demo_delay_seconds` | `orders_daily_aggregate` | Holds the transaction before publication |
| `demo_fail_first_attempt` | `orders_daily_aggregate` | Fails the first attempt after reading the old aggregate but before publication |
| `demo_fail_after_commit_once` | `orders_ingestion` | Simulates a failure after the PostgreSQL commit but before Asset registration |

These parameters are rejected by default. Enable them only for explicit test runs:

```bash
DEMO_FAULT_INJECTION_ENABLED=true \
  docker compose up -d --wait --wait-timeout 300
```

Return to normal mode using the value from `.env`, which defaults to `false`:

```bash
docker compose up -d --wait --wait-timeout 300
```

## Airflow-level acceptance suite

The V-01 suite uses:

- the real Airflow scheduler and Celery worker;
- Airflow API v2;
- Asset events and the late-data dispatcher;
- task states, timestamps, logs, and XCom;
- the Airflow metadata and `orders_demo` PostgreSQL databases.

Start the services with test-only fault injection enabled, then run:

```bash
DEMO_FAULT_INJECTION_ENABLED=true \
  docker compose up -d --wait --wait-timeout 300

docker run --rm \
  --network airflow-orchestration_default \
  -e AIRFLOW_API_BASE_URL=http://airflow-apiserver:8080 \
  -e AIRFLOW_API_USERNAME=airflow \
  -e AIRFLOW_API_PASSWORD=airflow \
  -e ORDERS_TEST_DSN=postgresql://airflow:airflow@postgres:5432/orders_demo \
  -v "$PWD/tests:/opt/airflow/tests:ro" \
  -w /opt/airflow \
  --entrypoint pytest \
  airflow-orchestration:3.1.6 \
  -q tests/acceptance/test_airflow_v01.py

# Return to normal mode using the value from .env.
docker compose up -d --wait --wait-timeout 300
```

Together with the SQL, unit, and PostgreSQL integration tests, the suite covers acceptance criteria AC-01 through AC-13, including:

- the canonical R1-R4 fixture;
- repeated calculation and idempotent publication;
- UTC date boundaries;
- successful and conflicting late-data recalculations;
- retry and real execution timeout behavior;
- ingestion recovery after a failure between commit and Asset registration;
- same-date serialization and different-date parallelism;
- enforcement of the two-slot `orders_compute` pool;
- validation of the actual successful-task XCom value.

In Airflow 3.1.6, a task waiting for a pool slot may appear as either `scheduled` or `queued`. The acceptance suite verifies the capacity limit using task states, timestamps, and the pool API.

## Persistent test data

The acceptance suite preserves the original paused state of each DAG. It selects unused dates in the range 2200-2299 and unique `order_id` values.

Raw test events are intentionally not deleted from the append-only table. The suite does not clear the existing PostgreSQL volume or remove user-created data.
