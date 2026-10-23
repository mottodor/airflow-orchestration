# Airflow orchestration and safe backfills

Учебный локальный проект на Apache Airflow 3.1.6. Контракт поведения находится в [`docs/spec.md`](docs/spec.md), план реализации — в [`docs/bootstrap.md`](docs/bootstrap.md).

> Docker Compose предназначен только для локальной разработки и обучения, не для production.

## Требования

- Docker с Compose;
- не менее 4 GiB памяти для Docker;
- свободный порт `8080` либо другой `AIRFLOW_API_PORT` в `.env`.

## Первый запуск

```bash
cp .env.example .env
# На Linux/WSL замените AIRFLOW_UID в .env результатом команды: id -u

docker compose config --quiet
docker compose build
docker compose up airflow-init
docker compose up -d --wait --wait-timeout 300
```

Airflow UI по умолчанию: <http://localhost:8080>. Если `AIRFLOW_API_PORT` изменён, используйте `http://localhost:<AIRFLOW_API_PORT>`. Локальные учётные данные по умолчанию: `airflow` / `airflow`.

## Проверка среды и схемы

```bash
docker compose ps
docker compose run --rm airflow-cli airflow version
docker compose run --rm airflow-cli airflow connections get orders_db
docker compose run --rm airflow-cli python -c \
  'import airflow, pytest; from importlib.metadata import version; assert airflow.__version__ == "3.1.6"; assert pytest.__version__ == "8.4.2"; assert version("apache-airflow-providers-postgres") == "6.5.1"; print("image versions: ok")'

docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < sql/001_create_orders_schema.sql

docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < tests/integration/test_schema.sql
```

Повторное применение схемы намеренно проверяется: миграция должна оставаться идемпотентной.

## Приём изменений заказов

DAG `orders_ingestion` запускается вручную и принимает непустой `change_id`, причину изменения и массив append-only версий заказов. Все события одного запуска должны относиться ровно к одной календарной UTC-дате. После успешной записи DAG публикует Asset event таблицы `orders_demo.raw_order_events` с metadata `affected_date`, `reason` и `change_id`.

Пример запуска:

```bash
docker compose run --rm airflow-cli airflow dags unpause orders_ingestion

docker compose run --rm airflow-cli airflow dags trigger orders_ingestion \
  --run-id example-late-order-2025-03-03 \
  --conf '{
    "change_id": "late-order-c-2025-03-03",
    "reason": "late_order",
    "events": [{
      "order_id": "C",
      "order_ts": "2025-03-01T15:00:00Z",
      "amount": 30.00,
      "updated_at": "2025-03-03T09:00:00Z",
      "ingested_at": "2025-03-03T09:01:00Z"
    }]
  }'
```

## Дневной агрегат

DAG `orders_daily_aggregate` работает по дневному UTC-расписанию. Scheduled/backfill run берёт дату из своего data interval; для ручного целевого запуска требуется параметр `affected_date` в формате `YYYY-MM-DD`. Расчёт и `UPSERT` одной активной строки даты выполняются в одной PostgreSQL-транзакции.

```bash
docker compose run --rm airflow-cli airflow dags unpause orders_daily_aggregate

docker compose run --rm airflow-cli airflow dags trigger orders_daily_aggregate \
  --run-id example-daily-2025-03-01 \
  --conf '{"affected_date": "2025-03-01"}'
```

На этапе IMP-03 дата с несколькими raw-записями одного `order_id` отклоняется до публикации, чтобы не публиковать неоднозначный результат. Выбор последней версии, игнорирование точных копий и точная классификация конфликтов входят в следующий отдельный шаг IMP-04.

Локальные unit tests DAG:

```bash
docker run --rm --entrypoint pytest \
  -v "$PWD/dags:/opt/airflow/dags:ro" \
  -v "$PWD/tests:/opt/airflow/tests:ro" \
  -w /opt/airflow \
  airflow-orchestration:3.1.6 \
  -q tests/unit
```

После применения схемы integration-проверка дневного расчёта запускается так:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < tests/integration/test_daily_aggregate.sql
```

## Остановка

```bash
docker compose down
```

Команда сохраняет named volume PostgreSQL. Удаление volume с локальными данными не входит в обычную остановку и не должно выполняться автоматически.
