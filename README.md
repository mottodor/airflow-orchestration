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

DAG `orders_ingestion` запускается вручную и принимает непустой `change_id`, причину изменения и массив append-only версий заказов. Все события одного запуска должны относиться ровно к одной календарной UTC-дате. После успешной записи DAG публикует Asset event таблицы `orders_demo.raw_order_events` с metadata `affected_date`, `reason` и `change_id`. Asset-triggered DAG `orders_late_data_dispatcher` группирует накопленные события по `affected_date` и создаёт по одному целевому запуску `orders_daily_aggregate` на уникальную дату.

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

## Диспетчер поздних данных

`orders_late_data_dispatcher` использует официальный `TriggerDagRunOperator`. Для каждой уникальной даты в одном Asset-triggered запуске он формирует конфигурацию с `affected_date`, причиной `late_data` и исходными `change_id`/причинами. Повторы одной даты в наборе triggering events схлопываются.

Run ID целевого запуска имеет вид `late_data__YYYY-MM-DD__<source-token>`, где `source-token` детерминированно вычисляется из run ID диспетчера. Поэтому retry диспетчера не создаёт второй логический запуск (`skip_when_already_exists=True`), а новое независимое обновление той же даты может создать новый пересчёт.

Перед локальным экспериментом снимите паузу со всех трёх DAG:

```bash
docker compose run --rm airflow-cli airflow dags unpause orders_ingestion
docker compose run --rm airflow-cli airflow dags unpause orders_late_data_dispatcher
docker compose run --rm airflow-cli airflow dags unpause orders_daily_aggregate
```

## Дневной агрегат

DAG `orders_daily_aggregate` работает по дневному UTC-расписанию. Scheduled/backfill run берёт дату из своего data interval; для ручного целевого запуска требуется параметр `affected_date` в формате `YYYY-MM-DD`. Расчёт и `UPSERT` одной активной строки даты выполняются в одной PostgreSQL-транзакции.

```bash
docker compose run --rm airflow-cli airflow dags unpause orders_daily_aggregate

docker compose run --rm airflow-cli airflow dags trigger orders_daily_aggregate \
  --run-id example-daily-2025-03-01 \
  --conf '{"affected_date": "2025-03-01"}'
```

Расчёт выбирает для каждого `order_id` версию с максимальным `updated_at`, а точные бизнес-копии (различающиеся только `ingested_at`) учитывает один раз. Разные записи с одинаковыми `order_id` и `updated_at`, а также попытка изменить `order_ts` устойчивого заказа, завершают расчёт ошибкой до публикации.

`airflow-init` создаёт pool `orders_compute` на 2 слота. Задача расчёта занимает 1 слот, имеет `execution_timeout=10` секунд, один retry через 5 секунд и берёт transaction-level PostgreSQL advisory lock по дате до чтения входа. Поэтому одна дата сериализуется, а разные даты могут вычисляться параллельно.

Каждая попытка расчёта пишет одной JSON-записью операторскую сводку: дату и причину запуска, число прочитанных и выбранных заказов, старый и новый агрегаты, состояние публикации, ожидание блокировки, номер попытки и ошибку. Успешная сводка возвращается задачей как XCom `return_value`; при ошибке, включая timeout, JSON записывается до повторного возбуждения исключения, а старый агрегат остаётся опубликованным.

Параметр `demo_delay_seconds` предназначен только для локальных concurrency/timeout probes. По умолчанию он отвергается; для явно тестового запуска stack нужно стартовать с `DEMO_FAULT_INJECTION_ENABLED=true`.

Локальные unit tests DAG:

```bash
docker run --rm --entrypoint pytest \
  -v "$PWD/dags:/opt/airflow/dags:ro" \
  -v "$PWD/tests:/opt/airflow/tests:ro" \
  -w /opt/airflow \
  airflow-orchestration:3.1.6 \
  -q tests/unit
```

После применения схемы integration-проверки дневного расчёта запускаются так:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U airflow -d orders_demo \
  < tests/integration/test_daily_aggregate.sql

docker run --rm --network airflow-orchestration_default \
  -e ORDERS_TEST_DSN=postgresql://airflow:airflow@postgres:5432/orders_demo \
  -v "$PWD/tests:/opt/airflow/tests:ro" \
  -w /opt/airflow \
  --entrypoint pytest \
  airflow-orchestration:3.1.6 \
  -q tests/integration/test_daily_aggregate_concurrency.py
```

## Остановка

```bash
docker compose down
```

Команда сохраняет named volume PostgreSQL. Удаление volume с локальными данными не входит в обычную остановку и не должно выполняться автоматически.
