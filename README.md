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

## Остановка

```bash
docker compose down
```

Команда сохраняет named volume PostgreSQL. Удаление volume с локальными данными не входит в обычную остановку и не должно выполняться автоматически.
