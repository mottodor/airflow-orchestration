#!/usr/bin/env bash
set -Eeuo pipefail

psql --set=ON_ERROR_STOP=1 \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_DB}" \
    <<'SQL'
CREATE DATABASE orders_demo OWNER airflow;
SQL

psql --set=ON_ERROR_STOP=1 \
    --username "${POSTGRES_USER}" \
    --dbname orders_demo \
    --file /project-sql/001_create_orders_schema.sql
