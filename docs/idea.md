# Airflow orchestration и безопасные backfills

## Назначение

Независимый учебный проект для изучения Apache Airflow как orchestration system. Он использует небольшие Python/SQL workloads и не требует полного lakehouse. Основной акцент — scheduling semantics, retries, timeouts, pools, datasets, backfill, idempotency и failure isolation.

## Главный учебный вопрос

> Как спроектировать DAG так, чтобы повторные запуски, backfills, частичные сбои и конкуренция не повреждали данные и оставались понятными оператору?
