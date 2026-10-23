# Статус реализации: Airflow orchestration и безопасные backfills

- **Нормативный контракт:** [`docs/spec.md`](spec.md)
- **Исторический план:** [`docs/bootstrap.md`](bootstrap.md)
- **Проверенный baseline реализации:** `3e0a519`
- **Исправление D-01:** текущий review-ready diff, commit ещё не создан
- **Дата проверки:** 2026-10-23
- **Общий статус:** `D-01_RESOLVED_OPEN_GAPS`

Этот документ фиксирует изменяемое состояние реализации: фактически выполненные проверки, известные отклонения от утверждённой спецификации, пробелы в acceptance coverage и следующие действия. Он не изменяет требования из `docs/spec.md`.

## 1. Краткий итог

Локальная среда, схема данных, ingestion, Asset-triggered dispatcher, ручной и scheduled дневной расчёт, двухдневный backfill, транзакционная публикация, advisory lock, timeout и структурированная сводка реализованы и в основном подтверждены тестами.

Блокирующий дефект D-01 исправлен и подтверждён реальными scheduled/backfill runs. Первый срез всё ещё имеет открытые gaps: требуется сделать schema integration test повторяемым на persistent environment и закрыть несколько Airflow-level acceptance-сценариев.

## 2. Статус компонентов

| Компонент | Статус | Комментарий |
|---|---|---|
| Version-pinned Docker Compose | PASS | Airflow 3.1.6, PostgreSQL 16, Redis 7.2 |
| Airflow services и health checks | PASS | Все обязательные сервисы запускались в healthy-состоянии |
| SQL schema | PASS | Идемпотентное применение подтверждено |
| Append-only ingestion DAG | PASS | Валидация, PostgreSQL insert и Asset metadata работают |
| Late-data dispatcher | PASS | Дедупликация дат и целевой `TriggerDagRunOperator` работают |
| Daily aggregate: manual/late-data | PASS | Реальный целевой запуск успешно публикует агрегат |
| Daily aggregate: scheduled/backfill | PASS | Pendulum regression test, реальный scheduled run и двухдневный backfill |
| Latest-version/dedup/conflict SQL | PASS | Подтверждено SQL integration tests |
| Advisory lock и rollback | PASS | Подтверждено PostgreSQL concurrency tests |
| Pool `orders_compute` | PARTIAL | Настройка подтверждена; Airflow-level сценарий трёх задач не автоматизирован |
| Execution timeout | PASS | Реальный timeout сохраняет старый результат |
| Structured JSON summary | PASS | Success и failure/timeout paths подтверждены логами и unit tests |
| Полный AC-01—AC-13 suite | PARTIAL | См. матрицу acceptance criteria |
| CI | NOT_IMPLEMENTED | Не входил в минимальный bootstrap scope |

## 3. Проверенная среда

Зафиксированные версии runtime:

- Apache Airflow `3.1.6`;
- Python `3.12` в Airflow image;
- `apache-airflow-providers-postgres==6.5.1`;
- `apache-airflow-providers-standard==1.10.2`;
- `pytest==8.4.2`;
- PostgreSQL `16`;
- Redis `7.2-bookworm`.

Результаты проверки среды:

| Проверка | Результат |
|---|---|
| `docker compose config --quiet` | PASS |
| Сборка `airflow-orchestration:3.1.6` | PASS |
| `airflow-init` и metadata migrations | PASS |
| Health checks полного Compose stack | PASS |
| `airflow dags list-import-errors` | PASS — import errors отсутствуют |
| Connection `orders_db` | PASS |
| Pool `orders_compute`, 2 слота | PASS |

Предупреждения Airflow/FAB о deprecated API и in-memory rate-limit storage наблюдаются, но для локального учебного окружения не блокируют первый срез.

Host LSP не разрешает импорты Airflow и `psycopg2`, потому что зависимости установлены только внутри project Docker image. Это ограничение локальной IDE-диагностики, а не runtime import error.

## 4. Автоматизированные и экспериментальные проверки

### 4.1 Выполненные тесты

| Набор | Результат |
|---|---|
| `tests/unit` | PASS — 40 tests |
| `tests/integration/test_daily_aggregate.sql` | PASS |
| `tests/integration/test_daily_aggregate_concurrency.py` | PASS — 3 tests |
| `tests/integration/test_schema.sql` на чистой временной БД | PASS |
| Повторное применение `sql/001_create_orders_schema.sql` | PASS |

`tests/integration/test_schema.sql` на заполненной persistent `orders_demo` завершается ошибкой из-за D-02; это не ошибка самой схемы.

### 4.2 Реальный Asset E2E

Проверен сквозной путь:

```text
orders_ingestion
  -> Asset event raw_order_events
  -> orders_late_data_dispatcher
  -> TriggerDagRunOperator
  -> orders_daily_aggregate
  -> daily_order_aggregates
```

Контрольный запуск для даты `2098-08-08` завершился успешно:

- ingestion run: success;
- dispatcher run: success;
- target daily run: success;
- Asset metadata содержит `affected_date`, `reason` и `change_id`;
- опубликован `order_count=1`, `sales_amount=42.50`;
- JSON-сводка содержит обязательные поля и `publication_state=published`.

Локальный persistent volume содержит данные этого probe с префиксом `status-1792766222`. Их удаление не выполнялось, поскольку raw table намеренно append-only, а удаление project volume требует отдельного решения.

### 4.3 Timeout probe

Реальный Celery task был остановлен по `execution_timeout=10 секунд`. Подтверждено:

- обе разрешённые попытки завершились timeout;
- транзакция не опубликовала частичный результат;
- старый агрегат сохранился;
- JSON-сводка содержит `publication_state=old_result_preserved` и причину ошибки;
- lock освобождается после rollback/cancellation, что дополнительно подтверждено PostgreSQL integration test.

## 5. Результаты bootstrap probes

| Probe | Статус | Evidence |
|---|---|---|
| P-01 — Compose и базовые сервисы | PASS | Init, migrations и полный healthy stack |
| P-02 — Asset metadata и dispatcher | PASS | Unit tests, Asset metadata и реальный E2E target run |
| P-03 — timeout, rollback и lock release | PASS | Реальный timeout и PostgreSQL cancellation test |
| P-04 — failure summary | PASS | Success, validation failure, retry и timeout summary paths проверены |

## 6. Известные дефекты

### D-01 — Scheduled data interval отвергается

- **Severity:** BLOCKER
- **Статус:** RESOLVED
- **Компонент:** `dags/orders_daily_aggregate.py`, `tests/unit/test_orders_daily_aggregate.py`
- **Затронутые критерии:** AC-01, AC-03 и обязательный backfill path
- **Commit:** не создан; исправление находится в текущем review-ready diff

#### Историческое поведение и причина

Airflow передавал границы data interval как `pendulum.DateTime`, а реализация сравнивала результат Pendulum-арифметики со значением, использующим stdlib timezone. Корректный дневной интервал ошибочно отвергался с сообщением:

```text
ValueError: the Airflow data interval must be one complete UTC day
```

#### Исправление

Обе границы теперь преобразуются в стандартный timezone-aware `datetime` в UTC через их абсолютный timestamp. После нормализации проверяются UTC-полночь начала и точное окончание в полночь следующей календарной даты. Добавлен отдельный regression test с реальными `pendulum.DateTime`; существующие отрицательные тесты по-прежнему отвергают неполный и многодневный интервалы.

#### Проверки

- regression probe на старой реализации воспроизвёл `ValueError` для корректного Pendulum-интервала;
- targeted regression и negative tests: PASS — 4 tests;
- весь `tests/unit`: PASS — 40 tests;
- `airflow dags list-import-errors`: PASS — ошибок нет;
- реальный scheduled run `scheduled__2026-10-20T00:00:00+00:00`: PASS, опубликован независимый эталон `order_count=2`, `sales_amount=170.00`;
- `airflow backfill create` для `2026-10-18`—`2026-10-19`, `max_active_runs=2`: PASS, оба run завершены успешно и опубликовали по одной корректной строке;
- в финальных attempt logs этих трёх запусков отсутствуют `complete UTC day`, `ValueError` и traceback.

### D-02 — Schema integration test зависит от пустой БД

- **Severity:** MEDIUM
- **Компонент:** `tests/integration/test_schema.sql:69`

#### Наблюдаемое поведение

Тест проходит на чистой БД, но падает на заполненной persistent `orders_demo`, потому что проверяет общее число строк `raw_order_events`:

```sql
IF (SELECT count(*) FROM orders_demo.raw_order_events) <> 4 THEN
```

Raw table append-only, поэтому ранее созданные строки нельзя штатно удалить перед повторным запуском теста.

#### Требуемое исправление

Предпочтительный вариант:

- использовать уникальный префикс fixture;
- считать и проверять только строки этого fixture;
- сохранить обёртку `BEGIN ... ROLLBACK`.

Альтернативный вариант — создавать отдельную временную test database и явно документировать этот интерфейс.

#### Definition of Done

Одна и та же документированная команда schema integration test успешно выполняется два раза подряд без удаления PostgreSQL volume и пользовательских данных.

## 7. Verification gaps

### V-01 — Не все критерии проверяются на уровне Airflow

- **Тип:** Verification gap
- **Затронутые критерии:** AC-02, AC-07, AC-08, AC-09, AC-10, AC-12

Существующие unit и PostgreSQL integration tests подтверждают основные функции, но не полностью воспроизводят:

- fail-first Airflow task с последующим успешным retry в том же run;
- failed late-data target run со старым опубликованным агрегатом;
- два конкурирующих Airflow runs одной даты, где второй после lock читает новый заказ;
- перекрывающиеся Airflow runs разных дат;
- три compute tasks при pool ёмкостью 2 с наблюдением queued/running states;
- чтение и сравнение фактического success XCom через Airflow metadata/API.

#### Definition of Done

Добавлен автоматизированный acceptance/e2e слой, который проверяет task/run states, timestamps, Asset metadata, XCom и независимо вычисленные SQL-результаты.

## 8. Принятый технический риск

### R-01 — Commit ingestion и Asset event не атомарны

- **Тип:** Reliability risk
- **Статус:** OPEN, не блокирует текущий учебный срез

`orders_ingestion` коммитит события во внешнюю PostgreSQL до завершения task и регистрации Asset event. Сбой worker после commit может оставить входные данные без события пересчёта. Ingestion task также не имеет собственного retry.

Возможные дальнейшие меры:

- настроить ограниченный retry ingestion;
- сделать `change_id` идемпотентным на уровне хранения;
- добавить outbox table;
- добавить reconciliation DAG, находящий даты с непересчитанными изменениями.

Решение о реализации этих мер должно учитывать учебный scope и не требуется для закрытия D-01/D-02.

## 9. Матрица acceptance criteria

| AC | Статус | Evidence / ограничение |
|---|---|---|
| AC-01 | PASS | Реальный scheduled run на независимом fixture R1–R4 опубликовал ровно `2 / 170.00` |
| AC-02 | PARTIAL | rollback и сохранение старого результата подтверждены; нужен единый fail-then-success Airflow retry scenario |
| AC-03 | PASS | SQL-идемпотентность подтверждена ранее; реальный двухдневный Airflow backfill завершён успешно, по одной активной строке на дату |
| AC-04 | PASS | Latest version и exact duplicate проверены |
| AC-05 | PASS | Конфликт блокирует публикацию и сохраняет старый результат |
| AC-06 | PASS | Реальный ingestion -> Asset -> dispatcher -> target run выполнен |
| AC-07 | PARTIAL | Общий failure/rollback path работает; нужен failed late-data E2E |
| AC-08 | PARTIAL | PostgreSQL serialization проверена; полный Airflow scenario не автоматизирован |
| AC-09 | PARTIAL | PostgreSQL parallelism разных дат проверен; Airflow-level evidence отсутствует |
| AC-10 | PARTIAL | JSON logs и task return проверены; фактический XCom lookup не автоматизирован |
| AC-11 | PASS | UTC boundary fixture проверен SQL integration test |
| AC-12 | PARTIAL | Pool создан и task использует слот; сценарий трёх Airflow tasks не автоматизирован |
| AC-13 | PASS | Реальный timeout сохраняет старый агрегат и пишет JSON error summary |

## 10. Приоритет следующих изменений

1. Исправить D-02 и сделать документированную schema test command повторяемой.
2. Добавить минимальный Airflow-level acceptance suite для V-01.
3. После подтверждения всех обязательных режимов обновить AC-матрицу и общий статус.
4. Отдельным решением очистить или пересоздать локальную metadata DB, если для демонстрации нужна история без старых failed runs.

## 11. Правила обновления документа

- `docs/spec.md` меняется только при изменении утверждённого поведения или scope.
- `docs/bootstrap.md` сохраняется как исторический план и не переписывается под фактическую реализацию.
- После исправления дефект не удаляется: его статус меняется на `RESOLVED`, добавляются commit и выполненные проверки.
- Числа запусков и состояние локального volume являются evidence конкретной проверки, а не переносимым свойством проекта.
- Общий статус становится `READY_FOR_ACCEPTANCE`, когда нет blocker-дефектов и обязательные AC имеют достаточное runtime evidence.
