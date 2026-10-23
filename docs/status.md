# Статус реализации: Airflow orchestration и безопасные backfills

- **Нормативный контракт:** [`docs/spec.md`](spec.md)
- **Исторический план:** [`docs/bootstrap.md`](bootstrap.md)
- **Текущий проверенный baseline:** `bc93814`
- **V-01:** закрыт commit `8b8da7d`
- **IMP-08:** закрыт commit `bc93814`
- **Дата актуализации:** 2026-10-23
- **Общий статус:** `READY_FOR_ACCEPTANCE`

Этот документ фиксирует изменяемое состояние реализации: фактически выполненные проверки, известные отклонения от утверждённой спецификации, пробелы в acceptance coverage и следующие действия. Он не изменяет требования из `docs/spec.md`.

## 1. Краткий итог

Локальная среда, схема данных, ingestion, Asset-triggered dispatcher, ручной и scheduled дневной расчёт, двухдневный backfill, транзакционная публикация, advisory lock, timeout и структурированная сводка реализованы и подтверждены тестами.

D-01 и D-02 исправлены. Airflow-level acceptance suite закрыл V-01 для fail-first retry, ошибочного late-data target run, сериализации одной даты, параллельности разных дат, ограничения pool и чтения фактического success XCom. Пользователь явно уточнил AC-12: для ожидающей pool slot задачи разрешены Airflow-specific состояния `scheduled` или `queued` при доказанном лимите ёмкости.

## 2. Статус компонентов

| Компонент | Статус | Комментарий |
|---|---|---|
| Version-pinned Docker Compose | PASS | Airflow 3.1.6, PostgreSQL 16, Redis 7.2 |
| Airflow services и health checks | PASS | Все обязательные сервисы запускались в healthy-состоянии |
| SQL schema | PASS | Идемпотентное применение и повторяемый integration test на persistent БД подтверждены |
| Append-only ingestion DAG | PASS | Валидация, PostgreSQL insert и Asset metadata работают |
| Late-data dispatcher | PASS | Дедупликация дат и целевой `TriggerDagRunOperator` работают |
| Daily aggregate: manual/late-data | PASS | Реальный целевой запуск успешно публикует агрегат |
| Daily aggregate: scheduled/backfill | PASS | Pendulum regression test, реальный scheduled run и двухдневный backfill |
| Latest-version/dedup/conflict SQL | PASS | Подтверждено SQL integration tests |
| Advisory lock и rollback | PASS | Подтверждено PostgreSQL concurrency tests |
| Pool `orders_compute` | PASS | Максимум 2 занятых слота, состояние ожидания `scheduled` и отложенный старт третьей задачи подтверждены Airflow API |
| Execution timeout | PASS | Реальный timeout сохраняет старый результат |
| Structured JSON summary | PASS | Success и failure/timeout paths подтверждены логами и unit tests |
| Полный AC-01—AC-13 suite | PASS | SQL/unit/runtime evidence и Airflow-level V-01 suite закрывают матрицу acceptance criteria |
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
| `tests/unit` | PASS — 47 tests |
| `tests/acceptance/test_airflow_v01.py` | PASS — 7 Airflow-level scenarios |
| `tests/integration/test_daily_aggregate.sql` | PASS |
| `tests/integration/test_daily_aggregate_concurrency.py` | PASS — 3 tests |
| `tests/integration/test_schema.sql` на чистой временной БД | PASS |
| `tests/integration/test_schema.sql` два раза подряд на заполненной persistent `orders_demo` | PASS |
| Повторное применение `sql/001_create_orders_schema.sql` | PASS |

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
- **Commit:** `3e564c6`

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
- **Статус:** RESOLVED
- **Компонент:** `tests/integration/test_schema.sql`, `README.md`
- **Commit:** `d1bc900`

#### Наблюдаемое поведение

Тест проходит на чистой БД, но падает на заполненной persistent `orders_demo`, потому что проверяет общее число строк `raw_order_events`:

```sql
IF (SELECT count(*) FROM orders_demo.raw_order_events) <> 4 THEN
```

Raw table append-only, поэтому ранее созданные строки нельзя штатно удалить перед повторным запуском теста.

#### Исправление

Тест создаёт транзакционный fixture с уникальным префиксом `order_id`, проверяет только принадлежащие ему append-only события и динамически выбирает две незанятые даты для строк агрегатов. Обёртка `BEGIN ... ROLLBACK` сохранена, поэтому fixture не остаётся в persistent БД и не требует удаления пользовательских данных.

#### Проверки

- прежняя реализация воспроизвела D-02 на заполненной `orders_demo`: ожидаемый `raw table did not preserve all four append-only events`;
- одна и та же документированная команда schema integration test выполнена два раза подряд: PASS / PASS;
- PostgreSQL volume не удалялся, существующий Compose stack оставался запущенным.

## 7. Verification gaps

### V-01 — Не все критерии проверяются на уровне Airflow

- **Тип:** Verification gap
- **Статус:** RESOLVED
- **Затронутые критерии:** AC-02, AC-07, AC-08, AC-09, AC-10, AC-12
- **Компоненты:** `dags/orders_daily_aggregate.py`, `tests/acceptance/test_airflow_v01.py`, `tests/unit/test_orders_daily_aggregate.py`, `README.md`
- **Commit:** `8b8da7d`

Добавлен test-only параметр `demo_fail_first_attempt`, доступный только при `DEMO_FAULT_INJECTION_ENABLED=true`. Первая попытка получает advisory lock, читает старый агрегат и завершается до публикации; штатный retry того же Airflow run затем выполняет реальный расчёт. По умолчанию механизм отключён и отвергается, как и существующий `demo_delay_seconds`.

Автоматизированный acceptance suite через Airflow API v2 и реальный Celery stack подтверждает:

- первая попытка переходит в `up_for_retry`, старый агрегат остаётся `1 / 10.00`, а вторая попытка публикует `2 / 17.00`;
- success XCom `return_value` совпадает с JSON-сводкой финального task log и содержит `trigger_reason=retry`;
- ingestion публикует Asset metadata, dispatcher получает тот же upstream event, а конфликтный late-data target run завершается `failed` и сохраняет старый агрегат;
- два Airflow runs одной даты перекрываются по task timestamps, второй сообщает `lock_waited=true`, читает добавленный во время ожидания 11-й заказ и публикует `11 / 66.00`;
- задачи разных дат реально перекрываются по времени и публикуют независимые результаты;
- при трёх runs pool API показывает 2 running slots, 0 open slots и одну ожидающую задачу; третья стартует только после завершения одной из первых двух.

Airflow 3.1.6 удерживает задачу, ожидающую pool slot, в состоянии `scheduled`. Пользователь явно уточнил AC-12 и разрешил Airflow-specific состояние `scheduled` или `queued` при доказанном лимите pool. Task timestamps и счётчики pool API подтверждают ограничение ёмкости: одновременно заняты ровно два слота, третья задача стартует только после освобождения одного из них.

Suite выбирает незанятые даты 2200—2299 и уникальные `order_id`. Append-only raw fixtures намеренно не удаляются; existing persistent volume не очищался.

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
| AC-02 | PASS | Airflow run: первая попытка `up_for_retry` сохраняет старый `1 / 10.00`, retry публикует единственный `2 / 17.00` |
| AC-03 | PASS | SQL-идемпотентность подтверждена ранее; реальный двухдневный Airflow backfill завершён успешно, по одной активной строке на дату |
| AC-04 | PASS | Latest version и exact duplicate проверены |
| AC-05 | PASS | Конфликт блокирует публикацию и сохраняет старый результат |
| AC-06 | PASS | Реальный ingestion -> Asset -> dispatcher -> target run выполнен |
| AC-07 | PASS | Конфликтный ingestion -> Asset -> dispatcher -> target run завершается `failed`; старый `1 / 20.00` сохранён |
| AC-08 | PASS | Два перекрывающихся Airflow runs одной даты; второй ждёт lock и публикует актуальные `11 / 66.00` |
| AC-09 | PASS | Два delayed Airflow runs разных дат имеют перекрывающиеся task intervals и независимые результаты |
| AC-10 | PASS | Success/failure JSON logs разобраны через API; success XCom `return_value` получен и сопоставлен с SQL и task state |
| AC-11 | PASS | UTC boundary fixture проверен SQL integration test |
| AC-12 | PASS | Три Airflow runs: pool API фиксирует 2 running, 0 open и ожидающий `scheduled` task; третий task стартует после освобождения слота |
| AC-13 | PASS | Реальный timeout сохраняет старый агрегат и пишет JSON error summary |

## 10. Приоритет следующих изменений

1. Провести пользовательскую приёмку текущего baseline `bc93814`.
2. При необходимости добавить CI для unit/integration/acceptance уровней отдельной задачей.
3. Отдельно решить, требуется ли снижать риск R-01 через retry, идемпотентный `change_id`, outbox или reconciliation DAG.
4. При необходимости очистить или пересоздать локальную metadata DB, если для демонстрации нужна история без старых failed runs.

## 11. Правила обновления документа

- `docs/spec.md` меняется только при изменении утверждённого поведения или scope.
- `docs/bootstrap.md` сохраняется как исторический план и не переписывается под фактическую реализацию.
- После исправления дефект не удаляется: его статус меняется на `RESOLVED`, добавляются commit и выполненные проверки.
- Числа запусков и состояние локального volume являются evidence конкретной проверки, а не переносимым свойством проекта.
- Общий статус становится `READY_FOR_ACCEPTANCE`, когда нет blocker-дефектов и обязательные AC имеют достаточное runtime evidence.

## TASK_ID: IMP-08

- Source: `docs/bootstrap.md` §6, строка IMP-08; `docs/spec.md` §4 «Утверждённый эталон данных» и §5 AC-01—AC-13
- Status: COMPLETED
- Updated: 2026-10-23T23:36:11+03:00
- Baseline HEAD: 8b8da7d52a5e731ce6ec6c434090cdb1c774080c
- Result commit: bc93814512369f89fbfb3162c666ed62b4eda7a9

### Progress
- Implemented: extended `tests/acceptance/test_airflow_v01.py` to run the approved R1–R4 fixture, repeat calculation, adjacent UTC-boundary fixtures, successful late order C through ingestion/Asset/dispatcher, and a real two-attempt Airflow timeout; isolated all seven Airflow scenarios on 12 consecutive unused dates; updated `README.md` coverage description
- Remaining: none

### AC evidence
| AC / ссылка на источник | Result | Проверка / evidence / причина |
|---|---|---|
| `docs/spec.md` §5 AC-01 | PASS | acceptance; full suite exit 0, canonical Airflow run published exactly one `2 / 170.00` aggregate from four R1–R4 records |
| `docs/spec.md` §5 AC-02 | PASS | acceptance; full suite exit 0, fail-first attempt preserved `1 / 10.00`, retry published one `2 / 17.00` row |
| `docs/spec.md` §5 AC-03 | PASS | standard + acceptance; SQL repeat remained one `2 / 170.00` row, and repeated Airflow calculation remained one row with unchanged values |
| `docs/spec.md` §5 AC-04 | PASS | standard + acceptance; R1–R4 selected latest A=120 once, ignored its exact copy, and produced `2 / 170.00` |
| `docs/spec.md` §5 AC-05 | PASS | standard + acceptance; conflicting same-version rows failed before publication and preserved the old aggregate |
| `docs/spec.md` §5 AC-06 | PASS | acceptance; targeted scenario exit 0 in 18.79s: test held the date advisory lock, observed the automatic target task blocked on `pg_advisory_xact_lock`, verified the exact old `2 / 170.00` snapshot remained visible, then released the lock and observed one `3 / 200.00` row after success |
| `docs/spec.md` §5 AC-07 | PASS | acceptance; conflicting late-data target run failed after both attempts and preserved the prior `1 / 20.00` snapshot |
| `docs/spec.md` §5 AC-08 | PASS | standard + acceptance; same-date Airflow runs overlapped at task level, second reported lock wait and published current `11 / 66.00` input |
| `docs/spec.md` §5 AC-09 | PASS | standard + acceptance; different-date calculations overlapped and published independent aggregates |
| `docs/spec.md` §5 AC-10 | PASS | fast + acceptance; success/failure JSON logs had the exact required key set and success XCom matched the logged summary and SQL result |
| `docs/spec.md` §5 AC-11 | PASS | standard + acceptance; `23:59:59Z` and next-day `00:00:00Z` fixtures published only in their respective adjacent UTC dates |
| `docs/spec.md` §5 AC-12 | PASS | acceptance; pool API observed 2 running slots, 0 open slots and one `scheduled`/`queued` task; third task started after a slot was released |
| `docs/spec.md` §5 AC-13 | PASS | acceptance; both real 10-second timeout attempts failed before publication, retained the old snapshot and logged `AirflowTaskTimeout` with `old_result_preserved` |

### Issues
#### IMP-08/B01
- State: RESOLVED
- Category: ENVIRONMENT
- Affects: fast syntax check
- Problem: the first `py_compile` probe attempted to create bytecode in the read-only tests mount
- Evidence: `docker run ... python -m py_compile tests/acceptance/test_airflow_v01.py` exited 1 with `Errno 30 Read-only file system`
- Unblock: use a non-writing parser assertion for read-only mounted sources
- Resolution: `docker run ... python -c '... ast.parse(...) ...'` exited 0 and printed `acceptance syntax: ok`; `pytest --collect-only` then collected all 7 scenarios

#### IMP-08/B02
- State: RESOLVED
- Category: TEST
- Affects: `docs/spec.md` §5 AC-06; EVIDENCE_GATE; FRESH_READ_ONLY_REVIEW
- Problem: the successful automatic late-data acceptance scenario checks the baseline before ingestion and the result after target success, but does not observe the old aggregate while the target calculation is in progress
- Evidence: reviewer call 1 returned `VERDICT BLOCK`; first targeted fix run then exited 1 after 29.19s because the lock-wait observer filtered `pg_stat_activity.query` for the aggregate function while the blocked query was `pg_advisory_xact_lock`; the held lock exceeded 10 seconds and caused an expected task timeout before retry success
- Unblock: correct the observer to recognize the blocked `pg_advisory_xact_lock` query, then rerun the isolated automatic late-data scenario and confirm the snapshot assertion before timeout
- Resolution: observer now matches `pg_advisory_xact_lock`; targeted Airflow scenario passed in 18.79s and asserted the baseline snapshot while the automatic target task was blocked, before a successful `3 / 200.00` publication

### Review
- Verdict: OK
- Calls: 3
- Fix iterations: 1
- Details: Reviewer call 1 blocked missing in-progress AC-06 evidence. Targeted fix iteration 1 added and verified the external advisory-lock snapshot assertion. Reviewer calls 2 and 3 returned `VERDICT OK`; call 3 confirmed the status-only progress correction and full tracked diff, with no blocker or deferred issue. The previously noted P2 inconsistency in the global test table is corrected: the acceptance file contains 7 scenarios. Runtime is running/healthy with fault injection false; PostgreSQL volume is preserved.

### Next action
Use `bc93814` as the current acceptance baseline. Remaining work is optional and tracked in section 10: user acceptance, CI, a decision on R-01, and optional cleanup of local runtime history.
