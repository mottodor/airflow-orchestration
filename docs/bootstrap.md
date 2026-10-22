# Bootstrap-план: Airflow orchestration и безопасные backfills

**Версия skill:** Bootstrap 0.1.0  
**Статус плана:** READY_FOR_REVIEW — подтверждён пользователем 2026-10-22  
**Режим:** plan-only — этот документ не разрешает установку, запуск сервисов, probes или реализацию.  
**Дата инвентаризации:** 2026-10-22  
**Подтверждение:** пользователь явно подтвердил `docs/bootstrap.md`; подтверждение зафиксировано, но не запускает применение или реализацию.

## 1. Вход, scope и fingerprints

### Входные документы

- `docs/spec.md` — утверждённая спецификация, статус `APPROVED`.
- `docs/idea.md` — исходный brief.
- Git commit: `017ef2a1fe10b7e40ea375173892fe46e9b59fc2`.

| Файл | SHA-256 |
|---|---|
| `docs/idea.md` | `65bf47ea3a025c7c6ce72b686ba703580ef9a47258adbe45aa73eba437485287` |
| `docs/spec.md` | `5bfa3df127589e4e12fc275c2a77f433d7aa3697df531851aa86d340f1adcb8b` |

На момент записи плана рабочее дерево было чистым до создания `docs/bootstrap.md`; `docs/spec.md` уже входит в commit.

### Согласованный scope

Весь первый законченный срез из `docs/spec.md`:

- дневные агрегаты заказов;
- обычный запуск, retry, backfill, частичный сбой и конкуренция;
- поздние данные через Airflow Assets;
- сериализация пересчётов одной даты;
- pools и timeout;
- атомарная публикация;
- операторская JSON-сводка;
- критерии AC-01—AC-13.

### Исключения

Не включать в первый срез:

- production hardening;
- Kubernetes, облачную и внешнюю управляемую инфраструктуру;
- полный lakehouse;
- отмены, возвраты, валюты, налоги, скидки и перенос заказа между датами;
- общую платформенную обвязку, не нужную этому сценарию.

## 2. Наблюдаемое состояние

### Репозиторий

До создания этого плана наблюдалось локально:

- в репозитории были только `docs/idea.md` и `docs/spec.md`;
- отсутствовали `AGENTS.md`, dependency manifests, lock-файлы, Compose, Dockerfile, DAG, SQL, тесты и CI;
- symlink в пределах проекта не обнаружены;
- существующего bootstrap-плана не было.

### Машина и инструменты

| Компонент | Наблюдаемое значение | Ограничение вывода |
|---|---|---|
| ОС | Linux WSL2, x86_64 | Среда текущего агента |
| CPU | 16 логических CPU | Не означает разрешение использовать все |
| RAM | около 24 GiB | Не означает доступность всего объёма для Docker |
| Python host | 3.12.3 | Не предлагается как runtime Airflow |
| uv | 0.11.3 | Для первого среза не требуется |
| Docker client | 29.8.0 | Docker daemon не проверялся |
| Docker Compose | 5.5.1 | Версия команды подтверждена, запуск stack — нет |
| Git | 2.43.0 | Индекс и история не изменялись агентом |
| pi | 1.4.0 | Текущая агентская среда |
| Node.js | 24.15.0 | Соответствует требованию установленного pi-lens |

Не проверялось:

- доступность Docker daemon;
- возможность pull образов и пакетов;
- свободные порты;
- лимиты Docker Desktop/WSL;
- запуск Airflow 3.1.6 Compose;
- фактическая совместимость полной контейнерной топологии.

### Pi и установленные ресурсы

Настроены:

- `@upstash/context7-pi` 0.1.3;
- `pi-lens` 4.4.4;
- `pi-lmstudio` 1.5.0;
- `pi-subagents` 0.80.0.

Для pi-lens не обнаружено пользовательской или проектной конфигурации. `effective_config` показал встроенные настройки и доступные инструменты. Расширение имеет автоматическую post-write обработку, диагностику и возможное форматирование/autofix; при будущей реализации итоговый diff нужно проверять после каждого изменения.

Subagents на этапе Bootstrap не запускались. LM Studio для задачи не нужен. Context7 использовался только для публичных запросов к документации Airflow 3.1.6; секреты и полное частное описание проекта не передавались.

## 3. Выбор возможностей

| Потребность | Существующая возможность | Решение | Основание / ограничение |
|---|---|---|---|
| Навигация по проекту | Git, shell и средства чтения pi | Использовать | Репозиторий пока мал |
| Актуальная документация Airflow | Context7 0.1.3 | Оставить доступным по необходимости | Уже установлен; запросы передаются внешнему API |
| Проверка Python-кода | pytest в проектном image, pi-lens/LSP | Использовать при реализации | LSP/runtime пока не проверены |
| Структурный поиск | pi-lens | Оставить доступным | Сейчас исходников нет |
| Делегирование | pi-subagents | Не использовать на этом этапе | Не требуется для минимального применения |
| Локальная оркестрация | Docker Compose | Использовать | Соответствует спецификации |
| Управление командами | Прямые `docker compose`, Airflow CLI и pytest | Не добавлять Make/Just сначала | Набор команд небольшой |
| CI | Отсутствует | Не добавлять в первом минимальном применении | Scope требует локальной воспроизводимости |
| Новые extensions/MCP | Не требуются | Ничего не добавлять | Необходимые возможности уже доступны |
| Новый state/handoff | Handoff уже есть в `docs/spec.md` | Не создавать | Избежать дублирования и рассинхронизации |

**Решение:** новых pi-пакетов, MCP, глобальных настроек или agent skills не требуется.

## 4. Техническое предложение

Обозначения:

- **Наблюдалось** — проверено локально.
- **Документация** — подтверждено официальной документацией.
- **Предложено** — решение для будущей реализации.
- **Нужен probe** — требует ограниченного эксперимента.

Документация проверена 2026-10-22 через Context7 `/apache/airflow/3.1.6`; источники — официальные материалы Apache Airflow с tag `3.1.6`.

### 4.1 Версии и контейнерная среда

**Предложено:**

- закрепить image `apache/airflow:3.1.6`, без `latest`;
- взять за основу versioned Docker Compose Airflow 3.1.6, а не изменяемый URL `stable`;
- сохранить выбранную в спецификации топологию: CeleryExecutor, PostgreSQL, Redis и стандартные сервисы Airflow;
- версии PostgreSQL и Redis взять из Compose-файла tag `3.1.6` и явно закрепить;
- собрать небольшой производный Airflow image с нужными providers и test dependencies;
- версии providers брать из constraints Airflow 3.1.6 для Python minor базового image;
- host Python и `uv` не использовать как условие запуска проекта.

**Документация:** официальный Compose является локальным development/learning quick-start и не должен считаться production deployment.

### 4.2 Минимальная структура проекта

Предлагаемая структура без дополнительных генераторов:

```text
AGENTS.md
README.md
docker-compose.yaml
Dockerfile
requirements-airflow.txt
.env.example
.gitignore
dags/
src/orders_demo/
sql/
tests/unit/
tests/integration/
tests/e2e/
```

Отдельный host Python lock-файл пока не требуется. Источником совместимости Airflow-зависимостей должны быть versioned constraints.

### 4.3 Данные и транзакция

**Предложено:**

- использовать один PostgreSQL container;
- metadata Airflow и учебные данные хранить раздельно;
- минимальный вариант — отдельная БД `orders_demo` в том же container;
- Airflow connection назвать `orders_db`;
- raw-таблицу оставить append-only, чтобы точные копии действительно присутствовали во входе и дедупликация проверялась расчётом;
- итоговой таблице назначить `aggregate_date` как `PRIMARY KEY`.

Критическая секция дневного расчёта:

1. открыть транзакцию;
2. получить transaction-level PostgreSQL advisory lock даты;
3. прочитать старый агрегат;
4. прочитать актуальный raw input по UTC-интервалу;
5. проверить конфликты;
6. выбрать последние версии и удалить точные дубли;
7. вычислить агрегат;
8. выполнить тестовую fault/delay injection, если она разрешена локальным test profile;
9. выполнить один UPSERT агрегата;
10. commit.

До UPSERT старый агрегат не удаляется, поэтому другие соединения продолжают видеть прежнее значение до commit.

Для lock key не использовать Python `hash()`. Предлагается двухкомпонентный PostgreSQL advisory key: фиксированный namespace проекта и стабильный integer, выведенный из UTC-даты, например `date.toordinal()`.

### 4.4 DAG и причины запуска

Предлагаемые идентификаторы:

- `orders_ingest`;
- `orders_late_data_dispatch`;
- `orders_daily_aggregate`;
- pool `orders_compute`;
- Asset URI без credentials, например `postgresql://orders/raw_order_events`.

Нормализация даты:

- scheduled/backfill — дата из Airflow data interval;
- late data — `affected_date` из проверенного Asset event;
- ручной тестовый запуск — `affected_date` и одна из уже утверждённых причин через `dag_run.conf`;
- task attempt с `try_number > 1` получает причину `retry`, а исходная причина сохраняется дополнительным полем сводки.

Каждый Asset event содержит одну дату. Если входное изменение затрагивает несколько дат, ingestion должен разбить его на независимые изменения/события до публикации metadata.

Dispatcher должен:

- обработать все triggering events, попавшие в запуск;
- дедуплицировать даты;
- создавать целевой запуск для каждой даты;
- использовать детерминированный run ID, чтобы retry dispatcher не создавал неконтролируемые дубли.

Точный публичный API Airflow 3.1.6 для идемпотентного target trigger требует probe.

### 4.5 Pool, backfill и timeout

Сохранить утверждённые значения:

- `orders_compute`: 2 слота;
- compute task: 1 слот;
- DAG `max_active_runs=4`;
- backfill `--max-active-runs 2`;
- `execution_timeout=10 секунд`;
- `retries=1`;
- `retry_delay=5 секунд`.

**Документация:** Airflow 3.1.6 подтверждает, что backfill `max_active_runs` действует независимо от DAG `max_active_runs`. Поддерживается интерфейс `airflow backfill create` с диапазоном дат, reprocessing behavior и отдельным лимитом активных runs.

### 4.6 Fault injection

Для AC-02, AC-07, AC-08, AC-09 и AC-13 нужен детерминированный тестовый механизм.

**Предложено:**

- разрешать fault/delay параметры только при локальном `DEMO_FAULT_INJECTION_ENABLED=true`;
- по умолчанию механизм выключить;
- поддержать минимальные сценарии: ошибка первой попытки перед публикацией, задержка перед UPSERT, удержание lock и задержка дольше timeout;
- вне test/demo profile отвергать fault-параметры.

### 4.7 Операторская сводка

Единая JSON-схема должна всегда содержать ключи:

- `affected_date`;
- `trigger_reason`;
- `initial_trigger_reason`;
- `records_read`;
- `unique_orders`;
- `old_aggregate`;
- `new_aggregate`;
- `publication_state`;
- `lock_waited`;
- `lock_wait_ms`;
- `try_number`;
- `error`.

При раннем сбое неизвестные значения могут быть `null`, но ключи не исчезают.

Успех:

- одна JSON-запись в task logs;
- тот же объект через XCom.

Ошибка/timeout:

- JSON-запись через failure path/callback;
- старый результат проверяется после rollback.

Полнота callback при реальном `execution_timeout` пока не доказана. Если callback не сможет восстановить промежуточные счётчики и lock wait, резервный вариант — небольшая таблица состояния попытки, отдельная от опубликованных агрегатов. Добавлять её следует только после probe.

## 5. Необходимые probes

Ни один probe во время подготовки плана не выполнялся.

### P-01 — Compose и базовые сервисы

- **Неизвестность:** запускается ли Airflow 3.1.6 Compose в текущем WSL/Docker.
- **Минимальный эксперимент:** собрать image, выполнить init, поднять stack и дождаться health checks.
- **Ожидаемый сигнал:** metadata migration успешна; обязательные сервисы healthy.
- **Дальнейшее решение:** продолжить либо скорректировать только Compose/image dependencies.
- **Ресурсы:** project-specific network, containers и named volumes.
- **Критерий остановки:** первый нездоровый обязательный сервис или согласованный timeout.
- **Очистка:** `docker compose down`; volumes удалять только с отдельным подтверждением.

### P-02 — Asset metadata и dispatcher

- **Неизвестность:** точный API Airflow 3.1.6 для нескольких triggering events и идемпотентного запуска target DAG.
- **Минимальный эксперимент:** выпустить два события, включая повтор одной даты, и проверить target runs.
- **Ожидаемый сигнал:** metadata сохраняется; каждая уникальная дата создаёт один логический target run.
- **Дальнейшее решение:** закрепить operator/API и детерминированный run ID.
- **Очистка:** удалить только созданные тестовые данные и runs project stack.

### P-03 — timeout, rollback и lock release

- **Неизвестность:** гарантированно ли откатывается соединение и освобождается transaction advisory lock при реальном timeout Celery task.
- **Минимальный эксперимент:** вычислить новый результат, задержаться до UPSERT дольше 10 секунд, затем запустить второй расчёт даты.
- **Ожидаемый сигнал:** первый task failed по timeout; старый агрегат не изменён; второй task получает lock и завершается.
- **Дальнейшее решение:** оставить transaction pattern либо скорректировать управление соединением.
- **Очистка:** удалить только данные тестовой даты.

### P-04 — failure summary

- **Неизвестность:** достаточно ли callback context для полной JSON-сводки после timeout.
- **Минимальный эксперимент:** проверить обычное исключение, retry и timeout.
- **Ожидаемый сигнал:** для каждой попытки есть валидная JSON-запись со всеми обязательными ключами.
- **Дальнейшее решение:** использовать простой callback либо добавить attempt-state table.
- **Очистка:** удалить только записи тестовых попыток, если таблица будет создана.

## 6. План будущих изменений

### Работа агента

| Изменение | Зачем | Зависимость | Критерий проверки | Локальная отмена |
|---|---|---|---|---|
| Добавить короткий `AGENTS.md` | Указать source of truth, команды, границы и запрет разрушительных действий | Фактические команды среды уже определены | Файл ссылается на `docs/spec.md`, не копирует спецификацию | Удалить файл |
| Не добавлять project pi config | Текущих tools достаточно | Нет | Нет лишних `.pi/settings.json`/`.pi-lens.json` | Не требуется |
| Использовать handoff в spec | Не плодить state-файлы | `docs/spec.md` | Нет дублирующего журнала | Не требуется |

`AGENTS.md` следует добавлять после стабилизации реальных команд, чтобы не фиксировать гипотетический интерфейс.

### Среда и проверки

| Изменение | Зачем | Зависимость | Критерий проверки | Локальная отмена |
|---|---|---|---|---|
| Version-pinned `docker-compose.yaml` | Воспроизводимая среда | P-01 | `docker compose config --quiet`, health checks | Остановить только project stack и удалить файл |
| `Dockerfile` и pinned requirements | Providers и pytest внутри image | Constraints Airflow 3.1.6 | Image build и import smoke | Удалить image definition и project image |
| `.env.example` и `.gitignore` | Не хранить runtime credentials/state в Git | Compose | `.env`, logs и volumes не попадают в Git | Удалить только добавленные правила |
| Unit/integration/e2e test interface | Проверяемость AC | Реализация DAG/SQL | Непустые тесты с реальными assertions | Удалить только тестовую обвязку |
| Не добавлять CI сейчас | Scope локальный | Нет | Все проверки доступны локально | Не требуется |

### Реализация результата

| ID | Область | Вход | Наблюдаемый выход | Зависимости |
|---|---|---|---|---|
| IMP-01 | SQL schema | Контракты раздела 4 spec | Raw events и один активный агрегат на дату | ENV, P-01 |
| IMP-02 | Ingestion DAG | Изменение одной UTC-даты | Сохранён input и Asset event с `affected_date/reason/change_id` | IMP-01 |
| IMP-03 | Daily DAG | Data interval или `affected_date` | Транзакционный расчёт и UPSERT | IMP-01 |
| IMP-04 | Dedupe/conflict logic | Raw order versions | Последняя версия, игнорирование копий, отказ при конфликте | IMP-03 |
| IMP-05 | Date lock/pool/timeout/retry | Параллельные runs | Сериализация даты, параллельность дат, безопасный timeout | IMP-03, P-03 |
| IMP-06 | Late-data dispatcher | Asset events | Идемпотентные target runs исторических дат | IMP-02, P-02 |
| IMP-07 | Structured summary | Контекст попытки и результат | JSON logs; XCom на успехе; ошибка на failure | IMP-03, P-04 |
| IMP-08 | Fixtures и acceptance tests | Эталон R1–R4 и C | Автоматическая проверка AC-01—AC-13 | IMP-01—07 |

## 7. Связь AC с будущими проверками

Команды ниже — предлагаемый интерфейс, они ещё не реализованы и не запускались.

| AC | Проверка | Ожидаемый результат / evidence |
|---|---|---|
| AC-01 | E2E fixture R1–R4 + SQL query | Одна строка `2 / 170.00` |
| AC-02 | Fail-first-attempt перед UPSERT, затем retry | Snapshot до/после ошибки/после retry; одна строка |
| AC-03 | Повтор/backfill неизменного входа | Значения и количество строк неизменны |
| AC-04 | Unit + DB integration для A 100/120 и копии | A учитывается один раз как 120 |
| AC-05 | Конфликт одинакового `updated_at` | Failed task; старый aggregate неизменён |
| AC-06 | Asset event с заказом C | Event metadata, target run, переход `2/170` → `3/200` |
| AC-07 | Ошибка late-data run до UPSERT | Старый агрегат сохранён |
| AC-08 | Два runs даты с синхронизированной задержкой | Критические секции не перекрываются; B читает 11 заказов |
| AC-09 | Runs двух дат с задержкой | Временные интервалы расчётов перекрываются |
| AC-10 | JSON Schema assertion и XCom lookup | Все обязательные поля; XCom совпадает с success log |
| AC-11 | Boundary fixture `23:59:59Z` / `00:00:00Z` | Заказы попадают в разные даты |
| AC-12 | Три даты при pool size 2 | Не более двух compute tasks running одновременно |
| AC-13 | Задержка до публикации более 10 секунд | Timeout failure; старый aggregate сохранён; error в JSON |

Предлагаемые уровни проверки:

```text
docker compose config --quiet
docker compose build
docker compose up airflow-init
docker compose up -d

docker compose run --rm airflow-cli airflow dags list-import-errors
docker compose run --rm airflow-cli pytest -q tests/unit
docker compose run --rm airflow-cli pytest -q tests/integration
docker compose run --rm airflow-cli pytest -q tests/e2e
```

Предлагаемый backfill после появления DAG:

```text
airflow backfill create \
  --dag-id orders_daily_aggregate \
  --start-date 2025-03-01 \
  --end-date 2025-03-03 \
  --reprocessing-behavior completed \
  --max-active-runs 2
```

Evidence должно включать:

- реальные SQL-результаты, а не oracle из кода DAG;
- Airflow run/task states;
- Asset event metadata;
- JSON log records;
- XCom успешного запуска;
- timestamps pool/lock concurrency;
- pytest assertions;
- зафиксированные image tags и dependency versions.

Пустой набор тестов, импорт без сценариев или заглушка с `exit 0` AC не подтверждают.

## 8. Действия, требующие отдельного разрешения

При реализации потребуются:

- запись новых файлов проекта;
- pull/build контейнерных образов;
- сетевой доступ к registry и Python package index;
- запуск Docker services и создание project volumes/network;
- использование локального порта Airflow UI;
- миграции только project databases;
- тесты с намеренными task failures и задержками более 10 секунд.

Разрушительные defaults не предлагаются. Удаление volumes, Git commit или очистка пользовательских данных не должны выполняться автоматически.

## 9. Минимальное предложение и следующий шаг

1. Не добавлять новые pi extensions, MCP, CI, Makefile или отдельные state-файлы.
2. Закрепить Airflow 3.1.6 и построить минимальный versioned Compose stack.
3. Реализовать расчёт как одну PostgreSQL-транзакцию с transaction advisory lock и UPSERT в конце.
4. Добавить только необходимые DAG, SQL schema, fixtures и pytest-контур.
5. До окончательного выбора dispatcher/callback выполнить четыре ограниченных интеграционных probe.
6. Добавить короткий `AGENTS.md` только после появления реальных рабочих команд.

**Следующий ограниченный шаг:** по отдельной команде пользователя перейти к этапу реализации в границах подтверждённых `docs/spec.md` и `docs/bootstrap.md`. Текущий plan-only этап на этом остановлен.
