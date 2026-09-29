# cadastre-etl — план реализации

**Цель:** сервис, который инкрементально переносит кадастровые номера из лотов
trading_platform (Mongo) в PostGIS с геометрией и сведениями НСПД.

**Спека:** `docs/specs/2026-09-29-cadastre-etl-design.md`. План выполняется **после её
согласования**; ответы на открытые вопросы спеки могут поменять задачи 5, 7, 9 и 10 —
тогда сначала правится спека, потом план.

**Стек:** Python 3.12, uv, ruff, pytest + pytest-asyncio, pydantic-settings,
pymongo (`AsyncMongoClient`), psycopg 3 + psycopg_pool, pynspd[sqlite,redis] 1.1.x,
aiolimiter, pytest-httpx, GitHub Actions.

**Общие правила для всех задач:**

- Сначала тесты, потом код; задача закрыта, когда `uv run ruff check`,
  `uv run ruff format --check` и `uv run pytest` зелёные (с Mongo и PostGIS из
  compose — `docker compose --profile dev up -d mongo postgis`).
- Комментарии, докстринги, имена тестов, сообщения коммитов, README — по-русски.
  Имена тестов — как в trading_platform: `test_номер_из_деталей_без_пробелов`.
  Докстринг модуля — что он делает и почему так.
- Один коммит на задачу (или несколько, если задача большая), сообщение — как указано
  в задаче; в конце — строки соавторства, которые задаёт окружение.
- pynspd — только через публичные имена (`AsyncNspd`, `AsyncNspd.request`,
  `pynspd.errors`, `pynspd.schemas`, хранилища `hishel`). Если без приватного не
  обойтись — остановиться и спросить.
- Сеть в офлайн-тестах запрещена: НСПД — через `pytest-httpx`, живые — только под
  маркером `live`.

## Файлы

| файл | задача |
|---|---|
| `pyproject.toml`, `uv.lock`, `.python-version`, `src/cadastre_etl/__init__.py`, `conf.py`, `tests/test_conf.py`, `.github/workflows/ci.yml` (lint) | 1 |
| `src/cadastre_etl/extract.py`, `tests/extract/…`, `tests/fixtures/trading_platform/…` | 2 |
| `compose.yaml`, `src/cadastre_etl/db/{pool,migrate}.py`, `db/migrations/0001_init.sql`, `tests/conftest.py`, `tests/db/test_migrate.py`, CI job `test` | 3 |
| `src/cadastre_etl/mongo.py`, `tests/test_mongo.py` | 4 |
| `src/cadastre_etl/db/{lots,state}.py`, `tests/db/test_lots.py`, `test_state.py` | 5 |
| `src/cadastre_etl/nspd/{client,parse}.py`, `tests/nspd/…`, `tests/fixtures/nspd/*.json` | 6, 7 |
| `src/cadastre_etl/nspd/resolver.py`, `tests/nspd/test_resolver.py` | 8 |
| `src/cadastre_etl/db/objects.py`, `tests/db/test_objects.py` | 9 |
| `src/cadastre_etl/pipeline.py`, `tests/test_pipeline.py` | 10 |
| `src/cadastre_etl/{report,__main__}.py`, `tests/test_cli.py` | 11 |
| `Dockerfile`, `.dockerignore`, `compose.yaml` (etl, redis) | 12 |
| `tests/live/test_nspd_live.py` | 13 |
| `README.md`, `docs/trading_platform-proposal.md` | 14 |

---

### Задача 1. Каркас проекта и настройки

- `uv init --package --python 3.12`, раскладка `src/cadastre_etl`, зависимости из
  «Стека», dev-группа: pytest, pytest-asyncio, pytest-httpx, ruff.
- `pyproject.toml`: ruff (`line-length = 110`, правила `E F I UP B C4`, как в
  trading_platform), pytest (`asyncio_mode = "auto"`, `addopts = "-m 'not live'"`,
  маркеры `mongo`, `postgres`, `live` с описаниями).
- `conf.py`: группы `MongoSettings`, `PostgresSettings`, `EtlSettings`,
  `NspdSettings`, общий `Settings`, `get_settings()` с `lru_cache`, `.env` в корне,
  окружение приоритетнее. Тип `Duration` — `timedelta` с разбором `10m`/`7d`/`3600`.
  Проверки: `NSPD_CACHE in {sqlite, redis, none}`, положительные лимиты.
- CI: job `lint` (`ruff check`, `ruff format --check`).

**Тесты** (`tests/test_conf.py`): умолчания без окружения; переменная окружения
перекрывает `.env`; `OVERLAP=10m` → 600 с, `NSPD_CACHE_TTL=7d`; неверный
`NSPD_CACHE` — ошибка валидации.

Коммит: `Каркас: uv, ruff, pytest, настройки в conf.py`

### Задача 2. Извлечение кадастровых номеров

- `extract.py`:
  - `normalize(raw) -> str` — убрать пробелы (включая `   `, `\r\n`)
    вокруг `:`.
  - `find_numbers(text) -> list[str]` и `find_quarters(text) -> list[str]` по
    регуляркам из спеки (границы, суффикс `/N` отбрасывается, квартал — только если не
    начало номера); порядок первой встречи, без повторов.
  - `extract_lot(doc) -> Extracted(numbers: dict[str, str], quarters: list[str])` —
    `description` и рекурсивный обход `detail`; значение словаря `numbers` — где
    встретился впервые (`description` / `detail.<ключ>.<индекс>…`).
- Фикстуры: `tests/fixtures/trading_platform/` — копии HTML с номерами из
  trading_platform (`itender/listing_centerr.html`, `itender/lot_utender_766294.html`,
  `ruson/trade_496200.html`, `ruson/listing_rus_on.html`, `kendo/listing_trade_alliance.html`,
  `btorg/listing_atctrade.html` — cp1251) с `README` об источнике и коммите;
  `tests/extract/samples.py` — ~40 реальных фрагментов из этих файлов с ожидаемым
  результатом.

**Тесты:** параметризованные фрагменты (с КН, «кадастровым номером:», склейка
«кадастровымномером:26:27:062601:273,», «№ 77:08:0010001:8753», перевод строки перед
номером, `63:26:0000000:770`, квартал из 6 цифр, `47:07:0722001:127617`,
`02:26:…` с ведущим нулём, пробелы и NBSP вокруг `:`); квартал «кадастрового квартала
63:17:0519006» — в кварталы, не в номера; номер не даёт лишнего квартала; суффикс
`/1`; отрицательные — `12:30:00`, `10:00:2026`, телефон, `host:8080`, дата-время
ISO; целые файлы — множество номеров совпадает с выписанным вручную; `extract_lot` на
документах трёх видов `detail` (разделы iTender, пары Kendo с `attachments`, btorg с
`price_schedule`), дедуп между `description` и `detail`, `found_in`.

Коммит: `Извлечение кадастровых номеров и кварталов из лота`

### Задача 3. PostGIS: compose, миграции, тестовая база

- `compose.yaml`: `postgis` (`postgis/postgis:17-3.5`, `127.0.0.1:5432`, том
  `pg-data`, healthcheck `pg_isready`), `mongo` (профиль `dev`, `127.0.0.1:27017`).
- `db/pool.py`: `open_pool(dsn) -> AsyncConnectionPool` (закрытие — у создателя).
- `db/migrate.py`: `apply_migrations(conn)` — файлы `db/migrations/NNNN_*.sql` по
  порядку, `schema_migrations`, `pg_advisory_lock`, миграция = транзакция; возвращает
  список применённых. Миграции — пакетные данные (`importlib.resources`).
- `0001_init.sql` — схема из спеки целиком (включая `etl_state` и представление
  `lot_objects`).
- `tests/conftest.py`: фикстура `pg` (маркер `postgres`): отдельная база
  `cadastre_test_<uuid>` на сервере из `POSTGRES_DSN`, миграции, удаление после теста;
  нет сервера за 2 с — skip, с `REQUIRE_POSTGRES=1` — ошибка. Аналогично `mongo_db`
  (маркер `mongo`, база `trading_test_<uuid>`).
- CI: job `test` с сервисами `mongo:8` и `postgis/postgis:17-3.5`, `REQUIRE_*=1`.

**Тесты:** миграции на пустой базе создают таблицы, GIST-индекс, `postgis`;
повторный вызов ничего не применяет; две параллельные `apply_migrations` не
конфликтуют; `geom` принимает только SRID 4326.

Коммит: `PostGIS: compose, SQL-миграции, схема`

### Задача 4. Чтение Mongo

- `mongo.py`:
  - `create_client(uri)` — `AsyncMongoClient(uri, tz_aware=True)`.
  - `list_sources(db, exclude) -> list[str]` — `list_collection_names()`, без
    `system.*` и `exclude`, по алфавиту.
  - `changed_lots(db, source, since: datetime | None, batch_size)` — асинхронный
    итератор документов по фильтру `$or` из спеки (без фильтра при `since=None`),
    с проекцией полей лота + `description`, `detail`, трёх отметок времени.

**Тесты** (маркер `mongo`): лот с `updated_at` ровно `since` не выбирается, на 1 мс
позже — выбирается; лот, у которого старый `updated_at`, но свежий `detail_at`, —
выбирается; без `detail_at` вовсе — по `updated_at`; `since=None` — все; `system.*`
и `exclude` пропускаются; даты приходят с `tzinfo=UTC`; проекция не тащит `_id`.

Коммит: `Выборка изменённых лотов площадки из Mongo`

### Задача 5. Запись лотов, связей и состояния

- `db/lots.py`: `LotRow.from_doc(doc)` (поля из спеки; `price_value` и даты — если
  есть в документе); `write_lots(conn, source, items: list[(LotRow, Extracted)])` в
  одной транзакции: заготовки `cadastral_objects`, upsert лотов с номерами/кварталами,
  замена связей, удаление лотов, у которых номеров и кварталов больше нет.
- `db/state.py`: `get_state`, `source_lock(conn, source)` (асинхронный контекст с
  `pg_try_advisory_lock`/`unlock`, `None` если занято), `save_success(source,
  run_started, stats)`, `save_failure(source, started, error, stats)`,
  `all_states()`.

**Тесты** (маркер `postgres`): повторная запись того же лота — одна строка, поля
обновлены; номер пропал из описания — связь удалена, объект остался; все номера
пропали — лот удалён каскадом; новый номер → `cadastral_objects` со статусом
`pending`; `save_success` двигает `checked_at`, `save_failure` — нет, но пишет
ошибку; второй `source_lock` на ту же площадку с другого соединения → `None`.

Коммит: `Запись лотов, связей лот–объект и состояния площадок`

### Задача 6. Клиент НСПД: кэш и запрос

- `nspd/client.py`:
  - `build_cache_storage(conf) -> AsyncBaseStorage | None` — SQLite
    (`anysqlite.connect`, каталог создаётся), Redis, `none`; TTL в секундах.
  - `NspdClient` — асинхронный контекст над `AsyncNspd(client_timeout, client_retries=0,
    client_proxy, cache_storage, trust_env=False)`; `search(cad_num) -> RawAnswer`
    (`status_code`, `json | None`, `from_cache`); `NotFound` → ответ 404 с
    `from_cache` из `e.response.extensions`; остальные ошибки pynspd и httpx —
    пробрасываются как есть (решает резолвер). Первый вызов — под `asyncio.Lock`.
- `tests/fixtures/nspd/`: синтетические ответы поиска по схеме `SearchResponse`
  (ЗУ с полигоном в `EPSG:3857`, здание, сооружение, объект с `geocoderObject: true`,
  пустой `features`, два совпадения) — с пометкой «синтетика, заменить записанными
  (задача 13)».

**Тесты** (`pytest-httpx`, реальный `AsyncNspd` с `AsyncInMemoryStorage` и с
SQLite во временном каталоге): параметры запроса (`query`, `thematicSearchId=1`);
второй такой же запрос — `from_cache=True` без второго HTTP-запроса, для 200, пустого
200 и 404; 503 и 429 не кэшируются; `cache_storage` с TTL работает (в отличие от
`cache_sqlite_url + cache_ttl`, который даёт `ValueError` — тест фиксирует эту
причину нашей сборки); `trust_env=False` игнорирует `PYNSPD_CLIENT_TIMEOUT`.

Коммит: `Клиент НСПД поверх pynspd с настраиваемым кэшем Hishel`

### Задача 7. Разбор ответа НСПД

- `nspd/parse.py`: `parse_answer(cad_num, raw) -> Outcome` —
  `Outcome(cad_num, status, kind, key_fields: dict, attrs: dict, geom, geom_approx,
  nspd_id, from_cache, error)`:
  - `SearchResponse.model_validate`; точное совпадение по `cad_num`/`cad_number`;
    0 → `not_found`, >1 → `ambiguous` (кандидаты в `attrs`), 404 → `not_found`.
  - `feature.cast()` для типизированных полей; `UnknownLayer` — не ошибка, берём
    `options` как есть; ошибка валидации геометрии — атрибуты из сырого JSON,
    `found_no_geom`.
  - таблица соответствия ключевых колонок по категории из спеки + запасной список.
  - геометрия: из модели pynspd (уже 4326) → Multi*; проверка охвата и явное
    3857→4326 при координатах вне градусов; `geocoderObject` → `found_no_geom`,
    точка — в `geom_approx`.

**Тесты:** ЗУ из 3857 — координаты в градусах возле ожидаемой точки (±1e-6),
тип `MultiPolygon`; здание, сооружение — ключевые колонки по своим именам полей;
незнакомая категория → `other`, `attrs` полные; `geocoderObject` → `found_no_geom`;
пустой ответ, 404 → `not_found`; два совпадения → `ambiguous`; совпадение только в
родительском номере (помещение) не принимается за точное; геометрия без `crs` в
метрах → перепроецирована с предупреждением в логе.

Коммит: `Разбор ответа НСПД: исход, тип объекта, атрибуты, геометрия в 4326`

### Задача 8. Резолвер: дедуп, лимиты, повторы

- `nspd/resolver.py`: `Resolver(client, conf)` на прогон:
  - `resolve(cad_num) -> Outcome` — общий `dict[str, asyncio.Task]`, второй вызов
    ждёт ту же задачу;
  - `AsyncLimiter(NSPD_RATE, 1)` + `Semaphore(NSPD_CONCURRENCY)` вокруг каждого
    `client.search`;
  - повторы на `TimeoutException`, `TransportError`, `TooManyRequests`,
    `PynspdServerError` — backoff `min(30, 2**n) * U(0.5, 1.5)`; `BlockedIP` и
    прочее — сразу `error`;
  - предохранитель: `NSPD_BREAKER` ошибок подряд → `breaker_open`, дальнейшие
    `resolve` без запросов возвращают `pending`;
  - счётчики исходов для отчёта.

**Тесты** (фейковый клиент, `asyncio` с подменой `sleep`): один номер из трёх
«площадок» одновременно → один запрос; лимит параллельности не превышается;
503, 503, 200 → `found` за 3 попытки; 403 → одна попытка, `error`; предохранитель
после N ошибок подряд, успех сбрасывает счётчик; отменённый прогон отменяет
задачи резолвера.

Коммит: `Резолвер номеров: один запрос на номер за прогон, лимиты, повторы`

### Задача 9. Запись объектов и очередь повторов

- `db/objects.py`: `write_outcomes(conn, outcomes)` — upsert по исходам из спеки;
  `error` при уже окончательном статусе меняет только `attempts`, `last_error`,
  `attempted_at`, `next_attempt_at`; окончательный исход обнуляет `attempts`,
  ставит `fetched_at`; геометрия — WKB с SRID 4326.
  `due_for_retry(conn, limit) -> list[str]` — `pending` и `error` с
  `next_attempt_at IS NULL OR <= now()`.

**Тесты** (маркер `postgres`): найденный объект → колонки и `ST_SRID(geom)=4326`,
`GeometryType = MULTIPOLYGON`; потом ошибка → данные и статус на месте, `attempts=1`;
ошибка у нового → `error`, `next_attempt_at` растёт с попытками до потолка;
`not_found` после `found` (объект снят) → статус меняется, геометрия очищается;
`due_for_retry` учитывает время и лимит.

Коммит: `Запись кадастровых объектов и очередь повторов`

### Задача 10. Прогон

- `pipeline.py`:
  - `run_source(ctx, source) -> SourceStats` — шаги 1–7 раздела «Состояние» спеки;
  - `run_retry_queue(ctx) -> SourceStats` — очередь повторов в начале прогона;
  - `run_all(names | None) -> RunResult` — миграции, неизвестные имена — ошибка до
    старта, очередь повторов, площадки через `asyncio.Semaphore(SOURCE_CONCURRENCY)`
    и `asyncio.gather(return_exceptions=True)`, один `Resolver` на прогон;
  - `ctx` — пул PostGIS, клиент Mongo, клиент НСПД, настройки.

**Тесты** (маркеры `mongo`, `postgres`, НСПД — фейковый клиент): первый прогон
берёт всё и ставит `checked_at = run_started`; второй прогон без изменений читает
только перекрытие; лот, у которого появился `detail` с номером, подхватывается;
площадка с ошибкой записи → `failed`, её `checked_at` прежний, другая площадка
дошла; упавший на середине прогон, повторённый, не создаёт дублей; номер на двух
площадках — один запрос; НСПД «лежит» → лоты записаны, номера `pending`, площадка
`ok`, `breaker_open`; следующий прогон с живым НСПД разбирает очередь.

Коммит: `Прогон площадок с состоянием, очередь повторов, параллельность`

### Задача 11. CLI и отчёт

- `__main__.py` (argparse): `run [names…] [--loop]`, `migrate`, `status`, `sources`;
  коды выхода 0/1/3 по спеке; `--loop` — `INTERVAL` между прогонами, SIGTERM/SIGINT —
  отмена и выход; `LOG_LEVEL`.
- `report.py`: таблица из спеки (ширины по содержимому, строки «(повторы)» и
  «всего»).

**Тесты:** таблица на заданных `SourceStats` (снимок строки); код выхода 1 при
упавшей площадке, 3 при предохранителе, 0 иначе; неизвестная площадка — ошибка до
работы; `--loop` с малым интервалом делает два прогона и выходит по сигналу.

Коммит: `CLI: run/migrate/status/sources, итоговая таблица, режим цикла`

### Задача 12. Docker

- `Dockerfile` (`python:3.12-slim`, uv из `ghcr.io/astral-sh/uv`, `uv sync --locked
  --no-dev`, непривилегированный пользователь, `CMD ["python", "-m", "cadastre_etl",
  "run", "--loop"]`), `.dockerignore`.
- `compose.yaml`: сервис `etl` (сборка, `depends_on: postgis healthy`, том
  `nspd-cache:/app/.cache`, `MONGO_URI` из окружения, по умолчанию
  `host.docker.internal`), `redis` под профилем `redis`.

**Проверка:** `docker compose build`, `docker compose run --rm etl migrate`,
`docker compose run --rm etl status` на пустой базе. (Живой прогон — только с
доступом к НСПД.)

Коммит: `Docker: образ сервиса, compose с PostGIS, томом кэша и Redis`

### Задача 13. Живые тесты и записанные ответы

- `tests/live/test_nspd_live.py` (маркер `live`): ЗУ, здание, сооружение, помещение,
  ЕЗП (`48:06:0000000:111`), несуществующий номер — через `NspdClient` + `parse_answer`;
  проверяется СК (координаты в пределах РФ), статус, ключевые колонки.
- Опция `--record`: сохранить ответы в `tests/fixtures/nspd/recorded/`, после чего
  офлайн-тесты задачи 7 переводятся на записанные ответы (синтетика остаётся только
  для граничных случаев).
- Выяснить и занести в таблицу соответствия категории помещений, машино-мест, ЕЗП;
  проверить, что у `geocoderObject` в ответе вместо геометрии.

**Выполняет** человек с российского адреса (`uv run pytest -m live --record`) —
из облачного окружения НСПД недоступна. После записи — коммит фикстур и правок.

Коммит: `Живые тесты НСПД и записанные ответы`

### Задача 14. README и предложение в trading_platform

- `README.md`: назначение, быстрый старт (compose, migrate, run), команды, таблица
  настроек, схема БД, как устроено состояние и перекрытие, кэш НСПД, исходы объектов,
  коды выхода, тесты и маркеры, доступность НСПД.
- `docs/trading_platform-proposal.md`: индексы по `updated_at`/`detail_at` и
  `detail_saved_at` — текст для issue/PR в trading_platform (сами туда не пишем).

Коммит: `README и предложение индексов для trading_platform`
