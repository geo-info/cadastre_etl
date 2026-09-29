"""Общее для тестов: временные базы PostGIS и Mongo.

Каждый тест получает свою базу — ``cadastre_test_<uuid>`` на сервере из
``POSTGRES_DSN`` (с применёнными миграциями) и ``trading_test_<uuid>`` в Mongo из
``MONGO_URI`` — и после себя её удаляет. Сервер не отвечает за 2 с — тест
пропускается с причиной; в CI (``REQUIRE_POSTGRES=1``, ``REQUIRE_MONGO=1``) это
ошибка: там пропуск прятал бы непроверенное.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

from cadastre_etl.db.migrate import apply_migrations
from cadastre_etl.db.pool import connect, open_pool

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://etl:etl@localhost:5432/cadastre")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")


def _unavailable(what: str, env: str, error: Exception) -> None:
    message = f"{what} недоступен: {error}"
    if os.environ.get(env) == "1":
        pytest.fail(message)
    pytest.skip(message)


@pytest.fixture
async def pg_dsn() -> AsyncIterator[str]:
    """DSN временной базы с применёнными миграциями."""
    name = f"cadastre_test_{uuid4().hex[:12]}"
    try:
        admin = await psycopg.AsyncConnection.connect(POSTGRES_DSN, autocommit=True, connect_timeout=2)
    except psycopg.OperationalError as error:
        _unavailable("PostGIS", "REQUIRE_POSTGRES", error)
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        dsn = make_conninfo(POSTGRES_DSN, dbname=name)
        conn = await connect(dsn)
        try:
            await apply_migrations(conn)
        finally:
            await conn.close()
        yield dsn
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.close()


@pytest.fixture
async def pool(pg_dsn):
    async with open_pool(pg_dsn, max_size=4) as pool:
        yield pool


@pytest.fixture
async def mongo_db():
    """Временная база Mongo; клиент — как в сервисе, с ``tz_aware=True``."""
    client = AsyncMongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=2000)
    try:
        await client.admin.command("ping")
    except PyMongoError as error:
        await client.close()
        _unavailable("Mongo", "REQUIRE_MONGO", error)
    name = f"trading_test_{uuid4().hex[:12]}"
    try:
        yield client[name]
    finally:
        await client.drop_database(name)
        await client.close()


def pytest_collection_modifyitems(items):
    """Маркеры ``postgres``/``mongo`` — по фикстурам теста, чтобы не ставить их руками."""
    for item in items:
        names = set(getattr(item, "fixturenames", ()))
        if names & {"pg_dsn", "pool"}:
            item.add_marker(pytest.mark.postgres)
        if "mongo_db" in names:
            item.add_marker(pytest.mark.mongo)


def pytest_addoption(parser):
    parser.addoption(
        "--record",
        action="store_true",
        default=False,
        help="живые тесты НСПД: записать ответы в tests/fixtures/nspd/recorded/",
    )
