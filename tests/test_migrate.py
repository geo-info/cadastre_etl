"""Миграции PostGIS: схема создаётся, повтор ничего не делает, SRID геометрии — 4326."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from cadastre_etl.db.migrate import apply_migrations, migration_files
from cadastre_etl.db.pool import connect


async def test_схема_создана(pool):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1"
        )
        tables = {row[0] for row in await cur.fetchall()}
        assert {"lots", "cadastral_objects", "lot_cadastral", "etl_state", "schema_migrations"} <= tables
        cur = await conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'cadastral_objects_geom_gist'"
        )
        assert "gist" in (await cur.fetchone())[0]
        cur = await conn.execute(
            "SELECT srid, type FROM geometry_columns WHERE f_table_name = 'cadastral_objects'"
        )
        assert set(await cur.fetchall()) == {(4326, "GEOMETRY"), (4326, "POINT")}


async def test_повтор_ничего_не_применяет(pg_dsn):
    conn = await connect(pg_dsn)
    try:
        assert await apply_migrations(conn) == []
        cur = await conn.execute("SELECT count(*) FROM schema_migrations")
        assert (await cur.fetchone())[0] == len(migration_files())
    finally:
        await conn.close()


async def test_параллельные_миграции_не_мешают_друг_другу(pg_dsn):
    async with await connect(pg_dsn) as conn:
        await conn.execute(
            "DROP VIEW lot_objects; DROP TABLE lot_cadastral, lots, cadastral_objects, etl_state"
        )
        await conn.execute("DELETE FROM schema_migrations")
    first, second = await connect(pg_dsn), await connect(pg_dsn)
    try:
        results = await asyncio.gather(apply_migrations(first), apply_migrations(second))
    finally:
        await first.close()
        await second.close()
    assert sorted(results, key=len) == [[], ["0001_init"]]


async def test_геометрия_только_в_4326(pool):
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.InvalidParameterValue):
            await conn.execute(
                "INSERT INTO cadastral_objects (cad_num, geom) VALUES ('1:1:1:1', ST_GeomFromText('POINT(0 0)', 3857))"
            )
