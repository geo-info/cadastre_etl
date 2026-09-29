"""Запись лотов, связей и состояния площадок в PostGIS."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from cadastre_etl.db.lots import write_lots
from cadastre_etl.db.pool import connect
from cadastre_etl.db.state import all_states, get_checked_at, save_failure, save_success, source_lock
from cadastre_etl.extract import extract_lot
from cadastre_etl.stats import SourceStats

T0 = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)


def doc(lot_id: str, description: str | None, **extra) -> dict:
    return {
        "source": "bep",
        "lot_id": lot_id,
        "lot_url": f"https://bep/{lot_id}",
        "description": description,
        "created_at": T0,
        "updated_at": T0,
        **extra,
    }


async def write(conn, *docs):
    return await write_lots(conn, "bep", [(d, extract_lot(d)) for d in docs])


async def rows(conn, sql, *params):
    cur = await conn.execute(sql, params)
    return await cur.fetchall()


async def test_повторная_запись_не_дубль(pool):
    async with pool.connection() as conn:
        await write(conn, doc("1", "ЗУ 50:20:0010101:1", price="100 000,00", price_value=100000.0))
        result = await write(conn, doc("1", "ЗУ 50:20:0010101:1", status="Завершены", is_active=False))
        assert result.lots == 1 and result.numbers == {"50:20:0010101:1"}
        assert await rows(conn, "SELECT lot_id, status, is_active, price_value FROM lots") == [
            ("1", "Завершены", False, None)
        ]
        assert await rows(conn, "SELECT cad_num, found_in FROM lot_cadastral") == [
            ("50:20:0010101:1", "description")
        ]


async def test_новый_номер_заводится_заготовкой(pool):
    async with pool.connection() as conn:
        await write(conn, doc("1", "КН 50:20:0010101:1", detail={"Описание": "и 50:20:0010101:2"}))
        assert await rows(conn, "SELECT cad_num, status FROM cadastral_objects ORDER BY 1") == [
            ("50:20:0010101:1", "pending"),
            ("50:20:0010101:2", "pending"),
        ]
        assert await rows(conn, "SELECT found_in FROM lot_cadastral ORDER BY cad_num") == [
            ("description",),
            ("detail.Описание",),
        ]


async def test_номер_пропал_из_описания(pool):
    async with pool.connection() as conn:
        await write(conn, doc("1", "КН 50:20:0010101:1 и 50:20:0010101:2"))
        await write(conn, doc("1", "КН 50:20:0010101:2"))
        assert await rows(conn, "SELECT cad_num FROM lot_cadastral") == [("50:20:0010101:2",)]
        assert len(await rows(conn, "SELECT 1 FROM cadastral_objects")) == 2


async def test_номера_пропали_совсем_лот_удалён(pool):
    async with pool.connection() as conn:
        await write(conn, doc("1", "КН 50:20:0010101:1"), doc("2", "КН 50:20:0010101:1"))
        result = await write(conn, doc("1", "Автомобиль"))
        assert result.deleted == 1 and result.lots == 0
        assert await rows(conn, "SELECT lot_id FROM lots") == [("2",)]
        assert await rows(conn, "SELECT lot_id FROM lot_cadastral") == [("2",)]


async def test_лот_только_с_кварталом(pool):
    async with pool.connection() as conn:
        await write(conn, doc("1", "в кадастровом квартале 63:17:0519006"))
        assert await rows(conn, "SELECT cad_quarters FROM lots") == [(["63:17:0519006"],)]
        assert await rows(conn, "SELECT 1 FROM lot_cadastral") == []


async def test_лот_без_номеров_не_пишется(pool):
    async with pool.connection() as conn:
        result = await write(conn, doc("1", "Автомобиль"))
        assert result.lots == 0 and result.deleted == 0
        assert await rows(conn, "SELECT 1 FROM lots") == []


async def test_состояние_успех_и_падение(pool):
    stats = SourceStats("bep", lots=3, numbers=2, fetched=1, cached=1)
    async with pool.connection() as conn:
        assert await get_checked_at(conn, "bep") is None
        await save_success(conn, T0, stats)
        assert await get_checked_at(conn, "bep") == T0

        failed = SourceStats("bep", status="failed", lots=1, error="ServerSelectionTimeoutError")
        await save_failure(conn, T0 + timedelta(hours=1), failed)
        assert await get_checked_at(conn, "bep") == T0
        [state] = await all_states(conn)
        assert state["last_status"] == "failed"
        assert state["last_error"] == "ServerSelectionTimeoutError"
        assert state["last_started_at"] == T0 + timedelta(hours=1)
        assert state["last_lots"] == 1


async def test_первый_прогон_упал_граница_пустая(pool):
    async with pool.connection() as conn:
        await save_failure(conn, T0, SourceStats("new", status="failed", error="boom"))
        assert await get_checked_at(conn, "new") is None


async def test_замок_площадки(pg_dsn):
    first, second = await connect(pg_dsn), await connect(pg_dsn)
    try:
        async with source_lock(first, "bep") as got:
            assert got
            async with source_lock(second, "bep") as other:
                assert not other
            async with source_lock(second, "seltim") as other:
                assert other
        async with source_lock(second, "bep") as again:
            assert again
    finally:
        await first.close()
        await second.close()
