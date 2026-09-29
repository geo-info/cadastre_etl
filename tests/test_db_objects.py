"""Запись исходов НСПД: колонки и геометрия, ошибка не затирает найденное, очередь повторов."""

from __future__ import annotations

import json
from pathlib import Path

from cadastre_etl.db.objects import due_for_retry, write_outcomes
from cadastre_etl.nspd.client import RawAnswer
from cadastre_etl.nspd.parse import Outcome, parse_answer

FIXTURES = Path(__file__).parent / "fixtures" / "nspd"
CN = "50:20:0010101:123"


def parsed(name: str, cad_num: str) -> Outcome:
    data = json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    return parse_answer(cad_num, RawAnswer(200, data, False))


def error(cad_num: str = CN) -> Outcome:
    return Outcome(cad_num, "error", error="PynspdServerError: Серверная ошибка")


async def one(conn, sql, *params):
    cur = await conn.execute(sql, params)
    return await cur.fetchone()


async def test_найденный_объект(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [parsed("land_plot", CN)])
        row = await one(
            conn,
            "SELECT status, kind, kind_label, area_m2, cadastral_value, registered_at, attrs->>'cost_index',"
            " ST_SRID(geom), GeometryType(geom), ST_X(ST_Centroid(geom)), fetched_at IS NOT NULL"
            " FROM cadastral_objects WHERE cad_num = %s",
            CN,
        )
        assert row[:3] == ("found", "land_plot", "Земельный участок")
        assert (float(row[3]), float(row[4])) == (1500.0, 2345678.9)
        assert str(row[5]) == "2005-03-15"
        assert row[6] == "1563.79"
        assert row[7:9] == (4326, "MULTIPOLYGON")
        assert abs(row[9] - 37.60025) < 1e-6
        assert row[10]


async def test_ошибка_не_затирает_найденное(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [parsed("land_plot", CN)])
        await write_outcomes(conn, [error()])
        row = await one(
            conn,
            "SELECT status, area_m2, geom IS NOT NULL, attempts, last_error FROM cadastral_objects WHERE cad_num = %s",
            CN,
        )
        assert row[0] == "found" and float(row[1]) == 1500.0 and row[2]
        assert row[3:] == (1, "PynspdServerError: Серверная ошибка")
        assert await due_for_retry(conn, 10) == []


async def test_ошибки_нового_номера_и_паузы(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [error()])
        assert await due_for_retry(conn, 10) == [CN]
        await write_outcomes(conn, [error()])
        await write_outcomes(conn, [error()])
        row = await one(
            conn,
            "SELECT status, attempts, round(extract(epoch FROM next_attempt_at - attempted_at) / 60)"
            " FROM cadastral_objects WHERE cad_num = %s",
            CN,
        )
        assert (row[0], row[1], float(row[2])) == ("error", 3, 45.0)
        assert await due_for_retry(conn, 10) == []
        for _ in range(10):
            await write_outcomes(conn, [error()])
        hours = await one(
            conn, "SELECT extract(epoch FROM next_attempt_at - attempted_at) / 3600 FROM cadastral_objects"
        )
        assert float(hours[0]) == 24.0


async def test_успех_после_ошибок_сбрасывает_счётчик(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [error(), error()])
        await write_outcomes(conn, [parsed("land_plot", CN)])
        row = await one(conn, "SELECT status, attempts, last_error, next_attempt_at FROM cadastral_objects")
        assert row == ("found", 0, None, None)


async def test_объект_снят_с_учёта(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [parsed("land_plot", CN)])
        await write_outcomes(conn, [Outcome(CN, "not_found")])
        row = await one(conn, "SELECT status, geom, area_m2, attrs FROM cadastral_objects")
        assert row == ("not_found", None, None, None)


async def test_помещение_точка_геокодера_отдельно(pool):
    cad_num = "77:01:0001001:2345"
    async with pool.connection() as conn:
        await write_outcomes(conn, [parsed("room_geocoder", cad_num)])
        row = await one(conn, "SELECT status, kind, geom, ST_SRID(geom_approx) FROM cadastral_objects")
        assert row == ("found_no_geom", "room", None, 4326)


async def test_отложенный_номер_не_трогает_строку(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [parsed("land_plot", CN), Outcome("50:20:0010101:7", "pending")])
        await write_outcomes(conn, [Outcome(CN, "pending")])
        assert (await one(conn, "SELECT status FROM cadastral_objects WHERE cad_num = %s", CN))[0] == "found"
        assert await due_for_retry(conn, 10) == ["50:20:0010101:7"]


async def test_очередь_по_лимиту(pool):
    async with pool.connection() as conn:
        await write_outcomes(conn, [Outcome(f"50:20:0010101:{n}", "pending") for n in range(5)])
        assert len(await due_for_retry(conn, 3)) == 3
