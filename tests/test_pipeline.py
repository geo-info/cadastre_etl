"""Прогон целиком: Mongo и PostGIS настоящие, НСПД — подмена."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from pynspd.errors import BlockedIP

from cadastre_etl.conf import EtlSettings, MongoSettings, NspdSettings, PostgresSettings, Settings
from cadastre_etl.db.pool import connect
from cadastre_etl.pipeline import RETRY_QUEUE, UnknownSources, run_all
from tests.conftest import MONGO_URI
from tests.fakes import FakeNspd, http_error


@pytest.fixture
def settings(pg_dsn, mongo_db):
    def make(**etl) -> Settings:
        return Settings(
            mongo=MongoSettings(_env_file=None, uri=MONGO_URI, db=mongo_db.name),
            postgres=PostgresSettings(_env_file=None, dsn=pg_dsn),
            etl=EtlSettings(_env_file=None, **({"batch_size": 2} | etl)),
            nspd=NspdSettings(_env_file=None, rate=1000, cache="none", retries=1, breaker=3),
        )

    return make


def lot(lot_id, description, updated_at, source="bep", **extra):
    return {
        "source": source,
        "lot_id": lot_id,
        "lot_url": f"https://{source}/{lot_id}",
        "description": description,
        "created_at": updated_at,
        "updated_at": updated_at,
        **extra,
    }


async def query(dsn, sql, *params):
    async with await connect(dsn) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall() if cur.description else []


HOUR_AGO = datetime.now(UTC) - timedelta(hours=1)


async def test_первый_прогон_берёт_всё(settings, mongo_db, pg_dsn):
    await mongo_db.bep.insert_many(
        [
            lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO),
            lot("2", "ЗУ 50:20:0010101:2 и 50:20:0010101:1", HOUR_AGO - timedelta(days=300)),
            lot("3", "Автомобиль", HOUR_AGO),
        ]
    )
    before = datetime.now(UTC)
    result = await run_all(settings(), nspd=(nspd := FakeNspd()))
    after = datetime.now(UTC)

    [bep] = result.sources
    assert (bep.status, bep.lots, bep.numbers, bep.fetched) == ("ok", 3, 2, 2)
    assert result.exit_code == 0
    assert sorted(nspd.calls) == ["50:20:0010101:1", "50:20:0010101:2"]
    [(checked_at, status)] = await query(pg_dsn, "SELECT checked_at, last_status FROM etl_state")
    assert before <= checked_at <= after and status == "ok"
    assert await query(pg_dsn, "SELECT lot_id FROM lots ORDER BY 1") == [("1",), ("2",)]
    assert await query(pg_dsn, "SELECT count(*) FROM cadastral_objects WHERE status = 'found'") == [(2,)]


async def test_второй_прогон_читает_только_новое(settings, mongo_db, pg_dsn):
    await mongo_db.bep.insert_one(lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO))
    await run_all(settings(), nspd=FakeNspd())
    result = await run_all(settings(), nspd=FakeNspd())
    assert result.sources[0].lots == 0

    # Детали дописаны после прогона, листинг не менялся — лот подхватывается по detail_at.
    await mongo_db.bep.update_one(
        {"lot_id": "1"},
        {"$set": {"detail": {"Описание": "и ещё 50:20:0010101:9"}, "detail_at": datetime.now(UTC)}},
    )
    nspd = FakeNspd()
    result = await run_all(settings(), nspd=nspd)
    assert (result.sources[0].lots, result.sources[0].numbers) == (1, 2)
    assert await query(pg_dsn, "SELECT cad_num, found_in FROM lot_cadastral ORDER BY 1") == [
        ("50:20:0010101:1", "description"),
        ("50:20:0010101:9", "detail.Описание"),
    ]


async def test_перекрытие_перечитывает_край(settings, mongo_db, pg_dsn):
    """Лот, записанный в Mongo во время прошлого прогона, с меткой чуть раньше ``checked_at``."""
    await mongo_db.bep.insert_one(lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO))
    await run_all(settings(), nspd=FakeNspd())
    [(checked_at,)] = await query(pg_dsn, "SELECT checked_at FROM etl_state")
    await mongo_db.bep.insert_one(lot("2", "ЗУ 50:20:0010101:2", checked_at - timedelta(minutes=5)))
    result = await run_all(settings(overlap=timedelta(minutes=10)), nspd=FakeNspd())
    assert result.sources[0].lots == 1


async def test_упавшая_площадка_не_мешает_остальным(settings, mongo_db, pg_dsn):
    await mongo_db.bep.insert_one(lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO))
    # Документ без lot_id — запись лотов площадки упадёт.
    await mongo_db.seltim.insert_one(
        {"source": "seltim", "description": "ЗУ 50:20:0010101:2", "updated_at": HOUR_AGO}
    )
    result = await run_all(settings(), nspd=FakeNspd())
    by_source = {s.source: s for s in result.sources}
    assert by_source["bep"].status == "ok"
    assert by_source["seltim"].status == "failed"
    assert "KeyError" in by_source["seltim"].error
    assert result.exit_code == 1
    rows = dict(await query(pg_dsn, "SELECT source, checked_at IS NOT NULL FROM etl_state"))
    assert rows == {"bep": True, "seltim": False}
    [(error,)] = await query(pg_dsn, "SELECT last_error FROM etl_state WHERE source = 'seltim'")
    assert "lot_id" in error


async def test_повтор_прогона_без_дублей(settings, mongo_db, pg_dsn):
    await mongo_db.bep.insert_many([lot(str(n), f"ЗУ 50:20:0010101:{n}", HOUR_AGO) for n in range(5)])
    await run_all(settings(), nspd=FakeNspd())
    await query(pg_dsn, "UPDATE etl_state SET checked_at = NULL")
    await run_all(settings(), nspd=FakeNspd())
    counts = await query(
        pg_dsn,
        "SELECT (SELECT count(*) FROM lots), (SELECT count(*) FROM lot_cadastral), (SELECT count(*) FROM cadastral_objects)",
    )
    assert counts == [(5, 5, 5)]


async def test_номер_на_двух_площадках_один_запрос(settings, mongo_db):
    await mongo_db.bep.insert_one(lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO))
    await mongo_db.seltim.insert_one(lot("7", "ЗУ 50:20:0010101:1", HOUR_AGO, source="seltim"))
    nspd = FakeNspd(delay=0.01)
    result = await run_all(settings(), nspd=nspd)
    assert nspd.calls == ["50:20:0010101:1"]
    assert sorted((s.fetched, s.cached) for s in result.sources) == [(0, 1), (1, 0)]


async def test_нспд_недоступна_потом_очередь(settings, mongo_db, pg_dsn):
    await mongo_db.bep.insert_many([lot(str(n), f"ЗУ 50:20:0010101:{n}", HOUR_AGO) for n in range(6)])
    blocked = FakeNspd(default=lambda cad_num: http_error(BlockedIP, 403))
    result = await run_all(settings(), nspd=blocked)
    [bep] = result.sources
    assert bep.status == "ok" and result.breaker_open and result.exit_code == 3
    assert (bep.errors, bep.deferred) == (3, 3)
    assert len(blocked.calls) == 3
    assert await query(pg_dsn, "SELECT count(*) FROM lots") == [(6,)]
    assert await query(
        pg_dsn, "SELECT count(*) FROM cadastral_objects WHERE status IN ('pending', 'error')"
    ) == [(6,)]

    # Следующий прогон: лоты не менялись, номера берутся из очереди повторов. Номера с
    # ошибкой ждут своего срока (первая ошибка — сразу), отложенные — сразу.
    result = await run_all(settings(), nspd=FakeNspd())
    assert result.sources[0].lots == 0
    assert result.retry.source == RETRY_QUEUE
    assert (result.retry.numbers, result.retry.fetched) == (6, 6)
    assert result.exit_code == 0
    assert await query(pg_dsn, "SELECT count(*) FROM cadastral_objects WHERE status = 'found'") == [(6,)]


async def test_неизвестная_площадка(settings, mongo_db):
    await mongo_db.bep.insert_one(lot("1", "x", HOUR_AGO))
    with pytest.raises(UnknownSources, match="nope"):
        await run_all(settings(), ["bep", "nope"], nspd=FakeNspd())


async def test_только_названные_площадки(settings, mongo_db):
    await mongo_db.bep.insert_one(lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO))
    await mongo_db.seltim.insert_one(lot("2", "ЗУ 50:20:0010101:2", HOUR_AGO, source="seltim"))
    result = await run_all(settings(), ["seltim"], nspd=FakeNspd())
    assert [s.source for s in result.sources] == ["seltim"]


async def test_первый_прогон_с_даты(settings, mongo_db):
    await mongo_db.bep.insert_many(
        [
            lot("старый", "ЗУ 50:20:0010101:1", datetime(2025, 1, 1, tzinfo=UTC)),
            lot("новый", "ЗУ 50:20:0010101:2", datetime(2026, 6, 1, tzinfo=UTC)),
        ]
    )
    result = await run_all(settings(initial_since=date(2026, 1, 1)), nspd=FakeNspd())
    assert result.sources[0].lots == 1


async def test_журнал_хода(settings, mongo_db, caplog, monkeypatch):
    """По журналу видно, что прогон идёт: подключения, пачки, пульс, итог площадки."""
    import cadastre_etl.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "PROGRESS_EVERY", 0.01)
    await mongo_db.bep.insert_many([lot(str(n), f"ЗУ 50:20:0010101:{n}", HOUR_AGO) for n in range(3)])
    with caplog.at_level("INFO", logger="cadastre_etl"):
        await run_all(settings(), nspd=FakeNspd(delay=0.03))
    text = caplog.text
    assert "PostGIS: подключение к localhost:5432/cadastre_test_" in text
    assert "etl:etl" not in text
    assert "площадок: 1 (bep)" in text
    assert "bep: начало, читаю лоты после начала (первый прогон)" in text
    assert "bep: пачка из 2 лотов, с номерами 2; новых номеров 2 — спрашиваю НСПД" in text
    assert "bep: прочитано лотов 3, номеров 3 (из сети 3," in text
    assert "идёт прогон: площадок готово 0 из 1; НСПД — запросов" in text
    assert "bep: готово за" in text


async def test_поля_торгов_и_полное_перечитывание(settings, mongo_db, pg_dsn):
    """Лот, загруженный раньше, получает новые поля при ``run --full``."""
    await mongo_db.bep.insert_one(lot("1", "ЗУ 50:20:0010101:1", HOUR_AGO))
    await run_all(settings(), nspd=FakeNspd())
    await mongo_db.bep.update_one(
        {"lot_id": "1"},
        {
            "$set": {
                "trade_id": "10840",
                "trade_type": "ОАОФ",
                "auction_name": "Открытый аукцион",
                "winner": "ООО «Ромашка»",
                "bids_end": "01.10.2026 10:00",
                "auction_date": "02.10.2026",
            }
        },
    )
    assert (await run_all(settings(), nspd=FakeNspd())).sources[0].lots == 0

    result = await run_all(settings(), nspd=FakeNspd(), full=True)
    assert result.sources[0].lots == 1
    assert await query(
        pg_dsn,
        "SELECT trade_id, trade_type, auction_name, winner, bids_end, auction_date FROM lot_objects",
    ) == [("10840", "ОАОФ", "Открытый аукцион", "ООО «Ромашка»", "01.10.2026 10:00", "02.10.2026")]
