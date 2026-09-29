"""Выборка изменённых лотов из Mongo: граница по ``updated_at`` и ``detail_at``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from cadastre_etl.mongo import changed_lots, list_sources

T0 = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
MS = timedelta(milliseconds=1)


def lot(lot_id: str, updated_at: datetime, detail_at: datetime | None = None, **extra) -> dict:
    doc = {
        "source": "bep",
        "lot_id": lot_id,
        "lot_url": f"https://bep/{lot_id}",
        "created_at": updated_at,
        "updated_at": updated_at,
        **extra,
    }
    if detail_at is not None:
        doc["detail_at"] = detail_at
    return doc


async def ids(db, since, source="bep"):
    return sorted([doc["lot_id"] async for doc in changed_lots(db, source, since)])


async def test_граница_строго_позже(mongo_db):
    await mongo_db.bep.insert_many([lot("ровно", T0), lot("позже", T0 + MS), lot("раньше", T0 - MS)])
    assert await ids(mongo_db, T0) == ["позже"]


async def test_свежие_детали_при_старом_листинге(mongo_db):
    await mongo_db.bep.insert_many(
        [
            lot("детали", T0 - timedelta(days=3), detail_at=T0 + timedelta(minutes=1)),
            lot("старые_детали", T0 - timedelta(days=3), detail_at=T0 - timedelta(days=2)),
            lot("без_деталей", T0 - timedelta(days=3)),
        ]
    )
    assert await ids(mongo_db, T0) == ["детали"]


async def test_без_границы_все_лоты(mongo_db):
    await mongo_db.bep.insert_many([lot("1", T0), lot("2", T0 - timedelta(days=400))])
    assert await ids(mongo_db, None) == ["1", "2"]


async def test_даты_с_часовым_поясом_и_без_id(mongo_db):
    await mongo_db.bep.insert_one(lot("1", T0, detail_at=T0, detail={"Описание": "КН 50:20:0010101:1"}))
    [doc] = [doc async for doc in changed_lots(mongo_db, "bep", None)]
    assert doc["updated_at"].tzinfo is not None
    assert doc["updated_at"] == T0
    assert "_id" not in doc
    assert doc["detail"] == {"Описание": "КН 50:20:0010101:1"}


async def test_площадки_без_служебных_и_исключённых(mongo_db):
    for name in ("seltim", "bep", "tmp"):
        await mongo_db[name].insert_one({"x": 1})
    await mongo_db.create_collection("system.views")
    assert await list_sources(mongo_db, exclude=["tmp"]) == ["bep", "seltim"]
