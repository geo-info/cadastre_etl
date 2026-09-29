"""Чтение лотов из Mongo trading_platform.

У каждой площадки своя коллекция, названная её именем (``trading.bep``,
``trading.seltim``); площадка = коллекция, список площадок — список коллекций
без служебных ``system.*``.

Изменённые лоты — те, у кого ``updated_at`` или ``detail_at`` позже границы.
Оба поля: номер часто появляется только в ``detail``, который второй проход
trading_platform дописывает позже листинга, не трогая ``updated_at``; а
изменившийся лот (``updated_at`` сдвигается только при изменении на площадке)
мог получить новое описание.

Клиент — с ``tz_aware=True``: иначе pymongo отдаёт наивные даты, и сравнение с
``checked_at`` из PostGIS (``timestamptz``) пришлось бы чинить в каждом месте.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

#: Поля документа, которые нужны ETL: ключ, то, что копируется в ``lots``, и
#: то, где ищутся номера. ``detail`` бывает большим, но номера бывают только в нём.
LOT_FIELDS = (
    "source",
    "lot_id",
    "lot_url",
    "trade_url",
    "trade_number",
    "lot_num",
    "description",
    "status",
    "is_active",
    "price",
    "price_value",
    "bids_end_at",
    "auction_at",
    "debtor",
    "organizer",
    "detail",
    "created_at",
    "updated_at",
    "detail_at",
)


def create_client(uri: str) -> AsyncMongoClient:
    """Клиент Mongo; соединение открывается лениво, на первом запросе."""
    return AsyncMongoClient(uri, tz_aware=True)


async def list_sources(db: AsyncDatabase, exclude: list[str] | tuple[str, ...] = ()) -> list[str]:
    """Площадки — коллекции базы без служебных и исключённых, по алфавиту."""
    names = await db.list_collection_names()
    return sorted(name for name in names if not name.startswith("system.") and name not in exclude)


def changed_filter(since: datetime | None) -> dict[str, Any]:
    """Фильтр «пришёл или изменился позже ``since``»; ``None`` — все лоты."""
    if since is None:
        return {}
    return {"$or": [{"updated_at": {"$gt": since}}, {"detail_at": {"$gt": since}}]}


async def changed_lots(
    db: AsyncDatabase, source: str, since: datetime | None, *, batch_size: int = 500
) -> AsyncIterator[dict[str, Any]]:
    """Документы лотов площадки ``source``, изменившиеся позже ``since``."""
    projection = {"_id": 0, **dict.fromkeys(LOT_FIELDS, 1)}
    cursor = db[source].find(changed_filter(since), projection, batch_size=batch_size)
    async for doc in cursor:
        yield doc
