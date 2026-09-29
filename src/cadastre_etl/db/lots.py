"""Запись лотов и связей лот ↔ кадастровый номер.

Всё идемпотентно: лот — upsert по ``(source, lot_id)``; связи лота заменяются
набором из текущего документа (номер пропал из описания — связь удаляется, новый
— добавляется); номера заводятся в ``cadastral_objects`` заготовками ``pending``
в той же транзакции, чтобы внешний ключ был цел, а прогон, упавший до ответа
НСПД, оставил номер в очереди повторов. Повторная запись того же лота — не дубль.

В ``lots`` попадают только лоты с номером или кварталом. Лот, у которого их не
стало, удаляется (связи — каскадом); кадастровые объекты остаются: на них могут
ссылаться другие лоты, а сведения НСПД от этого не устаревают.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import astuple, dataclass, fields
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection

from cadastre_etl.extract import Extracted


@dataclass(frozen=True)
class LotRow:
    """Строка ``lots`` из документа Mongo; порядок полей = порядок колонок."""

    source: str
    lot_id: str
    lot_url: str | None
    trade_url: str | None
    trade_number: str | None
    lot_num: str | None
    description: str | None
    status: str | None
    is_active: bool | None
    price: str | None
    price_value: float | None
    bids_end_at: datetime | None
    auction_at: datetime | None
    debtor: str | None
    organizer: str | None
    cad_quarters: list[str]
    mongo_created_at: datetime | None
    mongo_updated_at: datetime | None
    mongo_detail_at: datetime | None

    @classmethod
    def from_doc(cls, source: str, doc: Mapping[str, Any], quarters: list[str]) -> LotRow:
        def text(name: str) -> str | None:
            value = doc.get(name)
            return None if value is None else str(value)

        price_value = doc.get("price_value")
        is_active = doc.get("is_active")
        return cls(
            source=source,
            lot_id=str(doc["lot_id"]),
            lot_url=text("lot_url"),
            trade_url=text("trade_url"),
            trade_number=text("trade_number"),
            lot_num=text("lot_num"),
            description=text("description"),
            status=text("status"),
            is_active=is_active if isinstance(is_active, bool) else None,
            price=text("price"),
            price_value=price_value if isinstance(price_value, int | float) else None,
            bids_end_at=_datetime(doc.get("bids_end_at")),
            auction_at=_datetime(doc.get("auction_at")),
            debtor=text("debtor"),
            organizer=text("organizer"),
            cad_quarters=list(quarters),
            mongo_created_at=_datetime(doc.get("created_at")),
            mongo_updated_at=_datetime(doc.get("updated_at")),
            mongo_detail_at=_datetime(doc.get("detail_at")),
        )


def _datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


_COLUMNS = [f.name for f in fields(LotRow)]
_UPSERT_LOT = (
    f"INSERT INTO lots ({', '.join(_COLUMNS)}, loaded_at) "
    f"VALUES ({', '.join(['%s'] * len(_COLUMNS))}, now()) "
    "ON CONFLICT (source, lot_id) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in _COLUMNS[2:])
    + ", loaded_at = now()"
)


@dataclass
class LotsWritten:
    lots: int = 0
    deleted: int = 0
    numbers: set[str] | None = None


async def write_lots(
    conn: AsyncConnection, source: str, items: Sequence[tuple[Mapping[str, Any], Extracted]]
) -> LotsWritten:
    """Записать пачку документов площадки с тем, что в них нашлось.

    Возвращает, сколько лотов записано и удалено, и все номера пачки.
    """
    keep = [(doc, found) for doc, found in items if found]
    drop = [str(doc["lot_id"]) for doc, found in items if not found]
    # Сортировка — чтобы параллельные площадки брали строки одних номеров в
    # одном порядке и не ловили взаимную блокировку.
    numbers = sorted({number for _, found in keep for number in found.numbers})
    rows = [LotRow.from_doc(source, doc, found.quarters) for doc, found in keep]

    async with conn.transaction():
        async with conn.cursor() as cur:
            if numbers:
                await cur.executemany(
                    "INSERT INTO cadastral_objects (cad_num) VALUES (%s) ON CONFLICT DO NOTHING",
                    [(n,) for n in numbers],
                )
            if rows:
                await cur.executemany(_UPSERT_LOT, [astuple(row) for row in rows])
                await cur.executemany(
                    "DELETE FROM lot_cadastral WHERE source = %s AND lot_id = %s AND NOT (cad_num = ANY(%s))",
                    [
                        (source, row.lot_id, list(found.numbers))
                        for row, (_, found) in zip(rows, keep, strict=True)
                    ],
                )
                links = [
                    (source, row.lot_id, number, path)
                    for row, (_, found) in zip(rows, keep, strict=True)
                    for number, path in found.numbers.items()
                ]
                if links:
                    await cur.executemany(
                        "INSERT INTO lot_cadastral (source, lot_id, cad_num, found_in) VALUES (%s, %s, %s, %s) "
                        "ON CONFLICT (source, lot_id, cad_num) DO UPDATE SET found_in = EXCLUDED.found_in",
                        links,
                    )
            deleted = 0
            if drop:
                await cur.execute("DELETE FROM lots WHERE source = %s AND lot_id = ANY(%s)", (source, drop))
                deleted = cur.rowcount
    return LotsWritten(lots=len(rows), deleted=deleted, numbers=set(numbers))
