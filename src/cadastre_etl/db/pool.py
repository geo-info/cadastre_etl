"""Подключение к PostGIS — одно место, где создаётся пул.

Соединения пула — в режиме autocommit: транзакции везде явные
(``async with conn.transaction()``), и по коду видно, что пишется вместе.
Без autocommit psycopg неявно открывает транзакцию на первом запросе, и
advisory-замок площадки, взятый на таком соединении, жил бы до её конца.

Закрывает пул тот, кто его открыл: ``async with open_pool(dsn) as pool``.
"""

from __future__ import annotations

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool


def open_pool(dsn: str, *, max_size: int = 10) -> AsyncConnectionPool:
    """Пул к ``dsn``; открывается входом в ``async with``."""
    return AsyncConnectionPool(dsn, min_size=1, max_size=max_size, kwargs={"autocommit": True}, open=False)


async def connect(dsn: str) -> AsyncConnection:
    """Отдельное соединение вне пула (миграции, замки площадок)."""
    return await AsyncConnection.connect(dsn, autocommit=True)
