"""Подключение к PostGIS — одно место, где создаётся пул.

Соединения пула — в режиме autocommit: транзакции везде явные
(``async with conn.transaction()``), и по коду видно, что пишется вместе.
Без autocommit psycopg неявно открывает транзакцию на первом запросе, и
advisory-замок площадки, взятый на таком соединении, жил бы до её конца.

Закрывает пул тот, кто его открыл: ``async with open_pool(dsn) as pool``.

Таймаут подключения — ``CONNECT_TIMEOUT``: без него недоступный сервер держал бы
прогон молча, пока не сдастся TCP.
"""

from __future__ import annotations

from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import AsyncConnectionPool

#: Секунд на подключение к PostGIS (если в DSN не задан свой ``connect_timeout``).
CONNECT_TIMEOUT = 10


def _kwargs(dsn: str) -> dict:
    kwargs: dict = {"autocommit": True}
    if "connect_timeout" not in conninfo_to_dict(dsn):
        kwargs["connect_timeout"] = CONNECT_TIMEOUT
    return kwargs


def describe_dsn(dsn: str) -> str:
    """``host:port/база`` для журнала — без пользователя и пароля."""
    params = conninfo_to_dict(dsn)
    return f"{params.get('host', 'localhost')}:{params.get('port', 5432)}/{params.get('dbname', '')}"


def open_pool(dsn: str, *, max_size: int = 10) -> AsyncConnectionPool:
    """Пул к ``dsn``; открывается входом в ``async with``."""
    return AsyncConnectionPool(dsn, min_size=1, max_size=max_size, kwargs=_kwargs(dsn), open=False)


async def connect(dsn: str) -> AsyncConnection:
    """Отдельное соединение вне пула (миграции, замки площадок)."""
    return await AsyncConnection.connect(dsn, **_kwargs(dsn))
