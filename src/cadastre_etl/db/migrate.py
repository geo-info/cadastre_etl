"""SQL-миграции: файлы ``migrations/NNNN_имя.sql``, применённые — в ``schema_migrations``.

Почему не alembic: ORM-моделей нет, alembic тянул бы SQLAlchemy ради того,
чтобы выполнить наш же SQL, а autogenerate без моделей не работает. Файлы
читаются глазами и применяются в тестах той же функцией, что в работе.

Каждая миграция — своя транзакция; весь проход — под advisory-замком, так что
два одновременно стартовавших экземпляра не применят одно дважды. Откатов нет:
ошибку исправляет следующая миграция.
"""

from __future__ import annotations

import logging
from importlib.resources import files

from psycopg import AsyncConnection

log = logging.getLogger(__name__)

#: Ключ advisory-замка миграций (произвольное число, общее для всех экземпляров).
LOCK_KEY = 0x0CAD_0001


def migration_files() -> list[tuple[str, str]]:
    """(версия, SQL) по порядку имён; версия — имя файла без ``.sql``."""
    folder = files("cadastre_etl.db") / "migrations"
    names = sorted(item.name for item in folder.iterdir() if item.name.endswith(".sql"))
    return [(name.removesuffix(".sql"), (folder / name).read_text(encoding="utf-8")) for name in names]


async def apply_migrations(conn: AsyncConnection) -> list[str]:
    """Применить недостающие миграции; вернуть их версии.

    Соединение — в autocommit (``db.pool.connect``): замок сессионный и должен
    пережить транзакции миграций.
    """
    await conn.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
    try:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        cur = await conn.execute("SELECT version FROM schema_migrations")
        done = {row[0] for row in await cur.fetchall()}
        applied = []
        for version, sql in migration_files():
            if version in done:
                continue
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
            log.info("миграция %s применена", version)
            applied.append(version)
        return applied
    finally:
        await conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
