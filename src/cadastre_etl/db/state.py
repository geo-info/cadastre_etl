"""Состояние площадок: ``etl_state``, строка на площадку (коллекцию Mongo).

``checked_at`` — граница, до которой лоты площадки прочитаны и записаны; следующий
прогон выбирает всё, что позже ``checked_at − OVERLAP``. Двигается только
успешный прогон и только на момент своего *старта* (``run_started``): всё, что
пришло в Mongo во время прогона, попадёт в следующий. Упавший прогон пишет
ошибку и итог, но границу не трогает.

Два экземпляра сервиса не обрабатывают одну площадку разом: прогон держит
advisory-замок площадки на отдельном соединении.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from cadastre_etl.stats import SourceStats

_LOCK_SQL = "hashtext('cadastre_etl:' || %s)"


async def get_checked_at(conn: AsyncConnection, source: str) -> datetime | None:
    cur = await conn.execute("SELECT checked_at FROM etl_state WHERE source = %s", (source,))
    row = await cur.fetchone()
    return row[0] if row else None


async def all_states(conn: AsyncConnection) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM etl_state ORDER BY source")
        return await cur.fetchall()


@asynccontextmanager
async def source_lock(conn: AsyncConnection, source: str) -> AsyncIterator[bool]:
    """Замок площадки на время прогона; ``False`` — её уже обрабатывает другой.

    Соединение — отдельное и в autocommit: замок сессионный и держится между
    транзакциями прогона.
    """
    cur = await conn.execute(f"SELECT pg_try_advisory_lock({_LOCK_SQL})", (source,))
    acquired = bool((await cur.fetchone())[0])
    try:
        yield acquired
    finally:
        if acquired and not conn.closed:
            await conn.execute(f"SELECT pg_advisory_unlock({_LOCK_SQL})", (source,))


_SAVE = """
INSERT INTO etl_state (
    source, checked_at, last_started_at, last_finished_at, last_status, last_error,
    last_lots, last_numbers, last_fetched, last_cached, last_not_found, last_errors
) VALUES (%(source)s, %(checked_at)s, %(started)s, now(), %(status)s, %(error)s,
          %(lots)s, %(numbers)s, %(fetched)s, %(cached)s, %(not_found)s, %(errors)s)
ON CONFLICT (source) DO UPDATE SET
    checked_at = COALESCE(EXCLUDED.checked_at, etl_state.checked_at),
    last_started_at = EXCLUDED.last_started_at,
    last_finished_at = EXCLUDED.last_finished_at,
    last_status = EXCLUDED.last_status,
    last_error = EXCLUDED.last_error,
    last_lots = EXCLUDED.last_lots,
    last_numbers = EXCLUDED.last_numbers,
    last_fetched = EXCLUDED.last_fetched,
    last_cached = EXCLUDED.last_cached,
    last_not_found = EXCLUDED.last_not_found,
    last_errors = EXCLUDED.last_errors
"""


def _params(stats: SourceStats, started: datetime, checked_at: datetime | None, status: str) -> dict:
    return {
        "source": stats.source,
        "checked_at": checked_at,
        "started": started,
        "status": status,
        "error": stats.error if status == "failed" else None,
        "lots": stats.lots,
        "numbers": stats.numbers,
        "fetched": stats.fetched,
        "cached": stats.cached,
        "not_found": stats.not_found,
        "errors": stats.errors + stats.deferred,
    }


async def save_success(conn: AsyncConnection, run_started: datetime, stats: SourceStats) -> None:
    """Прогон удался: граница — на момент его старта."""
    await conn.execute(_SAVE, _params(stats, run_started, run_started, "ok"))


async def save_failure(conn: AsyncConnection, run_started: datetime, stats: SourceStats) -> None:
    """Прогон упал: ошибка и итог записаны, граница прежняя."""
    await conn.execute(_SAVE, _params(stats, run_started, None, "failed"))
