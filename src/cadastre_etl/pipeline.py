"""Прогон: очередь повторов, затем площадки параллельно, у каждой — своё состояние.

Прогон площадки ``s`` (подробно — спека, раздел «Состояние»):

1. advisory-замок площадки на отдельном соединении; занят — ``skipped``;
2. ``run_started = now()`` — до чтения Mongo;
3. граница ``checked_at − OVERLAP``; первый прогон — ``INITIAL_SINCE`` или все лоты;
4. пачками: извлечь номера → записать лоты, связи и заготовки объектов → спросить
   НСПД (общий резолвер прогона) → записать исходы;
5. успех — ``checked_at = run_started``; исключение — ошибка в ``etl_state``,
   граница прежняя, остальные площадки продолжают.

Ошибки НСПД площадку не роняют: номер остаётся в очереди повторов
(``pending``/``error``), а она разбирается в начале следующего прогона — даже
если лот больше не изменится.

Журнал (INFO) — чтобы по нему было видно, что прогон идёт, а не висит: куда
подключились, сколько площадок, по каждой — граница, каждая пачка и итог, и
раз в ``PROGRESS_EVERY`` секунд — пульс: сколько номеров спрошено у НСПД, сколько
ждут и сколько площадок готово. Первый прогон по всем площадкам при 2 запросах в
секунду может идти часами — пульс это показывает.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from datetime import time as dtime
from typing import Any

from psycopg_pool import AsyncConnectionPool
from pymongo.asynchronous.database import AsyncDatabase

from cadastre_etl.conf import Settings
from cadastre_etl.db.lots import write_lots
from cadastre_etl.db.migrate import apply_migrations
from cadastre_etl.db.objects import due_for_retry, write_outcomes
from cadastre_etl.db.pool import connect, describe_dsn, open_pool
from cadastre_etl.db.state import get_checked_at, save_failure, save_success, source_lock
from cadastre_etl.extract import extract_lot
from cadastre_etl.mongo import changed_lots, create_client, describe_uri, list_sources
from cadastre_etl.nspd.client import NspdClient
from cadastre_etl.nspd.resolver import Resolved, Resolver, Searcher
from cadastre_etl.stats import SourceStats

log = logging.getLogger(__name__)

RETRY_QUEUE = "(повторы)"
#: Как часто писать в журнал пульс прогона, с.
PROGRESS_EVERY = 15.0


class UnknownSources(ValueError):
    """Названы площадки, которых нет в Mongo."""


@dataclass
class Context:
    settings: Settings
    pool: AsyncConnectionPool
    mongo: AsyncDatabase
    resolver: Resolver


@dataclass
class RunResult:
    sources: list[SourceStats] = field(default_factory=list)
    retry: SourceStats | None = None
    breaker_open: bool = False

    @property
    def failed(self) -> list[SourceStats]:
        rows = [*self.sources, *([self.retry] if self.retry else [])]
        return [s for s in rows if s.status == "failed"]

    @property
    def exit_code(self) -> int:
        """0 — всё хорошо; 1 — площадка упала; 3 — площадки прошли, но НСПД недоступна."""
        if self.failed:
            return 1
        return 3 if self.breaker_open else 0


def count(stats: SourceStats, resolved: Resolved) -> None:
    """Учесть исход номера ровно в одной колонке отчёта."""
    outcome = resolved.outcome
    if outcome.status == "pending":
        stats.deferred += 1
    elif outcome.status == "error":
        stats.errors += 1
    elif outcome.status == "not_found":
        stats.not_found += 1
    elif resolved.shared or outcome.from_cache:
        stats.cached += 1
    else:
        stats.fetched += 1


async def resolve_and_write(ctx: Context, numbers: Sequence[str], stats: SourceStats) -> None:
    """Спросить НСПД о номерах и записать исходы.

    Исход, полученный для другой площадки (``shared``), записывает та площадка;
    повторная запись была бы безвредна, но ошибку посчитала бы дважды.
    """
    results = await ctx.resolver.resolve_many(list(numbers))
    for resolved in results.values():
        count(stats, resolved)
    own = [r.outcome for r in results.values() if not r.shared]
    if own:
        async with ctx.pool.connection() as conn:
            await write_outcomes(conn, own)


async def _process_batch(
    ctx: Context, source: str, batch: list[Mapping[str, Any]], counted: set[str], stats: SourceStats
) -> None:
    items = [(doc, extract_lot(doc)) for doc in batch]
    async with ctx.pool.connection() as conn:
        written = await write_lots(conn, source, items)
    new = sorted((written.numbers or set()) - counted)
    counted.update(new)
    stats.numbers = len(counted)
    log.info(
        "%s: пачка из %d лотов, с номерами %d; новых номеров %d — спрашиваю НСПД",
        source,
        len(batch),
        written.lots,
        len(new),
    )
    await resolve_and_write(ctx, new, stats)
    log.info(
        "%s: прочитано лотов %d, номеров %d (из сети %d, из кэша %d, не найдено %d, ошибок %d, отложено %d)",
        source,
        stats.lots,
        stats.numbers,
        stats.fetched,
        stats.cached,
        stats.not_found,
        stats.errors,
        stats.deferred,
    )


def _initial_since(settings: Settings) -> datetime | None:
    day = settings.etl.initial_since
    return None if day is None else datetime.combine(day, dtime.min, tzinfo=UTC)


async def run_source(ctx: Context, source: str) -> SourceStats:
    stats = SourceStats(source)
    started = time.monotonic()
    try:
        lock_conn = await connect(ctx.settings.postgres.dsn)
    except Exception as error:
        log.exception("%s: нет соединения для замка площадки", source)
        stats.status, stats.error = "failed", f"{type(error).__name__}: {error}"
        return stats
    try:
        async with source_lock(lock_conn, source) as acquired:
            if not acquired:
                log.warning("%s: площадку обрабатывает другой экземпляр, пропускаем", source)
                stats.status = "skipped"
                return stats
            run_started = datetime.now(UTC)
            try:
                await _run_locked(ctx, source, run_started, stats)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.exception("%s: прогон упал", source)
                stats.status = "failed"
                stats.error = f"{type(error).__name__}: {error}".strip()
                try:
                    async with ctx.pool.connection() as conn:
                        await save_failure(conn, run_started, stats)
                except Exception:
                    log.exception("%s: не удалось записать ошибку в etl_state", source)
    finally:
        await lock_conn.close()
        stats.seconds = time.monotonic() - started
    if stats.status == "ok":
        log.info(
            "%s: готово за %.0f с — лотов %d, номеров %d", source, stats.seconds, stats.lots, stats.numbers
        )
    return stats


async def _run_locked(ctx: Context, source: str, run_started: datetime, stats: SourceStats) -> None:
    async with ctx.pool.connection() as conn:
        checked_at = await get_checked_at(conn, source)
    since = checked_at - ctx.settings.etl.overlap if checked_at else _initial_since(ctx.settings)
    log.info(
        "%s: начало, читаю лоты после %s", source, since.isoformat() if since else "начала (первый прогон)"
    )

    batch_size = ctx.settings.etl.batch_size
    counted: set[str] = set()
    batch: list[Mapping[str, Any]] = []
    async for doc in changed_lots(ctx.mongo, source, since, batch_size=batch_size):
        batch.append(doc)
        stats.lots += 1
        if len(batch) >= batch_size:
            await _process_batch(ctx, source, batch, counted, stats)
            batch = []
    if batch:
        await _process_batch(ctx, source, batch, counted, stats)

    async with ctx.pool.connection() as conn:
        await save_success(conn, run_started, stats)


async def run_retry_queue(ctx: Context) -> SourceStats:
    """Номера, оставшиеся с прошлых прогонов в ``pending``/``error``."""
    stats = SourceStats(RETRY_QUEUE)
    started = time.monotonic()
    try:
        async with ctx.pool.connection() as conn:
            numbers = await due_for_retry(conn, ctx.settings.nspd.retry_limit)
        stats.numbers = len(numbers)
        if numbers:
            log.info("очередь повторов: %d номеров с прошлых прогонов", len(numbers))
        await resolve_and_write(ctx, numbers, stats)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        log.exception("очередь повторов упала")
        stats.status = "failed"
        stats.error = f"{type(error).__name__}: {error}"
    stats.seconds = time.monotonic() - started
    return stats


async def run_all(
    settings: Settings, names: Sequence[str] | None = None, *, nspd: Searcher | None = None
) -> RunResult:
    """Один прогон: миграции, очередь повторов, площадки ``names`` (или все).

    ``nspd`` — подменить клиент НСПД (тесты); по умолчанию — ``NspdClient`` из настроек.
    """
    log.info("PostGIS: подключение к %s", describe_dsn(settings.postgres.dsn))
    conn = await connect(settings.postgres.dsn)
    try:
        applied = await apply_migrations(conn)
    finally:
        await conn.close()
    log.info("PostGIS: %s", "применены миграции " + ", ".join(applied) if applied else "схема актуальна")

    log.info("Mongo: подключение к %s, база %s", describe_uri(settings.mongo.uri), settings.mongo.db)
    mongo_client = create_client(settings.mongo.uri)
    try:
        db = mongo_client[settings.mongo.db]
        available = await list_sources(db, settings.etl.exclude_sources)
        if names:
            unknown = sorted(set(names) - set(available))
            if unknown:
                raise UnknownSources(f"нет таких площадок в {settings.mongo.db}: {', '.join(unknown)}")
        targets = list(dict.fromkeys(names)) if names else available
        log.info(
            "площадок: %d (%s), одновременно до %d",
            len(targets),
            ", ".join(targets) or "нет коллекций",
            settings.etl.source_concurrency,
        )
        log.info(
            "НСПД: кэш %s, срок %s; не чаще %g запросов/с, одновременно до %d",
            _describe_cache(settings),
            settings.nspd.cache_ttl,
            settings.nspd.rate,
            settings.nspd.concurrency,
        )

        own_client = nspd is None
        client: Any = await NspdClient.create(settings.nspd) if own_client else nspd
        resolver = Resolver(client, settings.nspd)
        try:
            async with open_pool(settings.postgres.dsn, max_size=settings.etl.source_concurrency + 2) as pool:
                ctx = Context(settings, pool, db, resolver)
                result = RunResult()
                result.retry = await run_retry_queue(ctx)
                semaphore = asyncio.Semaphore(settings.etl.source_concurrency)

                async def guarded(source: str) -> SourceStats:
                    async with semaphore:
                        return await run_source(ctx, source)

                done: list[SourceStats] = []

                async def tracked(source: str) -> SourceStats:
                    stats = await guarded(source)
                    done.append(stats)
                    return stats

                pulse = asyncio.create_task(_pulse(resolver, done, len(targets)))
                try:
                    result.sources = list(await asyncio.gather(*(tracked(s) for s in targets)))
                finally:
                    pulse.cancel()
                result.breaker_open = resolver.breaker_open
                return result
        finally:
            await resolver.close()
            if own_client:
                await client.close()
    finally:
        await mongo_client.close()


def _describe_cache(settings: Settings) -> str:
    match settings.nspd.cache:
        case "sqlite":
            return f"sqlite {settings.nspd.cache_path}"
        case "redis":
            return f"redis {describe_uri(settings.nspd.redis_url)}"
        case _:
            return "выключен"


async def _pulse(resolver: Resolver, done: list[SourceStats], total: int) -> None:
    """Раз в ``PROGRESS_EVERY`` секунд — строка о ходе прогона."""
    while True:
        await asyncio.sleep(PROGRESS_EVERY)
        log.info(
            "идёт прогон: площадок готово %d из %d; НСПД — запросов %d (из кэша %d, неудачных %d), "
            "ждут ответа %d номеров%s",
            len(done),
            total,
            resolver.requests,
            resolver.cache_hits,
            resolver.failures,
            resolver.waiting,
            "; НСПД недоступна, предохранитель" if resolver.breaker_open else "",
        )
