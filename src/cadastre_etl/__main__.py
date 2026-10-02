"""Командная строка: ``python -m cadastre_etl run|migrate|status|sources``.

    python -m cadastre_etl run                 # все площадки, один прогон
    python -m cadastre_etl run bep seltim      # только эти
    python -m cadastre_etl run --loop          # прогон, пауза INTERVAL, снова
    python -m cadastre_etl migrate             # схема PostGIS
    python -m cadastre_etl status              # etl_state таблицей
    python -m cadastre_etl sources             # коллекции Mongo и их состояние

Коды выхода ``run``: 0 — всё хорошо; 1 — площадка упала (или не применились
миграции, недоступна база); 2 — неверные аргументы или неизвестная площадка;
3 — площадки прошли, но НСПД недоступна (сработал предохранитель). В режиме
``--loop`` ошибка прогона цикл не останавливает; SIGTERM/SIGINT отменяют текущий
прогон (его состояние не двигается) и завершают цикл с кодом последнего прогона.

Таблица — в stdout, журнал хода — в stderr (уровень ``LOG_LEVEL``, ``-v`` —
подробно, вплоть до каждого HTTP-запроса).

Windows: асинхронный psycopg не работает с циклом событий по умолчанию
(``ProactorEventLoop``), поэтому там запускаем ``SelectorEventLoop``; сигналы —
через ``signal.signal``, ``add_signal_handler`` в Windows нет.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Callable, Coroutine, Sequence
from typing import Any

from cadastre_etl import pipeline
from cadastre_etl.conf import Settings, get_settings
from cadastre_etl.db.migrate import apply_migrations
from cadastre_etl.db.pool import connect
from cadastre_etl.db.state import all_states
from cadastre_etl.mongo import create_client, list_sources
from cadastre_etl.report import format_run, format_states

log = logging.getLogger("cadastre_etl")

EXIT_USAGE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cadastre_etl", description="Кадастровые объекты лотов: Mongo → НСПД → PostGIS."
    )
    # -v понимается и до команды, и после неё: `-v run` и `run -v`.
    verbose = argparse.ArgumentParser(add_help=False)
    verbose.add_argument(
        "-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="подробный журнал (DEBUG)"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="подробный журнал (DEBUG)")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="прогон площадок", parents=[verbose])
    run.add_argument("sources", nargs="*", help="имена площадок (коллекций); без имён — все")
    run.add_argument("--loop", action="store_true", help="повторять с паузой INTERVAL")
    commands.add_parser("migrate", help="применить миграции PostGIS", parents=[verbose])
    commands.add_parser("status", help="состояние площадок из etl_state", parents=[verbose])
    commands.add_parser("sources", help="коллекции Mongo и их состояние", parents=[verbose])
    return parser


async def run_once(settings: Settings, names: Sequence[str]) -> int:
    try:
        result = await pipeline.run_all(settings, names or None)
    except pipeline.UnknownSources as error:
        log.error("%s", error)
        return EXIT_USAGE
    except Exception:
        log.exception("прогон не состоялся")
        return 1
    print(format_run(result), flush=True)
    return result.exit_code


def _on_stop_signals(stop: asyncio.Event) -> Callable[[], None]:
    """SIGTERM/SIGINT → ``stop``; вернуть функцию, снимающую обработчики."""
    loop = asyncio.get_running_loop()
    sigs = (signal.SIGTERM, signal.SIGINT)
    try:
        for sig in sigs:
            loop.add_signal_handler(sig, stop.set)
    except NotImplementedError:  # Windows
        previous = {sig: signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set)) for sig in sigs}
        return lambda: [signal.signal(sig, handler) for sig, handler in previous.items()] and None
    return lambda: [loop.remove_signal_handler(sig) for sig in sigs] and None


async def run_loop(settings: Settings, names: Sequence[str]) -> int:
    stop = asyncio.Event()
    restore = _on_stop_signals(stop)
    code = 0
    try:
        while not stop.is_set():
            current = asyncio.create_task(run_once(settings, names))
            stopping = asyncio.create_task(stop.wait())
            await asyncio.wait({current, stopping}, return_when=asyncio.FIRST_COMPLETED)
            stopping.cancel()
            if not current.done():
                log.warning("остановка: текущий прогон отменён")
                current.cancel()
                await asyncio.gather(current, return_exceptions=True)
                break
            code = current.result()
            if code == EXIT_USAGE:
                break
            interval = settings.etl.interval.total_seconds()
            log.info("следующий прогон через %.0f с", interval)
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                pass
    finally:
        restore()
    return code


async def migrate(settings: Settings) -> int:
    conn = await connect(settings.postgres.dsn)
    try:
        applied = await apply_migrations(conn)
    finally:
        await conn.close()
    print("применены: " + ", ".join(applied) if applied else "схема актуальна")
    return 0


async def status(settings: Settings, with_sources: bool) -> int:
    conn = await connect(settings.postgres.dsn)
    try:
        await apply_migrations(conn)
        states = await all_states(conn)
    finally:
        await conn.close()
    names = None
    if with_sources:
        client = create_client(settings.mongo.uri)
        try:
            names = await list_sources(client[settings.mongo.db], settings.etl.exclude_sources)
        finally:
            await client.close()
        states = [s for s in states if s["source"] in names]
    print(format_states(states, names))
    return 0


def run_async(coro: Coroutine[Any, Any, int]) -> int:
    """``asyncio.run`` с циклом, который понимает psycopg, и в Windows тоже."""
    if sys.platform == "win32":
        return asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(coro)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    logging.basicConfig(
        level="DEBUG" if args.verbose else settings.etl.log_level.upper(),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx пишет строку на каждый запрос — это уровень отладки, не работы.
    if logging.getLogger().level > logging.DEBUG:
        logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "run":
        runner = run_loop if args.loop else run_once
        log.info("старт: %s", "цикл, пауза " + str(settings.etl.interval) if args.loop else "один прогон")
        try:
            return run_async(runner(settings, args.sources))
        except KeyboardInterrupt:
            log.warning("прервано")
            return 130
    try:
        if args.command == "migrate":
            return run_async(migrate(settings))
        return run_async(status(settings, with_sources=args.command == "sources"))
    except Exception as error:
        log.error("%s: %s", type(error).__name__, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
