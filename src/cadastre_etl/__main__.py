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

Таблица — в stdout, журнал — в stderr (уровень ``LOG_LEVEL``).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

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
    parser = argparse.ArgumentParser(prog="cadastre_etl", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="прогон площадок")
    run.add_argument("sources", nargs="*", help="имена площадок (коллекций); без имён — все")
    run.add_argument("--loop", action="store_true", help="повторять с паузой INTERVAL")
    commands.add_parser("migrate", help="применить миграции PostGIS")
    commands.add_parser("status", help="состояние площадок из etl_state")
    commands.add_parser("sources", help="коллекции Mongo и их состояние")
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


async def run_loop(settings: Settings, names: Sequence[str]) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
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
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
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


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    logging.basicConfig(
        level=settings.etl.log_level.upper(),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx пишет строку на каждый запрос — это уровень отладки, не работы.
    if logging.getLogger().level > logging.DEBUG:
        logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "run":
        runner = run_loop if args.loop else run_once
        return asyncio.run(runner(settings, args.sources))
    try:
        if args.command == "migrate":
            return asyncio.run(migrate(settings))
        return asyncio.run(status(settings, with_sources=args.command == "sources"))
    except Exception as error:
        log.error("%s: %s", type(error).__name__, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
