"""CLI и отчёт: таблица, коды выхода, режим цикла."""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import UTC, datetime, timedelta

from cadastre_etl import __main__ as cli
from cadastre_etl import pipeline
from cadastre_etl.conf import EtlSettings, Settings
from cadastre_etl.pipeline import RunResult
from cadastre_etl.report import format_run, format_states
from cadastre_etl.stats import SourceStats


def result(**kwargs) -> RunResult:
    return RunResult(
        sources=[
            SourceStats("bep", lots=124, numbers=37, fetched=12, cached=22, not_found=3, seconds=41.2),
            SourceStats("utender", status="failed", error="ServerSelectionTimeoutError: localhost:27017"),
        ],
        retry=SourceStats(pipeline.RETRY_QUEUE, numbers=15, fetched=15, seconds=8),
        **kwargs,
    )


def test_таблица_прогона():
    assert format_run(result()).splitlines() == [
        "площадка   лотов  номеров  из сети  из кэша  не найдено  ошибок  отложено  статус  время",
        "bep          124       37       12       22           3       0         0  ok       41 с",
        "utender        0        0        0        0           0       0         0  failed    0 с"
        "  ServerSelectionTimeoutError: localhost:27017",
        "(повторы)      0       15       15        0           0       0         0  ok        8 с",
        "всего        124       52       27       22           3       0         0",
    ]


def test_таблица_без_пустой_очереди_и_с_предохранителем():
    run = RunResult(sources=[SourceStats("bep")], retry=SourceStats(pipeline.RETRY_QUEUE), breaker_open=True)
    text = format_run(run)
    assert "(повторы)" not in text
    assert "предохранитель" in text


def test_таблица_состояния_с_новыми_площадками():
    states = [
        {
            "source": "bep",
            "checked_at": datetime(2026, 9, 29, 12, tzinfo=UTC),
            "last_finished_at": datetime(2026, 9, 29, 12, 1, tzinfo=UTC),
            "last_status": "ok",
            "last_lots": 5,
            "last_numbers": 2,
            "last_error": None,
        }
    ]
    lines = format_states(states, ["bep", "seltim"]).splitlines()
    assert lines[1].startswith("bep") and "ok" in lines[1]
    assert lines[2].split() == ["seltim", "—", "—", "новая"]


def test_коды_выхода():
    assert result().exit_code == 1
    assert RunResult(sources=[SourceStats("bep")], breaker_open=True).exit_code == 3
    assert RunResult(sources=[SourceStats("bep"), SourceStats("x", status="skipped")]).exit_code == 0


def settings() -> Settings:
    return Settings(etl=EtlSettings(_env_file=None, interval=timedelta(milliseconds=10)))


async def test_неизвестная_площадка_код_2(monkeypatch):
    async def fake_run_all(settings, names):
        raise pipeline.UnknownSources("нет таких площадок: nope")

    monkeypatch.setattr(pipeline, "run_all", fake_run_all)
    assert await cli.run_once(settings(), ["nope"]) == 2


async def test_недоступная_база_код_1(monkeypatch):
    async def fake_run_all(settings, names):
        raise OSError("connection refused")

    monkeypatch.setattr(pipeline, "run_all", fake_run_all)
    assert await cli.run_once(settings(), []) == 1


async def test_цикл_до_сигнала(monkeypatch, capsys):
    calls = []

    async def fake_run_all(settings, names):
        calls.append(names)
        if len(calls) == 2:
            os.kill(os.getpid(), signal.SIGTERM)
        return RunResult(sources=[SourceStats("bep")], breaker_open=len(calls) == 2)

    monkeypatch.setattr(pipeline, "run_all", fake_run_all)
    code = await cli.run_loop(settings(), ["bep"])
    assert calls == [["bep"], ["bep"]]
    assert code == 3
    assert capsys.readouterr().out.count("площадка") == 2


async def test_сигнал_отменяет_текущий_прогон(monkeypatch):
    cancelled = asyncio.Event()

    async def slow_run_all(settings, names):
        asyncio.get_running_loop().call_later(0.01, os.kill, os.getpid(), signal.SIGTERM)
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(pipeline, "run_all", slow_run_all)
    assert await cli.run_loop(settings(), []) == 0
    assert cancelled.is_set()


def test_разбор_аргументов():
    args = cli.build_parser().parse_args(["run", "bep", "seltim", "--loop"])
    assert (args.command, args.sources, args.loop) == ("run", ["bep", "seltim"], True)
