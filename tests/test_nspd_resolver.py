"""Резолвер: один запрос на номер за прогон, лимит параллельности, повторы, предохранитель."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from pynspd.errors import BlockedIP, PynspdServerError

from cadastre_etl.conf import NspdSettings
from cadastre_etl.nspd.client import RawAnswer
from cadastre_etl.nspd.resolver import BACKOFF_CAP, Resolver, backoff

FIXTURES = Path(__file__).parent / "fixtures" / "nspd"
LAND = RawAnswer(200, json.loads((FIXTURES / "land_plot.json").read_text(encoding="utf-8")), False)
CN = "50:20:0010101:123"


def http_error(cls, status):
    return cls(httpx.Response(status, json={"message": "x"}))


class FakeClient:
    """Ответы по номерам: список — по очереди на каждую попытку; всё остальное — пусто."""

    def __init__(self, script=None, delay: float = 0.0):
        self.script = {k: list(v) for k, v in (script or {}).items()}
        self.calls: list[str] = []
        self.delay = delay
        self.active = 0
        self.max_active = 0

    async def search(self, cad_num):
        self.calls.append(cad_num)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delay)
            queue = self.script.get(cad_num)
            item = queue.pop(0) if queue else RawAnswer(404, None, False)
            if isinstance(item, Exception):
                raise item
            return item
        finally:
            self.active -= 1


def conf(**overrides) -> NspdSettings:
    values = {"rate": 1000.0, "concurrency": 4, "retries": 3, "breaker": 20} | overrides
    return NspdSettings(_env_file=None, **values)


class Sleeps(list):
    async def __call__(self, seconds):
        self.append(seconds)


async def test_номер_с_трёх_площадок_один_запрос():
    client = FakeClient({CN: [LAND]}, delay=0.01)
    resolver = Resolver(client, conf(), sleep=Sleeps())
    results = await asyncio.gather(*(resolver.resolve(CN) for _ in range(3)))
    assert client.calls == [CN]
    assert [r.outcome.status for r in results] == ["found"] * 3
    assert sorted(r.shared for r in results) == [False, True, True]
    assert (await resolver.resolve(CN)).shared


async def test_параллельность_не_выше_лимита():
    client = FakeClient(delay=0.01)
    resolver = Resolver(client, conf(concurrency=2), sleep=Sleeps())
    await resolver.resolve_many([f"50:20:0010101:{n}" for n in range(10)])
    assert client.max_active == 2
    assert resolver.requests == 10


async def test_повторы_на_503_с_паузами():
    error = http_error(PynspdServerError, 503)
    client = FakeClient({CN: [error, error, LAND]})
    sleeps = Sleeps()
    resolver = Resolver(client, conf(), sleep=sleeps)
    resolved = await resolver.resolve(CN)
    assert resolved.outcome.status == "found"
    assert len(client.calls) == 3
    assert len(sleeps) == 2 and 0.5 <= sleeps[0] <= 1.5 and 1.0 <= sleeps[1] <= 3.0


async def test_повторы_кончились_ошибка():
    client = FakeClient({CN: [httpx.ConnectTimeout("timeout")] * 5})
    resolved = await Resolver(client, conf(retries=2), sleep=Sleeps()).resolve(CN)
    assert resolved.outcome.status == "error"
    assert resolved.outcome.error.startswith("ConnectTimeout")
    assert len(client.calls) == 3


async def test_403_без_повторов():
    client = FakeClient({CN: [http_error(BlockedIP, 403), LAND]})
    resolved = await Resolver(client, conf(), sleep=Sleeps()).resolve(CN)
    assert resolved.outcome.status == "error"
    assert "BlockedIP" in resolved.outcome.error
    assert len(client.calls) == 1


async def test_предохранитель():
    numbers = [f"50:20:0010101:{n}" for n in range(10)]
    client = FakeClient({n: [http_error(BlockedIP, 403)] for n in numbers})
    resolver = Resolver(client, conf(breaker=3, concurrency=1), sleep=Sleeps())
    results = await resolver.resolve_many(numbers)
    statuses = [results[n].outcome.status for n in numbers]
    assert resolver.breaker_open
    assert statuses[:3] == ["error"] * 3
    assert set(statuses[3:]) == {"pending"}
    assert len(client.calls) == 3


async def test_успех_сбрасывает_счётчик_ошибок():
    blocked = http_error(BlockedIP, 403)
    script = {"1:1:111111:1": [blocked], "1:1:111111:2": [blocked], "1:1:111111:3": [LAND]}
    script |= {"1:1:111111:4": [blocked], "1:1:111111:5": [blocked]}
    resolver = Resolver(FakeClient(script), conf(breaker=3, concurrency=1), sleep=Sleeps())
    for number in script:
        await resolver.resolve(number)
    assert not resolver.breaker_open


async def test_отмена_прогона_отменяет_запросы():
    client = FakeClient(delay=10)
    resolver = Resolver(client, conf(), sleep=Sleeps())
    waiter = asyncio.create_task(resolver.resolve(CN))
    await asyncio.sleep(0.01)
    waiter.cancel()
    await resolver.close()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert all(task.done() for task in resolver._tasks.values())


def test_пауза_растёт_до_потолка():
    assert 0.5 <= backoff(1) <= 1.5
    assert BACKOFF_CAP * 0.5 <= backoff(20) <= BACKOFF_CAP * 1.5
