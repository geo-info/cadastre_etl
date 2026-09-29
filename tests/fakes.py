"""Подмена НСПД для тестов резолвера и прогона."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from cadastre_etl.nspd.client import RawAnswer

FIXTURES = Path(__file__).parent / "fixtures" / "nspd"


def fixture_answer(name: str, from_cache: bool = False) -> RawAnswer:
    return RawAnswer(200, json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8")), from_cache)


def found(cad_num: str) -> RawAnswer:
    """Ответ «найден ЗУ» для любого номера: фикстура ЗУ с подменённым номером."""
    answer = fixture_answer("land_plot")
    answer.data["data"]["features"][0]["properties"]["options"]["cad_num"] = cad_num
    return answer


def http_error(cls, status: int) -> Exception:
    return cls(httpx.Response(status, json={"message": "x"}))


class FakeNspd:
    """Ответы по номерам: список — по очереди на каждую попытку.

    Номер без сценария: ``default(cad_num)`` (по умолчанию «найден ЗУ»); ``None`` —
    404. Считает вызовы и одновременность.
    """

    def __init__(self, script=None, delay: float = 0.0, default=found):
        self.script = {k: list(v) for k, v in (script or {}).items()}
        self.default = default
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
            if queue:
                item = queue.pop(0)
            elif self.default is None:
                item = RawAnswer(404, None, False)
            else:
                item = self.default(cad_num)
            if isinstance(item, Exception):
                raise item
            return item
        finally:
            self.active -= 1
