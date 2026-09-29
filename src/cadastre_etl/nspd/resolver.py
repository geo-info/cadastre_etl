"""Резолвер номеров на прогон: каждый номер — один раз, в пределах лимитов НСПД.

**Дедупликация.** Один резолвер на весь прогон, общий для площадок: номер → задача.
Кто первым спросил номер, тот её создал, остальные ждут её же — номер, который
встретился на трёх площадках, запрашивается один раз. Между прогонами повторы
гасит кэш pynspd, своего кэша здесь нет.

**Лимиты** — на процесс: не больше ``NSPD_CONCURRENCY`` запросов разом и не чаще
``NSPD_RATE`` в секунду, ровным шагом, без всплесков. Кэш живёт внутри
транспорта pynspd, заранее попадание в него не отличить — лимит тратят и они.

**Повторы** — на таймаут, обрыв соединения, 429 и 5xx: до ``NSPD_RETRIES`` раз с
паузой 1, 2, 4… с (потолок 30 с) и разбросом ±50 %, чтобы параллельные повторы
не шли залпом. 403 (``BlockedIP``) не повторяем: адрес заблокирован, повторы
только продлят бан.

**Предохранитель.** ``NSPD_BREAKER`` неудачных запросов подряд — и до конца
прогона НСПД больше не спрашиваем: номера получают исход ``pending`` и уйдут в
очередь повторов следующего прогона. Это случай «НСПД недоступна» (зарубежный
адрес, бан), когда продолжать бессмысленно.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

import httpx
from aiolimiter import AsyncLimiter
from pynspd.errors import PynspdError, PynspdServerError, TooManyRequests

from cadastre_etl.conf import NspdSettings
from cadastre_etl.nspd.client import RawAnswer
from cadastre_etl.nspd.parse import Outcome, parse_answer

log = logging.getLogger(__name__)

#: Ошибки, которые стоит повторить: сеть и перегрузка сервера.
RETRYABLE: tuple[type[Exception], ...] = (httpx.TransportError, TooManyRequests, PynspdServerError)
BACKOFF_CAP = 30.0


class Searcher(Protocol):
    async def search(self, cad_num: str) -> RawAnswer: ...


@dataclass(frozen=True)
class Resolved:
    """Исход по номеру и был ли он получен для другого запроса этого прогона."""

    outcome: Outcome
    shared: bool


def backoff(attempt: int) -> float:
    """Пауза перед повтором ``attempt`` (1, 2, …): экспонента с потолком и разбросом."""
    return min(BACKOFF_CAP, 2.0 ** (attempt - 1)) * random.uniform(0.5, 1.5)


class Resolver:
    def __init__(
        self,
        client: Searcher,
        conf: NspdSettings,
        *,
        sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._retries = conf.retries
        self._breaker_limit = conf.breaker
        self._limiter = AsyncLimiter(1, 1 / conf.rate)
        self._semaphore = asyncio.Semaphore(conf.concurrency)
        self._sleep = sleep
        self._tasks: dict[str, asyncio.Task[Outcome]] = {}
        self._failures_in_row = 0
        #: Сработал предохранитель: до конца прогона в НСПД не ходим.
        self.breaker_open = False
        #: Сколько запросов ушло в ``client.search`` (включая попадания в кэш).
        self.requests = 0

    async def resolve(self, cad_num: str) -> Resolved:
        task = self._tasks.get(cad_num)
        shared = task is not None
        if task is None:
            task = asyncio.create_task(self._fetch(cad_num), name=f"nspd:{cad_num}")
            self._tasks[cad_num] = task
        # shield: отмена одной площадки не должна отменять запрос, который ждут другие.
        return Resolved(await asyncio.shield(task), shared)

    async def resolve_many(self, numbers: list[str]) -> dict[str, Resolved]:
        results = await asyncio.gather(*(self.resolve(n) for n in numbers))
        return dict(zip(numbers, results, strict=True))

    async def close(self) -> None:
        """Отменить недоделанные запросы (прогон отменён или упал)."""
        pending = [task for task in self._tasks.values() if not task.done()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _fetch(self, cad_num: str) -> Outcome:
        attempt = 0
        while True:
            try:
                async with self._semaphore, self._limiter:
                    # Проверка — после очереди: пока номер ждал слота, предохранитель
                    # мог сработать на других.
                    if self.breaker_open:
                        return Outcome(cad_num, "pending", error="НСПД недоступна: сработал предохранитель")
                    self.requests += 1
                    raw = await self._client.search(cad_num)
            except RETRYABLE as error:
                self._failed(cad_num, error)
                if attempt < self._retries and not self.breaker_open:
                    attempt += 1
                    await self._sleep(backoff(attempt))
                    continue
                return Outcome(cad_num, "error", error=_describe(error))
            except (PynspdError, httpx.HTTPError) as error:
                self._failed(cad_num, error)
                return Outcome(cad_num, "error", error=_describe(error))
            self._failures_in_row = 0
            return parse_answer(cad_num, raw)

    def _failed(self, cad_num: str, error: Exception) -> None:
        self._failures_in_row += 1
        log.debug("%s: %s", cad_num, _describe(error))
        if not self.breaker_open and self._failures_in_row >= self._breaker_limit:
            self.breaker_open = True
            log.error(
                "НСПД: %d неудачных запросов подряд, последний — %s; до конца прогона не спрашиваем",
                self._failures_in_row,
                _describe(error),
            )


def _describe(error: Exception) -> str:
    text = str(error).strip().splitlines()
    return f"{type(error).__name__}: {text[0]}" if text else type(error).__name__
