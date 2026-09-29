"""Запрос в НСПД по кадастровому номеру поверх ``pynspd.AsyncNspd``.

Один номер — один ``AsyncNspd.request`` к поиску
``/api/geoportal/v2/search/geoportal?query=<номер>&thematicSearchId=1`` — тот же
запрос, что делает ``AsyncNspd.find``, поэтому и ключ кэша тот же. Сам ``find``
не берём: у него свои повторы без пауз в обход нашего лимита частоты, он не
говорит, пришёл ли ответ из кэша, и падает целиком на ответе, который не
проходит валидацию моделей. Разбор ответа — в ``nspd.parse``.

**Кэш** — встроенный в pynspd (Hishel 0.1): ответы 200 и 404 кэшируются на
``NSPD_CACHE_TTL`` независимо от заголовков сервера, то есть «не найдено» тоже;
403/429/5xx — нет. Хранилище собираем сами и отдаём через ``cache_storage``:
``AsyncNspd(cache_sqlite_url=…, cache_ttl=…)`` в pynspd 1.1.15 бросает
``ValueError`` — TTL там считается ещё одним «вариантом хранилища».

**Прочее из исходников pynspd:**

- ``client_retries=0`` — повторы (и транспорта httpx тоже) только наши, в
  резолвере, под лимитером;
- ``trust_env=False`` — иначе ``PYNSPD_*`` из окружения перекрыли бы аргументы,
  а настройки у нас только в ``conf.py``;
- HTTP-клиент pynspd создаёт лениво на первом запросе, без замка: несколько
  одновременных первых запросов построили бы несколько клиентов, и лишние не
  закрылись бы. Первый запрос идёт под замком.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import anysqlite
import redis.asyncio as aioredis
from hishel import AsyncBaseStorage, AsyncRedisStorage, AsyncSQLiteStorage
from httpx import Response
from pynspd import AsyncNspd
from pynspd.errors import NotFound

from cadastre_etl.conf import NspdSettings

SEARCH_URL = "/api/geoportal/v2/search/geoportal"
#: ``ThemeId.REAL_ESTATE_OBJECTS``: ЗУ, здания, сооружения, ОНС, помещения, ЕЗП.
REAL_ESTATE_THEME = 1


@dataclass(frozen=True)
class RawAnswer:
    """Ответ поиска как есть: код, тело (у 404 — ``None``) и откуда он взялся."""

    status_code: int
    data: dict[str, Any] | None
    from_cache: bool


async def build_cache_storage(conf: NspdSettings) -> AsyncBaseStorage | None:
    """Хранилище кэша по настройкам; ``None`` при ``NSPD_CACHE=none``."""
    ttl = conf.cache_ttl.total_seconds()
    if conf.cache == "none":
        return None
    if conf.cache == "sqlite":
        conf.cache_path.parent.mkdir(parents=True, exist_ok=True)
        connection = await anysqlite.connect(str(conf.cache_path), check_same_thread=False)
        return AsyncSQLiteStorage(connection=connection, ttl=ttl)
    return AsyncRedisStorage(client=aioredis.Redis.from_url(conf.redis_url), ttl=ttl)


def _from_cache(response: Response) -> bool:
    return bool(response.extensions.get("from_cache", False))


class NspdClient:
    """Поиск объекта недвижимости по номеру; асинхронный контекст.

    Ошибки pynspd (``BlockedIP``, ``TooManyRequests``, ``PynspdServerError``…) и
    httpx (таймаут, обрыв) пробрасываются как есть — что повторять, решает
    резолвер. 404 — не ошибка, а ответ «не найдено».
    """

    def __init__(self, conf: NspdSettings, storage: AsyncBaseStorage | None) -> None:
        self._storage = storage
        self._api = AsyncNspd(
            client_timeout=conf.timeout,  # type: ignore[arg-type]  # httpx примет и float
            client_retries=0,
            client_proxy=conf.proxy,
            cache_storage=storage,
            trust_env=False,
        )
        self._warm = False
        self._warm_lock = asyncio.Lock()

    @classmethod
    async def create(cls, conf: NspdSettings) -> NspdClient:
        return cls(conf, await build_cache_storage(conf))

    async def __aenter__(self) -> NspdClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        # Хранилище закрывает транспорт pynspd — но только если клиент успели
        # построить, то есть был хоть один запрос.
        if self._warm:
            await self._api.close()
        elif self._storage is not None:
            await self._storage.aclose()

    async def search(self, cad_num: str) -> RawAnswer:
        if self._warm:
            return await self._search(cad_num)
        async with self._warm_lock:
            try:
                return await self._search(cad_num)
            finally:
                self._warm = True

    async def _search(self, cad_num: str) -> RawAnswer:
        params = {"query": cad_num, "thematicSearchId": REAL_ESTATE_THEME}
        try:
            response = await self._api.request("get", SEARCH_URL, params=params)
        except NotFound as error:
            return RawAnswer(404, None, _from_cache(error.response))
        return RawAnswer(response.status_code, response.json(), _from_cache(response))
