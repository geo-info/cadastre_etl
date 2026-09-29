"""Клиент НСПД поверх pynspd: запрос, кэш Hishel, настройки без окружения.

Сеть подменяет ``pytest-httpx`` под транспортом pynspd — кэширующий транспорт
Hishel при этом настоящий, и проверяется именно его поведение: что кэшируется
«найдено» и «не найдено», а ошибки — нет.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
from hishel import AsyncInMemoryStorage
from pynspd import AsyncNspd
from pynspd.errors import PynspdServerError, TooManyRequests

from cadastre_etl.conf import NspdSettings
from cadastre_etl.nspd.client import NspdClient, build_cache_storage

FIXTURES = Path(__file__).parent / "fixtures" / "nspd"


def answer(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def settings(tmp_path, **overrides) -> NspdSettings:
    return NspdSettings(_env_file=None, cache_path=tmp_path / "cache" / "nspd.sqlite", **overrides)


@pytest.fixture
async def client(tmp_path):
    async with NspdClient(settings(tmp_path), AsyncInMemoryStorage()) as client:
        yield client


async def test_запрос_поиска_по_номеру(client, httpx_mock):
    httpx_mock.add_response(json=answer("land_plot"))
    result = await client.search("50:20:0010101:123")
    assert result.status_code == 200
    assert result.data["data"]["features"][0]["properties"]["options"]["cad_num"] == "50:20:0010101:123"
    [request] = httpx_mock.get_requests()
    assert request.url.host == "nspd.gov.ru"
    assert request.url.path == "/api/geoportal/v2/search/geoportal"
    assert dict(request.url.params) == {"query": "50:20:0010101:123", "thematicSearchId": "1"}


@pytest.mark.parametrize(
    ("status", "body", "expected_data"),
    [
        (200, answer("land_plot"), True),
        (200, answer("empty"), True),
        (404, {"message": "not found"}, False),
    ],
    ids=["найдено", "пусто", "404"],
)
async def test_второй_запрос_из_кэша(client, httpx_mock, status, body, expected_data):
    httpx_mock.add_response(status_code=status, json=body)
    first = await client.search("50:20:0010101:123")
    second = await client.search("50:20:0010101:123")
    assert (first.from_cache, second.from_cache) == (False, True)
    assert first.status_code == second.status_code == status
    assert (second.data is not None) is expected_data
    assert len(httpx_mock.get_requests()) == 1


@pytest.mark.parametrize(("status", "error"), [(503, PynspdServerError), (429, TooManyRequests)])
async def test_ошибки_не_кэшируются(client, httpx_mock, status, error):
    httpx_mock.add_response(status_code=status, json={"message": "busy"})
    httpx_mock.add_response(json=answer("land_plot"))
    with pytest.raises(error):
        await client.search("50:20:0010101:123")
    result = await client.search("50:20:0010101:123")
    assert (result.status_code, result.from_cache) == (200, False)


async def test_разные_номера_разные_ключи(client, httpx_mock):
    httpx_mock.add_response(json=answer("land_plot"))
    httpx_mock.add_response(json=answer("building"))
    await client.search("50:20:0010101:123")
    result = await client.search("77:03:0001001:3030")
    assert not result.from_cache
    assert len(httpx_mock.get_requests()) == 2


async def test_sqlite_кэш_с_ttl_переживает_клиента(tmp_path, httpx_mock):
    conf = settings(tmp_path, cache_ttl=timedelta(days=1))
    httpx_mock.add_response(json=answer("land_plot"))
    async with await NspdClient.create(conf) as client:
        assert not (await client.search("50:20:0010101:123")).from_cache
    async with await NspdClient.create(conf) as client:
        assert (await client.search("50:20:0010101:123")).from_cache
    assert (tmp_path / "cache" / "nspd.sqlite").exists()


def test_ttl_вместе_с_sqlite_url_pynspd_не_принимает():
    """Причина, по которой хранилище собирается нами и передаётся через ``cache_storage``."""
    with pytest.raises(ValueError, match="только один вариант"):
        AsyncNspd(cache_sqlite_url="x.sqlite", cache_ttl=60, trust_env=False)


async def test_без_кэша(tmp_path, httpx_mock):
    assert await build_cache_storage(settings(tmp_path, cache="none")) is None
    httpx_mock.add_response(json=answer("land_plot"))
    httpx_mock.add_response(json=answer("land_plot"))
    async with await NspdClient.create(settings(tmp_path, cache="none")) as client:
        await client.search("50:20:0010101:123")
        assert not (await client.search("50:20:0010101:123")).from_cache


def test_окружение_pynspd_не_действует(tmp_path, monkeypatch):
    """С ``trust_env=True`` эта переменная плюс наше хранилище дали бы ``ValueError``."""
    monkeypatch.setenv("PYNSPD_CACHE_SQLITE_URL", str(tmp_path / "чужой.sqlite"))
    with pytest.raises(ValueError):
        AsyncNspd(cache_storage=AsyncInMemoryStorage())
    NspdClient(settings(tmp_path), AsyncInMemoryStorage())


async def test_закрытие_без_запросов(tmp_path):
    client = await NspdClient.create(settings(tmp_path))
    await client.close()
