"""Настройки: умолчания, окружение против ``.env``, короткая запись длительностей."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from cadastre_etl.conf import EtlSettings, NspdSettings, Settings, parse_duration


@pytest.fixture(autouse=True)
def _чистое_окружение(monkeypatch):
    for name in ("OVERLAP", "INTERVAL", "EXCLUDE_SOURCES", "NSPD_CACHE", "NSPD_CACHE_TTL", "MONGO_DB"):
        monkeypatch.delenv(name, raising=False)


def test_умолчания_без_окружения(tmp_path):
    etl = EtlSettings(_env_file=tmp_path / "нет.env")
    nspd = NspdSettings(_env_file=tmp_path / "нет.env")
    assert etl.overlap == timedelta(minutes=10)
    assert etl.initial_since is None
    assert etl.exclude_sources == []
    assert nspd.cache == "sqlite"
    assert nspd.cache_ttl == timedelta(days=7)


def test_окружение_приоритетнее_env_файла(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("OVERLAP=5m\nINTERVAL=1h\n", encoding="utf-8")
    monkeypatch.setenv("OVERLAP", "15m")
    etl = EtlSettings(_env_file=env_file)
    assert etl.overlap == timedelta(minutes=15)
    assert etl.interval == timedelta(hours=1)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10m", timedelta(minutes=10)),
        ("7d", timedelta(days=7)),
        ("90s", timedelta(seconds=90)),
        ("1.5h", timedelta(minutes=90)),
        ("600", timedelta(seconds=600)),
        ("PT10M", timedelta(minutes=10)),
    ],
)
def test_длительности(monkeypatch, tmp_path, raw, expected):
    monkeypatch.setenv("NSPD_CACHE_TTL", raw)
    assert NspdSettings(_env_file=tmp_path / "нет.env").cache_ttl == expected


def test_короткая_запись_не_трогает_прочее():
    assert parse_duration("PT1H") == "PT1H"
    assert parse_duration(5) == 5


def test_исключённые_коллекции_через_запятую(monkeypatch, tmp_path):
    monkeypatch.setenv("EXCLUDE_SOURCES", "tmp, backup ,")
    assert EtlSettings(_env_file=tmp_path / "нет.env").exclude_sources == ["tmp", "backup"]


def test_неверный_вид_кэша(monkeypatch, tmp_path):
    monkeypatch.setenv("NSPD_CACHE", "memcached")
    with pytest.raises(ValidationError):
        NspdSettings(_env_file=tmp_path / "нет.env")


def test_группы_собираются_вместе(monkeypatch):
    monkeypatch.setenv("MONGO_DB", "trading_x")
    assert Settings().mongo.db == "trading_x"
