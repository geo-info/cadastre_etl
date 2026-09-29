"""Настройки сервиса: одно место, где читается окружение.

Настройки разложены по тому, кого они касаются: откуда читать лоты
(``MongoSettings``), куда писать (``PostgresSettings``), как вести прогон
(``EtlSettings``) и как ходить в НСПД (``NspdSettings``). Умолчания подобраны
так, что без единой переменной работает ``compose.yaml`` из корня.

Читается из окружения и из ``.env`` в корне репозитория; переменные окружения
приоритетнее файла, чтобы настройки из CI или docker не перетирались чьим-то
локальным ``.env``. Имена переменных — ``MONGO_URI``, ``OVERLAP``,
``NSPD_CACHE_TTL`` и т. д., таблица — в README.

Длительности принимают секунды (``600``), ISO 8601 (``PT10M``) и короткую
запись: ``90s``, ``10m``, ``2h``, ``7d``.

    from cadastre_etl.conf import get_settings
    conf = get_settings()
    conf.etl.overlap, conf.nspd.cache_ttl
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[2]

ENV = SettingsConfigDict(env_file=ROOT_DIR / ".env", env_file_encoding="utf-8", extra="ignore")

_SHORT_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$")
_UNIT_SECONDS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: Any) -> Any:
    """Короткая запись длительности (``10m``, ``7d``, ``600``) → ``timedelta``.

    Число секунд строкой pydantic сам не берёт — из окружения всё приходит
    строками. Всё остальное (``PT10M``, число, готовый ``timedelta``) отдаётся
    pydantic как есть — он разберёт сам или скажет, что не так.
    """
    if isinstance(value, str) and (match := _SHORT_DURATION.match(value)):
        number, unit = match.groups()
        return timedelta(seconds=float(number) * _UNIT_SECONDS[unit])
    return value


Duration = Annotated[timedelta, BeforeValidator(parse_duration)]


class MongoSettings(BaseSettings):
    """Откуда читать лоты: Mongo trading_platform. Переменные ``MONGO_URI``, ``MONGO_DB``."""

    model_config = SettingsConfigDict(**ENV, env_prefix="MONGO_")

    uri: str = "mongodb://localhost:27017"
    db: str = "trading"


class PostgresSettings(BaseSettings):
    """Куда писать: PostGIS. Переменная ``POSTGRES_DSN``."""

    model_config = SettingsConfigDict(**ENV, env_prefix="POSTGRES_")

    dsn: str = "postgresql://etl:etl@localhost:5432/cadastre"


class EtlSettings(BaseSettings):
    """Как вести прогон: окно выборки, параллельность площадок, цикл."""

    model_config = ENV

    #: Насколько раньше ``checked_at`` начинать выборку. Закрывает запись в Mongo
    #: во время чтения, расхождение часов и ``detail_at`` = время *запроса*
    #: страницы деталей (см. спеку, раздел «Состояние»).
    overlap: Duration = timedelta(minutes=10)
    #: Первый прогон площадки — с этой даты; без неё — все лоты.
    initial_since: date | None = None
    source_concurrency: int = Field(default=4, ge=1)
    #: Коллекции базы, которые не площадки.
    exclude_sources: Annotated[list[str], NoDecode] = []
    batch_size: int = Field(default=500, ge=1)
    #: Пауза между прогонами в режиме ``--loop``.
    interval: Duration = timedelta(minutes=30)
    log_level: str = "INFO"

    @field_validator("exclude_sources", mode="before")
    @classmethod
    def _split_names(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [name.strip() for name in value.split(",") if name.strip()]
        return value


class NspdSettings(BaseSettings):
    """Как ходить в НСПД: частота, повторы, кэш. Переменные ``NSPD_*``."""

    model_config = SettingsConfigDict(**ENV, env_prefix="NSPD_")

    #: Таймаут одного запроса, с.
    timeout: float = Field(default=30.0, gt=0)
    #: Запросов в секунду на процесс (попадания в кэш тоже считаются).
    rate: float = Field(default=2.0, gt=0)
    concurrency: int = Field(default=4, ge=1)
    #: Повторов одного номера внутри прогона (сверх первой попытки).
    retries: int = Field(default=3, ge=0)
    #: Столько ошибок подряд — и до конца прогона НСПД больше не спрашиваем.
    breaker: int = Field(default=20, ge=1)
    #: Номеров из очереди повторов за один прогон.
    retry_limit: int = Field(default=1000, ge=0)
    proxy: str | None = None
    cache: Literal["sqlite", "redis", "none"] = "sqlite"
    cache_path: Path = ROOT_DIR / ".cache" / "nspd.sqlite"
    redis_url: str = "redis://localhost:6379/0"
    #: Срок жизни ответа в кэше — и «найдено», и «не найдено».
    cache_ttl: Duration = timedelta(days=7)

    @field_validator("proxy", mode="before")
    @classmethod
    def _empty_proxy(cls, value: Any) -> Any:
        """Пустая переменная (так её передаёт compose) — без прокси."""
        return value or None


class Settings(BaseModel):
    """Все настройки сервиса, по группам."""

    mongo: MongoSettings = Field(default_factory=MongoSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    etl: EtlSettings = Field(default_factory=EtlSettings)
    nspd: NspdSettings = Field(default_factory=NspdSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Настройки одним экземпляром на процесс.

    Кэш — ради единственности: иначе два модуля прочитают ``.env`` в разные
    моменты и разойдутся в значениях.
    """
    return Settings()
