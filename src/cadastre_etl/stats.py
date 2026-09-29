"""Итог прогона площадки — то, что попадает в таблицу отчёта и в ``etl_state``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SourceStats:
    """Счётчики одной площадки (или очереди повторов) за прогон.

    Каждый уникальный номер площадки попадает ровно в одну из колонок исхода:
    ``fetched`` — ответ пришёл из сети, ``cached`` — из кэша НСПД или уже получен в
    этом прогоне для другой площадки (найден, без геометрии или неоднозначен),
    ``not_found``, ``errors``, ``deferred`` — не спрашивали: сработал
    предохранитель, номер ждёт следующего прогона.
    """

    source: str
    #: ``ok`` / ``failed`` / ``skipped`` (площадку обрабатывает другой экземпляр).
    status: str = "ok"
    lots: int = 0
    numbers: int = 0
    fetched: int = 0
    cached: int = 0
    not_found: int = 0
    errors: int = 0
    deferred: int = 0
    error: str | None = None
    seconds: float = 0.0
