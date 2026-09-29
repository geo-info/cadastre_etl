"""Таблицы для терминала: итог прогона по площадкам и состояние из ``etl_state``."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from cadastre_etl.pipeline import RunResult
from cadastre_etl.stats import SourceStats

RUN_HEADER = (
    "площадка",
    "лотов",
    "номеров",
    "из сети",
    "из кэша",
    "не найдено",
    "ошибок",
    "отложено",
    "статус",
    "время",
    "",
)
NUMERIC = ("lots", "numbers", "fetched", "cached", "not_found", "errors", "deferred")
ERROR_WIDTH = 100


def _table(header: Sequence[str], rows: Sequence[Sequence[str]], right: set[int]) -> str:
    widths = [max(len(str(row[i])) for row in [header, *rows]) for i in range(len(header))]

    def line(row: Sequence[str]) -> str:
        cells = [
            str(v).rjust(w) if i in right else str(v).ljust(w)
            for i, (v, w) in enumerate(zip(row, widths, strict=True))
        ]
        return "  ".join(cells).rstrip()

    return "\n".join(line(row) for row in [header, *rows])


def _row(stats: SourceStats) -> list[str]:
    note = (stats.error or "")[:ERROR_WIDTH]
    return [
        stats.source,
        *(str(getattr(stats, name)) for name in NUMERIC),
        stats.status,
        f"{stats.seconds:.0f} с",
        note,
    ]


def format_run(result: RunResult) -> str:
    """Итог прогона: площадки, очередь повторов, «всего»; ошибка — в последней колонке."""
    rows_stats = [*result.sources, *([result.retry] if result.retry and result.retry.numbers else [])]
    rows = [_row(s) for s in rows_stats]
    total = SourceStats("всего", status="")
    for stats in rows_stats:
        for name in NUMERIC:
            setattr(total, name, getattr(total, name) + getattr(stats, name))
    rows.append([total.source, *(str(getattr(total, name)) for name in NUMERIC), "", "", ""])
    text = _table(RUN_HEADER, rows, right=set(range(1, 8)) | {9})
    if result.breaker_open:
        text += "\n\nНСПД недоступна: сработал предохранитель, номера ждут следующего прогона."
    return text


def _when(value: datetime | None) -> str:
    return "—" if value is None else value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def format_states(states: Sequence[dict[str, Any]], sources: Sequence[str] | None = None) -> str:
    """``etl_state`` таблицей; ``sources`` — показать и площадки без строки состояния."""
    by_source = {s["source"]: s for s in states}
    names = sorted(set(by_source) | set(sources or ()))
    header = ("площадка", "прочитано до", "последний прогон", "статус", "лотов", "номеров", "ошибка")
    rows = []
    for name in names:
        state = by_source.get(name)
        if state is None:
            rows.append((name, "—", "—", "новая", "", "", ""))
            continue
        rows.append(
            (
                name,
                _when(state["checked_at"]),
                _when(state["last_finished_at"]),
                state["last_status"] or "",
                "" if state["last_lots"] is None else str(state["last_lots"]),
                "" if state["last_numbers"] is None else str(state["last_numbers"]),
                (state["last_error"] or "")[:ERROR_WIDTH],
            )
        )
    return _table(header, rows, right={4, 5})
