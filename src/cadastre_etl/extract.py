"""Кадастровые номера и кварталы из текста лота.

Номер — ``регион:район:квартал:номер``: ``50:20:0010101:123``. Квартал — те же
первые три части без номера (``63:17:0519006``); он не объект недвижимости,
поэтому извлекается отдельным списком.

Площадки пишут номер как придётся: «с КН 50:20:…», «кадастровым номером:
50:20:…», склеивают со словом («кадастровымномером:26:27:062601:273,»), ставят
пробелы и неразрывные пробелы вокруг двоеточий, переносят строку перед номером.
Поэтому вокруг ``:`` допускается любой пробельный символ (``\\s`` в Python
покрывает и ``\\u00a0``, ``\\u202f``, ``\\u2009``), а нормализованная форма — без
пробелов. Цифры не трогаем: ведущие нули — часть номера (``02:26:161802:5746``).

Границы. Перед номером не должно быть цифры или «цифры с двоеточием» — иначе
это хвост чего-то длиннее; буквы вплотную допустимы. После номера — не цифра и
не ``:цифра``. Суффикс части объекта (``…:1024/1``) в номер не входит: берётся
сам объект.

Где искать в документе лота — ``description`` и все строковые значения
``detail`` на любой глубине: вид ``detail`` у каждого движка trading_platform
свой (разделы iTender, пары «подпись: значение», ``attachments``,
``price_schedule``), разбирать его по движкам незачем.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

_SEP = r"\s*:\s*"
#: Не цифра перед номером, и не «цифра:» / «цифра :» — номер не хвост длинного.
_BEFORE = r"(?<!\d)(?<!\d:)(?<!\d\s:)"
#: После — не цифра и не продолжение через двоеточие.
_AFTER = r"(?!\d)(?!\s*:\s*\d)"
_QUARTER = rf"(\d{{2}}){_SEP}(\d{{2}}){_SEP}(\d{{6,7}})"

NUMBER_RE = re.compile(rf"{_BEFORE}{_QUARTER}{_SEP}(\d+){_AFTER}")
QUARTER_RE = re.compile(rf"{_BEFORE}{_QUARTER}{_AFTER}")


def find_numbers(text: str) -> list[str]:
    """Кадастровые номера из строки, нормализованные, без повторов, по порядку."""
    return _unique(":".join(m.groups()) for m in NUMBER_RE.finditer(text))


def find_quarters(text: str) -> list[str]:
    """Кадастровые кварталы, которые не начало полного номера."""
    return _unique(":".join(m.groups()) for m in QUARTER_RE.finditer(text))


@dataclass
class Extracted:
    """Что нашлось в лоте.

    ``numbers`` — номер → где встретился впервые: ``description`` или путь в
    ``detail`` (``detail.Сведения.Объект``, ``detail.attachments.0.name``). Путь
    нужен, чтобы разбирать ложные срабатывания, не открывая документ.
    """

    numbers: dict[str, str] = field(default_factory=dict)
    quarters: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.numbers or self.quarters)


def extract_lot(doc: Mapping[str, Any]) -> Extracted:
    """Номера и кварталы из ``description`` и ``detail`` документа лота."""
    found = Extracted()
    sources: list[tuple[str, Any]] = [("description", doc.get("description"))]
    sources.append(("detail", doc.get("detail")))
    for root, value in sources:
        for path, text in _strings(value, root):
            for number in find_numbers(text):
                found.numbers.setdefault(number, path)
            for quarter in find_quarters(text):
                if quarter not in found.quarters:
                    found.quarters.append(quarter)
    return found


def _strings(value: Any, path: str) -> Iterator[tuple[str, str]]:
    """Все строки внутри значения с путями до них; ключи словарей не смотрим."""
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _strings(item, f"{path}.{key}")
    elif isinstance(value, list | tuple):
        for index, item in enumerate(value):
            yield from _strings(item, f"{path}.{index}")


def _unique(items: Iterator[str]) -> list[str]:
    return list(dict.fromkeys(items))
