"""Живые тесты: настоящий НСПД через ``NspdClient`` и ``parse_answer``.

В CI не идут (маркер ``live``, ``addopts = -m 'not live'``). НСПД отвечает только
на российские адреса; с зарубежного — ``BlockedIP`` (403), тогда нужен
``NSPD_PROXY``. Запуск:

    uv run pytest -m live                # проверить
    uv run pytest -m live --record       # и записать ответы в tests/fixtures/nspd/recorded/

Записанные ответы — замена синтетике в ``tests/fixtures/nspd/``: на них стоит
перевести офлайн-тесты разбора и уточнить таблицу категорий в ``nspd.parse``
(помещения, машино-места, ЕЗП). Номера — из живых тестов самой pynspd.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cadastre_etl.conf import NspdSettings
from cadastre_etl.nspd.client import NspdClient
from cadastre_etl.nspd.parse import parse_answer

pytestmark = pytest.mark.live

RECORDED = Path(__file__).parents[1] / "fixtures" / "nspd" / "recorded"

# (номер, ожидаемый статус, ожидаемый вид)
CASES = [
    ("77:05:0001005:19", "found", "land_plot"),
    ("77:03:0001001:3030", "found", "building"),
    ("77:02:0021001:5304", None, None),  # машино-место: статус и вид — выяснить
    ("48:06:0000000:111", None, None),  # ЕЗП без координат (geocoderObject)
    ("77:02:0021001:5304111111", "not_found", None),
]


def _in_russia(geom) -> bool:
    minx, miny, maxx, maxy = geom.bounds
    lon_ok = (19 <= minx and maxx <= 180) or (-180 <= minx and maxx <= -168)
    return lon_ok and 41 <= miny and maxy <= 82


@pytest.fixture
async def client(tmp_path):
    conf = NspdSettings(cache="none", cache_path=tmp_path / "nspd.sqlite")
    async with await NspdClient.create(conf) as client:
        yield client


@pytest.mark.parametrize(("cad_num", "status", "kind"), CASES)
async def test_номер_в_нспд(client, request, cad_num, status, kind):
    raw = await client.search(cad_num)
    if request.config.getoption("--record"):
        RECORDED.mkdir(parents=True, exist_ok=True)
        body = {"status_code": raw.status_code, "data": raw.data}
        (RECORDED / f"{cad_num.replace(':', '_')}.json").write_text(
            json.dumps(body, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
    outcome = parse_answer(cad_num, raw)
    print(cad_num, outcome.status, outcome.kind, outcome.category_id, outcome.category_name, outcome.fields)
    assert outcome.status != "error", outcome.error
    if status is not None:
        assert outcome.status == status
    if kind is not None:
        assert outcome.kind == kind
    if outcome.geom is not None:
        assert _in_russia(outcome.geom), outcome.geom.bounds
    if outcome.geom_approx is not None:
        assert _in_russia(outcome.geom_approx)
