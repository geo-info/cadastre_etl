"""Разбор ответа НСПД: исходы, ключевые поля по категориям, геометрия в EPSG:4326."""

from __future__ import annotations

import copy
import json
import logging
from datetime import date
from pathlib import Path

import pytest

from cadastre_etl.nspd.client import RawAnswer
from cadastre_etl.nspd.parse import parse_answer

FIXTURES = Path(__file__).parent / "fixtures" / "nspd"


def answer(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def ok(data: dict, from_cache: bool = False) -> RawAnswer:
    return RawAnswer(200, data, from_cache)


def test_зу_из_3857_в_градусы():
    outcome = parse_answer("50:20:0010101:123", ok(answer("land_plot")))
    assert outcome.status == "found"
    assert outcome.kind == "land_plot"
    assert outcome.category_id == 36368 and outcome.nspd_id == 38001
    assert outcome.geom.geom_type == "MultiPolygon"
    minx, miny, maxx, maxy = outcome.geom.bounds
    assert minx == pytest.approx(37.6, abs=1e-6) and miny == pytest.approx(55.75, abs=1e-6)
    assert maxx == pytest.approx(37.6005, abs=1e-6) and maxy == pytest.approx(55.7505, abs=1e-6)
    assert outcome.fields == {
        "kind_label": "Земельный участок",
        "address": "Московская область, Одинцовский г.о., д. Дубки, уч. 5",
        "area_m2": 1500.0,
        "land_category": "Земли населенных пунктов",
        "permitted_use": "Для индивидуального жилищного строительства",
        "cadastral_value": 2345678.9,
        "egrn_status": "Учтенный",
        "registered_at": date(2005, 3, 15),
        "quarter": "50:20:0010101",
        "ownership_type": "Частная",
    }
    assert outcome.attrs["cost_index"] == 1563.79


def test_здание_поля_по_своим_именам():
    outcome = parse_answer("77:03:0001001:3030", ok(answer("building"), from_cache=True))
    assert (outcome.status, outcome.kind, outcome.from_cache) == ("found", "building", True)
    assert outcome.fields["area_m2"] == 5120.4
    assert outcome.fields["permitted_use"] == "Многоквартирный дом"
    assert outcome.fields["registered_at"] == date(2012, 7, 1)
    assert outcome.geom.geom_type == "MultiPolygon"


def test_сооружение_номер_в_cad_number():
    outcome = parse_answer("50:23:0000000:4567", ok(answer("structure")))
    assert (outcome.status, outcome.kind) == ("found", "structure")
    assert outcome.fields["address"] == "Московская область, Раменский г.о."
    assert outcome.fields["area_m2"] == 35.0
    assert outcome.fields["egrn_status"] == "Учтенный"
    assert outcome.geom.geom_type == "MultiPoint"
    point = outcome.geom.geoms[0]
    assert (point.x, point.y) == (pytest.approx(38.0, abs=1e-6), pytest.approx(55.5, abs=1e-6))


def test_помещение_без_своих_координат():
    outcome = parse_answer("77:01:0001001:2345", ok(answer("room_geocoder")))
    assert outcome.status == "found_no_geom"
    assert outcome.kind == "room"
    assert outcome.geom is None
    assert outcome.geom_approx.x == pytest.approx(37.62, abs=1e-6)
    assert outcome.fields["kind_label"] == "Помещение"
    assert outcome.attrs["parent_cad_number"] == "77:01:0001001:1024"


@pytest.mark.parametrize(
    "raw",
    [ok(answer("empty")), RawAnswer(404, None, True)],
    ids=["пусто", "404"],
)
def test_не_найдено(raw):
    outcome = parse_answer("50:20:0010101:123", raw)
    assert outcome.status == "not_found"
    assert outcome.final


def test_найден_только_другой_объект():
    """Поиск по номеру помещения вернул здание-родителя — это не тот объект."""
    assert parse_answer("77:01:0001001:9999", ok(answer("parent_only"))).status == "not_found"


def test_два_объекта_с_одним_номером():
    outcome = parse_answer("50:20:0010101:123", ok(answer("ambiguous")))
    assert outcome.status == "ambiguous"
    assert [c["id"] for c in outcome.attrs["candidates"]] == [38001, 38099]
    assert outcome.geom is None


def test_один_объект_дважды_не_неоднозначность():
    data = answer("land_plot")
    data["data"]["features"].append(copy.deepcopy(data["data"]["features"][0]))
    assert parse_answer("50:20:0010101:123", ok(data)).status == "found"


def test_незнакомая_категория():
    data = answer("land_plot")
    props = data["data"]["features"][0]["properties"]
    props["category"], props["categoryName"] = 99999, "Что-то новое"
    props["options"] = {"cad_num": "50:20:0010101:123", "name": "Объект", "area": "1 234,5"}
    outcome = parse_answer("50:20:0010101:123", ok(data))
    assert (outcome.status, outcome.kind, outcome.category_id) == ("found", "other", 99999)
    assert outcome.fields["area_m2"] == 1234.5
    assert outcome.attrs == props["options"]


def test_объект_без_геометрии():
    data = answer("land_plot")
    data["data"]["features"][0]["geometry"] = None
    outcome = parse_answer("50:20:0010101:123", ok(data))
    assert outcome.status == "found_no_geom"
    assert outcome.geom is None and outcome.geom_approx is None
    assert outcome.fields["area_m2"] == 1500.0


def test_метры_без_crs_перепроецируются(caplog):
    data = answer("land_plot")
    del data["data"]["features"][0]["geometry"]["crs"]
    with caplog.at_level(logging.WARNING):
        outcome = parse_answer("50:20:0010101:123", ok(data))
    assert outcome.geom.bounds[0] == pytest.approx(37.6, abs=1e-6)
    assert "EPSG:3857" in caplog.text


def test_тело_не_того_вида():
    outcome = parse_answer("50:20:0010101:123", ok({"message": "unexpected"}))
    assert outcome.status == "error"
    assert not outcome.final
    assert "не разобран" in outcome.error
