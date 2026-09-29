"""Ответ поиска НСПД → исход по номеру: статус, тип объекта, ключевые поля, геометрия.

Исходы (``cadastral_objects.status``):

- ``found`` — объект найден, у него есть своя геометрия (контур или точка);
- ``found_no_geom`` — найден, но своих координат нет (``geocoderObject``: помещения,
  часть ЕЗП) или геометрии в ответе нет; точка геокодера, если есть, —
  в ``geom_approx``, а не в ``geom``: это не местоположение объекта;
- ``not_found`` — пусто, 404 или нет объекта ровно с этим номером (поиск находит
  и родителя помещения по его номеру — это не тот объект);
- ``ambiguous`` — несколько объектов ровно с этим номером; кандидаты в ``attrs``;
- ``error`` — ответ не разобрался (тело не того вида).

Каждый объект ответа разбирается отдельно: модель ``SearchResponse`` из pynspd
отвергла бы весь ответ из-за одного объекта без ``geometry``, а атрибуты нам
нужны и без неё. Геометрия проверяется моделью ``Geometry`` pynspd — она же
перепроецирует в EPSG:4326 по полю ``crs`` (НСПД отдаёт 3857). Если ``crs`` нет,
pynspd считает координаты градусами; метры 3857 без пометки видны по охвату —
такие перепроецируем явно и пишем предупреждение.

Атрибуты у категорий НСПД называются по-разному (``readable_address`` у ЗУ и
зданий, ``address_readable_address`` у сооружений, площадь — ``specified_area``,
``build_record_area``, ``params_area``…). Ключевые колонки берутся по спискам
имён, первое непустое; всё остальное остаётся в ``attrs`` как пришло.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from pydantic import TypeAdapter
from pynspd.schemas.geometries import Geometry
from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from cadastre_etl.nspd.client import RawAnswer

log = logging.getLogger(__name__)

#: Категории НСПД из автогенерённых слоёв pynspd → вид объекта.
KINDS = {36368: "land_plot", 36369: "building", 36383: "structure", 36384: "unfinished"}
#: Для категорий вне списка — по подписи вида объекта.
KIND_BY_LABEL = (
    ("помещ", "room"),
    ("машино", "parking"),
    ("незаверш", "unfinished"),
    ("сооруж", "structure"),
)

#: Ключевая колонка → имена полей ``options`` у разных категорий, по приоритету.
KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "kind_label": ("land_record_type", "build_record_type_value", "object_type_value", "type", "params_type"),
    "address": ("readable_address", "address_readable_address"),
    "area_m2": (
        "specified_area",
        "declared_area",
        "area",
        "build_record_area",
        "params_area",
        "params_built_up_area",
    ),
    "land_category": ("land_record_category_type",),
    "permitted_use": ("permitted_use_established_by_document", "purpose", "params_purpose"),
    "cadastral_value": ("cost_value",),
    "egrn_status": ("status", "object_previously_posted"),
    "registered_at": ("land_record_reg_date", "build_record_registration_date", "registration_date"),
    "quarter": ("quarter_cad_number",),
    "ownership_type": ("ownership_type",),
}
NUMERIC = {"area_m2", "cadastral_value"}

_GEOMETRY = TypeAdapter(Geometry)
_WEB_MERCATOR = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


@dataclass
class Outcome:
    """Исход по одному номеру — то, что пишется в ``cadastral_objects``."""

    cad_num: str
    status: str
    from_cache: bool = False
    kind: str | None = None
    category_id: int | None = None
    category_name: str | None = None
    nspd_id: int | None = None
    #: Ключевые колонки: ``kind_label``, ``address``, ``area_m2``… (см. ``KEY_FIELDS``).
    fields: dict[str, Any] = field(default_factory=dict)
    attrs: dict[str, Any] | None = None
    geom: BaseGeometry | None = None
    geom_approx: Point | None = None
    error: str | None = None

    @property
    def final(self) -> bool:
        """Окончательный ли исход (не ошибка и не отложенный номер)."""
        return self.status not in ("pending", "error")


def parse_answer(cad_num: str, raw: RawAnswer) -> Outcome:
    """Исход по номеру из ответа поиска; не бросает — неразобранное становится ``error``."""
    if raw.status_code == 404 or raw.data is None:
        return Outcome(cad_num, "not_found", from_cache=raw.from_cache)
    try:
        features = raw.data["data"]["features"] or []
        exact = _unique([f for f in features if _cad_num_of(f) == cad_num])
        if not exact:
            return Outcome(cad_num, "not_found", from_cache=raw.from_cache)
        if len(exact) > 1:
            candidates = [{"id": f.get("id"), **(f.get("properties") or {})} for f in exact]
            return Outcome(cad_num, "ambiguous", from_cache=raw.from_cache, attrs={"candidates": candidates})
        return _found(cad_num, exact[0], raw.from_cache)
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        return Outcome(cad_num, "error", from_cache=raw.from_cache, error=f"ответ не разобран: {error!r}")


def _cad_num_of(feature: dict[str, Any]) -> str | None:
    options = (feature.get("properties") or {}).get("options") or {}
    return options.get("cad_num") or options.get("cad_number")


def _unique(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Один объект может прийти дважды (из двух слоёв); различаем по id."""
    seen: dict[Any, dict[str, Any]] = {}
    for feature in features:
        seen.setdefault(feature.get("id", id(feature)), feature)
    return list(seen.values())


def _found(cad_num: str, feature: dict[str, Any], from_cache: bool) -> Outcome:
    props = feature["properties"]
    options: dict[str, Any] = dict(props.get("options") or {})
    category = props.get("category")
    fields = {name: _first(options, names) for name, names in KEY_FIELDS.items()}
    for name in NUMERIC:
        fields[name] = _number(fields[name])
    fields["registered_at"] = _date(fields["registered_at"])
    fields["kind_label"] = None if fields["kind_label"] is None else str(fields["kind_label"])

    shape = _shape(cad_num, feature.get("geometry"))
    no_coords = bool(options.get("geocoderObject"))
    outcome = Outcome(
        cad_num,
        "found" if shape is not None and not no_coords else "found_no_geom",
        from_cache=from_cache,
        kind=_kind(category, fields["kind_label"]),
        category_id=category if isinstance(category, int) else None,
        category_name=props.get("categoryName"),
        nspd_id=feature.get("id") if isinstance(feature.get("id"), int) else None,
        fields=fields,
        attrs=options,
    )
    if shape is not None and not no_coords:
        outcome.geom = _to_multi(shape)
    elif shape is not None:
        outcome.geom_approx = shape if isinstance(shape, Point) else shape.representative_point()
    return outcome


def _kind(category: Any, label: str | None) -> str:
    if category in KINDS:
        return KINDS[category]
    lowered = (label or "").lower()
    for marker, kind in KIND_BY_LABEL:
        if marker in lowered:
            return kind
    return "other"


def _first(options: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        value = options.get(name)
        if value not in (None, ""):
            return value
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(" ", "").replace(" ", "").replace(",", "."))
        except ValueError:
            return None
    return None


def _date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    for pattern in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value[:10], pattern).date()
        except ValueError:
            continue
    return None


def _shape(cad_num: str, geometry: Any) -> BaseGeometry | None:
    """Геометрия в EPSG:4326 или ``None``; неразобранная — предупреждение, не ошибка."""
    if not geometry:
        return None
    try:
        shape = _GEOMETRY.validate_python(geometry).to_shape()
    except ValueError as error:
        log.warning("%s: геометрия не разобрана, пишем без неё: %s", cad_num, error)
        return None
    if shape.is_empty:
        return None
    minx, miny, maxx, maxy = shape.bounds
    if max(abs(minx), abs(maxx)) > 180 or max(abs(miny), abs(maxy)) > 90:
        log.warning("%s: координаты вне градусов и без crs — считаем их EPSG:3857", cad_num)
        shape = transform(_WEB_MERCATOR.transform, shape)
    return shape


def _to_multi(shape: BaseGeometry) -> BaseGeometry:
    """Одиночную геометрию — в Multi*, чтобы тип в колонке был однороден по видам."""
    if isinstance(shape, Polygon):
        return MultiPolygon([shape])
    if isinstance(shape, LineString):
        return MultiLineString([shape])
    if isinstance(shape, Point):
        return MultiPoint([shape])
    return shape
