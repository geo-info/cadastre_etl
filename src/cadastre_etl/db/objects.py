"""Запись исходов НСПД в ``cadastral_objects`` и очередь повторов.

Хранится последняя версия объекта. Окончательный исход (``found``,
``found_no_geom``, ``not_found``, ``ambiguous``) переписывает строку целиком: объект
сняли с учёта — ``not_found`` очистит и геометрию. Ошибка уже найденное **не
затирает**: у объекта с окончательным статусом меняются только счётчик ошибок,
текст последней и время следующей попытки. ``pending`` (предохранитель, до НСПД
не дошли) строку не трогает.

Очередь повторов — строки ``pending`` и ``error`` со сроком ``next_attempt_at``:
первая ошибка — повтор в следующем прогоне, дальше паузы растут
(15 мин, 45 мин, 1 ч 45 мин…) с потолком в сутки. Так ошибка повторяется, даже
если лот больше не изменится, и не долбит НСПД каждый прогон.
"""

from __future__ import annotations

from collections.abc import Sequence

import shapely
from psycopg import AsyncConnection
from psycopg.types.json import Jsonb

from cadastre_etl.nspd.parse import Outcome

_UPSERT_FINAL = """
INSERT INTO cadastral_objects (
    cad_num, status, kind, kind_label, category_id, category_name, address, area_m2,
    land_category, permitted_use, cadastral_value, egrn_status, registered_at, quarter,
    ownership_type, attrs, nspd_id, geom, geom_approx,
    fetched_at, attempted_at, attempts, last_error, next_attempt_at
) VALUES (
    %(cad_num)s, %(status)s, %(kind)s, %(kind_label)s, %(category_id)s, %(category_name)s,
    %(address)s, %(area_m2)s, %(land_category)s, %(permitted_use)s, %(cadastral_value)s,
    %(egrn_status)s, %(registered_at)s, %(quarter)s, %(ownership_type)s, %(attrs)s, %(nspd_id)s,
    ST_SetSRID(ST_GeomFromWKB(%(geom)s), 4326), ST_SetSRID(ST_GeomFromWKB(%(geom_approx)s), 4326),
    now(), now(), 0, NULL, NULL
)
ON CONFLICT (cad_num) DO UPDATE SET
    status = EXCLUDED.status, kind = EXCLUDED.kind, kind_label = EXCLUDED.kind_label,
    category_id = EXCLUDED.category_id, category_name = EXCLUDED.category_name,
    address = EXCLUDED.address, area_m2 = EXCLUDED.area_m2, land_category = EXCLUDED.land_category,
    permitted_use = EXCLUDED.permitted_use, cadastral_value = EXCLUDED.cadastral_value,
    egrn_status = EXCLUDED.egrn_status, registered_at = EXCLUDED.registered_at,
    quarter = EXCLUDED.quarter, ownership_type = EXCLUDED.ownership_type, attrs = EXCLUDED.attrs,
    nspd_id = EXCLUDED.nspd_id, geom = EXCLUDED.geom, geom_approx = EXCLUDED.geom_approx,
    fetched_at = now(), attempted_at = now(), attempts = 0, last_error = NULL, next_attempt_at = NULL
"""

_UPSERT_ERROR = """
INSERT INTO cadastral_objects AS o (cad_num, status, attempted_at, attempts, last_error, next_attempt_at)
VALUES (%(cad_num)s, 'error', now(), 1, %(error)s, now())
ON CONFLICT (cad_num) DO UPDATE SET
    status = CASE WHEN o.status IN ('pending', 'error') THEN 'error' ELSE o.status END,
    attempted_at = now(),
    attempts = o.attempts + 1,
    last_error = EXCLUDED.last_error,
    next_attempt_at = now() + LEAST(interval '1 day', interval '15 minutes' * (power(2, o.attempts) - 1))
"""

_KEY_COLUMNS = (
    "kind_label",
    "address",
    "area_m2",
    "land_category",
    "permitted_use",
    "cadastral_value",
    "egrn_status",
    "registered_at",
    "quarter",
    "ownership_type",
)


def _final_params(outcome: Outcome) -> dict:
    params = {name: outcome.fields.get(name) for name in _KEY_COLUMNS}
    params.update(
        cad_num=outcome.cad_num,
        status=outcome.status,
        kind=outcome.kind,
        category_id=outcome.category_id,
        category_name=outcome.category_name,
        attrs=None if outcome.attrs is None else Jsonb(outcome.attrs),
        nspd_id=outcome.nspd_id,
        geom=None if outcome.geom is None else shapely.to_wkb(outcome.geom),
        geom_approx=None if outcome.geom_approx is None else shapely.to_wkb(outcome.geom_approx),
    )
    return params


async def write_outcomes(conn: AsyncConnection, outcomes: Sequence[Outcome]) -> None:
    """Записать исходы пачкой, в одной транзакции."""
    final = [_final_params(o) for o in sorted(outcomes, key=lambda o: o.cad_num) if o.final]
    errors = [
        {"cad_num": o.cad_num, "error": o.error}
        for o in sorted(outcomes, key=lambda o: o.cad_num)
        if o.status == "error"
    ]
    pending = sorted(o.cad_num for o in outcomes if o.status == "pending")
    async with conn.transaction(), conn.cursor() as cur:
        if final:
            await cur.executemany(_UPSERT_FINAL, final)
        if errors:
            await cur.executemany(_UPSERT_ERROR, errors)
        if pending:
            await cur.executemany(
                "INSERT INTO cadastral_objects (cad_num) VALUES (%s) ON CONFLICT DO NOTHING",
                [(n,) for n in pending],
            )


async def due_for_retry(conn: AsyncConnection, limit: int) -> list[str]:
    """Номера, которые пора спросить снова: ``pending`` и ``error`` со сроком."""
    cur = await conn.execute(
        "SELECT cad_num FROM cadastral_objects "
        "WHERE status IN ('pending', 'error') AND (next_attempt_at IS NULL OR next_attempt_at <= now()) "
        "ORDER BY next_attempt_at NULLS FIRST, cad_num LIMIT %s",
        (limit,),
    )
    return [row[0] for row in await cur.fetchall()]
