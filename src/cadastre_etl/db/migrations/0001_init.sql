-- Схема cadastre-etl: лоты, кадастровые объекты, связь между ними, состояние площадок.
-- Подробности решений — docs/specs/2026-09-29-cadastre-etl-design.md, раздел «PostGIS».

CREATE EXTENSION IF NOT EXISTS postgis;

-- Лоты с хотя бы одним кадастровым номером или кварталом. Ключ — как в Mongo.
CREATE TABLE lots (
    source           text NOT NULL,
    lot_id           text NOT NULL,
    lot_url          text,
    trade_url        text,
    trade_number     text,
    lot_num          text,
    description      text,
    status           text,
    is_active        boolean,
    price            text,              -- как на площадке (у iTender разобранной нет)
    price_value      numeric,
    bids_end_at      timestamptz,
    auction_at       timestamptz,
    debtor           text,
    organizer        text,
    cad_quarters     text[] NOT NULL DEFAULT '{}',
    mongo_created_at timestamptz,
    mongo_updated_at timestamptz,
    mongo_detail_at  timestamptz,
    loaded_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, lot_id)
);

-- Кадастровый объект: последняя версия сведений НСПД и исход последнего запроса.
CREATE TABLE cadastral_objects (
    cad_num         text PRIMARY KEY,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'found', 'found_no_geom', 'not_found', 'ambiguous', 'error')),
    kind            text,
    kind_label      text,
    category_id     integer,
    category_name   text,
    address         text,
    area_m2         numeric,
    land_category   text,
    permitted_use   text,
    cadastral_value numeric,
    egrn_status     text,
    registered_at   date,
    quarter         text,
    ownership_type  text,
    attrs           jsonb,
    nspd_id         bigint,
    geom            geometry(Geometry, 4326),
    geom_approx     geometry(Point, 4326),   -- точка геокодера у объектов без своих координат
    fetched_at      timestamptz,             -- последний окончательный ответ НСПД
    attempted_at    timestamptz,
    attempts        integer NOT NULL DEFAULT 0,   -- ошибок подряд
    last_error      text,
    next_attempt_at timestamptz
);
CREATE INDEX cadastral_objects_geom_gist ON cadastral_objects USING gist (geom);
CREATE INDEX cadastral_objects_retry ON cadastral_objects (next_attempt_at)
    WHERE status IN ('pending', 'error');

CREATE TABLE lot_cadastral (
    source     text NOT NULL,
    lot_id     text NOT NULL,
    cad_num    text NOT NULL REFERENCES cadastral_objects (cad_num),
    found_in   text,                         -- 'description' или путь в detail
    first_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, lot_id, cad_num),
    FOREIGN KEY (source, lot_id) REFERENCES lots (source, lot_id) ON DELETE CASCADE
);
CREATE INDEX lot_cadastral_cad_num ON lot_cadastral (cad_num);

-- Состояние площадки (коллекции Mongo): до какого момента всё прочитано и итог прогона.
CREATE TABLE etl_state (
    source           text PRIMARY KEY,
    checked_at       timestamptz,            -- NULL: успешного прогона ещё не было
    last_started_at  timestamptz,
    last_finished_at timestamptz,
    last_status      text CHECK (last_status IN ('ok', 'failed')),
    last_error       text,
    last_lots        integer,
    last_numbers     integer,
    last_fetched     integer,
    last_cached      integer,
    last_not_found   integer,
    last_errors      integer
);

-- Готовый слой для QGIS: лот × объект с геометрией.
CREATE VIEW lot_objects AS
SELECT
    l.source,
    l.lot_id,
    l.lot_url,
    l.description,
    l.status          AS lot_status,
    l.is_active,
    l.price_value,
    l.bids_end_at,
    l.auction_at,
    o.cad_num,
    o.status          AS fetch_status,
    o.kind,
    o.kind_label,
    o.address,
    o.area_m2,
    o.land_category,
    o.permitted_use,
    o.cadastral_value,
    o.egrn_status,
    ST_IsValid(o.geom) AS geom_valid,
    o.geom
FROM lot_cadastral lc
JOIN lots l USING (source, lot_id)
JOIN cadastral_objects o USING (cad_num);
