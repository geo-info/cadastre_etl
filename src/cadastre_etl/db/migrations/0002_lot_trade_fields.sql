-- Поля торгов, которые пишут все движки trading_platform: номер и тип торгов,
-- название аукциона, победитель и сроки строками, как на площадке (у iTender
-- разобранных bids_end_at/auction_at нет — остаются только строки).
--
-- У лотов, загруженных до этой миграции, поля пустые до перечитывания:
-- `python -m cadastre_etl run --full`.

ALTER TABLE lots
    ADD COLUMN trade_id     text,
    ADD COLUMN trade_type   text,
    ADD COLUMN auction_name text,
    ADD COLUMN winner       text,
    ADD COLUMN bids_end     text,
    ADD COLUMN auction_date text;

DROP VIEW lot_objects;
CREATE VIEW lot_objects AS
SELECT
    l.source,
    l.lot_id,
    l.lot_url,
    l.trade_id,
    l.trade_number,
    l.trade_type,
    l.auction_name,
    l.lot_num,
    l.description,
    l.status          AS lot_status,
    l.is_active,
    l.price,
    l.price_value,
    l.bids_end,
    l.bids_end_at,
    l.auction_date,
    l.auction_at,
    l.debtor,
    l.organizer,
    l.winner,
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
