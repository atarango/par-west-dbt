{{ config(materialized='view') }}

-- Robustly extract utm_* and gclid/gbraid/wbraid from:
--  (1) orders.meta_data (any type) using STRING-based JSON functions
--  (2) order_notes.note (regex)
-- Emits one row per order_id.

-- 1) Normalize meta_data to a JSON STRING for consistent parsing
WITH orders_meta AS (
  SELECT
    SAFE_CAST(id AS INT64)       AS order_id,
    TO_JSON_STRING(meta_data)    AS meta_json_str   -- works for JSON, ARRAY<STRUCT>, etc.
  FROM {{ source('raw_woocommerce','orders') }}
),

-- 2) Explode meta_data as key/value pairs (STRING -> JSON functions)
meta_pairs AS (
  SELECT
    order_id,
    LOWER(JSON_EXTRACT_SCALAR(md, '$.key'))   AS meta_key,
    JSON_EXTRACT_SCALAR(md, '$.value')        AS meta_value
  FROM orders_meta,
  UNNEST(COALESCE(JSON_EXTRACT_ARRAY(meta_json_str, '$'), [])) AS md
),

-- 3) Pick tracking keys; include popular plugin variants for gclid
from_meta AS (
  SELECT
    order_id,
    MAX(IF(meta_key = 'utm_source',   meta_value, NULL)) AS utm_source,
    MAX(IF(meta_key = 'utm_medium',   meta_value, NULL)) AS utm_medium,
    MAX(IF(meta_key = 'utm_campaign', meta_value, NULL)) AS utm_campaign,
    MAX(IF(meta_key = 'utm_term',     meta_value, NULL)) AS utm_term,
    MAX(IF(meta_key = 'utm_content',  meta_value, NULL)) AS utm_content,

    MAX(IF(meta_key IN ('gclid','_gclid','google_gclid','gads_gclid','pys_gclid'), meta_value, NULL)) AS gclid,
    MAX(IF(meta_key = 'wbraid', meta_value, NULL)) AS wbraid,
    MAX(IF(meta_key = 'gbraid', meta_value, NULL)) AS gbraid
  FROM meta_pairs
  GROUP BY order_id
),

-- 4) Fallback parse from order notes (landing URL text)
from_notes AS (
  SELECT
    SAFE_CAST(order_id AS INT64)                                        AS order_id,
    REGEXP_EXTRACT(note, r'(?:[?&]utm_source=)([^&\s]+)')               AS utm_source,
    REGEXP_EXTRACT(note, r'(?:[?&]utm_medium=)([^&\s]+)')               AS utm_medium,
    REGEXP_EXTRACT(note, r'(?:[?&]utm_campaign=)([^&\s]+)')             AS utm_campaign,
    REGEXP_EXTRACT(note, r'(?:[?&]utm_term=)([^&\s]+)')                 AS utm_term,
    REGEXP_EXTRACT(note, r'(?:[?&]utm_content=)([^&\s]+)')              AS utm_content,
    REGEXP_EXTRACT(note, r'(?:[?&]gclid=)([A-Za-z0-9_-]{10,})')         AS gclid,
    REGEXP_EXTRACT(note, r'(?:[?&]wbraid=)([A-Za-z0-9_-]{10,})')        AS wbraid,
    REGEXP_EXTRACT(note, r'(?:[?&]gbraid=)([A-Za-z0-9_-]{10,})')        AS gbraid
  FROM {{ source('raw_woocommerce','order_notes') }}
  WHERE REGEXP_CONTAINS(note, r'(utm_|gclid|wbraid|gbraid)=')
),

-- 5) Merge (prefer meta over notes)
merged AS (
  SELECT
    COALESCE(m.order_id, n.order_id)        AS order_id,
    COALESCE(m.utm_source,   n.utm_source)  AS utm_source,
    COALESCE(m.utm_medium,   n.utm_medium)  AS utm_medium,
    COALESCE(m.utm_campaign, n.utm_campaign)AS utm_campaign,
    COALESCE(m.utm_term,     n.utm_term)    AS utm_term,
    COALESCE(m.utm_content,  n.utm_content) AS utm_content,
    COALESCE(m.gclid,        n.gclid)       AS gclid,
    COALESCE(m.wbraid,       n.wbraid)      AS wbraid,
    COALESCE(m.gbraid,       n.gbraid)      AS gbraid
  FROM from_meta m
  FULL JOIN from_notes n ON n.order_id = m.order_id
)

SELECT * FROM merged