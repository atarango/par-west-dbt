-- models/staging/woocommerce/stg_woocommerce_shipping_lines.sql
{{ config(
    materialized='incremental',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['order_id'],
    alias='stg_woocommerce_shipping_lines',
    incremental_strategy='insert_overwrite'
) }}

-- Anchor on parent orders so child rows only exist for present orders
WITH orders AS (
  SELECT
    SAFE_CAST(order_id AS INT64) AS order_id,
    created_at_utc               AS order_ts,
    order_date
  FROM {{ ref('stg_woocommerce_orders') }}
  {% if is_incremental() %}
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
  {% endif %}
),

raw AS (
  SELECT
    SAFE_CAST(id AS INT64) AS order_id,
    shipping_lines         AS shipping_lines_json
  FROM {{ source('raw_woocommerce','orders') }}
),

base AS (
  SELECT
    o.order_id,
    o.order_ts,
    o.order_date,
    r.shipping_lines_json
  FROM orders o
  JOIN raw r
    ON r.order_id = o.order_id
),

shipping_lines AS (
  SELECT
    b.order_id,
    b.order_ts,
    b.order_date,
    SAFE_CAST(JSON_VALUE(l, '$.id') AS INT64)         AS shipping_line_id,
    JSON_VALUE(l, '$.tax_status')                     AS tax_status,
    SAFE_CAST(JSON_VALUE(l, '$.total')     AS NUMERIC) AS shipping_total,
    SAFE_CAST(JSON_VALUE(l, '$.total_tax') AS NUMERIC) AS shipping_tax_total,

    (SELECT CAST(JSON_VALUE(md, '$.value') AS STRING)
       FROM UNNEST(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      WHERE JSON_VALUE(md, '$.key') = 'carrier_service_code' LIMIT 1) AS carrier_service_code,

    (SELECT CAST(JSON_VALUE(md, '$.value') AS STRING)
       FROM UNNEST(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      WHERE JSON_VALUE(md, '$.key') = 'service_type' LIMIT 1)        AS service_type,

    (SELECT CAST(JSON_VALUE(md, '$.value') AS STRING)
       FROM UNNEST(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      WHERE JSON_VALUE(md, '$.key') = 'collection_point' LIMIT 1)    AS collection_point,

    (SELECT CAST(JSON_VALUE(md, '$.value') AS STRING)
       FROM UNNEST(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      WHERE JSON_VALUE(md, '$.key') = 'delivery_dates' LIMIT 1)      AS delivery_dates

  FROM base b,
  UNNEST(COALESCE(JSON_QUERY_ARRAY(b.shipping_lines_json, '$'), [])) AS l
)

SELECT * FROM shipping_lines
{% if is_incremental() %}
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
{% endif %}
