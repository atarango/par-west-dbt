-- models/staging/woocommerce/stg_woocommerce_order_items.sql
{{ config(
  alias='stg_woocommerce_order_items',
  materialized='incremental',
  partition_by={'field': 'order_date', 'data_type': 'date'},
  cluster_by=['order_id', 'sku'],
  incremental_strategy='insert_overwrite'
) }}

-- Anchor on deduped orders to avoid exploding multiple raw snapshots
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
    SAFE_CAST(id AS INT64)  AS order_id,
    CAST(line_items AS JSON) AS line_items_json,
    _airbyte_extracted_at
  FROM {{ source('raw_woocommerce','orders') }}
  WHERE line_items IS NOT NULL
),

base AS (
  SELECT
    o.order_id,
    o.order_ts,
    o.order_date,
    r.line_items_json,
    r._airbyte_extracted_at
  FROM orders o
  JOIN raw   r
    ON r.order_id = o.order_id
),

exploded AS (
  SELECT
    b.order_id,
    b.order_ts,
    b.order_date,
    b._airbyte_extracted_at,
    item AS item_json
  FROM base b,
  UNNEST(COALESCE(JSON_QUERY_ARRAY(b.line_items_json, '$'), [])) AS item
),

taxes AS (
  SELECT
    e.order_id,
    SAFE_CAST(JSON_VALUE(e.item_json, '$.id') AS INT64) AS order_item_id,
    SUM(SAFE_CAST(JSON_VALUE(t, '$.total') AS NUMERIC)) AS item_tax_total
  FROM exploded e,
  UNNEST(COALESCE(JSON_QUERY_ARRAY(e.item_json, '$.taxes'), [])) AS t
  GROUP BY e.order_id, order_item_id
),

products AS (
  SELECT
    SAFE_CAST(product_id AS INT64) AS id,
    NULLIF(TRIM(CAST(sku AS STRING)), '') AS sku
  FROM {{ ref('stg_woocommerce_products') }}
),

variations AS (
  SELECT
    id  AS variation_id,
    sku AS variation_sku
  FROM {{ ref('stg_woocommerce_product_variations') }}
),

joined AS (
  SELECT
    e.order_id,
    e.order_ts,
    e.order_date,
    e._airbyte_extracted_at,

    SAFE_CAST(JSON_VALUE(e.item_json, '$.id') AS INT64)                      AS order_item_id,
    NULLIF(SAFE_CAST(JSON_VALUE(e.item_json, '$.product_id')   AS INT64), 0) AS product_id,
    NULLIF(SAFE_CAST(JSON_VALUE(e.item_json, '$.variation_id') AS INT64), 0) AS variation_id,

    COALESCE(
      NULLIF(TRIM(JSON_VALUE(e.item_json, '$.sku')), ''),
      v.variation_sku,
      p.sku
    ) AS sku,

    CASE
      WHEN NULLIF(SAFE_CAST(JSON_VALUE(e.item_json, '$.product_id') AS INT64), 0) IS NOT NULL
           AND (v.variation_sku IS NOT NULL OR p.sku IS NOT NULL)
      THEN TRUE ELSE FALSE
    END AS require_sku,

    JSON_VALUE(e.item_json, '$.name')        AS product_name,
    JSON_VALUE(e.item_json, '$.parent_name') AS parent_name,

    SAFE_CAST(JSON_VALUE(e.item_json, '$.quantity')     AS INT64)   AS quantity,
    SAFE_CAST(JSON_VALUE(e.item_json, '$.price')        AS NUMERIC) AS price,
    SAFE_CAST(JSON_VALUE(e.item_json, '$.subtotal')     AS NUMERIC) AS subtotal,
    SAFE_CAST(JSON_VALUE(e.item_json, '$.subtotal_tax') AS NUMERIC) AS subtotal_tax,
    SAFE_CAST(JSON_VALUE(e.item_json, '$.total')        AS NUMERIC) AS line_total,
    SAFE_CAST(JSON_VALUE(e.item_json, '$.total_tax')    AS NUMERIC) AS line_total_tax,

    COALESCE(t.item_tax_total, SAFE_CAST(JSON_VALUE(e.item_json, '$.total_tax') AS NUMERIC)) AS item_tax_total,

    JSON_VALUE(e.item_json, '$.image.src') AS image_url
  FROM exploded e
  LEFT JOIN taxes t
    ON t.order_id = e.order_id
   AND t.order_item_id = SAFE_CAST(JSON_VALUE(e.item_json, '$.id') AS INT64)
  LEFT JOIN variations v
    ON v.variation_id = NULLIF(SAFE_CAST(JSON_VALUE(e.item_json, '$.variation_id') AS INT64), 0)
  LEFT JOIN products p
    ON p.id = NULLIF(SAFE_CAST(JSON_VALUE(e.item_json, '$.product_id') AS INT64), 0)
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id, order_item_id
      ORDER BY order_ts DESC, _airbyte_extracted_at DESC
    ) AS rn
  FROM joined
)

SELECT
  order_id,
  order_ts,
  order_date,
  order_item_id,
  product_id,
  variation_id,
  sku,
  require_sku,
  product_name,
  parent_name,
  quantity,
  price,
  subtotal,
  subtotal_tax,
  line_total,        -- keep contracted names/types
  line_total_tax,
  item_tax_total,
  image_url
FROM ranked
WHERE rn = 1
{% if is_incremental() %}
  AND order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 18 MONTH)
{% endif %}
