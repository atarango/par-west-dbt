{{ config(materialized='view') }}

WITH lines AS (
  SELECT
    l.order_id,                                  -- INT64 (from your staging contract)
    LOWER(l.sku)                         AS sku,
    l.quantity,
    l.line_total                        AS line_revenue,   -- <-- alias from line_total
    (l.quantity * c.cogs_per_unit)      AS line_cogs,
    l.order_ts,
    l.order_date
  FROM {{ ref('stg_woocommerce_order_items') }} l
  LEFT JOIN {{ ref('stg_netsuite__item_costs') }} c
    ON LOWER(l.sku) = c.sku
  WHERE COALESCE(l.line_total, 0) > 0 AND COALESCE(l.quantity, 0) > 0
)

SELECT
  l.order_id,
  b.campaign_id,
  b.campaign_name,
  b.click_dt,
  l.order_ts,
  l.order_date,
  l.sku,
  l.line_revenue                         AS attributed_revenue,
  (l.line_revenue - l.line_cogs)         AS attributed_margin
FROM lines l
LEFT JOIN {{ ref('bridge_order_ads_click') }} b
  ON b.order_id = CAST(l.order_id AS STRING)   -- bridge emits order_id as STRING