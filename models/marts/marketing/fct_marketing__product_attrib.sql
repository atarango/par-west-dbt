{{ config(materialized='view') }}

WITH lines AS (
  SELECT
    l.order_id,
    l.order_ts,
    l.order_date,
    l.sku,                                                    -- keep raw for display
    REGEXP_REPLACE(LOWER(l.sku), r'[^a-z0-9]+','') AS sku_key, -- normalized for joins
    l.quantity,
    l.line_total AS line_revenue
  FROM {{ ref('stg_woocommerce_order_items') }} l
  WHERE COALESCE(l.line_total, 0) > 0
    AND COALESCE(l.quantity,   0) > 0
),

costed AS (
  SELECT
    ln.order_id, ln.order_ts, ln.order_date,
    ln.sku, ln.sku_key, ln.quantity, ln.line_revenue,
    c.cogs_per_unit,
    (ln.quantity * c.cogs_per_unit) AS line_cogs
  FROM lines ln
  LEFT JOIN {{ ref('stg_netsuite__item_costs') }} c
    ON ln.sku_key = c.sku_key
),

b AS (
  SELECT
    order_id,
    order_number,
    created_at,
    gclid,
    campaign_id,
    campaign_name,
    click_dt
  FROM {{ ref('bridge_order_ads_click') }}
)

SELECT
  c.order_id,
  b.campaign_id,
  b.campaign_name,
  b.click_dt,
  c.order_ts,
  c.order_date,
  c.sku,

  -- revenue & margin
  c.line_revenue                               AS attributed_revenue,
  (c.line_revenue - c.line_cogs)               AS attributed_margin,

  -- attribution date for cost allocation
  COALESCE(DATE(b.click_dt), c.order_date)     AS attrib_date
FROM costed c
LEFT JOIN b ON b.order_id = CAST(c.order_id AS STRING)