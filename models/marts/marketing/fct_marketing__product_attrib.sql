-- models/marts/marketing/fct_marketing__product_attrib.sql
{{ config(materialized='view') }}

WITH lines AS (
  SELECT
    l.order_id,                               -- INT64
    LOWER(l.sku)                    AS sku,
    l.quantity,
    l.line_total                   AS line_revenue,
    (l.quantity * c.cogs_per_unit) AS line_cogs,
    l.order_ts,
    l.order_date
  FROM {{ ref('stg_woocommerce_order_items') }} l
  LEFT JOIN {{ ref('stg_netsuite__item_costs') }} c
    ON LOWER(l.sku) = c.sku
  WHERE COALESCE(l.line_total, 0) > 0
    AND COALESCE(l.quantity,   0) > 0
),

attrib AS (
  SELECT
    l.order_id,
    l.sku,
    l.quantity,
    l.line_revenue,
    l.line_cogs,
    l.order_ts,
    l.order_date,
    b.campaign_id,
    b.campaign_name,
    b.click_dt,
    -- Align to spend by day: use click date if we have it, else order date
    COALESCE(DATE(b.click_dt), l.order_date) AS attrib_date
  FROM lines l
  LEFT JOIN {{ ref('bridge_order_ads_click') }} b
    ON CAST(l.order_id AS STRING) = b.order_id
)

-- Keep only attributed line items so downstream ROI isn’t swamped by nulls
SELECT
  order_id,
  sku,
  quantity,
  line_revenue            AS attributed_revenue,
  (line_revenue - line_cogs) AS attributed_margin,
  order_ts,
  order_date,
  attrib_date,
  campaign_id,
  campaign_name,
  click_dt
FROM attrib
WHERE campaign_id IS NOT NULL
