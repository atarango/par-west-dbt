{{ config(materialized='view') }}

-- Aggregates SKU-level ROI using:
--  - fct_marketing__product_attrib (line-level revenue & margin with campaign_id/name + click_dt/order_date)
--  - stg_google_ads__campaign_daily_cost (daily spend by campaign_id from keyword_view)
--  - stg_google_ads__campaign_dim (optional: enrich names from raw_google_ads.campaign)

WITH cost AS (
  -- Daily campaign spend (no campaign_name here)
  SELECT
    date,
    campaign_id,
    cost
  FROM {{ ref('stg_google_ads__campaign_daily_cost') }}
),

facts AS (
  -- Product-line facts with attribution
  SELECT
    f.sku,
    f.campaign_id,
    -- Keep the name coming from click_view (via bridge). May be null if no click matched.
    f.campaign_name,
    COALESCE(DATE(f.click_dt), f.order_date) AS date_key,
    f.attributed_revenue,
    f.attributed_margin
  FROM {{ ref('fct_marketing__product_attrib') }} f
),

names AS (
  -- Optional enrichment: campaign names from the campaign table.
  -- If your campaign table uses a different column (e.g., campaign_name), swap "name" below accordingly.
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    ANY_VALUE(campaign_name)    AS campaign_name
  FROM {{ source('raw_google_ads','campaign') }}
  GROUP BY 1
)

SELECT
  f.sku,
  ANY_VALUE(f.campaign_id)                                        AS campaign_id,
  -- Prefer the campaign name from facts (click_view). Fall back to the campaign table when missing.
  ANY_VALUE(COALESCE(f.campaign_name, n.campaign_name))           AS sample_campaign,
  SUM(f.attributed_revenue)                                       AS revenue,
  SUM(f.attributed_margin)                                        AS margin,
  SUM(IFNULL(c.cost, 0))                                          AS ad_cost,
  SAFE_DIVIDE(SUM(f.attributed_margin) - SUM(IFNULL(c.cost, 0)),
              NULLIF(SUM(IFNULL(c.cost, 0)), 0))                  AS roi_margin,
  SAFE_DIVIDE(SUM(f.attributed_revenue) - SUM(IFNULL(c.cost, 0)),
              NULLIF(SUM(IFNULL(c.cost, 0)), 0))                  AS roi_revenue
FROM facts f
LEFT JOIN cost  c
  ON c.campaign_id = f.campaign_id
 AND c.date        = f.date_key
LEFT JOIN names n
  ON n.campaign_id = f.campaign_id
GROUP BY f.sku
ORDER BY roi_margin DESC