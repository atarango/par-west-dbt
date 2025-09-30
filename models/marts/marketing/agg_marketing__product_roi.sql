-- models/marts/marketing/agg_marketing__product_roi.sql
{{ config(materialized='view') }}

-- 1) Attributed order lines (already at campaign_id × attrib_date grain)
WITH f AS (
  SELECT
    order_id,
    LOWER(sku)                AS sku,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name,
    CAST(attrib_date AS DATE) AS attrib_date,
    COALESCE(attributed_revenue, 0) AS attributed_revenue,
    COALESCE(attributed_margin, 0)  AS attributed_margin
  FROM {{ ref('fct_marketing__product_attrib') }}
  WHERE campaign_id IS NOT NULL
),

-- 2) Daily campaign cost from staging (campaign_id × date → spend)
cost AS (
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(date AS DATE)          AS attrib_date,
    CAST(spend AS NUMERIC)      AS spend
  FROM {{ ref('stg_google_ads__campaign_daily_cost') }}
),

-- 3) Campaign-day revenue totals (for cost allocation share)
rev_by_cd AS (
  SELECT
    campaign_id,
    attrib_date,
    SUM(attributed_revenue) AS revenue_cd
  FROM f
  GROUP BY 1,2
),

-- 4) Allocate cost to SKUs by revenue share within (campaign_id, attrib_date)
alloc AS (
  SELECT
    f.sku,
    f.campaign_id,
    ANY_VALUE(f.campaign_name) AS sample_campaign,
    f.attrib_date,
    SUM(f.attributed_revenue)  AS revenue,
    SUM(f.attributed_margin)   AS margin,
    SUM( COALESCE(c.spend, 0) * SAFE_DIVIDE(f.attributed_revenue, NULLIF(r.revenue_cd, 0)) ) AS ad_cost
  FROM f
  LEFT JOIN cost     c ON c.campaign_id = f.campaign_id AND c.attrib_date = f.attrib_date
  LEFT JOIN rev_by_cd r ON r.campaign_id = f.campaign_id AND r.attrib_date = f.attrib_date
  GROUP BY 1,2,4
)

-- 5) Final rollup by SKU × campaign (do not group by sample_campaign)
SELECT
  sku,
  campaign_id,
  ANY_VALUE(sample_campaign) AS sample_campaign,  -- keep one label
  SUM(revenue) AS revenue,
  SUM(margin)  AS margin,
  SUM(ad_cost) AS ad_cost,
  -- ROI metrics
  SUM(margin)  - SUM(ad_cost) AS roi_margin,
  SAFE_DIVIDE(SUM(margin) - SUM(ad_cost), NULLIF(SUM(ad_cost),0)) AS roi_margin_pct,
  SAFE_DIVIDE(SUM(revenue),        NULLIF(SUM(ad_cost),0))        AS roas
FROM alloc
GROUP BY 1,2
