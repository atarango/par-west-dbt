-- models/marts/marketing/agg_marketing__product_roi.sql
{{ config(materialized='view') }}

WITH f AS (
  SELECT
    order_id,
    LOWER(sku) AS sku,
    campaign_id,
    campaign_name,
    attrib_date,
    attributed_revenue,
    attributed_margin
  FROM {{ ref('fct_marketing__product_attrib') }}
  WHERE campaign_id IS NOT NULL
),

-- cost by (campaign_id, date)
cost_campaign AS (
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(segments_date AS DATE) AS cost_date,
    SUM(SAFE_DIVIDE(metrics_cost_micros, 1e6)) AS spend_amount
  FROM {{ source('raw_google_ads','campaign') }}
  WHERE segments_date IS NOT NULL
  GROUP BY 1,2
),
cost_kw AS (
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(segments_date AS DATE) AS cost_date,
    SUM(SAFE_DIVIDE(metrics_cost_micros, 1e6)) AS spend_amount
  FROM {{ source('raw_google_ads','keyword_view') }}
  WHERE segments_date IS NOT NULL
  GROUP BY 1,2
),
cost_dkw AS (
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(segments_date AS DATE) AS cost_date,
    SUM(SAFE_DIVIDE(metrics_cost_micros, 1e6)) AS spend_amount
  FROM {{ source('raw_google_ads','display_keyword_view') }}
  WHERE segments_date IS NOT NULL
  GROUP BY 1,2
),
cost_union AS (
  SELECT * FROM cost_campaign
  UNION ALL SELECT * FROM cost_kw
  UNION ALL SELECT * FROM cost_dkw
),
cost AS (
  SELECT campaign_id, cost_date AS attrib_date, SUM(spend_amount) AS spend
  FROM cost_union
  GROUP BY 1,2
),

rev_by_cd AS (
  SELECT campaign_id, attrib_date, SUM(attributed_revenue) AS revenue_cd
  FROM f
  GROUP BY 1,2
),

alloc AS (
  SELECT
    f.sku,
    f.campaign_id,
    ANY_VALUE(f.campaign_name) AS sample_campaign,
    f.attrib_date,
    SUM(f.attributed_revenue) AS revenue,
    SUM(f.attributed_margin)  AS margin,
    SUM( COALESCE(c.spend, 0) * SAFE_DIVIDE(f.attributed_revenue, NULLIF(r.revenue_cd, 0)) ) AS ad_cost
  FROM f
  LEFT JOIN cost      c ON c.campaign_id = f.campaign_id AND c.attrib_date = f.attrib_date
  LEFT JOIN rev_by_cd r ON r.campaign_id = f.campaign_id AND r.attrib_date = f.attrib_date
  GROUP BY 1,2,4
)

SELECT
  sku,
  campaign_id,
  sample_campaign,
  SUM(revenue) AS revenue,
  SUM(margin)  AS margin,
  SUM(ad_cost) AS ad_cost,
  SUM(margin)  - SUM(ad_cost) AS roi_margin,
  SUM(revenue) - SUM(ad_cost) AS roi_revenue
FROM alloc
GROUP BY 1,2,3