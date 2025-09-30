-- models/staging/google_ads/stg_google_ads__campaign_daily_cost.sql
{{ config(materialized='view') }}

-- Robust date parser (handles DATE, YYYY-MM-DD, YYYYMMDD)
WITH kw AS (
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CASE
      WHEN REGEXP_CONTAINS(CAST(segments_date AS STRING), r'^\d{4}-\d{2}-\d{2}$')
        THEN CAST(segments_date AS DATE)
      WHEN REGEXP_CONTAINS(CAST(segments_date AS STRING), r'^\d{8}$')
        THEN PARSE_DATE('%Y%m%d', CAST(segments_date AS STRING))
      ELSE CAST(segments_date AS DATE)
    END AS date,
    SAFE_DIVIDE(SUM(metrics_cost_micros), 1e6) AS spend,
    SUM(metrics_clicks)     AS clicks,
    SUM(metrics_impressions) AS impressions
  FROM {{ source('raw_google_ads','keyword_view') }}
  WHERE segments_date IS NOT NULL AND metrics_cost_micros IS NOT NULL
  GROUP BY 1,2
),
dkw AS (
  -- display_keyword_view can carry Display costs
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CASE
      WHEN REGEXP_CONTAINS(CAST(segments_date AS STRING), r'^\d{4}-\d{2}-\d{2}$')
        THEN CAST(segments_date AS DATE)
      WHEN REGEXP_CONTAINS(CAST(segments_date AS STRING), r'^\d{8}$')
        THEN PARSE_DATE('%Y%m%d', CAST(segments_date AS STRING))
      ELSE CAST(segments_date AS DATE)
    END AS date,
    SAFE_DIVIDE(SUM(metrics_cost_micros), 1e6) AS spend,
    SUM(metrics_clicks)     AS clicks,
    SUM(metrics_impressions) AS impressions
  FROM {{ source('raw_google_ads','display_keyword_view') }}
  WHERE segments_date IS NOT NULL AND metrics_cost_micros IS NOT NULL
  GROUP BY 1,2
),
camp AS (
  -- campaign table covers PMax and anything not keyword-based
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    CASE
      WHEN REGEXP_CONTAINS(CAST(segments_date AS STRING), r'^\d{4}-\d{2}-\d{2}$')
        THEN CAST(segments_date AS DATE)
      WHEN REGEXP_CONTAINS(CAST(segments_date AS STRING), r'^\d{8}$')
        THEN PARSE_DATE('%Y%m%d', CAST(segments_date AS STRING))
      ELSE CAST(segments_date AS DATE)
    END AS date,
    SAFE_DIVIDE(SUM(metrics_cost_micros), 1e6) AS spend,
    SUM(metrics_clicks)     AS clicks,
    SUM(metrics_impressions) AS impressions
  FROM {{ source('raw_google_ads','campaign') }}
  WHERE segments_date IS NOT NULL AND metrics_cost_micros IS NOT NULL
  GROUP BY 1,2
),
u AS (
  SELECT * FROM kw
  UNION ALL
  SELECT * FROM dkw
  UNION ALL
  SELECT * FROM camp
)

SELECT
  campaign_id,
  date,
  SUM(spend)        AS spend,
  SUM(clicks)       AS clicks,
  SUM(impressions)  AS impressions
FROM u
GROUP BY 1,2
