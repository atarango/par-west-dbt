{{ config(materialized='view') }}

-- Daily campaign spend from keyword_view (no campaign_name here)
SELECT
  DATE(segments_date)            AS date,
  CAST(campaign_id AS STRING)    AS campaign_id,
  SUM(metrics_cost_micros)/1e6   AS cost,
  SUM(metrics_clicks)            AS clicks,
  SUM(metrics_impressions)       AS impressions
FROM {{ source('raw_google_ads','keyword_view') }}
GROUP BY 1, 2
