{{ config(materialized='view') }}

SELECT
  DATE(segments_date)                            AS date,
  CAST(campaign_id AS STRING)                    AS campaign_id,
  ANY_VALUE(campaign_name)                       AS campaign_name,
  SUM(metrics_cost_micros) / 1e6                 AS cost,
  SUM(metrics_clicks)                            AS clicks,
  SUM(metrics_impressions)                       AS impressions
FROM {{ source('raw_google_ads','account_performance_report') }}
GROUP BY 1,2