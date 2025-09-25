{{ config(materialized='view') }}

-- Airbyte exposes GCLID as click_view_gclid and a date (segments_date)
SELECT
  click_view_gclid                               AS gclid,
  CAST(campaign_id AS STRING)                    AS campaign_id,
  ANY_VALUE(campaign_name)                       AS campaign_name,
  DATETIME(segments_date)                        AS click_dt
FROM {{ source('raw_google_ads','click_view') }}
WHERE click_view_gclid IS NOT NULL
GROUP BY gclid, campaign_id, click_dt