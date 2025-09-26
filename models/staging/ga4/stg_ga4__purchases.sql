{{ config(materialized='view', alias='stg_ga4__purchases') }}

SELECT
  CAST(transaction_id AS STRING)       AS transaction_id,
  CAST(event_date AS DATE)             AS event_date,
  TIMESTAMP(CAST(event_date AS DATE))  AS event_ts,
  CAST(NULL AS STRING)                 AS gclid,  -- keep string type

  -- Null out direct/none and (not set)
  CASE
    WHEN LOWER(TRIM(session_source)) IN ('(direct)','direct')
     AND LOWER(TRIM(session_medium)) IN ('(none)','none') THEN NULL
    WHEN LOWER(TRIM(session_campaign)) IN ('(not set)','not set') THEN NULL
    ELSE NULLIF(TRIM(CAST(session_campaign AS STRING)), '')
  END AS utm_campaign,

  NULLIF(TRIM(CAST(session_source AS STRING)), '') AS session_source,
  NULLIF(TRIM(CAST(session_medium AS STRING)), '') AS session_medium
FROM `par-west-ai-dashboard.raw_ga4.purchase_export`
WHERE transaction_id IS NOT NULL AND transaction_id != ''
