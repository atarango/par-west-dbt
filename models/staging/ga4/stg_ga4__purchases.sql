{{ config(materialized='view') }}

-- Minimal purchase feed based on your CSV export.
-- Fields match what the bridge expects (transaction_id, utm_campaign, event_ts).
WITH src AS (
  SELECT
    CAST(transaction_id AS STRING)                                 AS transaction_id,
    CAST(event_date AS DATE)                                       AS event_date,
    CAST(session_campaign AS STRING)                               AS utm_campaign,
    -- split "source / medium" if provided
    SPLIT(CAST(session_source AS STRING), ' / ')[OFFSET(0)] AS utm_source,
    SPLIT(CAST(session_medium AS STRING), ' / ')[OFFSET(1)] AS utm_medium,
    CAST(purchase_revenue AS NUMERIC)                              AS purchase_revenue
  FROM `par-west-ai-dashboard.raw_ga4.purchase_export`
  WHERE transaction_id IS NOT NULL
)
SELECT
  event_date,
  TIMESTAMP(event_date) AS event_ts,   -- GA4 export has only date; good enough for our join
  transaction_id,
  NULL AS gclid,                       -- CSV won’t include gclid; bridge will use UTM path
  utm_campaign
FROM src
