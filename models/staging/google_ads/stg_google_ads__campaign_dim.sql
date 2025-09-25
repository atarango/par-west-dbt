{{ config(materialized='view') }}

-- Inspect the campaign table: some connectors call the name field `name` (not campaign_name)
-- If your table has `name`, use that. If it has `campaign_name`, swap accordingly.
SELECT
  CAST(campaign_id AS STRING) AS campaign_id,
  ANY_VALUE(name)             AS campaign_name   -- <-- change to campaign_name if that's your column
FROM {{ source('raw_google_ads','campaign') }}
GROUP BY 1