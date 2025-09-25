{{ config(materialized='view') }}

SELECT
  LOWER(itemid)                AS sku,
  CAST(averageCost AS NUMERIC) AS cogs_per_unit
FROM {{ source('raw_netsuite','inventoryitem') }}
WHERE COALESCE(isInactive, FALSE) = FALSE
  AND averageCost IS NOT NULL