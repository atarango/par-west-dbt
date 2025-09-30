-- models/staging/netsuite/stg_netsuite__item_costs.sql
-- {{ config(materialized='view', alias='stg_netsuite__item_costs') }}

-- SELECT
--   REGEXP_REPLACE(LOWER(CAST(itemid AS STRING)), r'[^a-z0-9]+', '') AS sku_key,
--   SAFE_CAST(lastPurchasePrice AS NUMERIC) AS cogs_per_unit
-- FROM {{ source('raw_netsuite','inventoryitem') }}
-- WHERE itemid IS NOT NULL
--   AND TRIM(itemid) <> ''
--   AND lastPurchasePrice IS NOT NULL

-- models/staging/netsuite/stg_netsuite__item_costs.sql
{{ config(materialized='view', alias='stg_netsuite__item_costs') }}

SELECT
  REGEXP_REPLACE(LOWER(CAST(itemid AS STRING)), r'[^a-z0-9]+','') AS sku_key,
  SAFE_CAST(COALESCE(averageCost, lastPurchasePrice) AS NUMERIC)  AS cogs_per_unit
FROM {{ source('raw_netsuite','inventoryitem') }}
WHERE itemid IS NOT NULL
  AND TRIM(itemid) <> ''
  AND COALESCE(averageCost, lastPurchasePrice) IS NOT NULL