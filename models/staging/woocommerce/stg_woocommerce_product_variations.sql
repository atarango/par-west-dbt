-- models/staging/woocommerce/stg_woocommerce_product_variations.sql
{{ config(materialized='view') }}

-- Dedupe by variation id, keep the latest record from Airbyte,
-- and normalize the SKU (trim -> NULL if blank).
with ranked as (
  select
    safe_cast(id as int64)                         as id,    -- variation id
    nullif(trim(cast(sku as string)), '')          as sku,
    _airbyte_extracted_at,
    row_number() over (
      partition by id
      order by _airbyte_extracted_at desc
    ) as rn
  from {{ source('raw_woocommerce', 'product_variations') }}
)

select
  id,
  sku
from ranked
where rn = 1
