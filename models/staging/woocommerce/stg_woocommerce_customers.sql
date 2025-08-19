{{ config(
  alias='stg_woocommerce_customers',
  materialized='view'
) }}

select
  -- WooCommerce customer ID as string
  cast(id as string) as wc_customer_id,

  -- Prefer top-level email; fall back to billing.email if missing
  coalesce(
    lower(trim(email)),
    lower(trim(json_value(billing, '$.email')))
  ) as customer_email,

  first_name,
  last_name,

  -- Timestamps from source schema
  date_created  as first_seen_at,   -- TIMESTAMP
  date_modified as last_seen_at     -- TIMESTAMP

from {{ source('raw_woocommerce','customers') }}
