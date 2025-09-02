{{ config(
  alias='stg_woocommerce_customers',
  materialized='view'
) }}

with base as (
  select
    -- IDs & identifiers
    cast(id as string) as wc_customer_id,

    -- Prefer top-level email; fall back to billing.email if missing
    coalesce(
      lower(trim(email)),
      lower(trim(json_value(billing, '$.email')))
    ) as customer_email,

    -- Normalize names to strings so types don't drift
    cast(first_name as string) as first_name,
    cast(last_name  as string) as last_name,

    -- Source timestamps normalized
    cast(date_created  as timestamp) as first_seen_at,      -- raw created
    cast(date_modified as timestamp) as raw_last_seen_at    -- raw last seen (may be null)
  from {{ source('raw_woocommerce','customers') }}
)

select
  wc_customer_id,
  customer_email,
  first_name,
  last_name,

  -- Keep the raw fields + an imputed, downstream-friendly field
  first_seen_at,
  raw_last_seen_at,
  cast(coalesce(raw_last_seen_at, first_seen_at) as timestamp) as last_seen_at,
  (raw_last_seen_at is null) as last_seen_at_imputed
from base
