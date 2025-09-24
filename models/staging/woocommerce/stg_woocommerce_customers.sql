{{ config(
    materialized='table',
    alias='stg_woocommerce_customers'
) }}

with raw as (
  select
    -- ids
    SAFE_CAST(id AS INT64)                                         as wc_customer_id_int,

    -- email (prefer top-level; fallback to billing JSON)
    LOWER(TRIM(COALESCE(email, JSON_VALUE(billing, '$.email'))))   as email_norm,

    -- names (keep if useful)
    LOWER(TRIM(first_name))                                        as first_name,
    LOWER(TRIM(last_name))                                         as last_name,

    -- timestamps
    SAFE_CAST(date_created_gmt  AS TIMESTAMP)                      as created_at_utc,
    SAFE_CAST(date_modified_gmt AS TIMESTAMP)                      as modified_at_utc
  from {{ source('raw_woocommerce','customers') }}
)

select
  -- ids (both forms for compatibility)
  wc_customer_id_int,
  CAST(wc_customer_id_int AS STRING)           as wc_customer_id,

  -- emails (both forms for compatibility)
  email_norm,
  email_norm                                   as customer_email,

  -- names
  first_name,
  last_name,

  -- timeline fields expected by the contract
  created_at_utc,
  modified_at_utc,
  -- raw_last_seen_at: whatever the source last told us; fall back to created
  COALESCE(modified_at_utc, created_at_utc)    as raw_last_seen_at,
  -- first_seen_at: creation time
  created_at_utc                               as first_seen_at,
  -- last_seen_at: same as raw_last_seen_at for staging
  COALESCE(modified_at_utc, created_at_utc)    as last_seen_at,
  -- imputation flag: true when we had to fall back
  (modified_at_utc IS NULL)                    as last_seen_at_imputed
from raw
where wc_customer_id_int is not null
