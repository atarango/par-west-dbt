{{ config(
  materialized='incremental',
  alias='stg_woocommerce_orders',
  partition_by={'field': 'order_date', 'data_type': 'date'},
  cluster_by=['customer_email'],
  unique_key='order_id',
  incremental_strategy='merge'
) }}

with raw as (
  select
    cast(id as string)                              as order_id,
    cast(number as string)                          as order_number,

    -- normalize status
    lower(trim(status))                             as order_status_raw,

    -- money: always SAFE_CAST
    SAFE_CAST(total          AS NUMERIC)            as order_total,
    SAFE_CAST(discount_total AS NUMERIC)            as discount_total,
    SAFE_CAST(total_tax      AS NUMERIC)            as total_tax,
    SAFE_CAST(shipping_total AS NUMERIC)            as shipping_total,
    SAFE_CAST(discount_tax   AS NUMERIC)            as discount_tax,
    SAFE_CAST(shipping_tax   AS NUMERIC)            as shipping_tax,

    -- billing is often a STRING with JSON; cast to JSON first
    lower(trim(JSON_VALUE(SAFE_CAST(billing AS JSON), '$.email'))) as raw_customer_email,

    -- Woo guest orders commonly have 0/null/-1/"guest"
    cast(customer_id as string)                     as raw_wc_customer_id,

    -- use *_gmt fields for stable UTC, derive DATE from created
    SAFE_CAST(date_created_gmt   AS timestamp)      as created_at_utc,
    SAFE_CAST(date_modified_gmt  AS timestamp)      as modified_at_utc,
    SAFE_CAST(date_paid_gmt      AS timestamp)      as paid_at_utc,
    SAFE_CAST(date_completed_gmt AS timestamp)      as completed_at_utc,

    -- optional extras
    currency                                           as currency_code,
    payment_method,
    payment_method_title,
    customer_note,
    customer_ip_address,
    customer_user_agent,

    -- if you have an ingestion timestamp, keep it
    _ingested_at
  from {{ source('raw_woocommerce','orders') }}
),

/* Keep the latest version per order_id (handles re-ingests / late updates) */
dedup as (
  select *
  from raw
  qualify row_number() over (
    partition by order_id
    order by modified_at_utc desc, _ingested_at desc
  ) = 1
),

final as (
  select
    order_id,
    order_number,

    -- map to canonical statuses if you like; fallback 'unknown'
    case
      when order_status_raw in ('pending','processing','on-hold','completed','cancelled','refunded','failed','trash') then order_status_raw
      else 'unknown'
    end                                                as order_status,

    order_total, discount_total, total_tax, shipping_total,
    discount_tax, shipping_tax,

    -- normalized identifiers
    lower(trim(raw_customer_email))                    as customer_email,

    case
      when raw_wc_customer_id is null then null
      when trim(raw_wc_customer_id) in ('','0','-1','guest') then null
      else trim(raw_wc_customer_id)
    end                                                as wc_customer_id,

    -- guest flag (good for FK tests and downstream logic)
    (raw_wc_customer_id is null
      or trim(raw_wc_customer_id) = ''
      or trim(raw_wc_customer_id) in ('0','-1','guest')) as is_guest_order,

    -- dates
    DATE(created_at_utc)                               as order_date,
    created_at_utc,
    modified_at_utc,
    paid_at_utc,
    completed_at_utc,

    -- misc
    currency_code,
    payment_method,
    payment_method_title,
    customer_note,
    customer_ip_address,
    customer_user_agent,

    _ingested_at
  from dedup
)

select * from final

{% if is_incremental() %}
-- Pull newly modified records since last build, with a small safety buffer for late-arriving updates.
where modified_at_utc >= (
  select coalesce(datetime_sub(datetime(max(modified_at_utc)), interval 7 day), datetime('1970-01-01'))
  from {{ this }}
)
{% endif %}
