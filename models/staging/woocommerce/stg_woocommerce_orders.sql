{{ config(
    materialized='incremental',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['customer_email'],
    alias='stg_woocommerce_orders',
    incremental_strategy='insert_overwrite',
    unique_key='order_id'
) }}

with raw as (
  select
    -- ids
    SAFE_CAST(id AS INT64)                                  as order_id,
    CAST(number AS STRING)                                   as order_number,

    -- status (raw -> lower)
    LOWER(TRIM(status))                                      as order_status_raw,

    -- normalized keys straight from raw columns
    SAFE_CAST(NULLIF(TRIM(CAST(customer_id AS STRING)), '') AS INT64) as wc_customer_id_int,
    LOWER(TRIM(JSON_VALUE(billing, '$.email')))             as customer_email_norm,

    -- money: always safe_cast
    SAFE_CAST(total          AS NUMERIC)                      as order_total,
    SAFE_CAST(discount_total AS NUMERIC)                      as discount_total,
    SAFE_CAST(total_tax      AS NUMERIC)                      as total_tax,
    SAFE_CAST(shipping_total AS NUMERIC)                      as shipping_total,
    SAFE_CAST(discount_tax   AS NUMERIC)                      as discount_tax,
    SAFE_CAST(shipping_tax   AS NUMERIC)                      as shipping_tax,

    -- billing JSON email as fallback
    LOWER(TRIM(JSON_VALUE(SAFE_CAST(billing AS JSON), '$.email'))) as raw_customer_email,

    -- original wc customer id as string (for display/back-compat)
    CAST(customer_id AS STRING)                               as raw_wc_customer_id,

    -- canonical UTC timestamps from *_gmt
    SAFE_CAST(date_created_gmt   AS TIMESTAMP)                as created_at_utc,
    SAFE_CAST(date_modified_gmt  AS TIMESTAMP)                as modified_at_utc,
    SAFE_CAST(date_paid_gmt      AS TIMESTAMP)                as paid_at_utc,
    SAFE_CAST(date_completed_gmt AS TIMESTAMP)                as completed_at_utc,

    -- misc
    payment_method,
    payment_method_title,
    customer_note,
    customer_ip_address,
    customer_user_agent
  from {{ source('raw_woocommerce','orders') }}
),

/* Keep latest version per order_id (handles re-ingests / late updates) */
dedup as (
  select *
  from raw
  qualify row_number() over (
    partition by order_id
    order by modified_at_utc desc, created_at_utc desc
  ) = 1
),

final as (
  select
    -- ids
    order_id,
    order_number,

    -- map to canonical statuses; fallback 'unknown'
    case
      when order_status_raw in ('pending','processing','on-hold','completed','cancelled','refunded','failed','trash')
        then order_status_raw
      else 'unknown'
    end                                                     as order_status,

    -- amounts
    order_total, discount_total, total_tax, shipping_total,
    discount_tax, shipping_tax,

    -- customer identifiers (normalized + fallback)
    COALESCE(customer_email_norm, LOWER(TRIM(raw_customer_email))) as customer_email,
    case
      when raw_wc_customer_id is null then null
      when TRIM(raw_wc_customer_id) in ('','0','-1','guest') then null
      else TRIM(raw_wc_customer_id)
    end                                                     as wc_customer_id,       -- string view
    wc_customer_id_int,                                     -- int64 view (for joins)
    customer_email_norm,                                    -- normalized email (for joins)

    -- guest flag using normalized id
    (wc_customer_id_int is null or wc_customer_id_int = 0)  as is_guest_order,

    -- dates / timestamps
    DATE(created_at_utc)                                    as order_date,           -- partition key
    created_at_utc,
    modified_at_utc,
    paid_at_utc,
    completed_at_utc,

    -- misc
    payment_method,
    payment_method_title,
    customer_note,
    customer_ip_address,
    customer_user_agent
  from dedup
)

select * from final

{% if is_incremental() %}
-- For insert_overwrite, restrict to affected partitions (by partition field) AND
-- include a timestamp safety window to catch late updates.
where order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  and modified_at_utc >= (
        select COALESCE(
                 TIMESTAMP_SUB(MAX(modified_at_utc), INTERVAL 7 DAY),
                 TIMESTAMP('1970-01-01')
               )
        from {{ this }}
      )
{% endif %}
