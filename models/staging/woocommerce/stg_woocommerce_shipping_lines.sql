{{ config(
    materialized='incremental',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['order_id'],
    alias='stg_woocommerce_shipping_lines',
    incremental_strategy='insert_overwrite'
) }}

-- Anchor on parent orders so child rows only exist for present orders
with orders as (
  select
    order_id,
    created_at_utc as order_ts,   -- <-- contract expects this
    order_date
  from {{ ref('stg_woocommerce_orders') }}
),

raw as (
  select
    SAFE_CAST(id AS INT64) as order_id,
    shipping_lines          as shipping_lines_json
  from {{ source('raw_woocommerce','orders') }}
),

base as (
  select o.order_id, o.order_ts, o.order_date, r.shipping_lines_json
  from orders o
  join raw   r
    on r.order_id = o.order_id
),

shipping_lines as (
  select
    b.order_id,
    b.order_ts,
    b.order_date,
    SAFE_CAST(JSON_VALUE(l, '$.id')          AS INT64)  as shipping_line_id,
    JSON_VALUE(l, '$.tax_status')                        as tax_status,
    SAFE_CAST(JSON_VALUE(l, '$.total')      AS NUMERIC) as shipping_total,
    SAFE_CAST(JSON_VALUE(l, '$.total_tax')  AS NUMERIC) as shipping_tax_total,

    -- Optional carrier metadata extracted from meta_data key/value pairs
    (
      select cast(JSON_VALUE(md, '$.value') as string)
      from unnest(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      where JSON_VALUE(md, '$.key') = 'carrier_service_code'
      limit 1
    ) as carrier_service_code,

    (
      select cast(JSON_VALUE(md, '$.value') as string)
      from unnest(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      where JSON_VALUE(md, '$.key') = 'service_type'
      limit 1
    ) as service_type,

    (
      select cast(JSON_VALUE(md, '$.value') as string)
      from unnest(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      where JSON_VALUE(md, '$.key') = 'collection_point'
      limit 1
    ) as collection_point,

    (
      select cast(JSON_VALUE(md, '$.value') as string)
      from unnest(JSON_QUERY_ARRAY(l, '$.meta_data')) md
      where JSON_VALUE(md, '$.key') = 'delivery_dates'
      limit 1
    ) as delivery_dates

  from base b,
  unnest(JSON_QUERY_ARRAY(b.shipping_lines_json, '$')) as l
)

select * from shipping_lines
{% if is_incremental() %}
  -- Keep the same hot window as the parent
  where order_date >= date_sub(current_date(), interval 60 day)
{% endif %}
