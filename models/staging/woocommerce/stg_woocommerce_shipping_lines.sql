{{ config(
  alias='stg_woocommerce_shipping_lines',
  materialized='incremental',
  partition_by={'field': 'order_date', 'data_type': 'date'},
  cluster_by=['order_id', 'carrier_service_code']
) }}

with base as (
  select
    id as order_id,

    -- Source may be STRING or TIMESTAMP; cast and derive DATE for partitioning
    cast(date_created as timestamp) as order_ts,
    date(cast(date_created as timestamp)) as order_date,

    -- shipping_lines may be STRING; cast to JSON
    cast(shipping_lines as json) as shipping_lines_json
  from {{ source('raw_woocommerce','orders') }}
  where shipping_lines is not null
  {% if is_incremental() %}
    and date(cast(date_created as timestamp)) >= date_sub(current_date(), interval 18 month)
  {% endif %}
),

exploded as (
  select
    b.order_id,
    b.order_ts,
    b.order_date,
    ship as ship_json
  from base b
  , unnest(
      ifnull(
        json_query_array(b.shipping_lines_json, '$'),
        cast([] as array<json>)
      )
    ) as ship
),

meta as (
  -- explode meta_data to pull common keys (ups_service_code, service_type, etc.)
  select
    e.order_id,
    safe_cast(json_value(e.ship_json, '$.id') as int64) as shipping_line_id,
    m as meta_json
  from exploded e
  , unnest(
      ifnull(
        json_query_array(e.ship_json, '$.meta_data'),
        cast([] as array<json>)
      )
    ) as m
),

meta_pivot as (
  select
    order_id,
    shipping_line_id,
    -- common keys seen in samples; extend as needed
    any_value(case when json_value(meta_json, '$.key') = 'ups_service_code' then json_value(meta_json, '$.value') end) as ups_service_code,
    any_value(case when json_value(meta_json, '$.key') = 'service_type'     then json_value(meta_json, '$.value') end) as service_type,
    any_value(case when json_value(meta_json, '$.key') = 'collection_point' then json_value(meta_json, '$.value') end) as collection_point,
    any_value(case when json_value(meta_json, '$.key') = 'delivery_dates'   then json_value(meta_json, '$.value') end) as delivery_dates
  from meta
  group by order_id, shipping_line_id
)

select
  e.order_id,
  e.order_ts,
  e.order_date,

  safe_cast(json_value(e.ship_json, '$.id') as int64) as shipping_line_id,
  json_value(e.ship_json, '$.method_id')    as method_id,     -- e.g., flexible_shipping_ups / FEDEX_...
  json_value(e.ship_json, '$.method_title') as method_title,  -- human-friendly name
  json_value(e.ship_json, '$.tax_status')   as tax_status,

  safe_cast(json_value(e.ship_json, '$.total') as numeric)     as shipping_total,
  safe_cast(json_value(e.ship_json, '$.total_tax') as numeric) as shipping_tax_total,

  -- inferred carrier/service code
  coalesce(mp.ups_service_code, json_value(e.ship_json, '$.method_id')) as carrier_service_code,
  mp.service_type,
  mp.collection_point,
  mp.delivery_dates

from exploded e
left join meta_pivot mp
  on mp.order_id = e.order_id
 and mp.shipping_line_id = safe_cast(json_value(e.ship_json, '$.id') as int64)
