{{ config(
  alias='stg_woocommerce_order_items',
  materialized='incremental',
  partition_by={'field': 'order_date', 'data_type': 'date'},
  cluster_by=['order_id', 'sku'],
) }}

with base as (
  select
    id as order_id,
    -- Source may be STRING or TIMESTAMP; cast and derive DATE for partitioning
    cast(date_created as timestamp) as order_ts,
    date(cast(date_created as timestamp)) as order_date,
    -- line_items may be STRING; cast to JSON
    cast(line_items as json) as line_items_json
  from {{ source('raw_woocommerce','orders') }}
  where line_items is not null
  {% if is_incremental() %}
    and date(cast(date_created as timestamp)) >= date_sub(current_date(), interval 18 month)
  {% endif %}
),
exploded as (
  select
    b.order_id,
    b.order_ts,
    b.order_date,
    item as item_json
  from base b
  , unnest(
      ifnull(
        json_query_array(b.line_items_json, '$'),
        cast([] as array<json>)
      )
    ) as item
),
taxes as (
  -- Sum item-level taxes (if present)
  select
    e.order_id,
    safe_cast(json_value(e.item_json, '$.id') as int64) as order_item_id,
    sum(safe_cast(json_value(t, '$.total') as numeric)) as item_tax_total
  from exploded e
  , unnest(
      ifnull(
        json_query_array(e.item_json, '$.taxes'),
        cast([] as array<json>)
      )
    ) as t
  group by e.order_id, order_item_id
),
-- Products (parent/simple)
products as (
  select
    safe_cast(product_id as int64) as id,
    nullif(trim(cast(sku as string)), '') as sku
  from {{ ref('stg_woocommerce_products') }}
),
-- Variations (built from your raw stream via staging model)
variations as (
  select
    id  as variation_id,
    sku as variation_sku
  from {{ ref('stg_woocommerce_product_variations') }}
)

select
  e.order_id,
  e.order_ts,
  e.order_date,

  -- item identity
  safe_cast(json_value(e.item_json, '$.id') as int64) as order_item_id,
  nullif(safe_cast(json_value(e.item_json, '$.product_id') as int64), 0) as product_id,

  -- Normalize 0 → NULL so joins & analytics behave
  nullif(safe_cast(json_value(e.item_json, '$.variation_id') as int64), 0) as variation_id,

  -- Prefer: order-line sku → variation sku → product sku
  coalesce(
    nullif(trim(json_value(e.item_json, '$.sku')), ''),
    v.variation_sku,
    p.sku
  ) as sku,

  case
    when nullif(safe_cast(json_value(e.item_json, '$.product_id') as int64), 0) is not null
     and (v.variation_sku is not null or p.sku is not null)
    then true else false
  end as require_sku,

  json_value(e.item_json, '$.name') as product_name,
  json_value(e.item_json, '$.parent_name') as parent_name,

  -- qty/price/amounts
  safe_cast(json_value(e.item_json, '$.quantity') as int64) as quantity,
  safe_cast(json_value(e.item_json, '$.price') as numeric) as price,
  safe_cast(json_value(e.item_json, '$.subtotal') as numeric) as subtotal,
  safe_cast(json_value(e.item_json, '$.subtotal_tax') as numeric) as subtotal_tax,
  safe_cast(json_value(e.item_json, '$.total') as numeric) as line_total,
  safe_cast(json_value(e.item_json, '$.total_tax') as numeric) as line_total_tax,

  -- rolled item tax (fallback to line_total_tax if tax rows missing)
  coalesce(t.item_tax_total, safe_cast(json_value(e.item_json, '$.total_tax') as numeric)) as item_tax_total,

  -- image url (handy for merch dashboards)
  json_value(e.item_json, '$.image.src') as image_url

from exploded e
left join taxes t
  on t.order_id = e.order_id
 and t.order_item_id = safe_cast(json_value(e.item_json, '$.id') as int64)

-- Join variations first (by variation_id), then products (by product_id)
left join variations v
  on v.variation_id = nullif(safe_cast(json_value(e.item_json, '$.variation_id') as int64), 0)
left join products p
  on p.id = nullif(safe_cast(json_value(e.item_json, '$.product_id') as int64), 0)
