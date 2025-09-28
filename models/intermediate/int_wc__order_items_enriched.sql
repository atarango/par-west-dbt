{{ config(materialized='view') }}

{# paid statuses & default currency #}
{% set paid_statuses = "('completed','processing','wc-completed','wc-processing')" %}
{% set default_currency = env_var('DBT_WC_DEFAULT_CURRENCY', 'USD') %}

with items as (
  -- your existing Woo line-items table (schema you pasted)
  select
    order_id,
    order_ts,
    order_date,
    order_item_id,
    product_id,
    variation_id,
    sku,
    require_sku,
    product_name,
    parent_name,
    quantity,
    price,
    subtotal,
    subtotal_tax,
    line_total,
    line_total_tax,
    item_tax_total,
    image_url
  from {{ ref('stg_woocommerce_order_items') }}
),
orders as (
  -- NOTE: table uses order_status (not status) and has no currency column
  select
    safe_cast(order_id as int64)  as order_id,
    cast(order_number as string)  as order_number,
    lower(cast(order_status as string)) as order_status
  from {{ ref('stg_woocommerce_orders') }}
)

select
  i.*,
  -- 🔑 GA4 join key: prefer Woo order_id, fallback to order_number
  coalesce(cast(i.order_id as string), o.order_number) as transaction_id_wc,
  -- currency isn't present on orders; use a configurable default
  '{{ default_currency }}' as currency
from items i
left join orders o using (order_id)
where o.order_status in {{ paid_statuses }}
  and i.sku is not null
