-- fail on any order where the absolute diff > 1 cent
with items as (
  select cast(order_id as int64) order_id,
         sum(cast(subtotal as numeric)) items_subtotal,
         sum(cast(total_tax as numeric)) items_tax
  from {{ ref('stg_woocommerce__order_items') }}
  group by 1
),
ship as (
  select cast(order_id as int64) order_id,
         sum(cast(shipping_total as numeric)) shipping_lines_total,
         sum(cast(shipping_tax_total as numeric)) shipping_lines_tax
  from {{ ref('stg_woocommerce_shipping_lines') }}
  group by 1
),
orders as (
  select
    order_id,
    cast(order_total as numeric) order_total,
    cast(discount_total as numeric) discount_total
  from {{ ref('stg_woocommerce_orders') }}
)
select o.order_id
from orders o
left join items i using (order_id)
left join ship  s using (order_id)
where abs(
  o.order_total - (
    coalesce(i.items_subtotal,0) + coalesce(i.items_tax,0)
    + coalesce(s.shipping_lines_total,0) + coalesce(s.shipping_lines_tax,0)
    - coalesce(o.discount_total,0)
  )
) > 0.01
