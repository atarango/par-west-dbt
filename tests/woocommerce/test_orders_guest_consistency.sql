with o as (
  select * from {{ ref('stg_woocommerce_orders') }}
),
violations as (
  select order_id, is_guest_order, wc_customer_id
  from o
  where (is_guest_order = true  and wc_customer_id is not null)
     or (is_guest_order = false and wc_customer_id is null)
)
select * from violations
