-- tests/woocommerce/test_wc_temporal_sanity.sql
with o as (
  select
    order_id,
    order_date,
    paid_at_utc      as paid_at,
    completed_at_utc as completed_at,
    modified_at_utc  as modified_at
  from {{ ref('stg_woocommerce_orders') }}
)
select order_id
from o
where (paid_at      is not null and order_date is not null and paid_at      < timestamp(order_date))
   or (completed_at is not null and order_date is not null and completed_at < timestamp(order_date))
   or (modified_at  is not null and order_date is not null and modified_at  < timestamp(order_date))
