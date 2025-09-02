select order_id
from {{ ref('stg_woocommerce_orders') }}
where (paid_at       is not null and order_date is not null and paid_at       < timestamp(order_date))
   or (completed_at  is not null and order_date is not null and completed_at  < timestamp(order_date))
   or (modified_at   is not null and order_date is not null and modified_at   < timestamp(order_date))
