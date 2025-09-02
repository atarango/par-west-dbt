-- Fail any *reliably checkable* recent, non-guest order (good statuses)
-- that cannot find a matching customer by ID OR by email.

with earliest_customer as (
  select date(min(first_seen_at)) as min_customer_date
  from {{ ref('stg_woocommerce_customers') }}
)

select
  o.order_id,
  o.order_date,
  o.order_status,
  o.wc_customer_id,
  o.customer_email
from {{ ref('stg_woocommerce_orders') }} o
cross join earliest_customer ec
left join {{ ref('stg_woocommerce_customers') }} c_id
  on o.wc_customer_id = c_id.wc_customer_id
left join {{ ref('stg_woocommerce_customers') }} c_email
  on lower(o.customer_email) = lower(c_email.customer_email)
where
  -- only enforce after we *started* capturing customers
  o.order_date >= ec.min_customer_date

  -- only enforce for “real” order states
  and o.order_status in ('processing','completed','on-hold')

  -- only non-guest orders by ID
  and o.wc_customer_id is not null
  and trim(o.wc_customer_id) not in ('', '0')

  -- fail when neither join finds a parent
  and c_id.wc_customer_id is null
  and c_email.customer_email is null
