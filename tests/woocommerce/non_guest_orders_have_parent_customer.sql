-- Orders must resolve to a parent:
--  - Service accounts: email match within window (bridge)
--  - Normal accounts: ID OR email match within window

with rules as (
  select cast(wc_customer_id as string) wc_customer_id
  from {{ ref('customer_id_rules') }}
  where rule = 'prefer_email'
),
earliest as (
  select date(min(first_seen_at)) min_customer_date
  from {{ ref('stg_woocommerce_customers') }}
)

select
  o.order_id, o.order_date, o.order_status,
  o.wc_customer_id, lower(o.customer_email) as customer_email
from {{ ref('stg_woocommerce_orders') }} o
cross join earliest e
left join {{ ref('stg_woocommerce_customers') }} c_id
  on o.wc_customer_id = c_id.wc_customer_id
left join {{ ref('bridge_customer_emails') }} b
  on lower(o.customer_email) = b.email
 and o.order_date between b.valid_from and b.valid_to
left join rules svc
  on o.wc_customer_id = svc.wc_customer_id
where
  o.order_date >= e.min_customer_date
  and o.order_status in ('processing','completed','on-hold')
  and trim(o.wc_customer_id) not in ('','0')
  and (
    (svc.wc_customer_id is not null and b.wc_customer_id is null) -- service acct: require email
    or
    (svc.wc_customer_id is null and c_id.wc_customer_id is null and b.wc_customer_id is null) -- normal: id/email both missing
  )
