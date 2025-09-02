{{ config(materialized='table', alias='bridge_customer_emails') }}

-- Base emails from the customers table (high confidence)
with cust as (
  select
    cast(wc_customer_id as string)                                 as wc_customer_id,
    lower(trim(customer_email))                                    as email,
    date(first_seen_at)                                            as first_seen_date,  -- DATE
    coalesce(date(last_seen_at), current_date())                   as last_seen_date    -- DATE (no TIMESTAMP/DATE mix)
  from {{ ref('stg_woocommerce_customers') }}
  where customer_email is not null and trim(customer_email) != ''
),

cust_window as (
  -- small grace window for account creation/update drift
  select
    wc_customer_id,
    email,
    date_sub(first_seen_date, interval 7 day)   as valid_from,     -- DATE
    date_add(last_seen_date,  interval 7 day)   as valid_to,       -- DATE
    'customers'                                  as source
  from cust
),

-- Emails observed on orders where the ID/email isn't already covered by cust
ord as (
  select
    cast(o.wc_customer_id as string)                             as wc_customer_id,
    lower(trim(o.customer_email))                                as email,
    min(o.order_date)                                            as first_seen_date,   -- DATE
    max(o.order_date)                                            as last_seen_date     -- DATE
  from {{ ref('stg_woocommerce_orders') }} o
  left join cust c_id on o.wc_customer_id = c_id.wc_customer_id
  left join cust c_em on lower(trim(o.customer_email)) = c_em.email
  where trim(o.wc_customer_id) not in ('', '0')
    and o.customer_email is not null and trim(o.customer_email) != ''
    and o.order_status in ('processing','completed','on-hold')
    and c_id.wc_customer_id is null   -- not already covered by customers (ID)
    and c_em.email is null            -- not already covered by customers (email)
  group by 1,2
),

ord_window as (
  -- bigger grace to accommodate back-office delays and handover
  select
    wc_customer_id,
    email,
    date_sub(first_seen_date, interval 30 day) as valid_from,      -- DATE
    date_add(last_seen_date,  interval 30 day) as valid_to,        -- DATE
    'orders'                                  as source
  from ord
),

collapsed as (
  select
    wc_customer_id,
    email,
    min(valid_from) as valid_from,  -- DATE
    max(valid_to)   as valid_to,    -- DATE
    any_value(source) as source
  from (
    select * from cust_window
    union all
    select * from ord_window
  )
  group by 1,2
)

select * from collapsed
