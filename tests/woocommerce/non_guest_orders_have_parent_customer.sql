-- Non-guest orders must link to a customer by id OR by any historical email key we know about.

with o as (
  select order_id, wc_customer_id_int, customer_email_norm, is_guest_order
  from {{ ref('stg_woocommerce_orders') }}
),
o_norm as (
  select
    order_id,
    wc_customer_id_int,
    is_guest_order,
    {{ email_key('customer_email_norm') }} as email_key_o
  from o
),
ids as (
  select distinct wc_customer_id_int, email_key
  from {{ ref('stg_woocommerce_customer_identifiers') }}
)

select o.order_id
from o_norm o
left join ids i_id
  on o.wc_customer_id_int is not null
 and o.wc_customer_id_int > 0
 and o.wc_customer_id_int = i_id.wc_customer_id_int
left join ids i_em
  on o.email_key_o is not null
 and o.email_key_o = i_em.email_key
where o.is_guest_order = false
  and i_id.wc_customer_id_int is null
  and i_em.email_key        is null
