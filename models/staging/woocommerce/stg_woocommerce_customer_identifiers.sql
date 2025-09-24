{{ config(materialized='view', alias='stg_woocommerce_customer_identifiers') }}

with cust as (
  select
    wc_customer_id_int,
    email_norm as email_raw
  from {{ ref('stg_woocommerce_customers') }}
  where wc_customer_id_int is not null
     or (email_norm is not null and email_norm != '')
),
ord as (
  select
    wc_customer_id_int,
    customer_email_norm as email_raw
  from {{ ref('stg_woocommerce_orders') }}
  where (wc_customer_id_int is not null and wc_customer_id_int > 0)
     or (customer_email_norm is not null and customer_email_norm != '')
)

select distinct
  wc_customer_id_int,
  email_raw                         as email_norm,
  {{ email_key('email_raw') }}      as email_key
from (
  select * from cust
  union all
  select * from ord
)
