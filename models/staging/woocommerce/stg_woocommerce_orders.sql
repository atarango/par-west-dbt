{{ config(
  materialized='incremental',
  partition_by={'field': 'order_date', 'data_type':'date'},
  cluster_by=['customer_email'],
  alias='stg_woocommerce_orders'
) }}

with raw as (
  select
    id as order_id,
    number as order_number,
    status as order_status,
    cast(total as numeric) as order_total,
    cast(discount_total as numeric) as discount_total,
    cast(total_tax as numeric) as total_tax,
    cast(shipping_total as numeric) as shipping_total,
    cast(discount_tax as numeric) as discount_tax,
    cast(shipping_tax as numeric) as shipping_tax,

    json_extract_scalar(billing, '$.email') as raw_customer_email,
    cast(customer_id as string) as raw_wc_customer_id,

    date(cast(date_created as timestamp)) as order_date,  -- DATE
    cast(date_paid_gmt as timestamp) as paid_at,
    cast(date_completed_gmt as timestamp) as completed_at,
    cast(date_modified_gmt as timestamp) as modified_at,

    payment_method,
    payment_method_title,
    customer_note,
    customer_ip_address,
    customer_user_agent
  from {{ source('raw_woocommerce','orders') }}
)

select
  order_id,
  order_number,
  order_status,
  order_total,
  discount_total,
  total_tax,
  shipping_total,
  discount_tax,
  shipping_tax,

  -- normalized identifiers
  lower(trim(raw_customer_email)) as customer_email,
  cast(raw_wc_customer_id as string) as wc_customer_id,

  -- guest flag for FK tests (Woo guest orders have customer_id 0 or null)
  (raw_wc_customer_id is null
    or trim(raw_wc_customer_id) = ''
    or trim(raw_wc_customer_id) = '0') as is_guest_order,

  -- timestamps
  order_date,
  paid_at,
  completed_at,
  modified_at,

  -- misc
  payment_method,
  payment_method_title,
  customer_note,
  customer_ip_address,
  customer_user_agent
from raw
{% if is_incremental() %}
where order_date >= date_sub(current_date(), interval 18 month)
{% endif %}
