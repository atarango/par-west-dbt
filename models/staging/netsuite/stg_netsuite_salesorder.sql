{{ config(
  alias='stg_netsuite_orders',
  materialized='incremental',
  partition_by={'field': 'order_date', 'data_type': 'date'},
  cluster_by=['customer_email']
) }}

with src as (
  select
    -- Order identity
    cast(id as string)                                       as order_id,

    -- Partition date
    cast(tranDate as date)                                   as order_date,

    -- Customer id from JSON 'entity'
    cast(json_value(entity, '$.id') as string)               as ns_customer_id,

    -- Email (top-level or custom field)
    lower(trim(coalesce(email, custbody_nps_buyer_email)))   as customer_email,

    -- Amount
    cast(total as numeric)                                   as order_amount,

    -- Currency: JSON object -> $.refName, or JSON scalar -> $
    upper(coalesce(
      json_value(currency, '$.refName'),
      json_value(currency, '$')
    ))                                                       as currency,

    -- Source lineage flag
    'netsuite'                                               as order_source,

    -- Status: prefer object.refName, fall back to scalar
    coalesce(
      json_value(orderStatus, '$.refName'),
      json_value(status,      '$.refName'),
      json_value(orderStatus, '$'),
      json_value(status,      '$')
    )                                                        as status

  from {{ source('raw_netsuite','salesorder') }}
  where coalesce(
          json_value(orderStatus, '$.refName'),
          json_value(status,      '$.refName'),
          json_value(orderStatus, '$'),
          json_value(status,      '$')
        ) not in ('VOID','CANCELLED')
)

select *
from src
{% if is_incremental() %}
where order_date >= date_sub(current_date(), interval 18 month)
{% endif %}
