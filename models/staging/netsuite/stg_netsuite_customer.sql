{{ config(materialized='view') }}

select
  cast(id as string) as ns_customer_id,
  lower(trim(email)) as customer_email,
  companyName as company_name,
  dateCreated as first_seen_at,
  lastModifiedDate as last_seen_at
from {{ source('raw_netsuite','customer') }}
