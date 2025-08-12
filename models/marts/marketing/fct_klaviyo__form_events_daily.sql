{{ config(
  materialized='table',
  partition_by={'field': 'event_date', 'data_type': 'date'},
  cluster_by=['form_id','page_url']
) }}

select
  event_date,
  coalesce(form_id, 'unknown') as form_id,
  coalesce(step_name, 'unknown') as step_name,
  page_url,
  host_name,
  device_type,
  count(*) as events
from {{ ref('stg_klaviyo__events') }}
where step_name is not null  -- e.g., "Email Opt-In"
group by 1,2,3,4,5,6
