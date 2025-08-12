{{ config(
    materialized = 'table',
    partition_by = {'field': 'event_date', 'data_type': 'date'},
    cluster_by = ['page_path']
) }}

select
  event_date,
  page_path,
  count(*) as page_view_events,
  sum(page_views) as total_page_views,
  avg(bounce_rate) as avg_bounce_rate
from {{ ref('stg_ga4__pages') }}
group by 1, 2