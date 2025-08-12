{{ config(materialized='view') }}

select
  date,
  session_default_channel_grouping as channel_grouping,
  session_source,
  session_medium,
  sessions,
  conversions,
  total_revenue
from {{ source('raw_ga4', 'traffic_acquisition_session_source_medium_report') }}
where date >= '2024-01-01'
