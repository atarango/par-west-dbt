{{ config(materialized='view') }}

select
  parse_date('%Y%m%d', date) as event_date,
  pagePathPlusQueryString as page_path,
  screenPageViews as page_views,
  bounceRate as bounce_rate,
  hostName as host_name
from {{ source('raw_ga4', 'pages') }}
where date is not null