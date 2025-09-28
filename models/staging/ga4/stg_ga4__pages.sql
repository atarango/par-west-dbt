{{ config(materialized='view') }}

with pv as (
  select
    parse_date('%Y%m%d', event_date)                               as event_date,
    {{ ga4_param_string('event_params','page_location') }}         as page_location,
    1                                                               as page_views
  from {{ ref('src_ga4__events') }}
  where event_name = 'page_view'
),
normalized as (
  select
    event_date,
    regexp_extract(page_location, r'^https?://([^/]+)')            as host_name,
    coalesce(
      regexp_extract(page_location, r'^https?://[^/]+(/.*)$'),
      '/'
    )                                                              as page_path,
    page_views
  from pv
),
agg as (
  select
    event_date,
    page_path,
    host_name,
    sum(page_views) as page_views
  from normalized
  where event_date is not null
  group by 1,2,3
)
select
  event_date,
  page_path,
  page_path as pagePathPlusQueryString,
  page_views,                          -- snake_case (your mart expects this)
  page_views as screenPageViews,       -- keep camelCase for compatibility
  cast(null as float64) as bounce_rate,
  cast(null as float64) as bounceRate,
  host_name,
  host_name as hostName
from agg
