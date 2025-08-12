{{ config(
  materialized='table',
  partition_by={'field': 'event_date', 'data_type': 'date'},
  cluster_by=['host_name','canonical_path']
) }}

-- 1) GA4 pages, normalize path (remove querystring, lowercase, trim trailing slash)
with ga4 as (
  select
    event_date,
    lower(nullif(regexp_replace(split(page_path, '?')[offset(0)], r'/$', ''), '')) as canonical_path,
    lower(host_name) as host_name,
    sum(page_views) as page_views
  from {{ ref('stg_ga4__pages') }}
  group by 1,2,3
),

-- 2) Klaviyo form events, extract path from full URL, normalize same way
klaviyo as (
  select
    event_date,
    lower(nullif(regexp_replace(
      -- path from full URL (e.g., https://example.com/path?a=b) -> /path
      coalesce(regexp_extract(page_url, r'https?://[^/]+(/.*)'), '/'),
      r'/$', ''
    ), '')) as canonical_path,
    lower(host_name) as host_name,
    countif(step_name is not null) as form_submissions   -- "Email Opt-In" etc.
  from {{ ref('stg_klaviyo__events') }}
  group by 1,2,3
),

-- 3) Full outer join to keep pages with views but zero forms and vice versa
joined as (
  select
    coalesce(g.event_date, k.event_date) as event_date,
    coalesce(g.host_name, k.host_name)   as host_name,
    coalesce(g.canonical_path, k.canonical_path) as canonical_path,
    coalesce(g.page_views, 0) as page_views,
    coalesce(k.form_submissions, 0) as form_submissions
  from ga4 g
  full outer join klaviyo k
    on g.event_date = k.event_date
   and g.host_name  = k.host_name
   and g.canonical_path = k.canonical_path
)

select * from joined
