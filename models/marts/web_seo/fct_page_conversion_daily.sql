{{ config(
  materialized='table',
  partition_by={'field': 'event_date', 'data_type': 'date'},
  cluster_by=['host_name','canonical_path']
) }}

select
  event_date,
  host_name,
  canonical_path,
  page_views,
  form_submissions,
  safe_divide(form_submissions, nullif(page_views, 0)) as form_submit_rate
from {{ ref('int__page_form_metrics_daily') }}
