{% set export_start   = env_var('DBT_GA4_EXPORT_START', '2025-09-24') %}
{% set backfill_table = env_var('DBT_GA4_BACKFILL_TABLE', '') %}

{{ config(materialized='view') }}

with export as (
  -- Your current GA4 export-based staging (already includes attribution + revenue)
  select
    event_date,
    event_ts,
    transaction_id,
    gclid,
    session_source,
    session_medium,
    utm_campaign,
    revenue
  from {{ ref('stg_ga4__purchases') }}
),
backfill as (
  {% if backfill_table %}
  -- CSV schema you shared:
  -- transaction_id (STRING), event_date (DATE), session_campaign, session_source, session_medium, purchase_revenue (NUMERIC)
  select
    cast(event_date as date)                              as event_date,
    timestamp(cast(event_date as date))                   as event_ts,          -- daily grain is fine
    cast(transaction_id as string)                        as transaction_id,
    cast(null as string)                                  as gclid,             -- CSV doesn’t have it
    cast(session_source as string)                        as session_source,
    cast(session_medium as string)                        as session_medium,
    cast(session_campaign as string)                      as utm_campaign,
    cast(purchase_revenue as float64)                     as revenue
  from `{{ backfill_table }}`
  where event_date < date('{{ export_start }}')           -- prevent overlaps
    and coalesce(transaction_id,'') != ''
  {% else %}
  select cast(null as date) event_date, cast(null as timestamp) event_ts,
         cast(null as string) transaction_id, cast(null as string) gclid,
         cast(null as string) session_source, cast(null as string) session_medium,
         cast(null as string) utm_campaign, cast(null as float64) revenue
  where 1=0
  {% endif %}
)

select * from export
union all
select * from backfill
