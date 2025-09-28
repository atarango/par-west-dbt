{% set enable_ads  = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}
{% set window_days = (env_var('DBT_ROI_WINDOW_DAYS','30')  | int) %}

{{ config(materialized='view', enabled=enable_ads) }}

with base as (
  select *
  from {{ ref('fct_roi_campaign_daily') }}
  where date >= date_sub(current_date('America/Los_Angeles'), interval {{ window_days }} day)
)
select
  campaign_name,
  sum(conversions) as conversions,
  sum(revenue)     as revenue,
  sum(cost)        as cost,
  case when sum(cost) > 0 then sum(revenue)/sum(cost) end                         as roas,
  case when sum(cost) > 0 then (sum(revenue)-sum(cost))/sum(cost) end             as roi
from base
group by 1
order by roi desc nulls last
