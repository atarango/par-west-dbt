{% set enable_ads = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}
{{ config(materialized='view', enabled=enable_ads) }}

with base as (
  select *
  from {{ ref('fct_roi_sku_daily') }}
  where date >= date_trunc(current_date('America/Los_Angeles'), month)
)
select
  sku,
  sum(revenue) as revenue,
  sum(cogs)    as cogs,
  sum(cost)    as cost,
  case when sum(cost) > 0 then sum(revenue)/sum(cost) end                         as roas,
  case when sum(cost) > 0 then (sum(revenue)-sum(cost))/sum(cost) end             as roi
from base
group by 1
order by revenue desc
