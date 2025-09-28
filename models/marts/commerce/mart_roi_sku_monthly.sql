{% set enable_ads = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}
{% set months_back = 6 %}
{{ config(materialized='view', enabled=enable_ads) }}

with base as (
  select
    date_trunc(date, month) as month,
    sku,
    sum(revenue) as revenue,
    sum(cogs)    as cogs,
    sum(cost)    as cost
  from {{ ref('fct_roi_sku_daily') }}
  where date >= date_add(date_trunc(current_date('America/Los_Angeles'), month), interval -({{ months_back }}-1) month)
  group by 1,2
)
select
  month,
  sku,
  revenue,
  cogs,
  cost,
  case when cost > 0 then revenue/cost end                         as roas,
  case when cost > 0 then (revenue - cost)/cost end                as roi
from base
order by month desc, revenue desc
