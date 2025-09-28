{% set enable_ads = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}
{{ config(materialized='view', enabled=enable_ads) }}

with rev as (
  select date, campaign_name, conversions, revenue
  from {{ ref('fct_ga4_revenue_by_campaign_daily') }}
),
spend as (
  -- ⬇️ was ref('fct_ads_spend_daily'); use staging directly to avoid cycles
  select date, campaign_name, cost, currency_code
  from {{ ref('stg_ads__spend_daily') }}
),
joined as (
  select
    coalesce(rev.date, spend.date)                                as date,
    coalesce(rev.campaign_name, spend.campaign_name, '(unknown)') as campaign_name,
    sum(rev.conversions)                                          as conversions,
    sum(rev.revenue)                                              as revenue,
    sum(spend.cost)                                               as cost
  from rev
  full outer join spend
    on rev.date = spend.date
   and lower(rev.campaign_name) = lower(spend.campaign_name)
  group by 1,2
)
select
  date,
  campaign_name,
  conversions,
  revenue,
  cost,
  case when cost > 0 then revenue / cost end          as roas,
  case when cost > 0 then (revenue - cost) / cost end as roi
from joined
