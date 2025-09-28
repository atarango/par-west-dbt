{% set enable_ads = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}

-- {{ config(materialized='view', enabled=enable_ads) }}
{{ config(materialized='view', enabled=false) }}

with rev as (
  -- GA4-attributed revenue by campaign/day (already built)
  select date, campaign_name, conversions, revenue
  from {{ ref('fct_ga4_revenue_by_campaign_daily') }}
),
spend as (
  -- Google Ads daily spend by campaign/day
  select date, campaign_name, cost, currency_code
  from {{ ref('fct_ads_spend_daily') }}
),
joined as (
  select
    coalesce(rev.date, spend.date)                                  as date,
    coalesce(rev.campaign_name, spend.campaign_name, '(unknown)')   as campaign_name,
    sum(rev.conversions)                                            as conversions,
    sum(rev.revenue)                                                as revenue,
    sum(spend.cost)                                                 as cost
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
  case when cost > 0 then revenue / cost else null end          as roas,
  case when cost > 0 then (revenue - cost) / cost else null end as roi
from joined
