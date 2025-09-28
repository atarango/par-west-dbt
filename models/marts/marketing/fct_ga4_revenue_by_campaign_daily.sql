{{ config(materialized='view') }}

with p as (
  select
    event_date                                                   as date,
    coalesce(utm_campaign, '(unknown)')                          as campaign_name,
    gclid,
    lower(coalesce(session_source,''))                           as src,
    lower(coalesce(session_medium,''))                           as med,
    transaction_id,
    revenue                                                      as revenue_ga4
  from {{ ref('int_ga4__purchases_unified') }}
),
wc as (
  -- if you built the enrichment view, use that; otherwise use your existing WC items model
  select
    transaction_id_wc,
    sum(line_total) as revenue_wc
  from {{ ref('int_wc__order_items_enriched') }}
  group by 1
),
joined as (
  select
    p.date,
    p.campaign_name,
    p.gclid, p.src, p.med,
    coalesce(wc.revenue_wc, p.revenue_ga4) as revenue
  from p
  left join wc
    on wc.transaction_id_wc = p.transaction_id
),
filtered as (
  select *
  from joined
  where gclid is not null
     or (src in ('google','googleads','google ads') and med in ('cpc','ppc','paid','paid_search'))
)
select
  date,
  campaign_name,
  count(*)     as conversions,
  sum(revenue) as revenue
from filtered
group by 1,2
