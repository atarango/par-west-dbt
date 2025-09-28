{{ config(materialized='view', enabled=env_var('DBT_ENABLE_ADS','false')|lower in ['true','1','yes']) }}

-- Paid GA4 purchases joined to Woo items (+ costs)
with p_raw as (
  select
    g.event_date                                           as date,
    lower(trim(coalesce(g.utm_campaign,'(unknown)')))      as campaign_key,
    lower(trim(coalesce(g.session_source,'')))             as src,
    lower(trim(coalesce(g.session_medium,'')))             as med,
    g.gclid,
    g.transaction_id
  from {{ ref('int_ga4__purchases_unified') }} g
  where coalesce(g.transaction_id,'') <> ''
),
p_paid as (
  -- keep only paid Google traffic
  select *
  from p_raw
  where gclid is not null
     or (src in ('google','googleads','google ads') and med in ('cpc','ppc','paid','paid_search'))
),
wc as (
  select
    w.transaction_id_wc,
    upper(trim(w.sku))                        as sku,
    sum(w.line_total)                         as item_revenue,
    sum(w.quantity)                           as qty
  from {{ ref('int_wc__order_items_enriched') }} w
  group by 1,2
),
costs as (
  select sku, coalesce(catalog_cost,0.0) as unit_cost
  from {{ ref('dim_sku_cost') }}
),
wc_paid as (
  -- line-items for paid orders only
  select
    p.date,
    p.campaign_key,
    x.sku,
    x.item_revenue                         as sku_revenue,
    x.qty,
    coalesce(c.unit_cost,0.0) * x.qty      as sku_cogs
  from p_paid p
  join wc x
    on x.transaction_id_wc = p.transaction_id
  left join costs c using (sku)
),

-- Revenue totals (for share-of-revenue allocation)
campaign_totals as (
  select date, campaign_key, sum(sku_revenue) as campaign_revenue
  from wc_paid
  group by 1,2
),
date_totals as (
  select date, sum(sku_revenue) as date_revenue
  from wc_paid
  group by 1
),

-- Spend by campaign (normalized) and by date
spend_campaign as (
  select date,
         lower(trim(campaign_name)) as campaign_key,
         sum(cost) as campaign_cost
  from {{ ref('stg_ads__spend_daily') }}
  group by 1,2
),
spend_date as (
  select date, sum(cost) as date_cost
  from {{ ref('stg_ads__spend_daily') }}
  group by 1
),

allocated as (
  select
    w.date,
    w.campaign_key,
    w.sku,
    w.sku_revenue,
    w.sku_cogs,
    coalesce(ct.campaign_revenue,0.0) as campaign_revenue,
    coalesce(dt.date_revenue,0.0)     as date_revenue,
    sc.campaign_cost,
    sd.date_cost,
    case
      -- primary: allocate from campaign-level spend if it exists
      when sc.campaign_cost is not null and coalesce(ct.campaign_revenue,0) > 0
        then sc.campaign_cost * (w.sku_revenue / ct.campaign_revenue)
      -- fallback: allocate from total date spend if campaign name doesn't match
      when sd.date_cost is not null and coalesce(dt.date_revenue,0) > 0
        then sd.date_cost * (w.sku_revenue / dt.date_revenue)
      else 0.0
    end as sku_allocated_cost
  from wc_paid w
  left join campaign_totals ct using (date, campaign_key)
  left join date_totals     dt using (date)
  left join spend_campaign  sc using (date, campaign_key)
  left join spend_date      sd using (date)
)

select
  date,
  -- keep a readable campaign name (optional for SKU rollups)
  case when campaign_key is null then '(unknown)' else campaign_key end as campaign_name,
  sku,
  sum(sku_revenue)       as revenue,
  sum(sku_cogs)          as cogs,
  sum(sku_allocated_cost) as cost,
  case when sum(sku_allocated_cost) > 0 then sum(sku_revenue) / sum(sku_allocated_cost) end as roas,
  case when sum(sku_allocated_cost) > 0 then (sum(sku_revenue) - sum(sku_allocated_cost)) / sum(sku_allocated_cost) end as roi
from allocated
group by 1,2,3
