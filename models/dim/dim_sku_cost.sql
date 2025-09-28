-- models/dim/dim_sku_cost.sql
{{ config(materialized='view') }}

{% set default_currency = env_var('DBT_NS_DEFAULT_CURRENCY','USD') %}

-- 1) NetSuite (preferred): inventoryitem
with ns as (
  select
    upper(trim(cast(itemId as string))) as sku_norm,
    coalesce(
      safe_cast(averageCost       as float64),
      safe_cast(lastPurchasePrice as float64),
      safe_cast(cost              as float64)
    ) as cost_val
  from {{ source('raw_netsuite','inventoryitem') }}
  where itemId is not null and itemId != ''
),

-- 2) Woo PRODUCTS fallback (raw)
p_prices as (
  select
    upper(trim(cast(sku as string)))        as sku_norm,
    safe_cast(nullif(price, '') as float64) as cost_val
  from {{ source('raw_woocommerce','products') }}
  where sku is not null and sku != ''
),

-- 3) Woo VARIATIONS fallback (raw)
v_prices as (
  select
    upper(trim(cast(sku as string)))        as sku_norm,
    safe_cast(nullif(price, '') as float64) as cost_val
  from {{ source('raw_woocommerce','product_variations') }}
  where sku is not null and sku != ''
),

-- 4) Combine with priority: NetSuite > variation > product
combined as (
  select sku_norm, cost_val, 'netsuite'     as src from ns        where cost_val is not null
  union all
  select sku_norm, cost_val, 'wc_variation' as src from v_prices  where cost_val is not null
  union all
  select sku_norm, cost_val, 'wc_product'   as src from p_prices  where cost_val is not null
),
resolved as (
  select
    sku_norm,
    coalesce(
      max(case when src = 'netsuite'     and cost_val > 0 then cost_val end),
      max(case when src = 'wc_variation' and cost_val > 0 then cost_val end),
      max(case when src = 'wc_product'   and cost_val > 0 then cost_val end),
      0.0
    ) as catalog_cost
  from combined
  group by 1
)

select
  sku_norm as sku,
  catalog_cost,
  '{{ default_currency }}' as currency
from resolved
where sku_norm is not null and sku_norm != ''

