{% set enable_ads = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}

{% set ads_table       = env_var('DBT_ADS_SPEND_TABLE','') %}          {# e.g. par-west-ai-dashboard.raw_google_ads.campaign #}
{% set customer_table  = env_var('DBT_ADS_CUSTOMER_TABLE','') %}       {# e.g. par-west-ai-dashboard.raw_google_ads.customer #}

{# columns in the CAMPAIGN stream #}
{% set date_col        = env_var('DBT_ADS_DATE_COL','segments_date') %}
{% set camp_id_col     = env_var('DBT_ADS_CAMPAIGN_ID_COL','campaign_id') %}
{% set camp_name_col   = env_var('DBT_ADS_CAMPAIGN_NAME_COL','campaign_name') %}
{% set cost_micros_col = env_var('DBT_ADS_COST_MICROS_COL','metrics_cost_micros') %}
{% set camp_customer_id_col = env_var('DBT_ADS_CAMPAIGN_CUSTOMER_ID_COL','customer_id') %}

{# columns in the CUSTOMER stream #}
{% set cust_id_col      = env_var('DBT_ADS_CUSTOMER_ID_COL','customer_id') %}
{% set cust_name_col    = env_var('DBT_ADS_CUSTOMER_NAME_COL','customer_descriptive_name') %}
{% set cust_currency_col= env_var('DBT_ADS_CURRENCY_COL','customer_currency_code') %}

{% set ads_start       = env_var('DBT_ADS_START','') %}

{{ config(materialized='view', enabled=enable_ads) }}

with base as (
  select
    cast({{ date_col }} as date)        as date,
    cast({{ camp_id_col }} as string)   as campaign_id,
    cast({{ camp_name_col }} as string) as campaign_name,
    cast({{ camp_customer_id_col }} as string) as account_id,
    coalesce(({{ cost_micros_col }} / 1000000.0), 0.) as cost
  from `{{ ads_table }}`
  {% if ads_start %}
    where {{ date_col }} >= date('{{ ads_start }}')
  {% endif %}
),
agg as (  -- ensure one row per date × campaign
  select
    date, campaign_id, campaign_name, account_id,
    sum(cost) as cost
  from base
  group by 1,2,3,4
)
{% if customer_table %}
, customer_dim as (
  select
    cast({{ cust_id_col }} as string)                  as account_id,
    any_value(cast({{ cust_name_col }} as string))     as account_name,
    any_value(cast({{ cust_currency_col }} as string)) as currency_code
  from `{{ customer_table }}`
  group by 1
)
{% endif %}

select
  a.date,
  'google' as source,
  'cpc'    as medium,
  a.campaign_id,
  a.campaign_name,
  a.account_id,
  {% if customer_table %} cd.account_name {% else %} cast(null as string) {% endif %} as account_name,
  {% if customer_table %} cd.currency_code {% else %} cast(null as string) {% endif %} as currency_code,
  a.cost
from agg a
{% if customer_table %} left join customer_dim cd using (account_id) {% endif %}
where a.date is not null
