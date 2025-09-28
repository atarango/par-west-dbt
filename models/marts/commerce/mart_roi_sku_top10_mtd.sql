{% set enable_ads = (env_var('DBT_ENABLE_ADS','false') | lower in ['true','1','yes']) %}
{{ config(materialized='view', enabled=enable_ads) }}

select *
from {{ ref('mart_roi_sku_mtd') }}
order by revenue desc
limit 10
