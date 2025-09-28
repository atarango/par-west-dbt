{{ config(materialized='view') }}

{% set intraday_exists = false %}
{% if execute %}
  {% set q = "
    select count(1) as c
    from `par-west-ai-dashboard.analytics_323321017`.INFORMATION_SCHEMA.TABLES
    where table_name like 'events_intraday_%'
  " %}
  {% set res = run_query(q) %}
  {% if res and (res.columns[0].values()[0] | int) > 0 %}
    {% set intraday_exists = true %}
  {% endif %}
{% endif %}

with daily as (
  select * from `par-west-ai-dashboard.analytics_323321017.events_*`
)
{% if intraday_exists %}
, intraday as (
  select * from `par-west-ai-dashboard.analytics_323321017.events_intraday_*`
)
{% endif %}

select * from daily
{% if intraday_exists %}
union all
select * from intraday
{% endif %}
