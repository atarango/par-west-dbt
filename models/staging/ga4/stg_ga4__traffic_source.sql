{{ config(materialized='view') }}

with sessions as (
  select
    parse_date('%Y%m%d', event_date) as date,
    coalesce(
      {{ ga4_param_string('event_params','session_source') }},
      {{ ga4_param_string('event_params','source') }}
    ) as session_source,
    coalesce(
      {{ ga4_param_string('event_params','session_medium') }},
      {{ ga4_param_string('event_params','medium') }}
    ) as session_medium,
    count(*) as sessions
  from {{ ref('src_ga4__events') }}
  where event_name = 'session_start'
  group by 1,2,3
),
purchases as (
  select
    parse_date('%Y%m%d', event_date) as date,
    coalesce(
      {{ ga4_param_string('event_params','session_source') }},
      {{ ga4_param_string('event_params','source') }}
    ) as session_source,
    coalesce(
      {{ ga4_param_string('event_params','session_medium') }},
      {{ ga4_param_string('event_params','medium') }}
    ) as session_medium,
    count(*) as conversions,
    sum(coalesce(
      ecommerce.purchase_revenue_in_usd,
      {{ ga4_param_float('event_params','purchase_revenue_in_usd') }},
      {{ ga4_param_float('event_params','value') }},
      0
    )) as total_revenue
  from {{ ref('src_ga4__events') }}
  where event_name in ('purchase','order_complete')
  group by 1,2,3
),
joined as (
  select
    coalesce(s.date, p.date) as event_date,
    coalesce(s.session_source, p.session_source) as session_source,
    coalesce(s.session_medium, p.session_medium) as session_medium,
    coalesce(s.sessions, 0) as sessions,
    coalesce(p.conversions, 0) as conversions,
    coalesce(p.total_revenue, 0) as total_revenue,
    case
      when lower(coalesce(s.session_medium, p.session_medium)) in ('cpc','ppc','paid') then 'Paid Search'
      when lower(coalesce(s.session_medium, p.session_medium)) like '%affiliate%' then 'Affiliate'
      when lower(coalesce(s.session_medium, p.session_medium)) like '%email%' then 'Email'
      when lower(coalesce(s.session_medium, p.session_medium)) in ('social','paid_social') then 'Social'
      when lower(coalesce(s.session_medium, p.session_medium)) in ('display','cpm') then 'Display'
      when lower(coalesce(s.session_medium, p.session_medium)) = 'referral' then 'Referral'
      when lower(coalesce(s.session_source, p.session_source)) in ('(direct)','direct')
         and lower(coalesce(s.session_medium, p.session_medium)) in ('(none)','none') then 'Direct'
      else 'Other'
    end as channel_grouping
  from sessions s
  full outer join purchases p
    on s.date = p.date
   and s.session_source = p.session_source
   and s.session_medium = p.session_medium
)

select *
from joined
where event_date >= date('2024-01-01')
