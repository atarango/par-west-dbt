{{ config(materialized='view') }}

with purchases_raw as (
  select
    -- keys for attribution
    user_pseudo_id,
    cast({{ ga4_param_int('event_params','ga_session_id') }} as int64) as session_id,

    -- purchase basics
    cast({{ ga4_param_string('event_params','transaction_id') }} as string) as transaction_id,
    parse_date('%Y%m%d', event_date)                                       as event_date,
    timestamp_micros(event_timestamp)                                      as event_ts,

    -- revenue (prefer ecommerce, else params)
    coalesce(
      ecommerce.purchase_revenue_in_usd,
      {{ ga4_param_float('event_params','purchase_revenue_in_usd') }},
      {{ ga4_param_float('event_params','value') }},
      0.0
    ) as revenue,

    -- sometimes gclid is on the purchase; keep for fallback
    coalesce(collected_traffic_source.gclid,
             {{ ga4_param_string('event_params','gclid') }}) as gclid_event

  from {{ ref('src_ga4__events') }}
  where event_name in ('purchase','order_complete')
),

session_attrs as (
  select
    user_pseudo_id,
    cast({{ ga4_param_int('event_params','ga_session_id') }} as int64) as session_id,
    timestamp_micros(event_timestamp)                                   as session_start_ts,

    -- prefer session_*; fallback to standard fields; final fallback = first user traffic_source.*
    nullif(trim(coalesce(
      {{ ga4_param_string('event_params','session_campaign') }},
      {{ ga4_param_string('event_params','campaign') }},
      traffic_source.name
    )), '') as utm_campaign,

    nullif(trim(coalesce(
      {{ ga4_param_string('event_params','session_source') }},
      {{ ga4_param_string('event_params','source') }},
      traffic_source.source
    )), '') as session_source_raw,

    nullif(trim(coalesce(
      {{ ga4_param_string('event_params','session_medium') }},
      {{ ga4_param_string('event_params','medium') }},
      traffic_source.medium
    )), '') as session_medium_raw,

    coalesce(collected_traffic_source.gclid,
             {{ ga4_param_string('event_params','gclid') }}) as gclid_session

  from {{ ref('src_ga4__events') }}
  where event_name = 'session_start'
),

-- Exact match on ga_session_id (best attribution)
exact as (
  select
    p.user_pseudo_id,
    p.event_ts,
    s.gclid_session     as ex_gclid,
    s.utm_campaign      as ex_campaign,
    s.session_source_raw as ex_source,
    s.session_medium_raw as ex_medium
  from purchases_raw p
  left join session_attrs s
    on p.user_pseudo_id = s.user_pseudo_id
   and p.session_id     = s.session_id
),

-- Fallback: most recent session_start for the user before the purchase
fallback as (
  select
    p.user_pseudo_id,
    p.event_ts,
    s.gclid_session      as fb_gclid,
    s.utm_campaign       as fb_campaign,
    s.session_source_raw as fb_source,
    s.session_medium_raw as fb_medium
  from purchases_raw p
  left join session_attrs s
    on p.user_pseudo_id = s.user_pseudo_id
   and s.session_start_ts <= p.event_ts
  qualify row_number() over (
    partition by p.user_pseudo_id, p.event_ts
    order by s.session_start_ts desc
  ) = 1
),

combined as (
  select
    p.event_date,
    p.event_ts,
    p.transaction_id,
    p.revenue,

    -- GCLID precedence: exact session → fallback session → purchase event
    coalesce(ex.ex_gclid, fb.fb_gclid, p.gclid_event) as gclid,

    -- Source/Medium precedence with Direct/(none) normalization → null
    case
      when lower(coalesce(ex.ex_source, fb.fb_source)) in ('(direct)','direct')
       and lower(coalesce(ex.ex_medium, fb.fb_medium)) in ('(none)','none') then null
      else coalesce(ex.ex_source, fb.fb_source)
    end as session_source,

    case
      when lower(coalesce(ex.ex_source, fb.fb_source)) in ('(direct)','direct')
       and lower(coalesce(ex.ex_medium, fb.fb_medium)) in ('(none)','none') then null
      else coalesce(ex.ex_medium, fb.fb_medium)
    end as session_medium,

    coalesce(ex.ex_campaign, fb.fb_campaign) as utm_campaign

  from purchases_raw p
  left join exact    ex on ex.user_pseudo_id = p.user_pseudo_id and ex.event_ts = p.event_ts
  left join fallback fb on fb.user_pseudo_id = p.user_pseudo_id and fb.event_ts = p.event_ts
),

-- Deduplicate: keep latest purchase event per transaction_id
deduped as (
  select
    *,
    row_number() over (partition by transaction_id order by event_ts desc) as rn
  from combined
)

select
  event_date,
  event_ts,
  transaction_id,
  gclid,
  session_source,
  session_medium,
  utm_campaign,
  revenue
from deduped
where coalesce(transaction_id, '') != ''
  and rn = 1
