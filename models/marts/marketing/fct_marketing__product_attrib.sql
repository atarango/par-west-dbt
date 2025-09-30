{{ config(materialized='view', alias='stg_ga4__purchases') }}

-- Set vars in dbt_project.yml:
-- vars:
--   ga4_dataset: "analytics_XXXXXXXX"   # or "raw_ga4" if you mirrored it there

WITH src AS (
  SELECT
    -- GA4 event_date is "YYYYMMDD" STRING → parse to DATE
    PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS event_date,
    TIMESTAMP_MICROS(event_timestamp)                AS event_ts,
    event_name,
    event_params,
    ecommerce,
    traffic_source,
    collected_traffic_source,
    -- New GA4 last-click object (may be absent on older exports)
    session_traffic_source_last_click
  FROM `{{ var('ga4_dataset','raw_ga4') }}`.events_*
  WHERE event_name = 'purchase'
),

ep AS (
  SELECT
    event_date,
    event_ts,

    -- Transaction id: event param first, then ecommerce.transaction_id
    COALESCE(
      (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'transaction_id'),
      CAST(ecommerce.transaction_id AS STRING)
    ) AS transaction_id,

    -- Event-param session scope (preferred when present)
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'session_source')   AS sess_src_param,
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'session_medium')   AS sess_med_param,
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'session_campaign') AS sess_cmp_param,

    -- Some exports only have plain "source/medium/campaign" params
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'source')   AS src_param,
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'medium')   AS med_param,
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'campaign') AS cmp_param,

    -- Last-click manual campaign (GA4 new nested object)
    session_traffic_source_last_click.manual_campaign.source         AS lc_source,
    session_traffic_source_last_click.manual_campaign.medium         AS lc_medium,
    session_traffic_source_last_click.manual_campaign.campaign_name  AS lc_campaign,

    -- Last-click Google Ads campaign name as another fallback
    session_traffic_source_last_click.google_ads_campaign.campaign_name AS lc_gads_campaign,

    -- First-touch fallbacks
    traffic_source.source  AS ft_source,
    traffic_source.medium  AS ft_medium,
    traffic_source.name    AS ft_campaign,

    -- GCLID from GA4 export
    CAST(collected_traffic_source.gclid AS STRING) AS gclid_param,
    -- (rare) some implementations also stash gclid in event_params
    (SELECT p.value.string_value FROM UNNEST(event_params) p WHERE p.key = 'gclid') AS gclid_param_ep
  FROM src
),

final AS (
  SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    event_date,
    event_ts,

    -- gclid as STRING
    CAST(COALESCE(gclid_param, gclid_param_ep) AS STRING) AS gclid,

    -- source / medium with layered fallbacks
    NULLIF(TRIM(COALESCE(sess_src_param, src_param, lc_source, ft_source)), '') AS session_source,
    NULLIF(TRIM(COALESCE(sess_med_param, med_param, lc_medium, ft_medium)), '') AS session_medium,

    -- campaign with layered fallbacks, but null out "direct/none" and "(not set)"
    CASE
      WHEN LOWER(TRIM(COALESCE(sess_src_param, src_param, lc_source, ft_source))) IN ('(direct)','direct')
       AND LOWER(TRIM(COALESCE(sess_med_param, med_param, lc_medium, ft_medium))) IN ('(none)','none')
      THEN NULL
      ELSE NULLIF(
             TRIM(COALESCE(sess_cmp_param, cmp_param, lc_campaign, lc_gads_campaign, ft_campaign)),
             ''
           )
    END AS utm_campaign
  FROM ep
  WHERE transaction_id IS NOT NULL AND transaction_id != ''
)

SELECT * FROM final