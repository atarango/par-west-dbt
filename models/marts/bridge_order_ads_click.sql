{{ config(materialized='view', alias='bridge_order_ads_click') }}

-- Orders
WITH base AS (
  SELECT
    CAST(order_id AS STRING) AS order_id,
    order_number,
    created_at_utc           AS created_at
  FROM {{ ref('stg_woocommerce_orders') }}
),

-- Woo UTMs/GCLID (may be empty)
utm AS (
  SELECT
    CAST(order_id AS STRING) AS order_id,
    utm_campaign,
    CAST(gclid AS STRING)     AS gclid
  FROM {{ ref('stg_woocommerce__utm') }}
),

-- Ads ClickView: gclid -> campaign
clk AS (
  SELECT
    CAST(click_view_gclid AS STRING) AS gclid,
    CAST(campaign_id      AS STRING) AS campaign_id,
    ANY_VALUE(campaign_name)         AS campaign_name,
    DATETIME(segments_date)          AS click_dt
  FROM {{ source('raw_google_ads','click_view') }}
  WHERE click_view_gclid IS NOT NULL
  GROUP BY click_view_gclid, campaign_id, click_dt
),

-- Ads campaign dimension (for UTM name matching and enrichment)
campaign_dim AS (
  SELECT
    CAST(campaign_id AS STRING) AS campaign_id,
    ANY_VALUE(campaign_name)    AS campaign_name
  FROM {{ source('raw_google_ads','campaign') }}
  GROUP BY 1
),

/* ---------- Path 1: Woo GCLID ---------- */
gclid_match AS (
  SELECT
    b.order_id, b.order_number, b.created_at,
    u.gclid,
    c.campaign_id,
    COALESCE(c.campaign_name, d.campaign_name) AS campaign_name,
    c.click_dt,
    30 AS pr
  FROM base b
  JOIN utm  u ON u.order_id = b.order_id
  JOIN clk  c ON c.gclid    = u.gclid
  LEFT JOIN campaign_dim d USING (campaign_id)
  WHERE u.gclid IS NOT NULL
),

/* ---------- Path 2: Woo UTM campaign ↔ Ads campaign name ---------- */
utm_match AS (
  SELECT
    b.order_id, b.order_number, b.created_at,
    CAST(NULL AS STRING)        AS gclid,
    d.campaign_id,
    d.campaign_name,
    CAST(NULL AS DATETIME)      AS click_dt,
    10 AS pr
  FROM base b
  JOIN utm u ON u.order_id = b.order_id
  JOIN campaign_dim d
    ON LOWER(REGEXP_REPLACE(TRIM(u.utm_campaign), r'[^a-z0-9]+', ' '))
     = LOWER(REGEXP_REPLACE(TRIM(d.campaign_name), r'[^a-z0-9]+', ' '))
  WHERE u.utm_campaign IS NOT NULL
),

/* ---------- Path 3: GA4 purchases → orders (txn_id = order_number OR order_id) ---------- */
ga4 AS (
  SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    CAST(utm_campaign AS STRING)   AS utm_campaign,
    CAST(gclid AS STRING)          AS gclid,
    event_ts
  FROM {{ ref('stg_ga4__purchases') }}
),

ga4_orders AS (
  SELECT
    b.order_id, b.order_number, b.created_at,
    g.gclid, g.utm_campaign, g.event_ts
  FROM base b
  JOIN ga4 g
    ON g.transaction_id IN (b.order_number, b.order_id)  -- order_id already STRING
),

/* ---------- Path 3a: GA4 GCLID ---------- */
ga4_gclid_match AS (
  SELECT
    go.order_id, go.order_number, go.created_at,
    go.gclid,
    c.campaign_id,
    COALESCE(c.campaign_name, d.campaign_name) AS campaign_name,
    c.click_dt,
    20 AS pr
  FROM ga4_orders go
  JOIN clk c ON c.gclid = go.gclid
  LEFT JOIN campaign_dim d USING (campaign_id)
  WHERE go.gclid IS NOT NULL
),

/* ---------- Path 4: GA4 UTM campaign ↔ Ads campaign name ---------- */
ga4_utm_match AS (
  SELECT
    go.order_id, go.order_number, go.created_at,
    CAST(NULL AS STRING)        AS gclid,
    d.campaign_id,
    d.campaign_name,
    CAST(NULL AS DATETIME)      AS click_dt,
    5 AS pr
  FROM ga4_orders go
  JOIN campaign_dim d
    ON LOWER(REGEXP_REPLACE(TRIM(go.utm_campaign), r'[^a-z0-9]+', ' '))
     = LOWER(REGEXP_REPLACE(TRIM(d.campaign_name), r'[^a-z0-9]+', ' '))
  WHERE go.utm_campaign IS NOT NULL
),

unioned AS (
  SELECT * FROM gclid_match
  UNION ALL
  SELECT * FROM ga4_gclid_match
  UNION ALL
  SELECT * FROM utm_match
  UNION ALL
  SELECT * FROM ga4_utm_match
)

SELECT
  order_id,
  order_number,
  created_at,
  gclid,
  campaign_id,
  campaign_name,
  click_dt
FROM unioned
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY pr DESC) = 1