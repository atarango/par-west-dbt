{{ config(materialized='view') }}

-- A) from order notes (e.g., plugins write the landing URL with ?gclid=...)
WITH from_notes AS (
  SELECT
    CAST(order_id AS STRING) AS order_id,
    REGEXP_EXTRACT(note, r'(?:^|[?&])gclid=([A-Za-z0-9_-]{10,})') AS gclid
  FROM {{ source('raw_woocommerce','order_notes') }}
  WHERE REGEXP_CONTAINS(note, r'(?:^|[?&])gclid=')
),

-- B) from orders.meta_data (JSON array of {key,value}) when present
from_meta AS (
  SELECT
    CAST(id AS STRING) AS order_id,
    JSON_VALUE(md, '$.value') AS gclid
  FROM {{ source('raw_woocommerce','orders') }},
       UNNEST(JSON_QUERY_ARRAY(meta_data)) AS md
  WHERE LOWER(JSON_VALUE(md, '$.key')) LIKE '%gclid%'
),

unioned AS (
  SELECT order_id, gclid FROM from_notes WHERE gclid IS NOT NULL
  UNION ALL
  SELECT order_id, gclid FROM from_meta  WHERE gclid IS NOT NULL
)

SELECT
  order_id,
  ANY_VALUE(gclid) AS gclid
FROM unioned
GROUP BY order_id