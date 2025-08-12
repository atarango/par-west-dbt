{{ config(materialized='view') }}

with base as (
  select
    id  as event_id,
    lower(type) as event_type,

    -- Prefer native TIMESTAMP column; otherwise parse ISO string or numeric epoch in attributes
    coalesce(
      datetime,
      parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_VALUE(attributes, '$.datetime')),
      timestamp_millis(SAFE_CAST(JSON_VALUE(attributes, '$.timestamp') AS INT64) * 1000)
    ) as event_ts,

    -- identifiers
    JSON_VALUE(relationships, '$.metric.data.id') as metric_id,
    JSON_VALUE(attributes, '$.uuid')              as event_uuid,

    -- event_properties (all optional)
    JSON_VALUE(attributes, '$.event_properties.device_type')          as device_type,
    JSON_VALUE(attributes, '$.event_properties.form_id')              as form_id,
    JSON_VALUE(attributes, '$.event_properties.form_type')            as form_type,
    JSON_VALUE(attributes, '$.event_properties.form_version_id')      as form_version_id,
    JSON_VALUE(attributes, '$.event_properties.form_version_c_id')    as form_version_c_id,
    JSON_VALUE(attributes, '$.event_properties.hostname')             as host_name,
    JSON_VALUE(attributes, '$.event_properties.page_url')             as page_url,
    JSON_VALUE(attributes, '$.event_properties.href')                 as href,
    JSON_VALUE(attributes, '$.event_properties.referrer')             as referrer,
    JSON_VALUE(attributes, '$.event_properties.first_referrer')       as first_referrer,
    JSON_VALUE(attributes, '$.event_properties.step_name')            as step_name,
    SAFE_CAST(JSON_VALUE(attributes, '$.event_properties.step_number') AS INT64) as step_number,
    JSON_VALUE(attributes, '$.event_properties.cid')                  as client_cid,

    _airbyte_raw_id,
    _airbyte_extracted_at
  from {{ source('raw_klaviyo', 'events') }}
  where datetime is not null
        or JSON_VALUE(attributes, '$.datetime') is not null
)

select
  event_id,
  event_type,
  event_ts,
  DATE(event_ts) as event_date,
  metric_id,
  event_uuid,
  device_type,
  form_id,
  form_type,
  form_version_id,
  form_version_c_id,
  host_name,
  page_url,
  href,
  referrer,
  first_referrer,
  step_name,
  step_number,
  client_cid,
  _airbyte_raw_id,
  _airbyte_extracted_at
from base
