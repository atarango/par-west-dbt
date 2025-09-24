-- models/staging/woocommerce/stg_woocommerce_products.sql
{{ config(
    materialized='incremental',
    partition_by={'field': 'product_date', 'data_type': 'date'},
    cluster_by=['product_id','sku'],
    alias='stg_woocommerce_products',
    incremental_strategy='insert_overwrite'
) }}

with raw as (
  select
    SAFE_CAST(id AS INT64)                          as product_id,
    LOWER(TRIM(sku))                                as sku,
    CAST(name AS STRING)                            as name,
    LOWER(TRIM(slug))                               as slug,
    LOWER(TRIM(type))                               as type_raw,
    LOWER(TRIM(status))                             as status_raw,

    -- money & measures (strings in raw → numeric)
    SAFE_CAST(price  AS NUMERIC)                    as price,
    SAFE_CAST(weight AS NUMERIC)                    as weight,

    on_sale,
    virtual,

    CAST(permalink AS STRING)                       as permalink,
    LOWER(TRIM(tax_class))                          as tax_class,
    LOWER(TRIM(backorders))                         as backorders,

    -- canonical UTC timestamps; derive date partition
    SAFE_CAST(date_created_gmt  AS TIMESTAMP)       as created_at_utc,
    SAFE_CAST(date_modified_gmt AS TIMESTAMP)       as modified_at_utc,

    CAST(description AS STRING)                     as description,

    -- keep raw JSON we’ll use in downstream bridges
    categories,
    images,

    -- ingestion metadata for tie-breaks
    _airbyte_extracted_at
  from {{ source('raw_woocommerce','products') }}
),

dedup as (
  select *
  from raw
  qualify row_number() over (
    partition by product_id
    order by modified_at_utc desc,
             created_at_utc  desc,
             _airbyte_extracted_at desc
  ) = 1
),

final as (
  select
    product_id,
    sku,
    name,
    slug,

    -- canonicalized enums
    case
      when type_raw in ('simple','variable','grouped','external','subscription','variable-subscription') then type_raw
      else 'unknown'
    end as type,

    case
      when status_raw in ('publish','draft','pending','private') then status_raw
      else 'unknown'
    end as product_status,

    price,
    weight,
    on_sale,
    virtual,
    tax_class,
    backorders,
    permalink,

    -- first image url (if present)
    (
      select JSON_VALUE(img, '$.src')
      from unnest(JSON_QUERY_ARRAY(images, '$')) img
      limit 1
    ) as primary_image_url,

    created_at_utc,
    modified_at_utc,
    DATE(coalesce(modified_at_utc, created_at_utc)) as product_date,  -- partition key

    description
  from dedup
)

select * from final

{% if is_incremental() %}
  -- rewrite recent product_date partitions (tune 120d if needed)
  where product_date >= date_sub(current_date(), interval 120 day)
{% endif %}
