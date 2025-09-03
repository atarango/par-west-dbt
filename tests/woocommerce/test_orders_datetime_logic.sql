-- fails if any row violates temporal logic
with o as (
  select * from {{ ref('stg_woocommerce_orders') }}
),
violations as (
  select
    order_id,
    order_date,
    paid_at,
    completed_at,
    modified_at
  from o
  where
    -- paid_at must be >= order_date (when present)
    (paid_at is not null and paid_at < timestamp(order_date))
    or
    -- completed_at must be >= paid_at (when both present)
    (completed_at is not null and paid_at is not null and completed_at < paid_at)
    or
    -- modified_at must be >= order_date
    (modified_at < timestamp(order_date))
    or
    -- any timestamp in "the future" beyond a small clock skew
    (coalesce(paid_at, completed_at, modified_at) > timestamp_add(current_timestamp(), interval 10 minute))
)
select * from violations
