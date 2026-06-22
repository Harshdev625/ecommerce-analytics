with source as (
    select * from {{ source('bronze', 'orders') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id
            order by
                case when order_channel is null then 1 else 0 end,
                _ingested_at desc nulls last
        ) as _dedupe_rank
    from source
),

parsed as (
    select
        order_id,
        customer_id,
        order_status,
        cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp_ts,
        cast(order_approved_at as timestamp) as order_approved_at_ts,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date_ts,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date_ts,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date_ts,
        _ingested_at
    from deduped
    where _dedupe_rank = 1
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp_ts,
    order_approved_at_ts,
    order_delivered_carrier_date_ts,
    order_delivered_customer_date_ts,
    order_estimated_delivery_date_ts,
    datediff(
        order_delivered_customer_date_ts,
        order_delivered_carrier_date_ts
    ) as delivery_duration_days,
    order_delivered_customer_date_ts > order_estimated_delivery_date_ts as delivery_late,
    year(order_purchase_timestamp_ts) as order_year,
    month(order_purchase_timestamp_ts) as order_month,
    _ingested_at,
    current_timestamp() as processed_at
from parsed
