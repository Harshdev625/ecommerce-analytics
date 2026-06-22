{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id'],
        incremental_strategy='merge',
        file_format='delta'
    )
}}

with base as (
    select
        b.date_key,
        dp.product_sk,
        ds.seller_sk,
        dc.customer_sk,
        b.order_id,
        b.order_item_id,
        b.price,
        b.freight_value,
        b.line_total_value as total_amount,
        b.delivery_duration_days,
        b.delivery_late,
        b.source_updated_at
    from {{ ref('int_delivered_order_items') }} as b
    inner join {{ source('gold', 'dim_date') }} as dd
        on b.date_key = dd.date_key
    inner join {{ source('gold', 'dim_product') }} as dp
        on b.product_id = dp.product_id
    inner join {{ source('gold', 'dim_seller') }} as ds
        on b.seller_id = ds.seller_id
    inner join {{ source('gold', 'dim_customer') }} as dc
        on b.customer_id = dc.customer_id
        and dc.is_current = true
    {% if is_incremental() %}
    where b.source_updated_at > (
        select coalesce(max(source_updated_at), timestamp '1900-01-01') from {{ this }}
    )
    {% endif %}
)

select
    date_key,
    product_sk,
    seller_sk,
    customer_sk,
    order_id,
    order_item_id,
    price,
    freight_value,
    total_amount,
    delivery_duration_days,
    delivery_late,
    source_updated_at,
    current_timestamp() as processed_at
from base
