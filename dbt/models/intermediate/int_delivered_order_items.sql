select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp_ts,
    cast(o.order_purchase_timestamp_ts as date) as order_date,
    cast(date_format(cast(o.order_purchase_timestamp_ts as date), 'yyyyMMdd') as int) as date_key,
    o.delivery_duration_days,
    o.delivery_late,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    i.line_total_value,
    i.category_name_en,
    greatest(o.processed_at, i.processed_at) as source_updated_at
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_order_items') }} as i
    on o.order_id = i.order_id
where o.order_status = 'delivered'
