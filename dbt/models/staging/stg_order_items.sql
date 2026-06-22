with items as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(price as double) as price,
        coalesce(cast(freight_value as double), 0.0) as freight_value,
        cast(price as double) + coalesce(cast(freight_value as double), 0.0) as line_total_value,
        _ingested_at,
        current_timestamp() as processed_at
    from {{ source('bronze', 'order_items') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_id, order_item_id
            order by price desc
        ) as _dup_rank
    from items
),

enriched as (
    select
        i.*,
        p.product_category_name,
        coalesce(t.product_category_name_english, 'unknown') as category_name_en
    from ranked as i
    left join {{ source('bronze', 'products') }} as p
        on i.product_id = p.product_id
    left join {{ source('bronze', 'product_category_translation') }} as t
        on p.product_category_name = t.product_category_name
    where i._dup_rank = 1
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    line_total_value,
    product_category_name,
    category_name_en,
    _ingested_at,
    processed_at
from enriched
