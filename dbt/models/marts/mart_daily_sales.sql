with delivered as (
    select
        order_id,
        customer_id,
        order_date,
        order_revenue,
        item_count
    from (
        select
            order_id,
            customer_id,
            order_date,
            sum(line_total_value) as order_revenue,
            count(*) as item_count
        from {{ ref('int_delivered_order_items') }}
        group by 1, 2, 3
    )
),

daily as (
    select
        order_date,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as customer_count,
        sum(order_revenue) as daily_revenue,
        sum(item_count) as item_count
    from delivered
    group by order_date
)

select
    order_date,
    order_count,
    customer_count,
    daily_revenue,
    item_count,
    sum(daily_revenue) over (order by order_date) as cumulative_revenue,
    current_timestamp() as processed_at
from daily
