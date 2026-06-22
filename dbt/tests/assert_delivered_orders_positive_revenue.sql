-- Fails when any delivered order has zero or negative total line revenue.
select
    order_id,
    sum(line_total_value) as order_revenue
from {{ ref('int_delivered_order_items') }}
group by order_id
having order_revenue <= 0
