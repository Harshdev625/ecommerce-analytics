with delivered as (
    select
        order_id,
        customer_id,
        order_date,
        sum(line_total_value) as order_revenue
    from {{ ref('int_delivered_order_items') }}
    group by 1, 2, 3
),

reference as (
    select max(order_date) as reference_date
    from delivered
),

customer_metrics as (
    select
        d.customer_id,
        max(d.order_date) as last_order_date,
        count(distinct d.order_id) as frequency,
        round(sum(d.order_revenue), 2) as monetary,
        datediff(r.reference_date, max(d.order_date)) as recency_days,
        r.reference_date
    from delivered as d
    cross join reference as r
    group by d.customer_id, r.reference_date
),

scored as (
    select
        customer_id,
        last_order_date,
        frequency,
        monetary,
        recency_days,
        reference_date,
        6 - ntile(5) over (order by recency_days asc) as r_score,
        ntile(5) over (order by frequency desc) as f_score,
        ntile(5) over (order by monetary desc) as m_score
    from customer_metrics
)

select
    customer_id,
    last_order_date,
    frequency,
    monetary,
    recency_days,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score as rfm_score,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 3 and f_score >= 3 and m_score >= 3 then 'Loyal'
        when r_score >= 4 and f_score <= 2 then 'Potential'
        when r_score <= 2 and f_score >= 3 then 'At Risk'
        when r_score <= 2 and f_score <= 2 then 'Lost'
        else 'Needs Attention'
    end as rfm_segment,
    row_number() over (order by monetary desc) as value_rank,
    reference_date,
    current_timestamp() as processed_at
from scored
