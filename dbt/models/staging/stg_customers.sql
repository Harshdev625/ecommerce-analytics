with source as (
    select * from {{ source('bronze', 'customers') }}
),

cleaned as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        {{ normalize_city('customer_city') }} as customer_city,
        {{ normalize_state('customer_state') }} as customer_state,
        _ingested_at,
        current_timestamp() as processed_at
    from source
),

deduped as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by customer_city
        ) as _rn
    from cleaned
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _ingested_at,
    processed_at
from deduped
where _rn = 1
