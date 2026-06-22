with source as (
    select * from {{ source('bronze', 'sellers') }}
),

cleaned as (
    select
        seller_id,
        seller_zip_code_prefix,
        {{ normalize_city('seller_city') }} as seller_city,
        {{ normalize_state('seller_state') }} as seller_state,
        _ingested_at,
        current_timestamp() as processed_at
    from source
),

deduped as (
    select
        *,
        row_number() over (
            partition by seller_id
            order by seller_city
        ) as _rn
    from cleaned
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _ingested_at,
    processed_at
from deduped
where _rn = 1
