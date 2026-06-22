{% snapshot snap_customers %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='processed_at',
    )
}}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    processed_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
