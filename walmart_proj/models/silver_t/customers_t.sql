{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

select
    cast(customer_id as bigint) as customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    province,
    country,
    cast(created_timestamp as timestamp) as created_timestamp,
    cast(updated_timestamp as timestamp) as updated_timestamp,
    is_active,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'customers') }}

{% if is_incremental() %}
    where
        cast(updated_timestamp as timestamp) > (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}

qualify
    row_number() over (
        partition by customer_id order by cast(updated_timestamp as timestamp) desc
    )
    = 1
