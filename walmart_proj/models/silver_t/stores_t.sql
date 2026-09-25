{{
    config(
        materialized='incremental',
        unique_key='store_id',
        merge_update_columns=[
            'store_name', 'city', 'province', 'country',
            'created_timestamp', 'updated_timestamp',
            'is_active', 'processed_at'
        ]
    )
}}

select
    cast(store_id as bigint) as store_id,
    store_name,
    city,
    province,
    country,
    cast(created_timestamp as timestamp) as created_timestamp,
    cast(updated_timestamp as timestamp) as updated_timestamp,
    is_active,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'stores') }}

{% if is_incremental() %}
    where
        cast(updated_timestamp as timestamp) >= (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}

qualify
    row_number() over (
        partition by cast(store_id as bigint)
        order by cast(updated_timestamp as timestamp) desc
    )
    = 1
