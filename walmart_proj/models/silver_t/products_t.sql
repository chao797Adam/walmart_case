{{
    config(
        materialized='incremental',
        unique_key='product_id',
        merge_update_columns=[
            'product_name', 'category', 'brand', 'price',
            'created_timestamp', 'updated_timestamp',
            'is_active', 'processed_at'
        ]
    )
}}

select
    cast(product_id as bigint) as product_id,
    product_name,
    category,
    brand,
    cast(price as decimal(18, 2)) as price,
    cast(created_timestamp as timestamp) as created_timestamp,
    cast(updated_timestamp as timestamp) as updated_timestamp,
    is_active,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'products') }}

{% if is_incremental() %}
    where
        cast(updated_timestamp as timestamp) >= (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}

qualify
    row_number() over (
        partition by cast(product_id as bigint)
        order by cast(updated_timestamp as timestamp) desc
    )
    = 1
