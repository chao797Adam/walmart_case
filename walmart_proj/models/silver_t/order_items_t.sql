{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        merge_update_columns=[
            'order_id', 'product_id',
            'quantity', 'unit_price', 'line_amount',
            'created_timestamp', 'updated_timestamp',
            'is_active', 'processed_at'
        ]
    )
}}

select
    cast(order_item_id as bigint) as order_item_id,
    cast(order_id as bigint) as order_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as int) as quantity,
    cast(unit_price as decimal(18, 2)) as unit_price,
    cast(line_amount as decimal(18, 2)) as line_amount,
    cast(created_timestamp as timestamp) as created_timestamp,
    cast(updated_timestamp as timestamp) as updated_timestamp,
    is_active,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'order_items') }}

{% if is_incremental() %}
    where
        cast(updated_timestamp as timestamp) >= (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}

qualify
    row_number() over (
        partition by cast(order_item_id as bigint)
        order by cast(updated_timestamp as timestamp) desc
    )
    = 1
