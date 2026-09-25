{{
    config(
        materialized='incremental',
        unique_key='order_id',
        merge_update_columns=[
            'customer_id', 'store_id',
            'order_timestamp',
            'payment_method', 'order_status',
            'total_amount',
            'created_timestamp', 'updated_timestamp',
            'is_active', 'processed_at'
        ]
    )
}}

select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(store_id as bigint) as store_id,
    cast(order_timestamp as timestamp) as order_timestamp,
    payment_method,
    order_status,
    cast(total_amount as decimal(18, 2)) as total_amount,
    cast(created_timestamp as timestamp) as created_timestamp,
    cast(updated_timestamp as timestamp) as updated_timestamp,
    is_active,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'orders') }}

{% if is_incremental() %}
    where
        cast(updated_timestamp as timestamp) >= (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}

qualify
    row_number() over (
        partition by cast(order_id as bigint)
        order by cast(updated_timestamp as timestamp) desc
    )
    = 1
