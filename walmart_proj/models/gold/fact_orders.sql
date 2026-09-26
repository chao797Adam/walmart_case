{{ config(
    materialized='incremental',
    unique_key='order_id',
    merge_update_columns=[
        'customer_id', 'store_id',
        'order_timestamp', 'payment_method', 'order_status',
        'total_amount', 'is_active', 'processed_at',
        'fct_processed_at'
    ],
    tags=['gold', 'fct']
) }}

select
    order_id,
    customer_id,
    store_id,
    order_timestamp,
    payment_method,
    order_status,
    total_amount,
    is_active,
    processed_at,
    updated_timestamp,
    current_timestamp() as fct_processed_at
from {{ ref('orders_t') }}  --
{% if is_incremental() %}
    where
        updated_timestamp >= (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}
qualify row_number() over (partition by order_id order by updated_timestamp desc) = 1
