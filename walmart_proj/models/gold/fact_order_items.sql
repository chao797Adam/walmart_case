{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
    merge_update_columns=['order_id','product_id','quantity','unit_price','line_amount','updated_timestamp','is_active','processed_at','fct_processed_at'],
    tags=['gold', 'fct']
  )
}}
with
    base_order_items as (
        select
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            line_amount,
            created_timestamp,
            updated_timestamp,
            is_active,
            processed_at
        from {{ ref('order_items_t') }}
        {% if is_incremental() %}
            where
                updated_timestamp > (
                    select coalesce(max(updated_timestamp), timestamp '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    )

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_amount,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at,
    current_timestamp() as fct_processed_at
from base_order_items
qualify
    row_number() over (partition by order_item_id order by updated_timestamp desc) = 1
