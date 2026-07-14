-- models/gold/fct_order_items.sql
-- verify first %sql
-- select order_item_id, count(*) from walmart.silver_t.order_items_t group by 1
-- having count(*) > 1
{{
  config(
    materialized='table',
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
