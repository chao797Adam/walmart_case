-- models/gold/dim_products.sql
{{
  config(
    materialized='table',
    alias='dim_products',
    tags=['gold', 'dim']
  )
}}

with
    base_products as (
        select
            product_id,
            product_name,
            category,
            brand,
            price,
            created_timestamp,
            updated_timestamp,
            is_active,
            processed_at
        from {{ ref('products_t') }}
    )

select
    product_id,
    product_name,
    category,
    brand,
    price,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at,
    current_timestamp() as product_gold_processed_at
from base_products
qualify row_number() over (partition by product_id order by updated_timestamp desc) = 1
