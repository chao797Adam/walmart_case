{{
  config(
    materialized='incremental',
    unique_key='product_id',
    merge_update_columns=['product_name','category','brand','price','updated_timestamp','is_active','processed_at','product_gold_processed_at'],
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
        {% if is_incremental() %}
            where
                updated_timestamp > (
                    select coalesce(max(updated_timestamp), timestamp '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
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
