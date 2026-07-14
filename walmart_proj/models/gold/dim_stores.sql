-- models/gold/dim_stores.sql
{{
  config(
    materialized='table',
    alias='dim_stores',
    tags=['gold', 'dim']
  )
}}

with
    base_stores as (
        select
            store_id,
            store_name,
            city,
            province,
            country,
            created_timestamp,
            updated_timestamp,
            is_active,
            processed_at
        from {{ ref('stores_t') }}
    )

select
    store_id,
    store_name,
    city,
    province,
    country,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at,
    current_timestamp() as store_gold_processed_at
from base_stores
qualify row_number() over (partition by store_id order by updated_timestamp desc) = 1
