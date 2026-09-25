{{
  config(
    materialized='incremental',
    unique_key='store_id',
    merge_update_columns=['store_name','city','province','country','updated_timestamp','is_active','processed_at','store_gold_processed_at'],
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
        {% if is_incremental() %}
            where
                updated_timestamp > (
                    select coalesce(max(updated_timestamp), timestamp '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
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
