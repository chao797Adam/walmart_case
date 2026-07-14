-- models/gold/dim_customers.sql
{{
  config(
    materialized='table',
    alias='dim_customers',
    tags=['gold', 'dim']
  )
}}
with
    base_customers as (
        select
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            city,
            province,
            country,
            created_timestamp,
            updated_timestamp,
            is_active,
            processed_at
        from {{ ref('customers_t') }}
    )

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    province,
    country,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at,
    current_timestamp() as customer_gold_processed_at
from base_customers
qualify row_number() over (partition by customer_id order by updated_timestamp desc) = 1
