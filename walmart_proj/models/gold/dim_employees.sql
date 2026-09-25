{{
  config(
    materialized='incremental',
    unique_key='employee_id',
    merge_update_columns=['store_id','first_name','last_name','email','job_title','salary','updated_timestamp','is_active','processed_at','employee_gold_processed_at'],
    alias='dim_employees',
    tags=['gold', 'dim']
  )
}}
with
    base_employees as (
        select
            employee_id,
            store_id,
            first_name,
            last_name,
            email,
            job_title,
            salary,
            created_timestamp,
            updated_timestamp,
            is_active,
            processed_at
        from {{ ref('employees_t') }}
        {% if is_incremental() %}
            where
                updated_timestamp > (
                    select coalesce(max(updated_timestamp), timestamp '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    )

select
    employee_id,
    store_id,
    first_name,
    last_name,
    email,
    job_title,
    salary,
    created_timestamp,
    updated_timestamp,
    is_active,
    processed_at,
    current_timestamp() as employee_gold_processed_at
from base_employees
qualify row_number() over (partition by employee_id order by updated_timestamp desc) = 1
