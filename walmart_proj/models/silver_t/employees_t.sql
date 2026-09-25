{{
    config(
        materialized='incremental',
        unique_key='employee_id',
        merge_update_columns=[
            'store_id', 'first_name', 'last_name', 'email',
            'job_title', 'salary',
            'created_timestamp', 'updated_timestamp',
            'is_active', 'processed_at'
        ]
    )
}}

select
    cast(employee_id as bigint) as employee_id,
    cast(store_id as bigint) as store_id,
    first_name,
    last_name,
    email,
    job_title,
    cast(salary as decimal(18, 2)) as salary,
    cast(created_timestamp as timestamp) as created_timestamp,
    cast(updated_timestamp as timestamp) as updated_timestamp,
    is_active,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'employees') }}

{% if is_incremental() %}
    where
        cast(updated_timestamp as timestamp) >= (
            select coalesce(max(updated_timestamp), timestamp '1900-01-01 00:00:00')
            from {{ this }}
        )
{% endif %}

qualify
    row_number() over (
        partition by cast(employee_id as bigint)
        order by cast(updated_timestamp as timestamp) desc
    )
    = 1
