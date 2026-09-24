{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

select *, current_timestamp() as processed_at
from {{ source('walmart_databricks', 'products') }}

{% if is_incremental() %}
    where
        updated_timestamp
        > (select coalesce(max(updated_timestamp), '1900-01-01') from {{ this }})
{% endif %}

qualify
    row_number() over (
        partition by product_id order by cast(updated_timestamp as timestamp) desc
    )
    = 1
