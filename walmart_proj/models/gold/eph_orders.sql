select distinct
    order_id,
    payment_method,
    order_status,
    order_timestamp,
    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    obt_b_processed_at,
    current_timestamp() as order_gold_processed_at
from {{ ref('obt_b') }}
