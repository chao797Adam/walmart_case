{{ config(materialized='table', schema='silver_b') }}

select
    -- Orders
    o.order_id,
    o.store_id,
    o.order_timestamp,
    o.payment_method,
    o.order_status,
    o.total_amount,
    o.created_timestamp as order_created_timestamp,
    o.updated_timestamp as order_updated_timestamp,
    o.is_active as order_is_active,
    o.processed_at as order_processed_at,
    current_timestamp() as obt_b_processed_at,

    -- Customers
    c.customer_id,
    c.first_name as customer_first_name,
    c.last_name as customer_last_name,
    c.email as customer_email,
    c.phone as customer_phone,
    c.city as customer_city,
    c.province as customer_province,
    c.country as customer_country,
    c.created_timestamp as customer_created_timestamp,
    c.updated_timestamp as customer_updated_timestamp,
    c.is_active as customer_is_active,
    c.processed_at as customer_processed_at,

    -- Order Items
    oi.order_item_id,
    oi.quantity,
    oi.unit_price,
    oi.line_amount,
    oi.created_timestamp as order_item_created_timestamp,
    oi.updated_timestamp as order_item_updated_timestamp,
    oi.is_active as order_item_is_active,
    oi.processed_at as order_item_processed_at,

    -- Products
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.created_timestamp as product_created_timestamp,
    p.updated_timestamp as product_updated_timestamp,
    p.is_active as product_is_active,
    p.processed_at as product_processed_at,

    -- Employees
    e.employee_id,
    e.first_name as employee_first_name,
    e.last_name as employee_last_name,
    e.email as employee_email,
    e.job_title,
    e.salary,
    e.created_timestamp as employee_created_timestamp,
    e.updated_timestamp as employee_updated_timestamp,
    e.is_active as employee_is_active,
    e.processed_at as employee_processed_at,

    -- Stores
    s.store_name,
    s.city as store_city,
    s.province as store_province,
    s.country as store_country,
    s.created_timestamp as store_created_timestamp,
    s.updated_timestamp as store_updated_timestamp,
    s.is_active as store_is_active,
    s.processed_at as store_processed_at

from {{ ref('orders_t') }} o
left join {{ ref('customers_t') }} c on o.customer_id = c.customer_id
left join {{ ref('order_items_t') }} oi on o.order_id = oi.order_id
left join {{ ref('products_t') }} p on oi.product_id = p.product_id
left join {{ ref('employees_t') }} e on o.store_id = e.store_id
left join {{ ref('stores_t') }} s on o.store_id = s.store_id
