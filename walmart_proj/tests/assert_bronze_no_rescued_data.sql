with
    counts as (
        select 'orders' as tbl, count(*) as c
        from walmart.bronze.orders
        where _rescued_data is not null
        union all
        select 'customers', count(*)
        from walmart.bronze.customers
        where _rescued_data is not null
        union all
        select 'products', count(*)
        from walmart.bronze.products
        where _rescued_data is not null
        union all
        select 'order_items', count(*)
        from walmart.bronze.order_items
        where _rescued_data is not null
        union all
        select 'stores', count(*)
        from walmart.bronze.stores
        where _rescued_data is not null
        union all
        select 'employees', count(*)
        from walmart.bronze.employees
        where _rescued_data is not null
    )
select *
from counts
where c > 0
