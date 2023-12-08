-- 10 ten supplier
with sales as (
    select * from {{ ref('fct_sales_order') }}
),

order_products as (
    select * from {{ ref('fct_order_products') }}
),

join_order as (
    select
        order_products.supplier_name,
        max(order_products.supplier_location) as supplier_location,
        count(order_products.orders_id) as total_order,
        sum(sales.total_amount) as total_amount_sold
    from order_products
    left join sales on sales.orders_id = order_products.orders_id
    group by 1
    order by 3 desc
    limit 10
)

select * from join_order