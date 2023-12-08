with sales as (
    select * from {{ ref('fct_sales_order') }}
),

order_products as (
    select * from {{ ref('fct_order_products') }}
),

join_order as (
    select
        sales.customer_gender,
        sales.customer_name,
        order_products.product_name,
        order_products.category,
        order_products.amount as qty,
        order_products.fix_total as total_spent,
        sales.order_date as order_date
    from sales
    left join order_products on sales.orders_id = order_products.orders_id
)

select * from join_order