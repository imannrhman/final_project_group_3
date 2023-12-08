with sales as (
    select * from {{ ref('fct_sales_order') }}
),

order_products as (
    select * from {{ ref('fct_order_products') }}
),

join_order as (
    select
        order_products.category,
        count(order_products.orders_id) as total_order,
        sum(order_products.amount) as total_qty_sold,
        sum(order_products.fix_total) as total_revenue,
        count(*) filter (where sales.customer_gender = 'M') as total_male_buy,
        count(*) filter (where sales.customer_gender = 'F') as total_female_buy
    from order_products
    left join sales on sales.orders_id = order_products.orders_id
    where order_products.discount is not null
    group by 1
    order by 2 desc
)

select * from join_order