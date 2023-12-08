with sales as (
    select * from {{ ref('fct_sales_order') }}
),

orders_product as (
    select
        *
    from facts.fct_order_products as orders_product
),

join_orders as (
    select
        orders_product.orders_id,
        count(orders_product.orders_id) as total_order_buy,
        sum(orders_product.amount) as total_amount,
        sum(orders_product.fix_total) as total_purchase,
        count(*) filter (where sales.using_coupon) as total_coupon_used,
        mode() within group (order by orders_product.product_name) as most_product_buy,
        mode() within group (order by orders_product.products_id) as most_product_buy_id
    from sales
    left join orders_product on orders_product.orders_id = sales.orders_id
    group by  1
),

join_with_customer as (
    select
        sales.customers_id,
        sales.customer_name,
        count(sales.orders_id) as total_orders,
        sum(join_orders.total_amount) as total_amount,
        sum(join_orders.total_purchase) as total_purchase,
        mode() within group (order by join_orders.most_product_buy) as most_product_buy,
        mode() within group (order by join_orders.most_product_buy_id) as most_product_buy_id
    from sales
    left join join_orders on join_orders.orders_id = sales.orders_id
    group by 1,2
    order by 3 desc
    limit 10
)

select * from join_with_customer
