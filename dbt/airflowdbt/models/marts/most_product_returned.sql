-- most product returned
with order_product as (
    select * from {{ ref('fct_order_products') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

joined_order as (
    select
        orders.orders_id,
        mode() within group (order by order_product.product_name) as product_name,
        max(order_product.category) as category,
        max(order_product.supplier_name) as suppliers,
        count(orders.orders_id) as total_order,
        sum(order_product.amount) as total_qty_sales,
        sum(order_product.fix_total) as total_purchase,
        count(*) filter (where order_product.use_coupons) as coupon_used
    from orders
    left join order_product on order_product.orders_id = orders.orders_id
    where orders.status = 'RETURNED'
    group by 1
    order by 5 desc
)

select * from joined_order