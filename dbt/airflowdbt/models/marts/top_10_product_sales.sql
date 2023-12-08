with order_product as (
    select * from {{ ref('fct_order_products') }}
),

sales as (
    select * from {{ ref('fct_sales_order') }}
),

joined_sales as (
    select
        order_product.products_id,
        order_product.product_name,
        max(order_product.category) as category,
        max(order_product.supplier_name) as suppliers,
        count(order_product.order_items_id) as total_order,
        max(order_product.product_price) as product_per_price,
        sum(order_product.amount) as total_qty_sales,
        sum(order_product.fix_total) as revenue_product,
        count(*) filter (where order_product.use_coupons) as coupon_used
    from sales
    left join order_product on sales.orders_id = order_product.orders_id
    group by 1,2
    order by 5 desc
    limit 10
)

select * from joined_sales