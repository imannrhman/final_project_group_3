with order_items as (
    select  * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

coupons as (
    select * from {{ ref('dim_coupons') }}
),

joind_orders as (
    select
        order_items.order_items_id,
        
        products.products_id,
        products.product_name,
        products.product_price,
        products.category,
        products.supplier_name,
        products.supplier_location,

        order_items.orders_id,
        order_items.amount,
        products.product_price * order_items.amount as total,
        (case
            when coupons.coupons_id is not null
            then
                 true
            else
                 false
            end
        ) use_coupons,
        (case
            when coupons.coupons_id is not null
            then
                 concat(coupons.discount_percent, '%')
            end
        ) as discount,
        (case
            when coupons.coupons_id is not null
            then
                 (products.product_price * order_items.amount) - ((products.product_price * order_items.amount) * coupons.discount_percent/100)
            else
                 (products.product_price * order_items.amount)
            end) as fix_total
    from order_items
    left join products on products.products_id = order_items.products_id
    left join coupons on coupons.coupons_id = order_items.coupons_id
)

select * from joind_orders