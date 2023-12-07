with products as (

    select * from {{ ref('stg_products') }}

),

category as (

    select * from {{ ref('stg_product_categories') }}

),

suppliers as (

    select * from {{ ref('stg_suppliers') }}

),


joined_with_product as (

    select
    products.products_id as products_id,
    products.name as product_name,
    products.price as product_price,
    category.name as category,
    suppliers.name as supplier_name,
    suppliers.country as supplier_location

    from products
    left join category on products.product_categories_id = category.product_categories_id
    left join suppliers on products.suppliers_id = suppliers.suppliers_id
    
)

select * from joined_with_product


