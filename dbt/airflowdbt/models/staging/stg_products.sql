with

source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        
        id as products_id,

        name,

        price,

        category_id as product_categories_id,

        supplier_id as suppliers_id
        
    from source
)

select * from renamed