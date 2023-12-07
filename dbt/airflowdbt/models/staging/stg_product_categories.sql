with

source as (

    select * from {{ source('raw', 'product_categories') }}

),

renamed as (

    select
        
        id as product_categories_id,

        name

    from source
)

select * from renamed