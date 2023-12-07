with

source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        
        id as orders_id,

        customer_id as customers_id,

        status,

        created_at
        
    from source
)

select * from renamed