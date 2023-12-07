with

source as (

    select * from {{ source('raw', 'coupons') }}

),

renamed as (

    select
        
        id as coupons_id,

        discount_percent
        
    from source

)

select * from renamed