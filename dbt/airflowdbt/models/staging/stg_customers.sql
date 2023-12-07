with

source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        
        id as customers_id,
        
        first_name,

        last_name,

        gender,

        address,

        zip_code
        
    from source

)

select * from renamed