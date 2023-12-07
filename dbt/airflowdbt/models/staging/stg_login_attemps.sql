with

source as (

    select * from {{ source('raw', 'login_attemps') }}

),

renamed as (

    select
        
        id as login_attemps_id,
        
        customer_id as customers_id,

        login_successful,

        attempted_at
        
    from source

)

select * from renamed