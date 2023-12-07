with

source as (

    select * from {{ source('raw', 'suppliers') }}

),

renamed as (

    select
        
        id as suppliers_id,

        name,

        country
        
    from source
)

select * from renamed