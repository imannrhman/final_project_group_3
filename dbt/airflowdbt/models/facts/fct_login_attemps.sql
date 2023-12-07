with login_attemps as (
     select * from {{ ref('stg_login_attemps') }}
),

customer as (
    select * from {{ ref('dim_customers') }}
),


joined_login_attemps as (
    select

    c.customers_id,
    c.full_name as users,
    max(login_attemps.login_attemps_id) as last_login_id,
    count(case when login_attemps.login_successful is true then login_attemps.login_successful end) as login_success,
    count(case when login_attemps.login_successful is false then login_attemps.login_successful end) login_failed,
    max(login_attemps.attempted_at) as last_login_attemps

    from login_attemps
    left join customer as c on login_attemps.customers_id = c.customers_id
    group by 1, 2
    
)

select * from joined_login_attemps where users is not null