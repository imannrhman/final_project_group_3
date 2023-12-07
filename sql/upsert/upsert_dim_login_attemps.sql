INSERT INTO dimensional.dim_login_attempts(login_key, customer_key, login_successful, attempted_at)
    (SELECT id           as login_key,
            customer_id  as customer_key,
            login_successful,
            attempted_at as DATE
     FROM login_attemps
     WHERE customer_id < (select MAX(id) FROM customers))
ON CONFLICT (login_key)
    DO UPDATE SET login_successful = excluded.login_successful,
                  attempted_at     = excluded.attempted_at;