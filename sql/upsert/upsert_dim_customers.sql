INSERT INTO dimensional.dim_customers (customers_key, full_name, first_name, last_name, gender, address, zip_code)
    (SELECT id                                                       as customers_key,
            (CONCAT(customers.first_name, ' ', customers.last_name)) as full_name,
            customers.first_name,
            customers.last_name,
            customers.gender,
            customers.address,
            customers.zip_code
     FROM customers)
ON CONFLICT (customers_key)
    DO UPDATE SET full_name  = excluded.full_name,
                  first_name = excluded.first_name,
                  last_name  = excluded.last_name,
                  gender     = excluded.gender,
                  address    = excluded.address,
                  zip_code   = excluded.zip_code;