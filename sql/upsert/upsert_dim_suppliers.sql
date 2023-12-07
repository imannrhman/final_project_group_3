INSERT INTO dimensional.dim_suppliers(suppliers_key, name, country)
    (SELECT id as suppliers_key,
            name,
            country
     FROM suppliers)
ON CONFLICT (suppliers_key)
    DO UPDATE SET name    = excluded.name,
                  country = excluded.country;