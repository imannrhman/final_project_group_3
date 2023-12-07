INSERT INTO dimensional.dim_product_categories(product_categories_key, name)
    (SELECT id as product_categories_key,
            name
     FROM product_categories)
ON CONFLICT (product_categories_key)
    DO UPDATE SET name = excluded.name;