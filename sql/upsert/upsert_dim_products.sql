INSERT INTO dimensional.dim_products(products_key, name, price, product_categories_key, suppliers_key)
    (SELECT id          as products_key,
            name,
            price,
            category_id as product_categories_key,
            supplier_id as suppliers_key
     FROM products)
ON CONFLICT (products_key)
    DO UPDATE SET name                   = excluded.name,
                  price                  = excluded.price,
                  product_categories_key = excluded.product_categories_key;