-- -- Create Schemas
-- CREATE SCHEMA dimensional;
-- CREATE SCHEMA facts;
-- CREATE SCHEMA presentations;

-- -- Crate Dimensional Tables 
-- DROP TABLE IF EXISTS dimensional.dim_date;
-- CREATE TABLE dimensional.dim_date
-- (
--     date_key    BIGINT PRIMARY KEY,
--     date        DATE    NOT NULL,
--     day_of_week INTEGER NOT NULL,
--     week        INTEGER NOT NULL,
--     month       INTEGER NOT NULL,
--     quarter     INTEGER NOT NULL,
--     year        INTEGER NOT NULL
-- );

-- DROP TABLE IF EXISTS dimensional.dim_coupons;
-- CREATE TABLE dimensional.dim_coupons
-- (
--     coupon_key       BIGINT PRIMARY KEY,
--     discount_percent DOUBLE PRECISION
-- );


-- DROP TABLE IF EXISTS dimensional.dim_customers;
-- CREATE TABLE dimensional.dim_customers
-- (
--     customers_key BIGINT PRIMARY KEY,
--     full_name     TEXT,
--     first_name    VARCHAR(150),
--     last_name     VARCHAR(150),
--     gender        VARCHAR(1),
--     address       TEXT,
--     zip_code      varchar(8)
-- );

-- DROP TABLE IF EXISTS dimensional.dim_login_attempts;
-- CREATE TABLE dimensional.dim_login_attempts
-- (
--     login_key        BIGINT PRIMARY KEY,
--     customer_key     BIGINT,
--     login_successful BOOLEAN,
--     attempted_at     DATE,
--     FOREIGN KEY (customer_key) REFERENCES dimensional.dim_customers (customers_key)
-- );
 
-- DROP TABLE IF EXISTS dimensional.dim_product_categories;
-- CREATE TABLE dimensional.dim_product_categories
-- (
--     product_categories_key BIGINT PRIMARY KEY,
--     name                   TEXT
-- );


-- DROP TABLE IF EXISTS dimensional.dim_suppliers;
-- CREATE TABLE dimensional.dim_suppliers
-- (
--     suppliers_key BIGINT PRIMARY KEY,
--     name          VARCHAR(150),
--     country       VARCHAR(150)
-- );


-- DROP TABLE IF EXISTS dimensional.dim_products;
-- CREATE TABLE dimensional.dim_products
-- (
--     products_key           BIGINT PRIMARY KEY,
--     name                   TEXT,
--     price                  double precision,
--     product_categories_key BIGINT,
--     suppliers_key          BIGINT,
--     FOREIGN KEY (product_categories_key) REFERENCES dimensional.dim_product_categories (product_categories_key),
--     FOREIGN KEY (suppliers_key) REFERENCES dimensional.dim_suppliers (suppliers_key)
-- );

-- DROP TABLE IF EXISTS dimensional.dim_orders;
-- CREATE TABLE dimensional.dim_orders
-- (
--     order_key  BIGINT PRIMARY KEY,
--     customers_key BIGINT,
--     status     TEXT,
--     created_at DATE,
--     FOREIGN KEY (customers_key) REFERENCES dimensional.dim_customers(customers_key)
-- );



-- -- Create Fact Table
-- DROP TABLE IF EXISTS facts.fact_sales_orders;
-- CREATE TABLE facts.fact_sales_orders
-- (
--     sale_order_key BIGINT PRIMARY KEY,
--     order_key      BIGINT,
--     customers_key  BIGINT,
--     product_key    BIGINT,
--     coupon_key     BIGINT,
--     date_key       BIGINT,
--     amount         BIGINT,
--     price_per_unit DOUBLE PRECISION,
--     total          DOUBLE PRECISION,
--     discount       DOUBLE PRECISION,
--     fixed_total    DOUBLE PRECISION,
--     status         TEXT,
--     created_at     DATE,
--     FOREIGN KEY (order_key) REFERENCES dimensional.dim_orders (order_key),
--     FOREIGN KEY (customers_key) REFERENCES dimensional.dim_customers (customers_key),
--     FOREIGN KEY (product_key) REFERENCES dimensional.dim_products (products_key),
--     FOREIGN KEY (coupon_key) REFERENCES dimensional.dim_coupons (coupon_key),
--     FOREIGN KEY (date_key) REFERENCES dimensional.dim_date (date_key)
-- );



-- DROP TABLE IF EXISTS facts.fact_product_performance;
-- CREATE TABLE facts.fact_product_performance
-- (
--    product_key BIGINT PRIMARY KEY,
--    name        TEXT,
--    count_of_sales      DOUBLE PRECISION,
--    total_revenue     DOUBLE PRECISION,
--    FOREIGN KEY (product_key) REFERENCES dimensional.dim_products (products_key)
-- );



-- DROP TABLE IF EXISTS facts.fact_product_category_performance;
-- CREATE TABLE facts.fact_product_category_performance
-- (
--    category_key      BIGINT PRIMARY KEY,
--    name              TEXT,
--    count_of_sales    DOUBLE PRECISION,
--    total_revenue     DOUBLE PRECISION,
--    FOREIGN KEY (category_key) REFERENCES dimensional.dim_product_categories (product_categories_key)
-- );
