DROP TABLE IF EXISTS clean_customers;
DROP TABLE IF EXISTS clean_products;
DROP TABLE IF EXISTS clean_orders;

CREATE TABLE clean_customers (
    customer_id INTEGER,
    full_name TEXT,
    email TEXT,
    country TEXT,
    registration_date TEXT
);

CREATE TABLE clean_products (
    product_id INTEGER,
    product_name TEXT,
    category TEXT,
    price REAL
);

CREATE TABLE clean_orders (
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    order_date TEXT,
    quantity REAL,
    total_amount REAL,
    currency TEXT
);