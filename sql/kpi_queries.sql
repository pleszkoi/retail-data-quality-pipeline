DROP VIEW IF EXISTS daily_sales_kpi;
DROP VIEW IF EXISTS category_sales_summary;
DROP VIEW IF EXISTS customer_country_summary;

CREATE VIEW daily_sales_kpi AS
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue
FROM clean_orders
GROUP BY order_date
ORDER BY order_date;

CREATE VIEW category_sales_summary AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.quantity) AS total_quantity,
    SUM(o.total_amount) AS total_revenue
FROM clean_orders o
JOIN clean_products p
    ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;

CREATE VIEW customer_country_summary AS
SELECT
    c.country,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(o.total_amount) AS total_revenue
FROM clean_orders o
JOIN clean_customers c
    ON o.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;
