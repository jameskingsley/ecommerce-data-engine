CREATE OR REPLACE TABLE `ecommerce_analytics.daily_sales_summary` AS
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount), 2) AS gross_revenue,
    ROUND(AVG(total_amount), 2) AS average_order_value
FROM
    `ecommerce_analytics.raw_orders`
GROUP BY
    order_date;
