CREATE TABLE IF NOT EXISTS `ecommerce_analytics.raw_orders` (
    order_id INT64,
    user_id INT64,
    product_id INT64,
    product_title STRING,
    unit_price FLOAT64,
    quantity INT64,
    total_amount FLOAT64,
    discount_percentage FLOAT64,
    discounted_price FLOAT64,
    order_date DATE,
    ingested_at TIMESTAMP
)
PARTITION BY order_date;
