import os
from dagster import asset, AssetIn
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BQ_DATASET_ID", "ecommerce_analytics")

@asset(
    ins={"raw_orders": AssetIn("raw_orders_bronze")},
    description="Aggregates raw orders into Silver layer daily sales metrics inside BigQuery.",
    group_name="silver_layer"
)
def daily_sales_summary(raw_orders) -> None:
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.daily_sales_summary` AS
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        SUM(quantity) AS total_units_sold,
        ROUND(SUM(total_amount), 2) AS gross_revenue,
        ROUND(AVG(total_amount), 2) AS average_order_value
    FROM
        `{PROJECT_ID}.{DATASET_ID}.raw_orders`
    GROUP BY
        order_date;
    """
    job = client.query(query)
    job.result()
