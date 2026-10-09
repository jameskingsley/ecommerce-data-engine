import os
import requests
import pandas as pd
import certifi
import urllib3
from datetime import datetime
from dagster import asset, Output
from google.cloud import bigquery

# Suppressing SSL warnings for API fallback
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BQ_DATASET_ID", "ecommerce_analytics")

@asset(
    description="Ingests raw e-commerce order payloads from API into BigQuery Bronze partitioned layer.",
    group_name="bronze_layer"
)
def raw_orders_bronze() -> Output[pd.DataFrame]:
    url = "https://dummyjson.com/carts"
    try:
        response = requests.get(url, timeout=10, verify=certifi.where())
    except requests.exceptions.SSLError:
        response = requests.get(url, timeout=10, verify=False)
        
    response.raise_for_status()
    carts = response.json().get("carts", [])

    records = []
    for cart in carts:
        order_id = cart.get("id")
        user_id = cart.get("userId")
        for item in cart.get("products", []):
            unit_price = float(item.get("price", 0.0))
            quantity = int(item.get("quantity", 0))
            discount_pct = float(item.get("discountPercentage", 0.0))
            
            discounted_price = float(
                item.get("discountedPrice", 
                item.get("discountedTotal", round(unit_price * (1 - discount_pct / 100), 2)))
            )

            records.append({
                "order_id": int(order_id),
                "user_id": int(user_id),
                "product_id": int(item.get("id", 0)),
                "product_title": str(item.get("title", "")),
                "unit_price": unit_price,
                "quantity": quantity,
                "total_amount": float(item.get("total", unit_price * quantity)),
                "discount_percentage": discount_pct,
                "discounted_price": discounted_price,
                "order_date": pd.to_datetime(datetime.now().strftime("%Y-%m-%d")).date(),
                "ingested_at": pd.to_datetime(datetime.now())
            })

    df = pd.DataFrame(records)

    # BigQuery Client Load Job
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_orders"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("order_id", "INTEGER"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("product_title", "STRING"),
            bigquery.SchemaField("unit_price", "FLOAT"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("discount_percentage", "FLOAT"),
            bigquery.SchemaField("discounted_price", "FLOAT"),
            bigquery.SchemaField("order_date", "DATE"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="order_date"
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    return Output(
        df,
        metadata={
            "num_records": len(df),
            "target_table": table_id,
            "partition_field": "order_date"
        }
    )
