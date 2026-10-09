import duckdb
import pandas as pd
import requests
import certifi
import urllib3
from datetime import datetime

# Suppressing SSL warnings if falling back to unverified HTTPS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def extract_api_orders():
    """Extract transactional cart payloads from the public e-commerce API."""
    url = "https://dummyjson.com/carts"
    print(f"Fetching data from platform API: {url}...")
    
    try:
        response = requests.get(url, timeout=10, verify=certifi.where())
    except requests.exceptions.SSLError:
        print("SSL verification failed. Falling back to verify=False for local dev...")
        response = requests.get(url, timeout=10, verify=False)
        
    response.raise_for_status()
    data = response.json()
    
    return data.get("carts", [])

def transform_and_verify_duckdb(carts):
    flattened_records = []
    
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

            flattened_records.append({
                "order_id": int(order_id),
                "user_id": int(user_id),
                "product_id": int(item.get("id", 0)),
                "product_title": str(item.get("title", "")),
                "unit_price": unit_price,
                "quantity": quantity,
                "total_amount": float(item.get("total", unit_price * quantity)),
                "discount_percentage": discount_pct,
                "discounted_price": discounted_price,
                "order_date": datetime.now().strftime("%Y-%m-%d"),
                "ingested_at": datetime.now().isoformat()
            })

    df = pd.DataFrame(flattened_records)

    # Initializing DuckDB In-Memory Session
    con = duckdb.connect(database=":memory:")
    con.register("stg_raw_orders", df)

    print("\nDuckDB Schema Inspection")
    con.sql("DESCRIBE stg_raw_orders").show()

    # Data Quality Checks (Null key validation & record count)
    print("Data Quality Verification")
    null_keys = con.execute("""
        SELECT COUNT(*) 
        FROM stg_raw_orders 
        WHERE order_id IS NULL OR user_id IS NULL OR product_id IS NULL
    """).fetchone()[0]
    
    total_records = len(df)
    print(f"Total Extracted Line-Items: {total_records}")
    print(f"Null Primary Key Records: {null_keys}")
    
    assert null_keys == 0, "Data Integrity Check Failed: Primary keys contain null values!"

    # Aggregation Logic Verification (Simulating Silver Layer transformation)
    print("\nDuckDB Aggregate Calculations (Silver Layer Preview)")
    silver_preview = con.execute("""
        SELECT 
            order_date,
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT user_id) AS total_users,
            SUM(quantity) AS total_units_sold,
            ROUND(SUM(total_amount), 2) AS gross_revenue,
            ROUND(AVG(total_amount), 2) AS average_order_value
        FROM stg_raw_orders
        GROUP BY order_date
    """).df()

    print(silver_preview.to_string(index=False))
    
    con.close()
    return df

if __name__ == "__main__":
    raw_carts = extract_api_orders()
    processed_df = transform_and_verify_duckdb(raw_carts)
    print("\nLocal DuckDB verification completed successfully.")
