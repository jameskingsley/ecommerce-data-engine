import pytest
import duckdb
import pandas as pd

@pytest.fixture
def sample_orders_df():
    return pd.DataFrame([
        {
            "order_id": 1,
            "user_id": 10,
            "quantity": 2,
            "total_amount": 100.0,
            "order_date": "2026-10-09"
        },
        {
            "order_id": 1,
            "user_id": 10,
            "quantity": 1,
            "total_amount": 50.0,
            "order_date": "2026-10-09"
        },
        {
            "order_id": 2,
            "user_id": 20,
            "quantity": 3,
            "total_amount": 150.0,
            "order_date": "2026-10-09"
        }
    ])

def test_daily_sales_summary_aggregation(sample_orders_df):
    con = duckdb.connect(database=":memory:")
    con.register("raw_orders", sample_orders_df)

    summary_df = con.execute("""
        SELECT
            order_date,
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT user_id) AS unique_customers,
            SUM(quantity) AS total_units_sold,
            ROUND(SUM(total_amount), 2) AS gross_revenue,
            ROUND(AVG(total_amount), 2) AS average_order_value
        FROM raw_orders
        GROUP BY order_date
    """).df()

    assert len(summary_df) == 1
    assert summary_df["total_orders"].iloc[0] == 2
    assert summary_df["unique_customers"].iloc[0] == 2
    assert summary_df["total_units_sold"].iloc[0] == 6
    assert summary_df["gross_revenue"].iloc[0] == 300.0
    con.close()
