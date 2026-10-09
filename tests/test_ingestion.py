import pytest
import pandas as pd
from datetime import datetime

MOCK_CARTS = [
    {
        "id": 1,
        "userId": 42,
        "products": [
            {
                "id": 101,
                "title": "Test Product",
                "price": 50.0,
                "quantity": 2,
                "total": 100.0,
                "discountPercentage": 10.0,
                "discountedPrice": 45.0
            }
        ]
    }
]

def flatten_cart_payload(carts):
    records = []
    for cart in carts:
        order_id = cart.get("id")
        user_id = cart.get("userId")
        for item in cart.get("products", []):
            unit_price = float(item.get("price", 0.0))
            quantity = int(item.get("quantity", 0))
            discount_pct = float(item.get("discountPercentage", 0.0))
            discounted_price = float(
                item.get("discountedPrice", round(unit_price * (1 - discount_pct / 100), 2))
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
                "order_date": datetime.now().strftime("%Y-%m-%d"),
                "ingested_at": datetime.now().isoformat()
            })
    return pd.DataFrame(records)

def test_flatten_cart_payload_structure():
    df = flatten_cart_payload(MOCK_CARTS)
    
    assert len(df) == 1
    assert df["order_id"].iloc[0] == 1
    assert df["user_id"].iloc[0] == 42
    assert df["product_id"].iloc[0] == 101
    assert df["total_amount"].iloc[0] == 100.0
    assert df["discounted_price"].iloc[0] == 45.0

def test_flatten_cart_payload_non_null_keys():
    df = flatten_cart_payload(MOCK_CARTS)
    assert not df[["order_id", "user_id", "product_id"]].isnull().any().any()
