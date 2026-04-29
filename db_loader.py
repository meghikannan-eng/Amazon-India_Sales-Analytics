"""
db_loader.py
------------
Loads the cleaned Amazon India dataset into a MySQL star-schema:

    dim_products  ──┐
    dim_customers ──┼──► fact_transactions
    dim_time      ──┘

Steps performed by load_to_mysql():
    1. Read cleaned CSV/parquet
    2. Build the 3 dimension DataFrames (deduplicated)
    3. Build the fact DataFrame (with foreign keys + date_id)
    4. Bulk-insert each into MySQL with chunking
"""

from __future__ import annotations

import os
from typing import Dict, Tuple
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm


# ────────────────────────────────────────────────────────────────────────────
# Engine
# ────────────────────────────────────────────────────────────────────────────
def get_engine(
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 3306,
    database: str = "amazon_india_db",
):
    """Return a SQLAlchemy engine bound to the warehouse database.

    Username and password are URL-encoded so passwords containing special
    characters like @, :, /, #, ?, % work correctly.
    """
    safe_user = quote_plus(user)
    safe_pwd = quote_plus(password)
    url = f"mysql+mysqlconnector://{safe_user}:{safe_pwd}@{host}:{port}/{database}"
    return create_engine(url, pool_pre_ping=True, future=True)


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────
CITY_TIER = {
    # Metro
    "Mumbai": "Metro", "Delhi": "Metro", "Bengaluru": "Metro",
    "Chennai": "Metro", "Kolkata": "Metro", "Hyderabad": "Metro",
    # Tier 1
    "Pune": "Tier 1", "Ahmedabad": "Tier 1", "Jaipur": "Tier 1",
    "Surat": "Tier 1", "Lucknow": "Tier 1", "Kanpur": "Tier 1",
    "Nagpur": "Tier 1", "Indore": "Tier 1", "Bhopal": "Tier 1",
    "Gurugram": "Tier 1", "Noida": "Tier 1", "Faridabad": "Tier 1",
    "Visakhapatnam": "Tier 1", "Coimbatore": "Tier 1", "Kochi": "Tier 1",
    "Thane": "Tier 1", "Vadodara": "Tier 1", "Ludhiana": "Tier 1",
    "Chandigarh": "Tier 1",
}


def _to_tinyint(s: pd.Series) -> pd.Series:
    """Convert a boolean-ish column to MySQL TINYINT (0/1/NULL)."""
    if s.dtype == "boolean" or s.dtype == bool:
        return s.astype("Int8")
    return s.astype(str).str.lower().map(
        {"true": 1, "false": 0, "yes": 1, "no": 0, "1": 1, "0": 0}
    ).astype("Int8")


def build_dim_time(min_date: str = "2015-01-01", max_date: str = "2025-12-31") -> pd.DataFrame:
    """Generate a calendar dimension table for the project window."""
    rng = pd.date_range(min_date, max_date, freq="D")
    df = pd.DataFrame({"full_date": rng})
    df["date_id"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year.astype("int16")
    df["quarter"] = df["full_date"].dt.quarter.astype("int8")
    df["month"] = df["full_date"].dt.month.astype("int8")
    df["month_name"] = df["full_date"].dt.month_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype("int8")
    df["day"] = df["full_date"].dt.day.astype("int8")
    df["day_of_week"] = df["full_date"].dt.day_name()
    df["is_weekend"] = df["full_date"].dt.dayofweek.isin([5, 6]).astype("int8")
    return df[[
        "date_id", "full_date", "year", "quarter", "month", "month_name",
        "week_of_year", "day", "day_of_week", "is_weekend",
    ]]


def build_dim_products(df: pd.DataFrame) -> pd.DataFrame:
    """One row per product_id, taking the most recent / non-null values."""
    cols = [
        "product_id", "product_name", "category", "subcategory", "brand",
        "base_price_2015", "weight_kg", "product_rating",
        "is_prime_eligible", "launch_year", "model",
    ]
    keep = [c for c in cols if c in df.columns]
    out = (df[keep].drop_duplicates(subset="product_id", keep="last").reset_index(drop=True))
    if "is_prime_eligible" in out.columns:
        out["is_prime_eligible"] = _to_tinyint(out["is_prime_eligible"])
    return out


def build_dim_customers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per customer with first/last purchase dates."""
    df = df.copy()
    if "order_date_dt" in df.columns:
        df["_dt"] = pd.to_datetime(df["order_date_dt"], errors="coerce")
    else:
        df["_dt"] = pd.to_datetime(df["order_date"], errors="coerce")

    grp = df.groupby("customer_id")
    cust = grp.agg(
        customer_city=("customer_city", "last") if "customer_city" in df.columns else ("customer_id", "size"),
        customer_state=("customer_state", "last") if "customer_state" in df.columns else ("customer_id", "size"),
        age_group=("age_group", "last") if "age_group" in df.columns else ("customer_id", "size"),
        is_prime_member=("is_prime_member", "max") if "is_prime_member" in df.columns else ("customer_id", "size"),
        customer_spending_tier=("customer_spending_tier", "last") if "customer_spending_tier" in df.columns else ("customer_id", "size"),
        first_purchase_date=("_dt", "min"),
        last_purchase_date=("_dt", "max"),
    ).reset_index()

    if "customer_city" in cust.columns:
        cust["city_tier"] = cust["customer_city"].map(CITY_TIER).fillna("Tier 2/Rural")
    if "is_prime_member" in cust.columns:
        cust["is_prime_member"] = _to_tinyint(cust["is_prime_member"])

    cust["first_purchase_date"] = cust["first_purchase_date"].dt.date
    cust["last_purchase_date"] = cust["last_purchase_date"].dt.date
    return cust


def build_fact_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Project the cleaned df down to the fact table columns."""
    df = df.copy()
    if "order_date_dt" in df.columns:
        df["_dt"] = pd.to_datetime(df["order_date_dt"], errors="coerce")
    else:
        df["_dt"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["_dt", "transaction_id", "customer_id", "product_id"])

    fact = pd.DataFrame({
        "transaction_id":    df["transaction_id"].astype(str),
        "customer_id":       df["customer_id"].astype(str),
        "product_id":        df["product_id"].astype(str),
        "date_id":           df["_dt"].dt.strftime("%Y%m%d").astype(int),
        "order_date":        df["_dt"].dt.date,
        "payment_method":    df.get("payment_method"),
        "original_price_inr":df.get("original_price_inr"),
        "discount_percent":  df.get("discount_percent"),
        "final_amount_inr":  df.get("final_amount_inr"),
        "delivery_charges":  df.get("delivery_charges"),
        "delivery_days":     df.get("delivery_days"),
        "return_status":     df.get("return_status"),
        "customer_rating":   df.get("customer_rating"),
        "is_prime_member":   _to_tinyint(df["is_prime_member"]) if "is_prime_member" in df.columns else None,
        "is_festival_sale":  _to_tinyint(df["is_festival_sale"]) if "is_festival_sale" in df.columns else None,
        "festival_name":     df.get("festival_name"),
        "is_bulk_order":     _to_tinyint(df["is_bulk_order"]) if "is_bulk_order" in df.columns else 0,
        "bulk_qty":          df.get("bulk_qty", 1),
    })
    return fact.drop_duplicates(subset="transaction_id", keep="first")


# ────────────────────────────────────────────────────────────────────────────
# Loader
# ────────────────────────────────────────────────────────────────────────────
def _bulk_insert(df: pd.DataFrame, table: str, engine, chunksize: int = 5000) -> None:
    """Append df to `table` in chunks with a progress bar."""
    total = len(df)
    if total == 0:
        print(f"  (skipping {table}: empty)")
        return
    chunks = max(1, (total + chunksize - 1) // chunksize)
    with tqdm(total=total, desc=f"  → {table}", unit="rows") as bar:
        for start in range(0, total, chunksize):
            sub = df.iloc[start:start + chunksize]
            sub.to_sql(table, engine, if_exists="append", index=False, method="multi")
            bar.update(len(sub))


def load_to_mysql(
    cleaned_df: pd.DataFrame,
    engine,
    truncate_first: bool = True,
) -> Dict[str, int]:
    """End-to-end load: dims → fact, returns row counts loaded."""
    print("Building dimensions and fact table…")
    dim_time = build_dim_time(
        min_date=str(pd.to_datetime(cleaned_df["order_date"], errors="coerce").min().date() if "order_date" in cleaned_df.columns else "2015-01-01"),
        max_date=str(pd.to_datetime(cleaned_df["order_date"], errors="coerce").max().date() if "order_date" in cleaned_df.columns else "2025-12-31"),
    )
    dim_products = build_dim_products(cleaned_df)
    dim_customers = build_dim_customers(cleaned_df)
    fact_transactions = build_fact_transactions(cleaned_df)

    if truncate_first:
        with engine.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            for tbl in ("fact_transactions", "dim_customers", "dim_products", "dim_time"):
                conn.execute(text(f"TRUNCATE TABLE {tbl}"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        print("  Truncated existing tables.")

    print("Loading rows…")
    _bulk_insert(dim_time, "dim_time", engine)
    _bulk_insert(dim_products, "dim_products", engine)
    _bulk_insert(dim_customers, "dim_customers", engine)
    _bulk_insert(fact_transactions, "fact_transactions", engine)

    counts = {
        "dim_time": len(dim_time),
        "dim_products": len(dim_products),
        "dim_customers": len(dim_customers),
        "fact_transactions": len(fact_transactions),
    }
    print(f"\n✅ Load complete: {counts}")
    return counts
