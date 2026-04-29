"""
data_cleaning.py
----------------
Implements the 10 data-cleaning challenges for the Amazon India project.

Each cleaning question is its own function so you can run them
independently, swap them out, or unit-test them.

Pipeline order (when calling clean_all):
    Q1  fix_dates
    Q2  fix_prices
    Q3  fix_ratings
    Q4  fix_cities
    Q5  fix_booleans
    Q6  fix_categories
    Q7  fix_delivery_days
    Q8  handle_duplicates
    Q9  fix_price_outliers
    Q10 fix_payment_methods
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

import numpy as np
import pandas as pd


# ────────────────────────────────────────────────────────────────────────────
# Q1 — Standardise order_date to YYYY-MM-DD
# ────────────────────────────────────────────────────────────────────────────
def fix_dates(df: pd.DataFrame, col: str = "order_date") -> pd.DataFrame:
    """Convert mixed-format dates ('DD/MM/YYYY', 'DD-MM-YY', 'YYYY-MM-DD',
    invalid like '32/13/2020') into a clean YYYY-MM-DD string column.

    Strategy:
      1. Try pandas' flexible parser with dayfirst=True (handles DD/MM/YYYY).
      2. Fall back to ISO parser for YYYY-MM-DD that didn't fit.
      3. Anything still NaT is invalid → drop or NaN (we keep NaN here so
         downstream code can decide what to do).
    """
    if col not in df.columns:
        return df

    raw = df[col].astype(str).str.strip()

    parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True)

    # For rows that failed dayfirst (e.g. ISO YYYY-MM-DD), retry with dayfirst=False
    mask_retry = parsed.isna() & raw.notna() & (raw != "nan")
    if mask_retry.any():
        retry = pd.to_datetime(raw[mask_retry], errors="coerce", dayfirst=False)
        parsed.loc[mask_retry] = retry

    df = df.copy()
    df[col] = parsed.dt.strftime("%Y-%m-%d")
    # Add a typed datetime column for downstream time-based analysis
    df[f"{col}_dt"] = parsed
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q2 — Clean original_price_inr to a numeric INR column
# ────────────────────────────────────────────────────────────────────────────
def fix_prices(df: pd.DataFrame, col: str = "original_price_inr") -> pd.DataFrame:
    """Remove ₹ symbols, comma separators, words like 'Price on Request',
    and coerce to float.
    """
    if col not in df.columns:
        return df

    df = df.copy()
    s = df[col].astype(str).str.strip()

    # Replace text-only entries with NaN
    text_mask = s.str.contains(r"[A-Za-z]", regex=True, na=False)
    s = s.where(~text_mask, np.nan)

    # Strip ₹, commas, whitespace; keep digits + decimal
    s = (
        s.str.replace("₹", "", regex=False)
         .str.replace("Rs.", "", regex=False)
         .str.replace("Rs", "", regex=False)
         .str.replace(",", "", regex=False)
         .str.strip()
    )

    df[col] = pd.to_numeric(s, errors="coerce")
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q3 — Standardise customer_rating to 1.0–5.0 numeric
# ────────────────────────────────────────────────────────────────────────────
def fix_ratings(df: pd.DataFrame, col: str = "customer_rating") -> pd.DataFrame:
    """Convert '5.0', '4 stars', '3/5', '2.5/5.0' → numeric 1.0–5.0."""
    if col not in df.columns:
        return df

    df = df.copy()
    s = df[col].astype(str).str.strip().str.lower()

    def _parse(val: str) -> float:
        if val in ("", "nan", "none"):
            return np.nan
        # Forms like '3/5' or '2.5/5.0'
        m = re.match(r"^([\d.]+)\s*/\s*([\d.]+)$", val)
        if m:
            num = float(m.group(1))
            denom = float(m.group(2))
            if denom == 5:
                return round(num, 2)
            if denom == 10:
                return round(num / 2.0, 2)
            # Otherwise scale to 5
            return round((num / denom) * 5.0, 2)
        # Forms like '4 stars', '5.0'
        m = re.match(r"^([\d.]+)", val)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return np.nan
        return np.nan

    parsed = s.map(_parse)
    # Clip to [1.0, 5.0]
    parsed = parsed.where((parsed >= 1.0) & (parsed <= 5.0), np.nan)
    df[col] = parsed
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Standardise customer_city
# ────────────────────────────────────────────────────────────────────────────
CITY_ALIASES = {
    "bangalore": "Bengaluru",
    "bengaluru": "Bengaluru",
    "bombay": "Mumbai",
    "mumbai": "Mumbai",
    "new delhi": "Delhi",
    "delhi": "Delhi",
    "calcutta": "Kolkata",
    "kolkata": "Kolkata",
    "madras": "Chennai",
    "chennai": "Chennai",
    "gurgaon": "Gurugram",
    "gurugram": "Gurugram",
    "hyderabad": "Hyderabad",
    "hydrabad": "Hyderabad",
    "pune": "Pune",
    "ahmedabad": "Ahmedabad",
    "ahemdabad": "Ahmedabad",
    "noida": "Noida",
    "jaipur": "Jaipur",
    "lucknow": "Lucknow",
    "kanpur": "Kanpur",
    "nagpur": "Nagpur",
    "indore": "Indore",
    "bhopal": "Bhopal",
    "patna": "Patna",
    "vadodara": "Vadodara",
    "baroda": "Vadodara",
    "ludhiana": "Ludhiana",
    "agra": "Agra",
    "nashik": "Nashik",
    "faridabad": "Faridabad",
    "meerut": "Meerut",
    "rajkot": "Rajkot",
    "varanasi": "Varanasi",
    "amritsar": "Amritsar",
    "allahabad": "Prayagraj",
    "prayagraj": "Prayagraj",
    "kochi": "Kochi",
    "cochin": "Kochi",
    "trivandrum": "Thiruvananthapuram",
    "thiruvananthapuram": "Thiruvananthapuram",
    "visakhapatnam": "Visakhapatnam",
    "vizag": "Visakhapatnam",
    "coimbatore": "Coimbatore",
    "thane": "Thane",
    "surat": "Surat",
    "guwahati": "Guwahati",
    "chandigarh": "Chandigarh",
}


def fix_cities(df: pd.DataFrame, col: str = "customer_city") -> pd.DataFrame:
    """Normalise city names: title-case, strip, expand aliases like
    'Bangalore'→'Bengaluru', 'Bombay'→'Mumbai'.
    """
    if col not in df.columns:
        return df

    df = df.copy()
    s = (
        df[col]
        .astype(str)
        .str.strip()
        .str.lower()
        # Take the first city if a slash-joined pair appears: 'Bangalore/Bengaluru'
        .str.split(r"[/|]").str[0]
        .str.strip()
    )

    df[col] = s.map(lambda x: CITY_ALIASES.get(x, x.title() if x not in ("nan", "") else np.nan))
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q5 — Boolean columns to consistent True/False
# ────────────────────────────────────────────────────────────────────────────
BOOL_TRUE = {"true", "t", "yes", "y", "1", "1.0"}
BOOL_FALSE = {"false", "f", "no", "n", "0", "0.0"}


def fix_booleans(
    df: pd.DataFrame,
    cols: Iterable[str] = ("is_prime_member", "is_prime_eligible", "is_festival_sale"),
) -> pd.DataFrame:
    """Convert mixed boolean values (Y/N, 1/0, True/False, Yes/No) to
    pandas nullable boolean (True/False/NA).
    """
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.strip().str.lower()
        out = pd.Series(pd.NA, index=s.index, dtype="boolean")
        out[s.isin(BOOL_TRUE)] = True
        out[s.isin(BOOL_FALSE)] = False
        df[col] = out
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q6 — Standardise product categories
# ────────────────────────────────────────────────────────────────────────────
CATEGORY_ALIASES = {
    # Electronics + common typos
    "electronics": "Electronics",
    "electronic": "Electronics",
    "electronicss": "Electronics",
    "electronnics": "Electronics",
    "electronicz": "Electronics",
    "electronics & accessories": "Electronics",
    "electronics and accessories": "Electronics",
    # Fashion
    "fashion": "Fashion",
    "fashions": "Fashion",
    "clothing": "Fashion",
    "clothes": "Fashion",
    "apparel": "Fashion",
    "apparels": "Fashion",
    # Home & Kitchen
    "home & kitchen": "Home & Kitchen",
    "home and kitchen": "Home & Kitchen",
    "home": "Home & Kitchen",
    "kitchen": "Home & Kitchen",
    "homekitchen": "Home & Kitchen",
    # Books
    "books": "Books",
    "book": "Books",
    "bookss": "Books",
    # Beauty
    "beauty": "Beauty & Personal Care",
    "beauty & personal care": "Beauty & Personal Care",
    "beauty and personal care": "Beauty & Personal Care",
    "personal care": "Beauty & Personal Care",
    # Sports
    "sports": "Sports & Fitness",
    "sports & fitness": "Sports & Fitness",
    "sports and fitness": "Sports & Fitness",
    "fitness": "Sports & Fitness",
    "sport": "Sports & Fitness",
    # Toys
    "toys": "Toys & Games",
    "toys & games": "Toys & Games",
    "toys and games": "Toys & Games",
    "games": "Toys & Games",
    "toy": "Toys & Games",
    # Grocery
    "grocery": "Grocery",
    "groceries": "Grocery",
    "food": "Grocery",
    "food & beverages": "Grocery",
    # Automotive
    "automotive": "Automotive",
    "auto": "Automotive",
    "automobile": "Automotive",
}


def fix_categories(df: pd.DataFrame, col: str = "category") -> pd.DataFrame:
    """Map every category variant to a canonical name."""
    if col not in df.columns:
        return df

    df = df.copy()
    s = df[col].astype(str).str.strip().str.lower()
    df[col] = s.map(lambda x: CATEGORY_ALIASES.get(x, x.title() if x and x != "nan" else np.nan))
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q7 — Clean delivery_days
# ────────────────────────────────────────────────────────────────────────────
def fix_delivery_days(df: pd.DataFrame, col: str = "delivery_days") -> pd.DataFrame:
    """Convert text values ('Same Day', '1-2 days') to numeric, drop
    negatives, cap unrealistic values (>30 days) at NaN.
    """
    if col not in df.columns:
        return df

    df = df.copy()
    s = df[col].astype(str).str.strip().str.lower()

    def _parse(val: str) -> float:
        if val in ("", "nan", "none"):
            return np.nan
        if "same day" in val or val == "0":
            return 0.0
        # '1-2 days' → average
        m = re.match(r"^(\d+)\s*-\s*(\d+)", val)
        if m:
            return (int(m.group(1)) + int(m.group(2))) / 2.0
        # plain number, possibly followed by 'days'
        m = re.match(r"^(-?\d+(?:\.\d+)?)", val)
        if m:
            return float(m.group(1))
        return np.nan

    parsed = s.map(_parse)
    # Negative or unrealistic (>30) → NaN
    parsed = parsed.where((parsed >= 0) & (parsed <= 30), np.nan)
    df[col] = parsed
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q8 — Duplicate handling
# ────────────────────────────────────────────────────────────────────────────
def handle_duplicates(
    df: pd.DataFrame,
    key_cols: Optional[Iterable[str]] = None,
    bulk_threshold: int = 5,
) -> pd.DataFrame:
    """Distinguish bulk-order duplicates from data-error duplicates.

    Strategy:
      • Group by (customer_id, product_id, order_date, final_amount_inr).
      • If the group has <= bulk_threshold rows → treat as legitimate bulk
        order, keep all rows but tag them with `is_bulk_order = True` and
        add `bulk_qty`.
      • If the group has > bulk_threshold rows → likely a data error;
        keep only the FIRST occurrence and drop the rest.
    """
    if key_cols is None:
        key_cols = ["customer_id", "product_id", "order_date", "final_amount_inr"]

    have = [c for c in key_cols if c in df.columns]
    if not have:
        return df

    df = df.copy()
    grp = df.groupby(have, dropna=False)
    counts = grp[have[0]].transform("count")

    df["is_bulk_order"] = counts > 1
    df["bulk_qty"] = counts.where(counts > 1, 1)

    likely_error = counts > bulk_threshold
    df = df.loc[~likely_error | ~df.duplicated(subset=have, keep="first")]
    return df.reset_index(drop=True)


# ────────────────────────────────────────────────────────────────────────────
# Q9 — Price outlier detection (decimal-point errors)
# ────────────────────────────────────────────────────────────────────────────
def fix_price_outliers(
    df: pd.DataFrame,
    price_col: str = "original_price_inr",
    group_col: str = "category",
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Detect prices that are outliers within their category and try to
    auto-correct decimal-point errors (price 100x higher than the category
    median).

    Strategy per category group:
      1. Compute median + MAD-based z-score on log(price).
      2. Rows with |z| > threshold AND price > 50× median → divide by 100.
      3. Rows with |z| > threshold AND price < median/50 → multiply by 100.
      4. Anything still extreme → flag in `price_was_corrected` for review.
    """
    if price_col not in df.columns:
        return df

    df = df.copy()
    df["price_was_corrected"] = False

    if group_col not in df.columns:
        # Fall back to whole-dataset stats
        df["_grp"] = "all"
        group_col = "_grp"

    for grp_val, grp in df.groupby(group_col, dropna=False):
        prices = grp[price_col].dropna()
        if len(prices) < 10:
            continue
        median = prices.median()
        # Decimal error: price > 50× median → divide by 100
        too_high = grp[grp[price_col] > 50 * median].index
        df.loc[too_high, price_col] = df.loc[too_high, price_col] / 100.0
        df.loc[too_high, "price_was_corrected"] = True
        # Decimal error: price < median/50 (could be x100 too small)
        if median > 0:
            too_low = grp[
                (grp[price_col] > 0) & (grp[price_col] < median / 50.0)
            ].index
            df.loc[too_low, price_col] = df.loc[too_low, price_col] * 100.0
            df.loc[too_low, "price_was_corrected"] = True

    if "_grp" in df.columns:
        df = df.drop(columns="_grp")
    return df


# ────────────────────────────────────────────────────────────────────────────
# Q10 — Standardise payment_method
# ────────────────────────────────────────────────────────────────────────────
PAYMENT_ALIASES = {
    # UPI family
    "upi": "UPI",
    "phonepe": "UPI",
    "googlepay": "UPI",
    "google pay": "UPI",
    "gpay": "UPI",
    "paytm": "UPI",
    "bhim": "UPI",
    # Card family
    "credit card": "Credit Card",
    "credit_card": "Credit Card",
    "cc": "Credit Card",
    "creditcard": "Credit Card",
    "debit card": "Debit Card",
    "debit_card": "Debit Card",
    "dc": "Debit Card",
    "debitcard": "Debit Card",
    # COD family
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "c.o.d": "Cash on Delivery",
    "c.o.d.": "Cash on Delivery",
    # Net banking
    "net banking": "Net Banking",
    "netbanking": "Net Banking",
    "internet banking": "Net Banking",
    "nb": "Net Banking",
    # EMI / wallets
    "emi": "EMI",
    "wallet": "Wallet",
    "amazon pay": "Wallet",
    "amazonpay": "Wallet",
}


def fix_payment_methods(df: pd.DataFrame, col: str = "payment_method") -> pd.DataFrame:
    """Map every payment-method variant to a canonical category."""
    if col not in df.columns:
        return df

    df = df.copy()
    s = (
        df[col]
        .astype(str)
        .str.strip()
        .str.lower()
        .str.split(r"[/|]").str[0]
        .str.strip()
    )
    df[col] = s.map(
        lambda x: PAYMENT_ALIASES.get(x, x.title() if x and x != "nan" else np.nan)
    )
    return df


# ────────────────────────────────────────────────────────────────────────────
# Master pipeline
# ────────────────────────────────────────────────────────────────────────────
def clean_all(df: pd.DataFrame) -> pd.DataFrame:
    """Run every cleaning step in order."""
    print("Q1  Cleaning dates…")
    df = fix_dates(df)
    print("Q2  Cleaning prices…")
    df = fix_prices(df)
    print("Q3  Cleaning ratings…")
    df = fix_ratings(df)
    print("Q4  Cleaning cities…")
    df = fix_cities(df)
    print("Q5  Cleaning booleans…")
    df = fix_booleans(df)
    print("Q6  Cleaning categories…")
    df = fix_categories(df)
    print("Q7  Cleaning delivery days…")
    df = fix_delivery_days(df)
    print("Q8  Handling duplicates…")
    df = handle_duplicates(df)
    print("Q9  Fixing price outliers…")
    df = fix_price_outliers(df)
    print("Q10 Cleaning payment methods…")
    df = fix_payment_methods(df)
    print(f"\n✅ Done. Final shape: {df.shape}")
    return df
