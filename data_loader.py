"""
data_loader.py
--------------
Reads the Amazon India year-wise transaction CSVs (2015-2025) and the
product catalog, concatenates them into a single DataFrame, and joins
with the catalog to produce a transactions-with-product-info DataFrame.

Usage:
    from data_loader import load_raw_data
    df, catalog = load_raw_data("data/raw")
"""

from __future__ import annotations

import glob
import os
from typing import Tuple

import pandas as pd
from tqdm import tqdm


# Project root (one level up from this src/ folder)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_path(path: str) -> str:
    """If a relative path is given, resolve it against the project root
    so the script works no matter which folder you run it from.
    """
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


def load_year_files(raw_dir: str) -> pd.DataFrame:
    """Load every amazon_india_<year>.csv in raw_dir and concat them."""
    raw_dir = _resolve_path(raw_dir)
    pattern = os.path.join(raw_dir, "amazon_india_*.csv")
    files = sorted(
        f for f in glob.glob(pattern)
        if "products_catalog" not in os.path.basename(f).lower()
    )
    if not files:
        raise FileNotFoundError(
            f"No year-wise transaction CSVs found in {raw_dir}.\n"
            f"Expected files like amazon_india_2015.csv ... amazon_india_2025.csv\n"
            f"Files actually present: {os.listdir(raw_dir) if os.path.isdir(raw_dir) else '(folder does not exist)'}"
        )

    frames = []
    for path in tqdm(files, desc="Loading year files"):
        try:
            df = pd.read_csv(path, low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, low_memory=False, encoding="latin-1")
        # Tag the source year (handy if order_date is messy)
        year_tag = os.path.basename(path).replace("amazon_india_", "").replace(".csv", "")
        df["_source_year"] = year_tag
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def load_catalog(raw_dir: str) -> pd.DataFrame:
    """Load the product catalog CSV."""
    raw_dir = _resolve_path(raw_dir)
    path = os.path.join(raw_dir, "amazon_india_products_catalog.csv")
    if not os.path.exists(path):
        # Try a case-insensitive fallback
        candidates = glob.glob(os.path.join(raw_dir, "*products_catalog*.csv"))
        if not candidates:
            raise FileNotFoundError(
                f"Could not find amazon_india_products_catalog.csv in {raw_dir}"
            )
        path = candidates[0]

    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")


def load_raw_data(raw_dir: str = "data/raw") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        transactions_df, catalog_df
    """
    transactions = load_year_files(raw_dir)
    catalog = load_catalog(raw_dir)
    print(f"Loaded {len(transactions):,} transactions and {len(catalog):,} catalog rows")
    return transactions, catalog


if __name__ == "__main__":
    tx, cat = load_raw_data("data/raw")
    print("\nTransactions sample:")
    print(tx.head())
    print("\nCatalog sample:")
    print(cat.head())
