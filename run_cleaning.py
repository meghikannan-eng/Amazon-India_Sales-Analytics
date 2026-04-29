"""
run_cleaning.py
---------------
End-to-end cleaning runner. Reads raw CSVs, cleans them, joins with the
catalog, writes the cleaned dataset, and produces a quality report.

Usage from the project root:
    python src/run_cleaning.py
"""

from __future__ import annotations

import os
import sys

import pandas as pd

# Ensure local imports work regardless of where the script is invoked from
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from data_loader import load_raw_data, PROJECT_ROOT
from data_cleaning import clean_all
from quality_report import generate_report


RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
OUT_DIR = os.path.join(PROJECT_ROOT, "data", "cleaned")
OUT_CSV = os.path.join(OUT_DIR, "amazon_india_cleaned.csv")
OUT_PARQUET = os.path.join(OUT_DIR, "amazon_india_cleaned.parquet")


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    transactions, catalog = load_raw_data(RAW_DIR)

    # Snapshot for the quality report
    before = transactions.copy()

    cleaned = clean_all(transactions)

    # Join with catalog (for category/brand etc. on transactions that
    # don't already carry them)
    if "product_id" in cleaned.columns and "product_id" in catalog.columns:
        catalog_cols = [c for c in catalog.columns if c not in cleaned.columns or c == "product_id"]
        cleaned = cleaned.merge(catalog[catalog_cols], on="product_id", how="left", suffixes=("", "_cat"))

    # Persist
    cleaned.to_csv(OUT_CSV, index=False)
    try:
        cleaned.to_parquet(OUT_PARQUET, index=False)
    except Exception as e:  # pyarrow/fastparquet may not be installed
        print(f"(skipping parquet: {e})")
    print(f"✅ Wrote cleaned dataset → {OUT_CSV}")

    # Quality report
    report_path = os.path.join(PROJECT_ROOT, "reports", "quality_report.txt")
    generate_report(before, cleaned, out_path=report_path)


if __name__ == "__main__":
    main()
