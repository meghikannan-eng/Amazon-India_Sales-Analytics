"""
run_eda.py
----------
End-to-end EDA runner. Reads the cleaned dataset and produces 20 PNG
plots in reports/figures/.

Usage from the project root:
    python src/run_eda.py
"""

from __future__ import annotations

import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from data_loader import PROJECT_ROOT
from eda import run_all


CLEANED_CSV = os.path.join(PROJECT_ROOT, "data", "cleaned", "amazon_india_cleaned.csv")
CLEANED_PARQUET = os.path.join(PROJECT_ROOT, "data", "cleaned", "amazon_india_cleaned.parquet")


def main() -> None:
    if os.path.exists(CLEANED_PARQUET):
        print(f"Loading {CLEANED_PARQUET}…")
        df = pd.read_parquet(CLEANED_PARQUET)
    elif os.path.exists(CLEANED_CSV):
        print(f"Loading {CLEANED_CSV}…")
        df = pd.read_csv(CLEANED_CSV, low_memory=False, parse_dates=["order_date_dt"])
    else:
        raise FileNotFoundError(
            f"Cleaned dataset not found at:\n  {CLEANED_CSV}\n"
            f"Run `python src/run_cleaning.py` first to generate it."
        )

    print(f"Loaded {len(df):,} rows × {len(df.columns)} cols")
    run_all(df)


if __name__ == "__main__":
    main()
