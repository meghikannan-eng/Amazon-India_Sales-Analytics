"""
quality_report.py
-----------------
Generates a side-by-side data-quality report (before vs after cleaning):
missing values, dtype changes, value distributions for the columns we
cleaned. Output goes to reports/quality_report.txt.
"""

from __future__ import annotations

import io
import os
from typing import Optional

import pandas as pd


def _section(title: str) -> str:
    line = "═" * 70
    return f"\n{line}\n  {title}\n{line}\n"


def _missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    miss = df.isna().sum()
    pct = (miss / len(df) * 100).round(2)
    return pd.DataFrame({"missing": miss, "missing_pct": pct})


def generate_report(
    before: pd.DataFrame,
    after: pd.DataFrame,
    out_path: str = "reports/quality_report.txt",
) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    buf = io.StringIO()

    buf.write(_section("DATA QUALITY REPORT — Amazon India Cleaning Pipeline"))
    buf.write(f"Rows BEFORE: {len(before):,}\nRows AFTER:  {len(after):,}\n")
    buf.write(f"Cols BEFORE: {len(before.columns)}\nCols AFTER:  {len(after.columns)}\n")

    buf.write(_section("1. Missing values — BEFORE"))
    buf.write(_missing_summary(before).to_string())
    buf.write(_section("2. Missing values — AFTER"))
    buf.write(_missing_summary(after).to_string())

    buf.write(_section("3. Dtype comparison"))
    dtypes = pd.DataFrame({
        "before": before.dtypes.astype(str),
        "after": after.dtypes.reindex(before.columns).astype(str),
    })
    buf.write(dtypes.to_string())

    cols_to_describe = [
        "order_date", "original_price_inr", "customer_rating", "customer_city",
        "is_prime_member", "is_festival_sale", "category", "delivery_days",
        "payment_method",
    ]

    buf.write(_section("4. Value samples — AFTER cleaning"))
    for c in cols_to_describe:
        if c in after.columns:
            buf.write(f"\n--- {c} ---\n")
            try:
                vc = after[c].value_counts(dropna=False).head(15)
                buf.write(vc.to_string())
                buf.write("\n")
            except TypeError:
                buf.write(f"{after[c].describe()}\n")

    if "is_bulk_order" in after.columns:
        buf.write(_section("5. Bulk-order flags from Q8"))
        buf.write(after["is_bulk_order"].value_counts(dropna=False).to_string())
        buf.write("\n")

    if "price_was_corrected" in after.columns:
        buf.write(_section("6. Decimal-error corrections from Q9"))
        buf.write(after["price_was_corrected"].value_counts(dropna=False).to_string())
        buf.write("\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    print(f"✅ Wrote quality report → {out_path}")
