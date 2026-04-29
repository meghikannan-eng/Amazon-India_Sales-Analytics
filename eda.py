"""
eda.py
------
20 EDA visualizations for the Amazon India project.

Each function takes the cleaned DataFrame and saves a PNG to
reports/figures/Q##_<name>.png.

Run all 20 with run_eda.py, or call them individually.
"""

from __future__ import annotations

import os
import warnings
from typing import Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore", category=FutureWarning)

# ────────────────────────────────────────────────────────────────────────────
# Style + paths
# ────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIG_DIR = os.path.join(PROJECT_ROOT, "reports", "figures")
os.makedirs(FIG_DIR, exist_ok=True)

PALETTE = "viridis"
PRIMARY = "#FF9900"   # Amazon orange
SECONDARY = "#146EB4" # Amazon dark blue
ACCENT = "#232F3E"    # Amazon navy

sns.set_theme(style="whitegrid", context="talk", font_scale=0.85)
plt.rcParams["figure.dpi"] = 110
plt.rcParams["savefig.dpi"] = 150
plt.rcParams["axes.titleweight"] = "bold"
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["axes.labelsize"] = 11
plt.rcParams["figure.facecolor"] = "white"


def _save(fig: plt.Figure, name: str) -> str:
    """Save figure to reports/figures/<name>.png."""
    out = os.path.join(FIG_DIR, f"{name}.png")
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  ✓ saved {out}")
    return out


def _ensure_dt(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure we have a typed datetime column to work with."""
    df = df.copy()
    if "order_date_dt" in df.columns and pd.api.types.is_datetime64_any_dtype(df["order_date_dt"]):
        df["_dt"] = df["order_date_dt"]
    elif "order_date" in df.columns:
        df["_dt"] = pd.to_datetime(df["order_date"], errors="coerce")
    else:
        df["_dt"] = pd.NaT
    df["_year"] = df["_dt"].dt.year
    df["_month"] = df["_dt"].dt.month
    df["_quarter"] = df["_dt"].dt.quarter
    return df


def _amount_col(df: pd.DataFrame) -> str:
    for c in ("final_amount_inr", "final_amount", "amount", "original_price_inr"):
        if c in df.columns:
            return c
    raise KeyError("No amount column found (expected final_amount_inr or original_price_inr)")


# ────────────────────────────────────────────────────────────────────────────
# Q1 — Yearly revenue trend with growth %
# ────────────────────────────────────────────────────────────────────────────
def q1_revenue_trend(df: pd.DataFrame) -> str:
    """Yearly revenue trend 2015-2025 with YoY growth annotations."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    yearly = df.groupby("_year")[amt].sum().dropna().sort_index()
    growth = yearly.pct_change() * 100

    fig, ax = plt.subplots(figsize=(13, 6))
    bars = ax.bar(yearly.index.astype(int).astype(str), yearly.values / 1e7,
                  color=PRIMARY, edgecolor=ACCENT)
    ax.plot(yearly.index.astype(int).astype(str), yearly.values / 1e7,
            color=SECONDARY, marker="o", linewidth=2.2, label="Trend")

    for bar, g in zip(bars, growth.values):
        if not np.isnan(g):
            ax.annotate(f"{g:+.1f}%", xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        ha="center", va="bottom", fontsize=9,
                        color="green" if g >= 0 else "red")

    ax.set_title("Q1 — Amazon India Yearly Revenue Trend (2015–2025)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Revenue (₹ Crores)")
    ax.legend()
    return _save(fig, "Q01_revenue_trend")


# ────────────────────────────────────────────────────────────────────────────
# Q2 — Monthly sales heatmap (year × month)
# ────────────────────────────────────────────────────────────────────────────
def q2_seasonal_heatmap(df: pd.DataFrame) -> str:
    """Year × Month revenue heatmap — peak months and seasonal patterns."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    pivot = df.pivot_table(index="_year", columns="_month",
                            values=amt, aggfunc="sum").fillna(0) / 1e7

    fig, ax = plt.subplots(figsize=(14, 7))
    sns.heatmap(pivot, cmap="YlOrRd", annot=True, fmt=".1f",
                cbar_kws={"label": "Revenue (₹ Crores)"}, ax=ax,
                linewidths=0.5, linecolor="white")
    ax.set_title("Q2 — Monthly Sales Heatmap (Year × Month)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Year")
    ax.set_xticklabels(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    return _save(fig, "Q02_seasonal_heatmap")


# ────────────────────────────────────────────────────────────────────────────
# Q3 — RFM customer segmentation
# ────────────────────────────────────────────────────────────────────────────
def q3_rfm_segmentation(df: pd.DataFrame) -> str:
    """Recency–Frequency–Monetary scatter with quintile-based segments."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    today = df["_dt"].max()

    rfm = df.groupby("customer_id").agg(
        recency=("_dt", lambda s: (today - s.max()).days),
        frequency=("transaction_id", "count") if "transaction_id" in df.columns
        else ("customer_id", "size"),
        monetary=(amt, "sum"),
    ).reset_index()

    rfm["R"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1], duplicates="drop").astype(int)
    rfm["F"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["M"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop").astype(int)
    rfm["score"] = rfm["R"] + rfm["F"] + rfm["M"]

    def label(row):
        if row["score"] >= 13:
            return "Champions"
        if row["score"] >= 10:
            return "Loyal"
        if row["score"] >= 7:
            return "Potential"
        if row["score"] >= 5:
            return "At Risk"
        return "Lost"

    rfm["segment"] = rfm.apply(label, axis=1)

    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    sns.scatterplot(data=rfm, x="recency", y="monetary", hue="segment",
                    size="frequency", sizes=(20, 250), palette="Set2",
                    alpha=0.7, ax=axes[0])
    axes[0].set_title("Recency × Monetary by Segment")
    axes[0].set_xlabel("Recency (days)")
    axes[0].set_ylabel("Monetary (₹)")

    seg_counts = rfm["segment"].value_counts()
    axes[1].pie(seg_counts.values, labels=seg_counts.index, autopct="%1.1f%%",
                colors=sns.color_palette("Set2"), startangle=90)
    axes[1].set_title("Segment Distribution")
    fig.suptitle("Q3 — RFM Customer Segmentation", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q03_rfm_segmentation")


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Payment method evolution (stacked area)
# ────────────────────────────────────────────────────────────────────────────
def q4_payment_evolution(df: pd.DataFrame) -> str:
    """Stacked-area share of each payment method by year."""
    df = _ensure_dt(df)
    if "payment_method" not in df.columns:
        return ""
    yearly = df.groupby(["_year", "payment_method"]).size().unstack(fill_value=0)
    share = yearly.div(yearly.sum(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(13, 6))
    ax.stackplot(share.index.astype(int), share.T.values,
                 labels=share.columns, alpha=0.85,
                 colors=sns.color_palette("Set2", len(share.columns)))
    ax.set_title("Q4 — Payment Method Evolution (% Share by Year)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Share of Transactions (%)")
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    ax.set_ylim(0, 100)
    return _save(fig, "Q04_payment_evolution")


# ────────────────────────────────────────────────────────────────────────────
# Q5 — Category-wise performance (3 panels)
# ────────────────────────────────────────────────────────────────────────────
def q5_category_performance(df: pd.DataFrame) -> str:
    """Revenue contribution, growth, and market share by category."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    if "category" not in df.columns:
        return ""

    cat_rev = df.groupby("category")[amt].sum().sort_values(ascending=False)
    cat_share = cat_rev / cat_rev.sum() * 100

    early = df[df["_year"].between(2015, 2017)].groupby("category")[amt].sum()
    late = df[df["_year"].between(2023, 2025)].groupby("category")[amt].sum()
    growth = ((late - early) / early.replace(0, np.nan) * 100).dropna().sort_values(ascending=False)

    fig, axes = plt.subplots(1, 3, figsize=(20, 6))

    # Bar of revenue
    sns.barplot(x=cat_rev.values / 1e7, y=cat_rev.index, palette="viridis", ax=axes[0])
    axes[0].set_title("Revenue by Category (₹ Cr)")
    axes[0].set_xlabel("Revenue (₹ Crores)")
    axes[0].set_ylabel("")

    # Pie of market share
    axes[1].pie(cat_share.values, labels=cat_share.index, autopct="%1.1f%%",
                colors=sns.color_palette("viridis", len(cat_share)), startangle=90)
    axes[1].set_title("Market Share")

    # Growth bar
    sns.barplot(x=growth.values, y=growth.index,
                palette=["green" if v > 0 else "red" for v in growth.values], ax=axes[2])
    axes[2].set_title("YoY Growth: 2015-17 vs 2023-25 (%)")
    axes[2].set_xlabel("Growth %")
    axes[2].set_ylabel("")

    fig.suptitle("Q5 — Category-wise Performance", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q05_category_performance")


# ────────────────────────────────────────────────────────────────────────────
# Q6 — Prime vs Non-Prime
# ────────────────────────────────────────────────────────────────────────────
def q6_prime_impact(df: pd.DataFrame) -> str:
    """AOV, frequency, and category preference: Prime vs non-Prime."""
    if "is_prime_member" not in df.columns:
        return ""
    amt = _amount_col(df)
    df = df.copy()
    df["prime_label"] = df["is_prime_member"].map({True: "Prime", False: "Non-Prime"}).fillna("Unknown")

    aov = df.groupby("prime_label")[amt].mean()
    freq = df.groupby("prime_label").size()
    if "category" in df.columns:
        cat_pref = (df.groupby(["prime_label", "category"]).size()
                      .unstack(fill_value=0).apply(lambda r: r / r.sum() * 100, axis=1))
    else:
        cat_pref = pd.DataFrame()

    fig, axes = plt.subplots(1, 3, figsize=(20, 6))
    sns.barplot(x=aov.index, y=aov.values, palette=[PRIMARY, SECONDARY], ax=axes[0])
    axes[0].set_title("Average Order Value (₹)")
    axes[0].set_ylabel("AOV (₹)")

    sns.barplot(x=freq.index, y=freq.values, palette=[PRIMARY, SECONDARY], ax=axes[1])
    axes[1].set_title("Order Frequency")
    axes[1].set_ylabel("Number of Orders")

    if not cat_pref.empty:
        cat_pref.T.plot(kind="bar", stacked=False, color=[PRIMARY, SECONDARY, "#999"],
                         ax=axes[2], width=0.8)
        axes[2].set_title("Category Preference (%)")
        axes[2].set_ylabel("Share (%)")
        axes[2].legend(title="")
        axes[2].tick_params(axis="x", rotation=45)

    fig.suptitle("Q6 — Prime Membership Impact", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q06_prime_impact")


# ────────────────────────────────────────────────────────────────────────────
# Q7 — Geographic analysis (top cities + tier mix)
# ────────────────────────────────────────────────────────────────────────────
def q7_geographic(df: pd.DataFrame) -> str:
    """Top cities by revenue + tier-wise revenue distribution."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    if "customer_city" in df.columns:
        top = df.groupby("customer_city")[amt].sum().sort_values(ascending=False).head(15) / 1e7
        sns.barplot(x=top.values, y=top.index, palette="rocket", ax=axes[0])
        axes[0].set_title("Top 15 Cities by Revenue")
        axes[0].set_xlabel("Revenue (₹ Crores)")
        axes[0].set_ylabel("")

    tier_col = next((c for c in ("customer_tier", "city_tier", "tier") if c in df.columns), None)
    if tier_col is None and "customer_city" in df.columns:
        # Fallback: assign tiers from city name
        metros = {"Mumbai", "Delhi", "Bengaluru", "Chennai", "Kolkata", "Hyderabad"}
        tier1 = {"Pune", "Ahmedabad", "Jaipur", "Surat", "Lucknow", "Kanpur",
                 "Nagpur", "Indore", "Bhopal", "Gurugram", "Noida", "Faridabad", "Visakhapatnam"}
        def to_tier(city):
            if city in metros: return "Metro"
            if city in tier1: return "Tier 1"
            return "Tier 2/Rural"
        df = df.copy()
        df["_tier"] = df["customer_city"].map(to_tier)
        tier_col = "_tier"

    if tier_col:
        tier_rev = df.groupby(tier_col)[amt].sum() / 1e7
        axes[1].pie(tier_rev.values, labels=tier_rev.index, autopct="%1.1f%%",
                    colors=sns.color_palette("rocket", len(tier_rev)), startangle=90)
        axes[1].set_title("Revenue by City Tier")

    fig.suptitle("Q7 — Geographic Sales Distribution", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q07_geographic")


# ────────────────────────────────────────────────────────────────────────────
# Q8 — Festival sales impact
# ────────────────────────────────────────────────────────────────────────────
def q8_festival_impact(df: pd.DataFrame) -> str:
    """Time series highlighting festival vs normal-day revenue."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    if "is_festival_sale" not in df.columns:
        return ""

    daily = df.groupby(["_dt", "is_festival_sale"])[amt].sum().reset_index()
    fig, ax = plt.subplots(figsize=(14, 6))
    for flag, sub in daily.groupby("is_festival_sale"):
        label = "Festival" if flag is True else "Normal"
        color = "#E63946" if flag is True else "#457B9D"
        ax.scatter(sub["_dt"], sub[amt] / 1e6, label=label, alpha=0.55, s=18, color=color)

    if "festival_name" in df.columns:
        festivals = (df[df["is_festival_sale"] == True]
                     .groupby("festival_name")[amt].sum().sort_values(ascending=False).head(5))
        if not festivals.empty:
            ax.text(0.02, 0.95,
                    "Top festivals:\n" + "\n".join(f"  • {n}: ₹{v/1e7:.1f} Cr"
                                                    for n, v in festivals.items()),
                    transform=ax.transAxes, fontsize=10, va="top",
                    bbox=dict(boxstyle="round", facecolor="white", alpha=0.85))

    ax.set_title("Q8 — Festival Sales Impact (Daily Revenue)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue (₹ Lakhs)")
    ax.legend()
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    return _save(fig, "Q08_festival_impact")


# ────────────────────────────────────────────────────────────────────────────
# Q9 — Age group behaviour
# ────────────────────────────────────────────────────────────────────────────
def q9_age_group(df: pd.DataFrame) -> str:
    """Spending and category preference by age group."""
    if "age_group" not in df.columns:
        return ""
    amt = _amount_col(df)

    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    spend = df.groupby("age_group")[amt].agg(["sum", "mean", "count"])
    sns.barplot(x=spend.index, y=spend["mean"], palette="mako", ax=axes[0])
    axes[0].set_title("Avg Order Value by Age Group")
    axes[0].set_ylabel("AOV (₹)")
    axes[0].tick_params(axis="x", rotation=30)

    if "category" in df.columns:
        ct = (df.groupby(["age_group", "category"]).size().unstack(fill_value=0)
              .apply(lambda r: r / r.sum() * 100, axis=1))
        sns.heatmap(ct, cmap="mako", annot=True, fmt=".1f", ax=axes[1],
                    cbar_kws={"label": "Share (%)"})
        axes[1].set_title("Category Preference by Age (%)")

    fig.suptitle("Q9 — Customer Age Group Behaviour", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q09_age_group")


# ────────────────────────────────────────────────────────────────────────────
# Q10 — Price vs demand
# ────────────────────────────────────────────────────────────────────────────
def q10_price_vs_demand(df: pd.DataFrame) -> str:
    """Price-volume scatter with category facets + correlation matrix."""
    amt = _amount_col(df)
    price_col = next((c for c in ("original_price_inr", "price", "base_price_2015")
                      if c in df.columns), None)
    if price_col is None or "category" not in df.columns:
        return ""

    sample = df[[price_col, amt, "category"]].dropna().sample(
        min(20000, len(df.dropna(subset=[price_col, amt]))), random_state=42)

    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    sns.scatterplot(data=sample, x=price_col, y=amt, hue="category",
                    alpha=0.45, s=15, palette="tab10", ax=axes[0])
    axes[0].set_title("Price vs Final Amount (sample)")
    axes[0].set_xlabel("Original Price (₹)")
    axes[0].set_ylabel("Final Amount (₹)")
    axes[0].set_xscale("log"); axes[0].set_yscale("log")
    axes[0].legend(loc="lower right", fontsize=8)

    num_cols = [c for c in ("original_price_inr", "discount_percent", "final_amount_inr",
                            "delivery_charges", "delivery_days", "customer_rating",
                            "product_rating") if c in df.columns]
    corr = df[num_cols].corr(numeric_only=True)
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f", center=0,
                square=True, ax=axes[1])
    axes[1].set_title("Numeric Feature Correlation")

    fig.suptitle("Q10 — Price vs Demand Analysis", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q10_price_vs_demand")


# ────────────────────────────────────────────────────────────────────────────
# Q11 — Delivery performance
# ────────────────────────────────────────────────────────────────────────────
def q11_delivery_performance(df: pd.DataFrame) -> str:
    """Delivery distribution + on-time rates by city tier."""
    if "delivery_days" not in df.columns:
        return ""
    fig, axes = plt.subplots(1, 2, figsize=(18, 6))

    sns.histplot(df["delivery_days"].dropna(), bins=20, color=PRIMARY,
                 kde=True, ax=axes[0])
    axes[0].axvline(df["delivery_days"].median(), color="red",
                    linestyle="--", label=f"Median = {df['delivery_days'].median():.1f}")
    axes[0].set_title("Delivery Days Distribution")
    axes[0].set_xlabel("Delivery Days")
    axes[0].legend()

    if "customer_city" in df.columns:
        tmp = df.copy()
        tmp["on_time"] = tmp["delivery_days"] <= 4
        top_cities = tmp["customer_city"].value_counts().head(12).index
        ot = (tmp[tmp["customer_city"].isin(top_cities)]
              .groupby("customer_city")["on_time"].mean().sort_values() * 100)
        sns.barplot(x=ot.values, y=ot.index, palette="crest", ax=axes[1])
        axes[1].set_title("On-time Delivery Rate by City (%)")
        axes[1].set_xlabel("% delivered in ≤4 days")

    fig.suptitle("Q11 — Delivery Performance", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q11_delivery_performance")


# ────────────────────────────────────────────────────────────────────────────
# Q12 — Returns & satisfaction
# ────────────────────────────────────────────────────────────────────────────
def q12_returns_satisfaction(df: pd.DataFrame) -> str:
    """Return rate by category and correlation with rating."""
    return_col = next((c for c in ("return_status", "is_returned") if c in df.columns), None)
    if return_col is None:
        return ""
    df = df.copy()
    df["_returned"] = df[return_col].astype(str).str.lower().isin(["yes", "true", "1", "returned"])

    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    if "category" in df.columns:
        rr = df.groupby("category")["_returned"].mean().sort_values(ascending=False) * 100
        sns.barplot(x=rr.values, y=rr.index, palette="rocket_r", ax=axes[0])
        axes[0].set_title("Return Rate by Category (%)")
        axes[0].set_xlabel("Return rate (%)")

    if "customer_rating" in df.columns:
        sns.boxplot(data=df, x="_returned", y="customer_rating",
                    palette=[PRIMARY, "#999"], ax=axes[1])
        axes[1].set_xticklabels(["Kept", "Returned"])
        axes[1].set_title("Customer Rating: Returned vs Kept")
        axes[1].set_xlabel("")

    fig.suptitle("Q12 — Return Patterns & Customer Satisfaction",
                 fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q12_returns_satisfaction")


# ────────────────────────────────────────────────────────────────────────────
# Q13 — Brand performance
# ────────────────────────────────────────────────────────────────────────────
def q13_brand_performance(df: pd.DataFrame) -> str:
    """Top brands by revenue and their share over time."""
    if "brand" not in df.columns:
        return ""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    top_brands = df.groupby("brand")[amt].sum().sort_values(ascending=False).head(10).index

    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    rev = df[df["brand"].isin(top_brands)].groupby("brand")[amt].sum().sort_values(ascending=False) / 1e7
    sns.barplot(x=rev.values, y=rev.index, palette="flare", ax=axes[0])
    axes[0].set_title("Top 10 Brands by Revenue (₹ Cr)")

    yearly = (df[df["brand"].isin(top_brands)].groupby(["_year", "brand"])[amt]
              .sum().unstack(fill_value=0))
    yearly_share = yearly.div(yearly.sum(axis=1), axis=0) * 100
    yearly_share.plot(ax=axes[1], colormap="tab10", linewidth=2)
    axes[1].set_title("Top-10 Brand Market Share (%)")
    axes[1].set_xlabel("Year")
    axes[1].set_ylabel("Share (%)")
    axes[1].legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize=8)

    fig.suptitle("Q13 — Brand Performance & Market Share",
                 fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q13_brand_performance")


# ────────────────────────────────────────────────────────────────────────────
# Q14 — Customer lifetime value (cohort retention)
# ────────────────────────────────────────────────────────────────────────────
def q14_clv_cohort(df: pd.DataFrame) -> str:
    """Cohort retention heatmap + CLV distribution."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    df = df.dropna(subset=["_dt"])
    df["cohort_month"] = df.groupby("customer_id")["_dt"].transform("min").dt.to_period("M")
    df["order_month"] = df["_dt"].dt.to_period("M")
    df["cohort_index"] = ((df["order_month"].astype("int64") - df["cohort_month"].astype("int64"))).astype(int)

    cohort = (df.groupby(["cohort_month", "cohort_index"])["customer_id"]
              .nunique().unstack(fill_value=0))
    retention = cohort.div(cohort[0], axis=0) * 100

    clv = df.groupby("customer_id")[amt].sum()

    fig, axes = plt.subplots(1, 2, figsize=(20, 7))
    sns.heatmap(retention.iloc[:24, :13], cmap="YlGnBu", annot=False,
                cbar_kws={"label": "Retention %"}, ax=axes[0])
    axes[0].set_title("Cohort Retention Heatmap (first 24 cohorts × 12 months)")
    axes[0].set_xlabel("Months since first purchase")
    axes[0].set_ylabel("Cohort (first-purchase month)")

    sns.histplot(np.log10(clv[clv > 0]), bins=50, color=SECONDARY, kde=True, ax=axes[1])
    axes[1].set_title(f"CLV Distribution (log₁₀ ₹) — median ₹{clv.median():,.0f}")
    axes[1].set_xlabel("log₁₀ Customer Lifetime Value")

    fig.suptitle("Q14 — Customer Lifetime Value & Cohort Retention",
                 fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q14_clv_cohort")


# ────────────────────────────────────────────────────────────────────────────
# Q15 — Discount effectiveness
# ────────────────────────────────────────────────────────────────────────────
def q15_discount_effectiveness(df: pd.DataFrame) -> str:
    """Discount % vs sales volume and revenue impact."""
    if "discount_percent" not in df.columns:
        return ""
    amt = _amount_col(df)
    df = df.copy()
    df["discount_bucket"] = pd.cut(df["discount_percent"],
                                    bins=[-0.1, 0, 10, 20, 30, 50, 100],
                                    labels=["0%", "1-10%", "11-20%", "21-30%", "31-50%", "50%+"])
    g = df.groupby("discount_bucket").agg(orders=(amt, "size"),
                                           revenue=(amt, "sum"),
                                           aov=(amt, "mean")).reset_index()

    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    sns.barplot(data=g, x="discount_bucket", y="orders", palette="viridis", ax=axes[0])
    axes[0].set_title("Order Volume by Discount Bucket")
    axes[0].set_ylabel("Orders")

    ax2 = axes[1]
    ax2.bar(g["discount_bucket"].astype(str), g["revenue"] / 1e7, color=PRIMARY,
            label="Revenue (₹ Cr)")
    ax2b = ax2.twinx()
    ax2b.plot(g["discount_bucket"].astype(str), g["aov"], color=SECONDARY,
              marker="o", linewidth=2, label="AOV (₹)")
    ax2.set_title("Revenue + AOV by Discount Bucket")
    ax2.set_ylabel("Revenue (₹ Cr)", color=PRIMARY)
    ax2b.set_ylabel("AOV (₹)", color=SECONDARY)

    fig.suptitle("Q15 — Discount & Promotional Effectiveness",
                 fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q15_discount_effectiveness")


# ────────────────────────────────────────────────────────────────────────────
# Q16 — Rating patterns
# ────────────────────────────────────────────────────────────────────────────
def q16_rating_patterns(df: pd.DataFrame) -> str:
    """Rating distribution and rating-vs-revenue correlation."""
    rating_col = next((c for c in ("customer_rating", "product_rating") if c in df.columns), None)
    if rating_col is None:
        return ""
    amt = _amount_col(df)
    fig, axes = plt.subplots(1, 2, figsize=(18, 6))

    sns.histplot(df[rating_col].dropna(), bins=20, kde=True, color=PRIMARY, ax=axes[0])
    axes[0].set_title(f"{rating_col.replace('_', ' ').title()} Distribution")
    axes[0].set_xlabel("Rating")

    if "category" in df.columns:
        cat_rating = df.groupby("category").agg(
            avg_rating=(rating_col, "mean"),
            revenue=(amt, "sum"),
        ).dropna()
        sns.scatterplot(data=cat_rating, x="avg_rating", y=cat_rating["revenue"] / 1e7,
                        s=200, color=SECONDARY, ax=axes[1])
        for cat, row in cat_rating.iterrows():
            axes[1].annotate(cat, (row["avg_rating"], row["revenue"] / 1e7),
                             fontsize=9, alpha=0.8)
        axes[1].set_title("Avg Rating vs Revenue (by Category)")
        axes[1].set_xlabel("Avg Rating")
        axes[1].set_ylabel("Revenue (₹ Cr)")

    fig.suptitle("Q16 — Product Rating Patterns", fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q16_rating_patterns")


# ────────────────────────────────────────────────────────────────────────────
# Q17 — Customer journey (category transitions)
# ────────────────────────────────────────────────────────────────────────────
def q17_customer_journey(df: pd.DataFrame) -> str:
    """Heatmap of category → category transitions (next purchase)."""
    df = _ensure_dt(df)
    if "category" not in df.columns:
        return ""
    df = df.dropna(subset=["_dt", "category"]).sort_values(["customer_id", "_dt"])
    df["next_cat"] = df.groupby("customer_id")["category"].shift(-1)
    trans = (df.dropna(subset=["next_cat"])
             .groupby(["category", "next_cat"]).size().unstack(fill_value=0))
    trans_pct = trans.div(trans.sum(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(11, 8))
    sns.heatmap(trans_pct, annot=True, fmt=".1f", cmap="YlOrRd",
                cbar_kws={"label": "Transition %"}, ax=ax)
    ax.set_title("Q17 — Customer Journey: Category-to-Category Transitions (%)")
    ax.set_xlabel("Next Category")
    ax.set_ylabel("Current Category")
    return _save(fig, "Q17_customer_journey")


# ────────────────────────────────────────────────────────────────────────────
# Q18 — Product lifecycle
# ────────────────────────────────────────────────────────────────────────────
def q18_product_lifecycle(df: pd.DataFrame) -> str:
    """Yearly product launches and category trend lines."""
    df = _ensure_dt(df)
    amt = _amount_col(df)
    fig, axes = plt.subplots(1, 2, figsize=(18, 6))

    if "launch_year" in df.columns:
        launches = df.drop_duplicates("product_id")["launch_year"].value_counts().sort_index()
        sns.barplot(x=launches.index.astype(int).astype(str), y=launches.values,
                    palette="cubehelix", ax=axes[0])
        axes[0].set_title("New Products Launched per Year")
        axes[0].set_ylabel("# Products")
        axes[0].tick_params(axis="x", rotation=45)
    else:
        axes[0].text(0.5, 0.5, "No launch_year column", ha="center", va="center",
                     transform=axes[0].transAxes)
        axes[0].set_axis_off()

    if "category" in df.columns:
        trend = df.groupby(["_year", "category"])[amt].sum().unstack(fill_value=0) / 1e7
        trend.plot(ax=axes[1], colormap="tab10", linewidth=2)
        axes[1].set_title("Category Revenue Trends")
        axes[1].set_xlabel("Year")
        axes[1].set_ylabel("Revenue (₹ Cr)")
        axes[1].legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize=8)

    fig.suptitle("Q18 — Product Lifecycle & Category Evolution",
                 fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q18_product_lifecycle")


# ────────────────────────────────────────────────────────────────────────────
# Q19 — Competitive pricing (box plots)
# ────────────────────────────────────────────────────────────────────────────
def q19_competitive_pricing(df: pd.DataFrame) -> str:
    """Box plots: price ranges by category and top brands."""
    price_col = next((c for c in ("original_price_inr", "price") if c in df.columns), None)
    if price_col is None:
        return ""

    fig, axes = plt.subplots(1, 2, figsize=(18, 6))

    if "category" in df.columns:
        sns.boxplot(data=df, x="category", y=price_col, palette="Set3",
                    showfliers=False, ax=axes[0])
        axes[0].set_yscale("log")
        axes[0].set_title("Price Range by Category (log scale)")
        axes[0].tick_params(axis="x", rotation=30)

    if "brand" in df.columns:
        top10 = df["brand"].value_counts().head(10).index
        sub = df[df["brand"].isin(top10)]
        sns.boxplot(data=sub, x="brand", y=price_col, palette="Set2",
                    showfliers=False, ax=axes[1])
        axes[1].set_yscale("log")
        axes[1].set_title("Price Range — Top 10 Brands (log scale)")
        axes[1].tick_params(axis="x", rotation=30)

    fig.suptitle("Q19 — Competitive Pricing Analysis",
                 fontsize=15, fontweight="bold", y=1.02)
    return _save(fig, "Q19_competitive_pricing")


# ────────────────────────────────────────────────────────────────────────────
# Q20 — Business health dashboard (multi-panel)
# ────────────────────────────────────────────────────────────────────────────
def q20_business_health(df: pd.DataFrame) -> str:
    """6-panel executive dashboard: revenue, growth, customers, AOV,
    Prime share, top categories.
    """
    df = _ensure_dt(df)
    amt = _amount_col(df)

    fig = plt.figure(figsize=(20, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.55, wspace=0.35)

    # 1. Revenue trend
    ax = fig.add_subplot(gs[0, :2])
    yearly = df.groupby("_year")[amt].sum().sort_index() / 1e7
    ax.fill_between(yearly.index.astype(int), yearly.values, color=PRIMARY, alpha=0.4)
    ax.plot(yearly.index.astype(int), yearly.values, color=PRIMARY, linewidth=2.5, marker="o")
    ax.set_title("Yearly Revenue (₹ Cr)")

    # 2. Top categories
    ax = fig.add_subplot(gs[0, 2])
    if "category" in df.columns:
        top = df.groupby("category")[amt].sum().sort_values(ascending=False).head(5) / 1e7
        sns.barplot(x=top.values, y=top.index, palette="rocket", ax=ax)
        ax.set_title("Top 5 Categories")
        ax.set_xlabel("₹ Cr")

    # 3. AOV by year
    ax = fig.add_subplot(gs[1, 0])
    aov = df.groupby("_year")[amt].mean()
    ax.plot(aov.index.astype(int), aov.values, color=SECONDARY, marker="o", linewidth=2)
    ax.set_title("Avg Order Value (₹)")

    # 4. Active customers per year
    ax = fig.add_subplot(gs[1, 1])
    if "customer_id" in df.columns:
        active = df.groupby("_year")["customer_id"].nunique()
        ax.bar(active.index.astype(int).astype(str), active.values, color=ACCENT)
        ax.set_title("Active Customers / Year")
        ax.tick_params(axis="x", rotation=45)

    # 5. Prime share
    ax = fig.add_subplot(gs[1, 2])
    if "is_prime_member" in df.columns:
        share = df["is_prime_member"].value_counts(normalize=True) * 100
        ax.pie(share.values, labels=["Prime" if x else "Non-Prime" for x in share.index],
               autopct="%1.1f%%", colors=[PRIMARY, "#CCCCCC"], startangle=90)
        ax.set_title("Prime vs Non-Prime")

    # 6. Payment mix
    ax = fig.add_subplot(gs[2, :2])
    if "payment_method" in df.columns:
        pm = df["payment_method"].value_counts(normalize=True).head(6) * 100
        sns.barplot(x=pm.index, y=pm.values, palette="viridis", ax=ax)
        ax.set_title("Payment Mix (%)")
        ax.tick_params(axis="x", rotation=20)

    # 7. Returns rate
    ax = fig.add_subplot(gs[2, 2])
    if "return_status" in df.columns:
        rr = df["return_status"].astype(str).str.lower().isin(["yes", "true", "returned"]).mean() * 100
        ax.bar(["Returned", "Kept"], [rr, 100 - rr], color=["#E63946", "#06D6A0"])
        ax.set_title(f"Overall Return Rate ({rr:.1f}%)")
        ax.set_ylabel("%")
    else:
        ax.set_axis_off()

    fig.suptitle("Q20 — Amazon India: Business Health Dashboard",
                 fontsize=18, fontweight="bold", y=0.995)
    return _save(fig, "Q20_business_health")


# ────────────────────────────────────────────────────────────────────────────
# Master runner
# ────────────────────────────────────────────────────────────────────────────
ALL_PLOTS = [
    q1_revenue_trend, q2_seasonal_heatmap, q3_rfm_segmentation,
    q4_payment_evolution, q5_category_performance, q6_prime_impact,
    q7_geographic, q8_festival_impact, q9_age_group, q10_price_vs_demand,
    q11_delivery_performance, q12_returns_satisfaction, q13_brand_performance,
    q14_clv_cohort, q15_discount_effectiveness, q16_rating_patterns,
    q17_customer_journey, q18_product_lifecycle, q19_competitive_pricing,
    q20_business_health,
]


def run_all(df: pd.DataFrame) -> None:
    print(f"\n📊 Running {len(ALL_PLOTS)} EDA plots…")
    for fn in ALL_PLOTS:
        try:
            fn(df)
        except Exception as e:
            print(f"  ✗ {fn.__name__} failed: {e}")
    print(f"\n✅ All plots saved to {FIG_DIR}")
