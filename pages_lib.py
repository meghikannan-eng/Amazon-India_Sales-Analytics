"""
pages_lib.py — implementations of all 6 dashboard pages.

Each page is a single function that pulls data from the MySQL views
and renders charts using Plotly. Easy to extend / reorder / restyle.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db import load_view, run_query


# Color palette
PALETTE_PRIMARY = "#FF9900"   # Amazon orange
PALETTE_SECONDARY = "#146EB4" # Amazon blue
PALETTE_DARK = "#232F3E"      # Amazon navy
PALETTE_GREEN = "#067D62"
PALETTE_RED = "#B12704"
PALETTE_QUAL = ["#FF9900", "#146EB4", "#067D62", "#B12704", "#37475A",
                "#E47911", "#7C7C7C", "#CD9042"]


def _money(n: float) -> str:
    """Format INR amounts as ₹X.XX Cr / Lakh."""
    if n is None or pd.isna(n) or n == 0:
        return "₹0"
    if abs(n) >= 1e7:
        return f"₹{n/1e7:,.2f} Cr"
    if abs(n) >= 1e5:
        return f"₹{n/1e5:,.2f} L"
    if abs(n) >= 1e3:
        return f"₹{n/1e3:,.1f}K"
    return f"₹{n:,.0f}"


def _short(n: float) -> str:
    """Compact number formatting (1.2M, 350K, etc.)."""
    if n is None or pd.isna(n) or n == 0:
        return "0"
    if abs(n) >= 1e6:
        return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"{n/1e3:.1f}K"
    return f"{n:,.0f}"


def _fmt(val, fmt_str: str = "{:.2f}", default: str = "N/A") -> str:
    """Safe formatter — returns default if val is None / NaN."""
    if val is None or pd.isna(val):
        return default
    try:
        return fmt_str.format(val)
    except (ValueError, TypeError):
        return default


# ════════════════════════════════════════════════════════════════════════════
# PAGE 1 — EXECUTIVE OVERVIEW
# ════════════════════════════════════════════════════════════════════════════
def page_executive() -> None:
    st.title("📊 Executive Overview")
    st.caption("High-level business health at a glance — Amazon India 2015-2025")

    # KPIs
    kpi = load_view("vw_kpi_summary")
    if kpi.empty:
        st.warning("No data available. Run the data loader first.")
        return

    k = kpi.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue", _money(k["total_revenue_inr"]))
    c2.metric("Active Customers", _short(k["active_customers"]))
    c3.metric("Total Orders", _short(k["total_orders"]))
    c4.metric("Avg Order Value", _money(k["avg_order_value_inr"]))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Active Products", _short(k["active_products"]))
    c6.metric("Avg Customer Rating", _fmt(k.get("avg_customer_rating"), "{:.2f} ⭐"))
    c7.metric("Festival Share", _fmt(k.get("festival_share_pct"), "{:.1f}%"))
    c8.metric("Prime Share", _fmt(k.get("prime_share_pct"), "{:.1f}%"))

    st.markdown("---")

    # Revenue trend + Top categories
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Yearly Revenue Trend")
        yr = load_view("vw_yearly_revenue")
        if not yr.empty:
            yr["revenue_cr"] = yr["revenue_inr"] / 1e7
            yr["yoy_pct"] = yr["revenue_inr"].pct_change() * 100
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=yr["year"], y=yr["revenue_cr"],
                name="Revenue (₹ Cr)", marker_color=PALETTE_PRIMARY,
                text=[f"{v:.0f}" for v in yr["revenue_cr"]],
                textposition="outside",
            ))
            fig.add_trace(go.Scatter(
                x=yr["year"], y=yr["revenue_cr"],
                mode="lines+markers", name="Trend",
                line=dict(color=PALETTE_DARK, width=2),
            ))
            fig.update_layout(
                height=420, margin=dict(t=20, b=20, l=20, r=20),
                yaxis_title="Revenue (₹ Cr)", xaxis_title="Year",
                hovermode="x unified", showlegend=True,
                plot_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Top Categories")
        cat = load_view("vw_category_performance")
        if not cat.empty:
            top5 = cat.nlargest(5, "revenue_inr").copy()
            top5["revenue_cr"] = top5["revenue_inr"] / 1e7
            fig = px.bar(top5, x="revenue_cr", y="category",
                          orientation="h", color_discrete_sequence=[PALETTE_SECONDARY],
                          text="revenue_cr")
            fig.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            fig.update_layout(
                height=420, margin=dict(t=20, b=20, l=20, r=20),
                yaxis_title="", xaxis_title="Revenue (₹ Cr)",
                yaxis={"categoryorder": "total ascending"},
                plot_bgcolor="white", showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Monthly trend + Festival impact
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Monthly Pattern")
        m = load_view("vw_monthly_revenue")
        if not m.empty:
            m["revenue_cr"] = m["revenue_inr"] / 1e7
            fig = px.line(m, x="month", y="revenue_cr", color="year",
                          color_discrete_sequence=PALETTE_QUAL,
                          markers=True)
            fig.update_layout(
                height=380, margin=dict(t=20, b=20, l=20, r=20),
                yaxis_title="Revenue (₹ Cr)", xaxis_title="Month",
                plot_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Festival vs Normal Days")
        fest = load_view("vw_festival_impact")
        if not fest.empty:
            agg = fest.groupby(
                fest["period_type"].apply(lambda x: "Festival" if x != "Normal" else "Normal")
            )["revenue_inr"].sum().reset_index()
            agg.columns = ["type", "revenue"]
            fig = px.pie(agg, names="type", values="revenue",
                          color_discrete_sequence=[PALETTE_PRIMARY, "#CCCCCC"],
                          hole=0.5)
            fig.update_layout(height=380, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE 2 — REVENUE ANALYTICS
# ════════════════════════════════════════════════════════════════════════════
def page_revenue() -> None:
    st.title("💰 Revenue Analytics")
    st.caption("Where the money comes from — yearly, geographic, festival, discount drill-downs")

    # Yearly + YoY
    yr = load_view("vw_yearly_revenue")
    if yr.empty:
        st.warning("No yearly data."); return
    yr["revenue_cr"] = yr["revenue_inr"] / 1e7
    yr["yoy_pct"] = yr["revenue_inr"].pct_change() * 100

    c1, c2 = st.columns(2)
    last_yoy = yr["yoy_pct"].iloc[-1] if not yr.empty else None
    c1.metric("Latest Year Revenue", _money(yr["revenue_inr"].iloc[-1]),
              delta=_fmt(last_yoy, "{:.1f}% YoY", default=None))

    cagr_text = "—"
    try:
        if len(yr) > 1 and yr["revenue_inr"].iloc[0] > 0:
            cagr = ((yr["revenue_inr"].iloc[-1] / yr["revenue_inr"].iloc[0]) ** (1 / (len(yr) - 1)) - 1) * 100
            cagr_text = f"{cagr:.1f}%"
    except (ZeroDivisionError, ValueError, TypeError):
        pass
    c2.metric("Decade CAGR", cagr_text)

    st.markdown("---")

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Yearly Revenue + Growth")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=yr["year"], y=yr["revenue_cr"],
                              name="Revenue (₹ Cr)", marker_color=PALETTE_PRIMARY))
        fig.add_trace(go.Scatter(x=yr["year"], y=yr["yoy_pct"],
                                  yaxis="y2", name="YoY Growth %",
                                  line=dict(color=PALETTE_RED, width=3),
                                  mode="lines+markers"))
        fig.update_layout(
            height=400, hovermode="x unified",
            yaxis=dict(title="Revenue (₹ Cr)"),
            yaxis2=dict(title="YoY %", overlaying="y", side="right"),
            margin=dict(t=20, b=20, l=20, r=20), plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Quarterly Pattern")
        q = run_query("""
            SELECT t.year, t.quarter, SUM(f.final_amount_inr) AS revenue
            FROM fact_transactions f JOIN dim_time t ON t.date_id = f.date_id
            GROUP BY t.year, t.quarter ORDER BY t.year, t.quarter
        """)
        if not q.empty:
            q["revenue_cr"] = q["revenue"] / 1e7
            q["quarter_label"] = "Q" + q["quarter"].astype(str)
            fig = px.bar(q, x="year", y="revenue_cr", color="quarter_label",
                          color_discrete_sequence=PALETTE_QUAL,
                          barmode="stack")
            fig.update_layout(height=400, plot_bgcolor="white",
                              yaxis_title="Revenue (₹ Cr)", xaxis_title="Year",
                              margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Geographic
    geo = load_view("vw_geographic_revenue")
    if not geo.empty:
        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Top 15 Cities by Revenue")
            top15 = geo.groupby("customer_city")["revenue_inr"].sum().nlargest(15).reset_index()
            top15["revenue_cr"] = top15["revenue_inr"] / 1e7
            fig = px.bar(top15, x="revenue_cr", y="customer_city",
                          orientation="h", color_discrete_sequence=[PALETTE_PRIMARY])
            fig.update_layout(height=440, yaxis={"categoryorder": "total ascending"},
                               yaxis_title="", xaxis_title="Revenue (₹ Cr)",
                               plot_bgcolor="white", margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Revenue by City Tier")
            tier = geo.groupby("city_tier")["revenue_inr"].sum().reset_index()
            tier["revenue_cr"] = tier["revenue_inr"] / 1e7
            fig = px.pie(tier, names="city_tier", values="revenue_cr", hole=0.4,
                          color_discrete_sequence=PALETTE_QUAL)
            fig.update_layout(height=440, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Festival + Discount
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Festival Impact by Year")
        fest = load_view("vw_festival_impact")
        if not fest.empty:
            fest["revenue_cr"] = fest["revenue_inr"] / 1e7
            fest["is_festival"] = fest["period_type"].apply(lambda x: "Festival" if x != "Normal" else "Normal")
            agg = fest.groupby(["year", "is_festival"])["revenue_cr"].sum().reset_index()
            fig = px.bar(agg, x="year", y="revenue_cr", color="is_festival",
                          barmode="group",
                          color_discrete_map={"Festival": PALETTE_RED, "Normal": PALETTE_SECONDARY})
            fig.update_layout(height=380, plot_bgcolor="white",
                               yaxis_title="Revenue (₹ Cr)",
                               margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Discount Effectiveness")
        d = load_view("vw_discount_effectiveness")
        if not d.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=d["discount_bucket"], y=d["revenue_inr"] / 1e7,
                                  name="Revenue (₹ Cr)", marker_color=PALETTE_PRIMARY))
            fig.add_trace(go.Scatter(x=d["discount_bucket"], y=d["avg_order_value"],
                                      yaxis="y2", name="AOV (₹)",
                                      line=dict(color=PALETTE_DARK, width=2),
                                      mode="lines+markers"))
            fig.update_layout(
                height=380, hovermode="x unified",
                yaxis=dict(title="Revenue (₹ Cr)"),
                yaxis2=dict(title="AOV (₹)", overlaying="y", side="right"),
                margin=dict(t=20, b=20, l=20, r=20), plot_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE 3 — CUSTOMER ANALYTICS
# ════════════════════════════════════════════════════════════════════════════
def page_customer() -> None:
    st.title("👥 Customer Analytics")
    st.caption("Segments, cohorts, lifecycle — who's buying and why")

    # Prime vs non-Prime
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Prime vs Non-Prime")
        p = load_view("vw_prime_vs_nonprime")
        if not p.empty:
            p["revenue_cr"] = p["revenue_inr"] / 1e7
            fig = px.bar(p, x="member_type", y=["revenue_cr", "avg_order_value"],
                          barmode="group", color_discrete_sequence=PALETTE_QUAL)
            fig.update_layout(height=380, plot_bgcolor="white",
                               margin=dict(t=20, b=20, l=20, r=20),
                               yaxis_title="Value", xaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Customers by Age Group")
        a = load_view("vw_age_group_behavior")
        if not a.empty:
            a["revenue_cr"] = a["revenue_inr"] / 1e7
            fig = px.pie(a, names="age_group", values="revenue_cr", hole=0.4,
                          color_discrete_sequence=PALETTE_QUAL)
            fig.update_layout(height=380, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Cohorts
    st.subheader("Customer Cohorts (acquisition month → revenue)")
    c = load_view("vw_customer_cohorts")
    if not c.empty:
        c["revenue_cr"] = c["revenue_inr"] / 1e7
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=c["cohort_month"], y=c["customers"],
                                  name="New Customers",
                                  line=dict(color=PALETTE_SECONDARY, width=2),
                                  mode="lines+markers"))
        fig.add_trace(go.Bar(x=c["cohort_month"], y=c["arpu"],
                              name="ARPU (₹)", yaxis="y2", marker_color=PALETTE_PRIMARY,
                              opacity=0.6))
        fig.update_layout(
            height=400, hovermode="x unified",
            yaxis=dict(title="New Customers"),
            yaxis2=dict(title="ARPU (₹)", overlaying="y", side="right"),
            margin=dict(t=20, b=20, l=20, r=20), plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Top customers table
    st.subheader("Top 20 Customers by Lifetime Value")
    t = load_view("vw_top_customers")
    if not t.empty:
        st.dataframe(
            t.head(20).style.format({
                "lifetime_value_inr": "₹{:,.0f}",
                "avg_order_value": "₹{:,.0f}",
            }),
            use_container_width=True, hide_index=True,
        )

    st.markdown("---")

    # Demographics behaviour
    if not load_view("vw_age_group_behavior").empty:
        st.subheader("Avg Order Value by Age Group")
        a = load_view("vw_age_group_behavior")
        fig = px.bar(a, x="age_group", y="avg_order_value",
                      color_discrete_sequence=[PALETTE_DARK])
        fig.update_layout(height=320, plot_bgcolor="white",
                           yaxis_title="AOV (₹)", xaxis_title="",
                           margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE 4 — PRODUCT & INVENTORY
# ════════════════════════════════════════════════════════════════════════════
def page_product() -> None:
    st.title("📦 Product & Inventory")
    st.caption("Brand share, category lifecycle, ratings, returns")

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Top 10 Brands by Revenue")
        b = load_view("vw_brand_performance")
        if not b.empty:
            top10 = b.head(10).copy()
            top10["revenue_cr"] = top10["revenue_inr"] / 1e7
            fig = px.bar(top10, x="revenue_cr", y="brand", color="category",
                          orientation="h", color_discrete_sequence=PALETTE_QUAL)
            fig.update_layout(height=460, yaxis={"categoryorder": "total ascending"},
                               yaxis_title="", xaxis_title="Revenue (₹ Cr)",
                               plot_bgcolor="white", margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Category Performance")
        c = load_view("vw_category_performance")
        if not c.empty:
            c["revenue_cr"] = c["revenue_inr"] / 1e7
            fig = px.treemap(c, path=["category"], values="revenue_cr",
                              color="market_share_pct",
                              color_continuous_scale="Oranges")
            fig.update_layout(height=460, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Returns
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Return Rate by Category")
        r = load_view("vw_returns_by_category")
        if not r.empty:
            fig = px.bar(r, x="return_rate_pct", y="category",
                          orientation="h", color_discrete_sequence=[PALETTE_RED],
                          text="return_rate_pct")
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(height=400, yaxis={"categoryorder": "total ascending"},
                               yaxis_title="", xaxis_title="Return Rate (%)",
                               plot_bgcolor="white", margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Product Launch Activity")
        launches = run_query("""
            SELECT launch_year, COUNT(*) AS new_products
            FROM dim_products WHERE launch_year IS NOT NULL
            GROUP BY launch_year ORDER BY launch_year
        """)
        if not launches.empty:
            fig = px.area(launches, x="launch_year", y="new_products",
                          color_discrete_sequence=[PALETTE_PRIMARY])
            fig.update_layout(height=400, plot_bgcolor="white",
                               yaxis_title="# Products Launched",
                               xaxis_title="Year",
                               margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE 5 — OPERATIONS & LOGISTICS
# ════════════════════════════════════════════════════════════════════════════
def page_operations() -> None:
    st.title("🚚 Operations & Logistics")
    st.caption("Delivery performance, payment evolution, service quality")

    # Delivery KPIs
    delivery = load_view("vw_delivery_performance")
    if not delivery.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Avg Delivery Days", _fmt(delivery["avg_delivery_days"].mean(), "{:.1f}"))
        try:
            best_city = delivery.nlargest(1, "on_time_pct")["customer_city"].iloc[0]
            c2.metric("Best On-Time City", str(best_city),
                      delta=_fmt(delivery["on_time_pct"].max(), "{:.1f}%", default=None))
        except (IndexError, ValueError):
            c2.metric("Best On-Time City", "—")
        c3.metric("Cities Tracked", f"{len(delivery)}")

    st.markdown("---")

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("On-Time Delivery by City")
        if not delivery.empty:
            top15 = delivery.nlargest(15, "orders")
            fig = px.bar(top15, x="on_time_pct", y="customer_city",
                          orientation="h", color="on_time_pct",
                          color_continuous_scale="RdYlGn")
            fig.update_layout(height=460, yaxis={"categoryorder": "total ascending"},
                               yaxis_title="", xaxis_title="On-time %",
                               plot_bgcolor="white", margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Payment Method Evolution")
        pm = load_view("vw_payment_mix")
        if not pm.empty:
            wide = pm.pivot_table(index="year", columns="payment_method",
                                    values="orders", fill_value=0)
            wide_pct = wide.div(wide.sum(axis=1), axis=0) * 100
            fig = go.Figure()
            for col in wide_pct.columns:
                fig.add_trace(go.Scatter(
                    x=wide_pct.index, y=wide_pct[col],
                    mode="lines", name=col, stackgroup="one",
                ))
            fig.update_layout(height=460, plot_bgcolor="white",
                               yaxis_title="Share (%)", xaxis_title="Year",
                               margin=dict(t=20, b=20, l=20, r=20),
                               hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.subheader("Returns by Category vs Volume")
    r = load_view("vw_returns_by_category")
    if not r.empty:
        fig = px.scatter(r, x="total_orders", y="return_rate_pct", size="returned_orders",
                          color="category", hover_data=["category"],
                          color_discrete_sequence=PALETTE_QUAL)
        fig.update_layout(height=400, plot_bgcolor="white",
                           yaxis_title="Return Rate (%)", xaxis_title="Total Orders",
                           margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE 6 — ADVANCED ANALYTICS
# ════════════════════════════════════════════════════════════════════════════
def page_advanced() -> None:
    st.title("🔮 Advanced Analytics")
    st.caption("Forecasting, seasonality, scorecard")

    # Seasonality heatmap
    st.subheader("Seasonality Heatmap (Year × Month)")
    m = load_view("vw_monthly_revenue")
    if not m.empty:
        m["revenue_cr"] = m["revenue_inr"] / 1e7
        pivot = m.pivot_table(index="year", columns="month", values="revenue_cr", fill_value=0)
        fig = px.imshow(pivot.values, x=pivot.columns, y=pivot.index,
                         color_continuous_scale="YlOrRd",
                         labels=dict(x="Month", y="Year", color="₹ Cr"),
                         aspect="auto", text_auto=".0f")
        fig.update_layout(height=440, margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Quarterly scorecard
    st.subheader("Quarterly Scorecard")
    q = run_query("""
        SELECT t.year, t.quarter,
               SUM(f.final_amount_inr) AS revenue,
               COUNT(*) AS orders,
               COUNT(DISTINCT f.customer_id) AS customers,
               AVG(f.final_amount_inr) AS aov
        FROM fact_transactions f JOIN dim_time t ON t.date_id = f.date_id
        GROUP BY t.year, t.quarter ORDER BY t.year, t.quarter
    """)
    if not q.empty:
        q["revenue_cr"] = q["revenue"] / 1e7
        q["period"] = q["year"].astype(str) + " Q" + q["quarter"].astype(str)
        st.dataframe(
            q[["period", "revenue_cr", "orders", "customers", "aov"]]
            .rename(columns={"revenue_cr": "Revenue (₹ Cr)", "aov": "AOV (₹)"})
            .style.format({"Revenue (₹ Cr)": "{:.1f}", "orders": "{:,}",
                           "customers": "{:,}", "AOV (₹)": "₹{:,.0f}"}),
            use_container_width=True, hide_index=True, height=400,
        )

    st.markdown("---")

    # Simple revenue forecast (linear extrapolation)
    st.subheader("Revenue Forecast — Next 3 Years (linear projection)")
    yr = load_view("vw_yearly_revenue")
    if not yr.empty and len(yr) >= 3:
        import numpy as np
        x = yr["year"].values
        y = (yr["revenue_inr"] / 1e7).values
        coef = np.polyfit(x, y, 1)
        future_x = np.arange(x.max() + 1, x.max() + 4)
        future_y = np.polyval(coef, future_x)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=x, y=y, name="Actual", marker_color=PALETTE_PRIMARY))
        fig.add_trace(go.Bar(x=future_x, y=future_y, name="Forecast",
                              marker_color="rgba(255, 153, 0, 0.4)"))
        fig.add_trace(go.Scatter(x=np.concatenate([x, future_x]),
                                  y=np.polyval(coef, np.concatenate([x, future_x])),
                                  mode="lines", line=dict(color=PALETTE_DARK, dash="dash"),
                                  name="Trend line"))
        fig.update_layout(height=400, plot_bgcolor="white",
                           yaxis_title="Revenue (₹ Cr)", xaxis_title="Year",
                           hovermode="x unified",
                           margin=dict(t=20, b=20, l=20, r=20))
        st.plotly_chart(fig, use_container_width=True)
