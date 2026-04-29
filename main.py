"""
main.py — Amazon India: A Decade of Sales Analytics

Streamlit dashboard with 6 pages mapping to the project's 30 dashboard
questions. Pulls data from the MySQL warehouse views.

Run:
    streamlit run streamlit_app/main.py
"""

from __future__ import annotations

import streamlit as st

from db import get_engine, run_query
from pages_lib import (
    page_executive,
    page_revenue,
    page_customer,
    page_product,
    page_operations,
    page_advanced,
)


# ────────────────────────────────────────────────────────────────────────────
# Page configuration
# ────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Amazon India — Sales Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS — Amazon-themed colors, cleaner cards
st.markdown(
    """
    <style>
        .main { padding-top: 1rem; }
        h1 { color: #232F3E; }
        h2, h3 { color: #FF9900; }
        [data-testid="stMetricValue"] { font-size: 28px; color: #232F3E; }
        [data-testid="stMetricLabel"] { font-size: 14px; color: #666; }
        .stButton > button { background-color: #FF9900; color: white; border: none; }
        section[data-testid="stSidebar"] { background: linear-gradient(180deg, #232F3E 0%, #37475A 100%); }
        section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] label, section[data-testid="stSidebar"] p { color: #FFFFFF !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ────────────────────────────────────────────────────────────────────────────
# Sidebar — branding + navigation + connection check
# ────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("# 🛒 Amazon India")
    st.markdown("##### Decade of Sales Analytics")
    st.markdown("---")

    page = st.radio(
        "Navigate",
        [
            "📊 Executive Overview",
            "💰 Revenue Analytics",
            "👥 Customer Analytics",
            "📦 Product & Inventory",
            "🚚 Operations & Logistics",
            "🔮 Advanced Analytics",
        ],
        index=0,
    )

    st.markdown("---")

    # Connection health indicator
    try:
        ver = run_query("SELECT VERSION() AS v")
        if not ver.empty:
            st.success(f"✓ MySQL {ver['v'].iloc[0]}")
        else:
            st.error("✗ Database unreachable")
    except Exception as e:
        st.error("✗ Database unreachable")
        st.caption(f"Check your .env file. {e}")

    st.markdown("---")
    st.caption("Data: Amazon India 2015-2025")
    st.caption("Built with Streamlit + MySQL")


# ────────────────────────────────────────────────────────────────────────────
# Route to the chosen page
# ────────────────────────────────────────────────────────────────────────────
if page.startswith("📊"):
    page_executive()
elif page.startswith("💰"):
    page_revenue()
elif page.startswith("👥"):
    page_customer()
elif page.startswith("📦"):
    page_product()
elif page.startswith("🚚"):
    page_operations()
elif page.startswith("🔮"):
    page_advanced()
