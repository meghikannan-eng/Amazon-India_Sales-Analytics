"""
db.py — shared MySQL connection + cached queries for the Streamlit app.
"""

from __future__ import annotations

import os
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass


# ────────────────────────────────────────────────────────────────────────────
# Engine
# ────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    """Create a SQLAlchemy engine. Cached so we don't open a new connection
    on every Streamlit re-run."""
    user = os.getenv("DB_USER", "root")
    password = quote_plus(os.getenv("DB_PASSWORD", ""))
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    db = os.getenv("DB_NAME", "amazon_india_db")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True, future=True)


# ────────────────────────────────────────────────────────────────────────────
# Cached query helpers
# ────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame. Cached for 10 minutes."""
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"Database query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def load_view(view_name: str) -> pd.DataFrame:
    """Convenience: load an entire view by name."""
    return run_query(f"SELECT * FROM {view_name}")


# ────────────────────────────────────────────────────────────────────────────
# Page-level filter helper (used by every page for consistent year filtering)
# ────────────────────────────────────────────────────────────────────────────
def year_filter_widget() -> tuple[int | None, int | None]:
    """Render a year-range slider in the sidebar. Returns (min, max)."""
    years = run_query("SELECT DISTINCT year FROM dim_time ORDER BY year")
    if years.empty:
        return None, None
    yr_min = int(years["year"].min())
    yr_max = int(years["year"].max())
    selected = st.sidebar.slider("Year range", yr_min, yr_max, (yr_min, yr_max))
    return selected
