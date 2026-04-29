"""
run_db_load.py
--------------
End-to-end loader. Reads the cleaned dataset, connects to MySQL using
credentials from .env, and loads the star-schema.

Usage:
    python src/run_db_load.py                # load everything
    python src/run_db_load.py --no-truncate  # append instead of truncate
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from data_loader import PROJECT_ROOT
from db_loader import get_engine, load_to_mysql

# Load .env from project root (if present)
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
try:
    from dotenv import load_dotenv
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)
        print(f"  ✓ Loaded credentials from {ENV_PATH}")
    else:
        print(f"  ⚠️  No .env file at {ENV_PATH}")
        print(f"     Create one by running: copy .env.example .env  (then edit it)")
except ImportError:
    print("  ⚠️  python-dotenv not installed — install with: pip install python-dotenv")


CLEANED_CSV = os.path.join(PROJECT_ROOT, "data", "cleaned", "amazon_india_cleaned.csv")
CLEANED_PARQUET = os.path.join(PROJECT_ROOT, "data", "cleaned", "amazon_india_cleaned.parquet")
SCHEMA_SQL = os.path.join(PROJECT_ROOT, "sql", "schema.sql")
VIEWS_SQL = os.path.join(PROJECT_ROOT, "sql", "dashboard_views.sql")


def _load_cleaned() -> pd.DataFrame:
    if os.path.exists(CLEANED_PARQUET):
        return pd.read_parquet(CLEANED_PARQUET)
    if os.path.exists(CLEANED_CSV):
        return pd.read_csv(CLEANED_CSV, low_memory=False, parse_dates=["order_date_dt"])
    raise FileNotFoundError(
        f"Cleaned dataset not found.\n"
        f"  Expected at: {CLEANED_CSV}\n"
        f"  Run `python src/run_cleaning.py` first."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-truncate", action="store_true",
                        help="Append rows instead of truncating tables first.")
    args = parser.parse_args()

    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "3306"))
    db_name = os.getenv("DB_NAME", "amazon_india_db")

    if not db_password:
        print("\n⚠️  DB_PASSWORD is empty in .env (or .env is missing).")
        db_password = getpass.getpass("Enter your MySQL password to continue: ")
        if not db_password:
            print("No password entered. Aborting.")
            sys.exit(1)

    print(f"Connecting to MySQL → {db_user}@{db_host}:{db_port}/{db_name}")
    engine = get_engine(db_user, db_password, db_host, db_port, db_name)

    # Quick connection test
    from sqlalchemy import text
    with engine.connect() as conn:
        ver = conn.execute(text("SELECT VERSION()")).scalar()
        print(f"  Connected to MySQL {ver}")

    df = _load_cleaned()
    print(f"Loaded cleaned dataset: {df.shape}")

    load_to_mysql(df, engine, truncate_first=not args.no_truncate)

    print("\nNext steps:")
    print(f"  1. Apply views:  mysql -u {db_user} -p {db_name} < {VIEWS_SQL}")
    print(f"  2. Connect PowerBI to {db_host}:{db_port}/{db_name}")


if __name__ == "__main__":
    main()
