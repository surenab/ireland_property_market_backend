#!/usr/bin/env python3
"""
CLI script to update PPR data: download PPR-ALL.zip, unzip CSV, and import
(same flow as admin/upload — create/update by address_hash, geocode new, Daft scrape new).

Run from backend directory:
  uv run python scripts/update_ppr.py
  # or
  python scripts/update_ppr.py

Uses backend/.env for DB and any API keys (e.g. Bing, PPR cookies if needed).
"""

import sys
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure backend root is on path when run as scripts/update_ppr.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

# Load .env before importing config/database
try:
    from dotenv import load_dotenv

    load_dotenv(_backend_root / ".env")
except ImportError:
    pass

from database import Database
from config import set_db_instance
from api.routes.upload import run_ppr_download_and_import_sync


def main() -> int:
    print("PPR update: initializing database...")
    db = Database()
    db.create_tables()
    set_db_instance(db)
    print("PPR update: running download and import (same flow as admin/upload)...")
    try:
        result = run_ppr_download_and_import_sync()
    except Exception as e:
        print(f"PPR update failed: {e}", file=sys.stderr)
        return 1
    print("PPR update complete.")
    print(f"  total_rows: {result.total_rows}")
    print(f"  unique_properties: {result.unique_properties}")
    print(f"  new (created): {result.created}")
    print(f"  existing (updated): {result.updated}")
    print(f"  geocoded: {result.geocoded} (failed: {result.failed_geocode})")
    print(f"  Daft.ie scraped: {result.daft_scraped} (failed: {result.failed_daft})")
    if result.errors:
        print(f"  errors: {len(result.errors)} (first 5: {result.errors[:5]})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
