"""
Centralized configuration for the backend application.
"""

import os
from pathlib import Path

# Database configuration
# Get the project root directory (two levels up from this file)
PROJECT_ROOT = Path(__file__).parent
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "properties.db"))

# Database instance - will be initialized in main.py
_db_instance = None


def get_db_path() -> str:
    """Get the database path."""
    return DB_PATH


def set_db_instance(db_instance):
    """Set the database instance (called from main.py)."""
    global _db_instance
    _db_instance = db_instance


def get_db_instance():
    """Get the database instance."""
    if _db_instance is None:
        raise RuntimeError(
            "Database instance not initialized. Call set_db_instance() first."
        )
    return _db_instance
