"""
Centralized configuration for the backend application.
"""

import os
from pathlib import Path
from typing import Optional

# Environment configuration
# Set to "production" to use PostgreSQL, "development" (or unset) to use SQLite
ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()

# Database configuration
# Get the project root directory (two levels up from this file)
PROJECT_ROOT = Path(__file__).parent
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "properties.db"))

# PostgreSQL configuration (for production)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

# Database instance - will be initialized in main.py
_db_instance = None


def get_db_path() -> str:
    """Get the database path (for SQLite)."""
    return DB_PATH


def get_database_url() -> Optional[str]:
    """Get PostgreSQL database URL if in production mode and configured, otherwise None (will use SQLite)."""
    # Only use PostgreSQL if ENVIRONMENT is set to "production"
    if ENVIRONMENT == "production":
        if DB_HOST and DB_USER and DB_PASSWORD and DB_NAME:
            return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"
        else:
            raise ValueError(
                "ENVIRONMENT is set to 'production' but PostgreSQL credentials are missing. "
                "Please set DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME environment variables."
            )
    # In development mode, return None to use SQLite
    return None


def is_production() -> bool:
    """Check if running in production mode."""
    return ENVIRONMENT == "production"


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
