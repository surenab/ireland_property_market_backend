"""
Shared dependencies for FastAPI routes.
"""

from sqlalchemy.orm import Session
from config import get_db_instance


def get_db() -> Session:
    """Get database session dependency for FastAPI routes."""
    db = get_db_instance()
    session = db.get_session()
    try:
        yield session
    finally:
        session.close()
