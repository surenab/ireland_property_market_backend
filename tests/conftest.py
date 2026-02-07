"""Pytest fixtures for backend tests."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure backend root is on path
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from config import set_db_instance
from database import Database


@pytest.fixture
def test_db():
    """Create an in-memory SQLite database for tests."""
    db = Database(db_path=":memory:")
    db.create_tables()
    set_db_instance(db)
    yield db
    db.close()


@pytest.fixture
def app(test_db):
    """Create FastAPI app with test database. Patch set_db_instance so main does not overwrite."""
    with patch("config.set_db_instance", lambda x: None):
        from main import app as fastapi_app
    return fastapi_app


@pytest.fixture
def client(app):
    """Test client for the FastAPI app."""
    from fastapi.testclient import TestClient
    return TestClient(app)
