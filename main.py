"""
FastAPI backend for Property Data API.
"""

import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import logging


from database import Database
from config import set_db_instance, get_db_instance, is_production, ENVIRONMENT
from api.routes import properties, map, statistics, address
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize database
# Set ENVIRONMENT=production to use PostgreSQL, or leave unset/default to use SQLite
# In development mode: uses SQLite with properties.db
# In production mode: uses PostgreSQL (requires DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
db_instance = Database()
logger.info(f"Database instance created: {db_instance}")
# Create tables if they don't exist
db_instance.create_tables()
logger.info("Database tables created")
set_db_instance(db_instance)
logger.info(f"Database instance set: {db_instance}")
# Create FastAPI app
app = FastAPI(
    title="Ireland Property Data API",
    description="API for accessing Irish property data with geocoding and statistics",
    version="1.0.0",
)

# Log each request method, path, and running time
class RequestTimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info("%s %s %.2f ms", request.method, request.url.path, elapsed_ms)
        return response


app.add_middleware(RequestTimingMiddleware)

# Configure CORS - Allow all origins (open everywhere)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=False,  # Must be False when allow_origins=["*"]
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
    expose_headers=["*"],  # Expose all headers
)

# Include routers
app.include_router(properties.router, prefix="/api/properties", tags=["properties"])
app.include_router(map.router, prefix="/api/maps", tags=["maps"])
app.include_router(statistics.router, prefix="/api/statistics", tags=["statistics"])
app.include_router(address.router, prefix="/api/addresses", tags=["addresses"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Ireland Property Data API", "version": "1.0.0", "docs": "/docs"}


@app.get("/health")
async def health():
    """Health check endpoint with environment and database connection info."""
    try:
        db = get_db_instance()

        # Check database connection by executing a simple query
        db_connected = False
        db_error = None
        try:
            session = db.get_session()
            # Try a simple query to verify connection
            session.execute(text("SELECT 1"))
            session.commit()
            session.close()
            db_connected = True
        except SQLAlchemyError as e:
            db_connected = False
            db_error = str(e)
        except Exception as e:
            db_connected = False
            db_error = str(e)

        # Get database type info
        db_type = db.db_type if hasattr(db, "db_type") else "unknown"
        db_info = {
            "type": db_type,
            "connected": db_connected,
        }

        # Add database-specific info
        if db_type == "sqlite" and hasattr(db, "db_path"):
            db_info["path"] = db.db_path
        elif db_type == "postgresql":
            from config import DB_HOST, DB_NAME

            db_info["host"] = DB_HOST if DB_HOST else "not configured"
            db_info["database"] = DB_NAME if DB_NAME else "not configured"

        if db_error:
            db_info["error"] = db_error

        return {
            "status": "healthy" if db_connected else "unhealthy",
            "environment": "production" if is_production() else "development",
            "environment_variable": ENVIRONMENT,
            "database": db_info,
        }
    except Exception as e:
        return {
            "status": "error",
            "environment": "production" if is_production() else "development",
            "environment_variable": ENVIRONMENT,
            "database": {
                "connected": False,
                "error": f"Failed to get database instance: {str(e)}",
            },
        }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
