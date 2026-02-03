"""
FastAPI backend for Property Data API.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import os


from database import Database
from config import set_db_instance, get_db_path
from api.routes import properties, map, statistics, address

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize database
db_path = get_db_path()
logger.info(f"Initializing database at {db_path}")
db_instance = Database(db_path=db_path)
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

# Configure CORS
# Get allowed origins from environment variable or use defaults
cors_origins = os.getenv(
    "CORS_ORIGINS", "http://localhost:3000,http://localhost:3001"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(properties.router, prefix="/api/properties", tags=["properties"])
app.include_router(map.router, prefix="/api/map", tags=["map"])
app.include_router(statistics.router, prefix="/api/statistics", tags=["statistics"])
app.include_router(address.router, prefix="/api/address", tags=["address"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Ireland Property Data API", "version": "1.0.0", "docs": "/docs"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
