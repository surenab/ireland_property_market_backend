# Property Data API Backend

FastAPI backend for serving Irish property data with statistical analysis and map clustering.

## Setup

This backend uses `uv` for dependency management.

1. Install dependencies:
```bash
cd backend
uv sync
```

2. Run the server:
```bash
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8080
```

Or use the development script from the project root:
```bash
./run-dev.sh
```

## API Endpoints

### Properties
- `GET /api/properties` - List properties with pagination and filtering
- `GET /api/properties/{id}` - Get property details
- `GET /api/properties/{id}/history` - Get price history

### Map
- `GET /api/map/clusters` - Get map clusters by viewport
- `GET /api/map/points` - Get individual map points

### Statistics
- `GET /api/statistics/price-trends` - Price trends over time
- `GET /api/statistics/clusters` - Price clustering analysis
- `GET /api/statistics/county` - County-level comparisons
- `GET /api/statistics/correlation` - Size-Price correlation

### Search
- `GET /api/search` - Search properties
- `GET /api/search/autocomplete` - Autocomplete suggestions

## API Documentation

Once the server is running, visit:
- Swagger UI: http://localhost:8080/docs
- ReDoc: http://localhost:8080/redoc

