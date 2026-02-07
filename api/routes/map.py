"""
Map routes for geographic data and clustering.
"""

from fastapi import APIRouter, Query, Depends
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging
import random


from models import PropertyModel, AddressModel, PriceHistoryModel
from api.schemas import (
    MapPointsResponse,
    MapPoint,
    MapAnalysisResponse,
    HeatmapPolygon,
    PropertyListItem,
)
from dependencies import get_db
from api.cache import cached

router = APIRouter()
logger = logging.getLogger(__name__)


# Zoom 0-9: sample cap for map performance; zoom 10+: more points but capped for speed
SAMPLE_POINTS_CAP = 500
MAX_POINTS_ZOOM_10_PLUS = (
    5000  # was 50k; lower cap avoids slow queries and huge responses
)
# Heatmap / analysis modes: load up to 20k points for density and heatmap visualizations
HEATMAP_MAX_POINTS = 140000


def get_max_points_for_zoom(zoom: Optional[int]) -> int:
    """Get maximum points to return based on zoom level. No clustering."""
    if zoom is None:
        return SAMPLE_POINTS_CAP
    if zoom <= 9:
        return SAMPLE_POINTS_CAP
    return MAX_POINTS_ZOOM_10_PLUS


@router.get("/points", response_model=MapPointsResponse)
@cached(ttl=300)  # Cache for 5 minutes
async def get_map_points(
    north: float = Query(..., description="North boundary"),
    south: float = Query(..., description="South boundary"),
    east: float = Query(..., description="East boundary"),
    west: float = Query(..., description="West boundary"),
    zoom: Optional[int] = Query(None, description="Map zoom level"),
    max_points: Optional[int] = Query(
        None, ge=1, description="Maximum points to return (overrides zoom-based limit)"
    ),
    county: Optional[str] = Query(None, description="Filter by county"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    has_geocoding: Optional[bool] = Query(
        None, description="Filter by geocoding status"
    ),
    has_daft_data: Optional[bool] = Query(
        None, description="Filter by Daft.ie data availability"
    ),
    min_sales: Optional[int] = Query(
        None, ge=1, description="Minimum number of sales (price history entries)"
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """Get individual map points for a viewport. Returns only id, lat, lng; details loaded on click via GET /api/properties/{id}."""
    from sqlalchemy import func
    from api.services.property_filtering import build_property_query
    from models import PriceHistoryModel

    # Build base query with filters using utility function (without price filters)
    query = build_property_query(
        db=db,
        north=north,
        south=south,
        east=east,
        west=west,
        county=county,
        start_date=start_date,
        end_date=end_date,
        has_geocoding=has_geocoding,
        has_daft_data=has_daft_data,
        min_sales=min_sales,
        min_price=None,  # We'll handle price filtering after getting latest prices
        max_price=None,
    )

    # Price filtering: Get latest price within date range, then filter
    if min_price is not None or max_price is not None:
        # Build date filter for latest price subquery
        price_date_filter = []
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                price_date_filter.append(PriceHistoryModel.date_of_sale >= start_dt)
            except ValueError:
                pass
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(
                    days=1
                )
                price_date_filter.append(PriceHistoryModel.date_of_sale < end_dt)
            except ValueError:
                pass

        # Get latest price by date (not max price value) for each property
        latest_price_subquery = db.query(
            PriceHistoryModel.property_id,
            func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
        )
        if price_date_filter:
            latest_price_subquery = latest_price_subquery.filter(
                and_(*price_date_filter)
            )
        latest_price_subquery = latest_price_subquery.group_by(
            PriceHistoryModel.property_id
        ).subquery()

        # Join with price history to get the actual price for the latest date
        price_with_date_subquery = (
            db.query(
                PriceHistoryModel.property_id,
                PriceHistoryModel.price.label("latest_price"),
            )
            .join(
                latest_price_subquery,
                and_(
                    PriceHistoryModel.property_id
                    == latest_price_subquery.c.property_id,
                    PriceHistoryModel.date_of_sale
                    == latest_price_subquery.c.latest_date,
                ),
            )
            .subquery()
        )

        # Join with main query
        query = query.join(
            price_with_date_subquery,
            PropertyModel.id == price_with_date_subquery.c.property_id,
        )

        # Apply price filters
        if min_price is not None:
            query = query.filter(price_with_date_subquery.c.latest_price >= min_price)
        if max_price is not None:
            query = query.filter(price_with_date_subquery.c.latest_price <= max_price)

    # Never run query.count() here - it is very slow on large joined result sets.
    effective_max_points = (
        max_points if max_points is not None else get_max_points_for_zoom(zoom)
    )
    if zoom is not None and zoom <= 9:
        # Fast path: ORDER BY id LIMIT N, then random.sample in Python.
        fetch_limit = min(effective_max_points * 3, 1000)
        results = query.order_by(PropertyModel.id).limit(fetch_limit).all()
        if len(results) > effective_max_points:
            results = random.sample(results, effective_max_points)
    else:
        # Zoom 10+: limit only (no count); cap at MAX_POINTS_ZOOM_10_PLUS.
        results = query.order_by(PropertyModel.id).limit(effective_max_points).all()
    total_count = len(results)

    # Return only id, latitude, longitude for map markers; details loaded on click via GET /api/properties/{id}
    points = [
        MapPoint(
            id=prop.id,
            latitude=address.latitude,
            longitude=address.longitude,
            price=None,
            address=None,
            county=None,
            date=None,
        )
        for prop, address in results
    ]

    return MapPointsResponse(points=points, total=total_count)


@router.get("/list")
@cached(ttl=300)
async def get_map_list(
    north: float = Query(..., description="North boundary"),
    south: float = Query(..., description="South boundary"),
    east: float = Query(..., description="East boundary"),
    west: float = Query(..., description="West boundary"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    county: Optional[str] = Query(None, description="Filter by county"),
    min_price: Optional[float] = Query(None),
    max_price: Optional[float] = Query(None),
    has_geocoding: Optional[bool] = Query(None),
    has_daft_data: Optional[bool] = Query(None),
    min_sales: Optional[int] = Query(
        None, ge=1, description="Minimum number of sales (price history entries)"
    ),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Paginated list of properties in viewport (for Map page sidebar). Same filters as map points."""
    from api.services.property_filtering import (
        build_property_query,
        get_latest_prices_in_date_range,
    )

    query = build_property_query(
        db=db,
        north=north,
        south=south,
        east=east,
        west=west,
        county=county,
        start_date=start_date,
        end_date=end_date,
        has_geocoding=has_geocoding,
        has_daft_data=has_daft_data,
        min_sales=min_sales,
        min_price=None,
        max_price=None,
    )

    if min_price is not None or max_price is not None:
        price_date_filter = []
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                price_date_filter.append(PriceHistoryModel.date_of_sale >= start_dt)
            except ValueError:
                pass
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(
                    days=1
                )
                price_date_filter.append(PriceHistoryModel.date_of_sale < end_dt)
            except ValueError:
                pass
        latest_price_subquery = db.query(
            PriceHistoryModel.property_id,
            func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
        )
        if price_date_filter:
            latest_price_subquery = latest_price_subquery.filter(
                and_(*price_date_filter)
            )
        latest_price_subquery = latest_price_subquery.group_by(
            PriceHistoryModel.property_id
        ).subquery()
        price_with_date_subquery = (
            db.query(
                PriceHistoryModel.property_id,
                PriceHistoryModel.price.label("latest_price"),
            )
            .join(
                latest_price_subquery,
                and_(
                    PriceHistoryModel.property_id
                    == latest_price_subquery.c.property_id,
                    PriceHistoryModel.date_of_sale
                    == latest_price_subquery.c.latest_date,
                ),
            )
            .subquery()
        )
        query = query.join(
            price_with_date_subquery,
            PropertyModel.id == price_with_date_subquery.c.property_id,
        )
        if min_price is not None:
            query = query.filter(price_with_date_subquery.c.latest_price >= min_price)
        if max_price is not None:
            query = query.filter(price_with_date_subquery.c.latest_price <= max_price)

    property_ids_query = (
        query.with_entities(PropertyModel.id).distinct().order_by(PropertyModel.id)
    )
    total = property_ids_query.count()
    if total == 0:
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 0,
        }
    offset = (page - 1) * page_size
    property_ids = [
        row[0] for row in property_ids_query.offset(offset).limit(page_size).all()
    ]

    results = (
        db.query(PropertyModel, AddressModel)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
        .filter(PropertyModel.id.in_(property_ids))
        .order_by(PropertyModel.id)
        .all()
    )
    price_map = get_latest_prices_in_date_range(
        db=db,
        property_ids=property_ids,
        start_date=start_date,
        end_date=end_date,
    )
    items = []
    for prop, address in results:
        price_data = price_map.get(prop.id)
        latest_price = price_data[0] if price_data else None
        latest_sale_date = None
        if price_data and len(price_data) > 1 and price_data[1]:
            d = price_data[1]
            latest_sale_date = (
                d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            )
        items.append(
            PropertyListItem(
                id=prop.id,
                address=address.address if address else None,
                county=address.county if address else None,
                latitude=address.latitude if address else None,
                longitude=address.longitude if address else None,
                latest_price=(
                    int(round(latest_price)) if latest_price is not None else None
                ),
                latest_sale_date=latest_sale_date,
            )
        )
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


@router.get("/analysis", response_model=MapAnalysisResponse)
@cached(ttl=300)  # Cache for 5 minutes
async def get_map_analysis(
    north: float = Query(..., description="North boundary"),
    south: float = Query(..., description="South boundary"),
    east: float = Query(..., description="East boundary"),
    west: float = Query(..., description="West boundary"),
    analysis_mode: str = Query(
        ...,
        description="Analysis mode: spatial-patterns, hotspots, cluster-identification, growth-decline, price-heatmap, sales-heatmap",
    ),
    zoom: Optional[int] = Query(None, description="Map zoom level"),
    county: Optional[str] = Query(None, description="Filter by county"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    pattern_type: Optional[str] = Query(None, description="Spatial pattern type"),
    radius: Optional[int] = Query(50, description="Hotspot radius"),
    intensity: Optional[float] = Query(0.5, description="Hotspot intensity"),
    has_geocoding: Optional[bool] = Query(
        None, description="Filter by geocoding status"
    ),
    has_daft_data: Optional[bool] = Query(
        None, description="Filter by Daft.ie data availability"
    ),
    db: Session = Depends(get_db),
):
    """Get map analysis data for different visualization modes."""
    from api.services.map_clustering import cluster_properties
    from api.services.property_filtering import (
        build_property_query,
        get_latest_prices_in_date_range,
    )

    # Build base query with filters using utility function
    query = build_property_query(
        db=db,
        north=north,
        south=south,
        east=east,
        west=west,
        county=county,
        start_date=start_date,
        end_date=end_date,
        min_price=min_price,
        max_price=max_price,
        has_geocoding=has_geocoding,
        has_daft_data=has_daft_data,
    )

    # Get total count
    count = query.count()
    logger.info(f"Found {count} properties in viewport at zoom {zoom}")

    # Heatmap-related modes: use higher cap (20k) for better visualization; others use zoom-based limit
    heatmap_modes = (
        "spatial-patterns",
        "hotspots",
        "cluster-identification",
        "price-heatmap",
        "sales-heatmap",
    )
    if analysis_mode in heatmap_modes:
        max_results = HEATMAP_MAX_POINTS
    else:
        max_results = (
            get_max_points_for_zoom(zoom) * 2
            if zoom and zoom <= 7
            else get_max_points_for_zoom(zoom)
        )
    if count > max_results:
        logger.info(
            f"Limiting results from {count} to {max_results} for zoom {zoom} analysis_mode={analysis_mode}"
        )
        query = query.limit(max_results)

    results = query.all()

    # Get all property IDs
    property_ids = [prop.id for prop, _ in results]

    # Optimize: Get latest price for all properties
    # If date filters are applied, get the latest price WITHIN the date range
    # Batch queries to avoid SQL parameter limits (typically 1000-2000)
    if property_ids:
        # Build date filter for price history if dates are provided
        # Note: date_of_sale is stored as String, so we compare as strings
        price_date_filter = []
        if start_date:
            try:
                # Compare as string (YYYY-MM-DD format)
                price_date_filter.append(PriceHistoryModel.date_of_sale >= start_date)
            except ValueError:
                pass
        if end_date:
            try:
                # Compare as string (YYYY-MM-DD format)
                end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                end_date_str = end_dt.strftime("%Y-%m-%d")
                price_date_filter.append(PriceHistoryModel.date_of_sale < end_date_str)
            except ValueError:
                pass

        # Batch queries to avoid SQL parameter limits
        BATCH_SIZE = 10000
        price_map = {}

        for i in range(0, len(property_ids), BATCH_SIZE):
            batch_ids = property_ids[i : i + BATCH_SIZE]

            # Query for latest prices for this batch
            latest_prices_query = db.query(
                PriceHistoryModel.property_id,
                func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
            ).filter(PriceHistoryModel.property_id.in_(batch_ids))

            # Apply date filter if provided - get latest price WITHIN the date range
            if price_date_filter:
                latest_prices_query = latest_prices_query.filter(
                    and_(*price_date_filter)
                )

            latest_prices_subquery = latest_prices_query.group_by(
                PriceHistoryModel.property_id
            ).subquery()

            latest_prices = (
                db.query(
                    PriceHistoryModel.property_id,
                    PriceHistoryModel.price,
                    PriceHistoryModel.date_of_sale,
                )
                .join(
                    latest_prices_subquery,
                    and_(
                        PriceHistoryModel.property_id
                        == latest_prices_subquery.c.property_id,
                        PriceHistoryModel.date_of_sale
                        == latest_prices_subquery.c.latest_date,
                    ),
                )
                .all()
            )

            # Add results to dict
            for pid, price, date in latest_prices:
                price_map[pid] = (price, date)
    else:
        price_map = {}

    # Prepare properties data
    properties_data = []
    for prop, address in results:
        latest_price = None
        latest_date = None
        if prop.id in price_map:
            latest_price, latest_date = price_map[prop.id]

        # Handle date conversion - it might be a string or datetime
        date_str = None
        if latest_date:
            if isinstance(latest_date, str):
                date_str = latest_date
            else:
                date_str = latest_date.isoformat()

        properties_data.append(
            {
                "id": prop.id,
                "latitude": address.latitude,
                "longitude": address.longitude,
                "price": int(round(latest_price)) if latest_price is not None else None,
                "date": date_str,
                "address": address.address,
                "county": address.county,
            }
        )
    logger.info(f"Processed {len(properties_data)} properties")

    # For zoom 0-7, use clustering with real counts
    if zoom is not None and zoom <= 7:
        from api.services.map_clustering import (
            cluster_properties_by_grid_with_real_counts,
        )

        clusters = cluster_properties_by_grid_with_real_counts(properties_data, zoom)

        # Convert clusters to heatmap data
        heatmap_data = []
        max_count = max([c["count"] for c in clusters]) if clusters else 1
        for cluster in clusters:
            intensity = min(cluster["count"] / max_count, 1.0) if max_count > 0 else 0
            heatmap_data.append(
                {
                    "lat": cluster["center_lat"],
                    "lng": cluster["center_lng"],
                    "intensity": intensity,
                    "data": {
                        "intensity": intensity,
                        "sales_count": cluster["count"],  # Real count
                        "avg_price": cluster.get("avg_price"),
                        "min_price": cluster.get("min_price"),
                        "max_price": cluster.get("max_price"),
                    },
                }
            )

        from api.services.heatmap import compute_heatmap_polygons

        heatmap_polygons_raw = compute_heatmap_polygons(
            properties_data, north, south, east, west, analysis_mode
        )
        heatmap_polygons = [HeatmapPolygon(**p) for p in heatmap_polygons_raw]

        response_data: Dict[str, Any] = {
            "analysis_mode": analysis_mode,
            "total_properties": len(properties_data),  # Total before clustering
            "viewport": {"north": north, "south": south, "east": east, "west": west},
            "heatmap_data": heatmap_data,
            "heatmap_polygons": heatmap_polygons,
            "clusters": clusters,  # Include cluster data with real counts
            "points": [],
        }

        return MapAnalysisResponse(**response_data)

    # Process based on analysis mode for zoom 8+
    response_data: Dict[str, Any] = {
        "analysis_mode": analysis_mode,
        "total_properties": len(properties_data),
        "viewport": {"north": north, "south": south, "east": east, "west": west},
        "heatmap_data": [],
        "clusters": [],
        "points": [],
    }
    logger.info(f"Response data: {response_data}")
    if analysis_mode == "spatial-patterns":
        # Spatial patterns - return density-based heatmap
        heatmap_data = []
        for prop in properties_data:
            intensity = 1.0
            if pattern_type == "Density":
                intensity = 1.0
            elif pattern_type == "Concentration":
                intensity = (prop["price"] / 1000000) if prop["price"] else 0.5
            heatmap_data.append(
                {
                    "lat": prop["latitude"],
                    "lng": prop["longitude"],
                    "intensity": min(intensity, 1.0),
                }
            )
        response_data["heatmap_data"] = heatmap_data

    elif analysis_mode == "hotspots":
        # Hotspots - identify high-activity areas
        grid_size = 0.01  # ~1km
        grid_counts: Dict[str, Dict] = {}
        for prop in properties_data:
            grid_lat = int(prop["latitude"] / grid_size)
            grid_lng = int(prop["longitude"] / grid_size)
            key = f"{grid_lat}_{grid_lng}"
            if key not in grid_counts:
                grid_counts[key] = {
                    "count": 0,
                    "lat": prop["latitude"],
                    "lng": prop["longitude"],
                }
            grid_counts[key]["count"] += 1

        heatmap_data = []
        max_count = (
            max([g["count"] for g in grid_counts.values()]) if grid_counts else 1
        )
        for grid in grid_counts.values():
            intensity_val = (grid["count"] / max_count) * (intensity or 0.5)
            heatmap_data.append(
                {
                    "lat": grid["lat"],
                    "lng": grid["lng"],
                    "intensity": intensity_val,
                    "data": {
                        "intensity": intensity_val,
                        "sales_count": grid["count"],
                    },
                }
            )
        response_data["heatmap_data"] = heatmap_data

    elif analysis_mode == "cluster-identification":
        # Cluster identification with heatmap style
        clusters = cluster_properties(properties_data, 10, "geographic")
        response_data["clusters"] = []
        for cluster in clusters:
            # Convert MapCluster to dict
            cluster_props = [
                {
                    "id": p.id,
                    "latitude": p.latitude,
                    "longitude": p.longitude,
                    "price": int(round(p.price)) if p.price is not None else None,
                    "address": p.address,
                    "county": p.county,
                }
                for p in cluster.properties
            ]

            prices = [p.price for p in cluster.properties if p.price]
            avg_price = int(round(sum(prices) / len(prices))) if prices else 0

            cluster_data = {
                "center_lat": cluster.center_lat,
                "center_lng": cluster.center_lng,
                "count": cluster.count,
                "properties": cluster_props,
                "avg_price": avg_price,
            }
            response_data["clusters"].append(cluster_data)

        # Also create heatmap data
        heatmap_data = []
        for cluster in clusters:
            prices = [p.price for p in cluster.properties if p.price]
            avg_price = int(round(sum(prices) / len(prices))) if prices else 0
            heatmap_data.append(
                {
                    "lat": cluster.center_lat,
                    "lng": cluster.center_lng,
                    "intensity": min(cluster.count / 100.0, 1.0),
                    "data": {
                        "intensity": min(cluster.count / 100.0, 1.0),
                        "sales_count": cluster.count,
                        "avg_price": avg_price,
                    },
                }
            )
        response_data["heatmap_data"] = heatmap_data

    elif analysis_mode == "growth-decline":
        # Growth/decline concentration - compare prices between two time periods
        if start_date and end_date:
            try:
                # Parse dates
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

                # Split date range into two periods (early and late)
                date_range = (end_dt - start_dt).days
                if date_range < 30:  # Need at least 30 days to compare
                    response_data["heatmap_data"] = []
                else:
                    mid_date = start_dt + timedelta(days=date_range // 2)
                    mid_date_str = mid_date.strftime("%Y-%m-%d")

                    # Get prices for early period (start_date to mid_date)
                    early_prices = get_latest_prices_in_date_range(
                        db, property_ids, start_date, mid_date_str
                    )

                    # Get prices for late period (mid_date to end_date)
                    late_prices = get_latest_prices_in_date_range(
                        db, property_ids, mid_date_str, end_date
                    )

                    # Group by location grid (0.01 degree grid)
                    grid_size = 0.01
                    grid_data: Dict[str, Dict] = {}

                    for prop in properties_data:
                        if prop["latitude"] and prop["longitude"]:
                            grid_lat = int(prop["latitude"] / grid_size)
                            grid_lng = int(prop["longitude"] / grid_size)
                            key = f"{grid_lat}_{grid_lng}"

                            if key not in grid_data:
                                grid_data[key] = {
                                    "lat": prop["latitude"],
                                    "lng": prop["longitude"],
                                    "early_prices": [],
                                    "late_prices": [],
                                }

                            # Add early period price if available
                            if prop["id"] in early_prices:
                                price, _ = early_prices[prop["id"]]
                                if price:
                                    grid_data[key]["early_prices"].append(price)

                            # Add late period price if available
                            if prop["id"] in late_prices:
                                price, _ = late_prices[prop["id"]]
                                if price:
                                    grid_data[key]["late_prices"].append(price)

                    # Calculate growth/decline for each grid cell
                    price_changes = []
                    all_changes = []

                    for key, grid in grid_data.items():
                        if grid["early_prices"] and grid["late_prices"]:
                            early_avg = sum(grid["early_prices"]) / len(
                                grid["early_prices"]
                            )
                            late_avg = sum(grid["late_prices"]) / len(
                                grid["late_prices"]
                            )

                            if early_avg > 0:
                                change_percent = (
                                    (late_avg - early_avg) / early_avg
                                ) * 100
                                all_changes.append(change_percent)

                                # Normalize intensity: -100% to +100% maps to 0.0 to 1.0
                                # Negative (decline) maps to 0.0-0.5, positive (growth) maps to 0.5-1.0
                                normalized_intensity = (change_percent / 200.0) + 0.5
                                normalized_intensity = max(
                                    0.0, min(1.0, normalized_intensity)
                                )

                                price_changes.append(
                                    {
                                        "lat": grid["lat"],
                                        "lng": grid["lng"],
                                        "intensity": normalized_intensity,
                                        "data": {
                                            "intensity": normalized_intensity,
                                            "change_percent": round(change_percent, 2),
                                            "early_avg": int(round(early_avg)),
                                            "late_avg": int(round(late_avg)),
                                        },
                                    }
                                )

                    response_data["heatmap_data"] = price_changes
                    logger.info(
                        f"Growth-decline: {len(price_changes)} grid cells with price changes"
                    )
            except ValueError as e:
                logger.error(f"Error parsing dates for growth-decline: {e}")
                response_data["heatmap_data"] = []
        else:
            response_data["heatmap_data"] = []

    elif analysis_mode == "price-heatmap":
        # Price heatmap (average/median)
        grid_size = 0.01
        grid_prices: Dict[str, Dict] = {}
        for prop in properties_data:
            if prop["price"]:
                grid_lat = int(prop["latitude"] / grid_size)
                grid_lng = int(prop["longitude"] / grid_size)
                key = f"{grid_lat}_{grid_lng}"
                if key not in grid_prices:
                    grid_prices[key] = {
                        "prices": [],
                        "lat": prop["latitude"],
                        "lng": prop["longitude"],
                    }
                grid_prices[key]["prices"].append(prop["price"])

            heatmap_data = []
            all_avg_prices = [
                sum(g["prices"]) / len(g["prices"])
                for g in grid_prices.values()
                if g["prices"]
            ]
            max_price = max(all_avg_prices) if all_avg_prices else 1
            for grid in grid_prices.values():
                if grid["prices"]:
                    avg_price = sum(grid["prices"]) / len(grid["prices"])
                    intensity_val = (avg_price / max_price) if max_price > 0 else 0
                    heatmap_data.append(
                        {
                            "lat": grid["lat"],
                            "lng": grid["lng"],
                            "intensity": intensity_val,
                            "data": {
                                "intensity": intensity_val,
                                "avg_price": int(round(avg_price)),
                            },
                        }
                    )
            response_data["heatmap_data"] = heatmap_data

    elif analysis_mode == "sales-heatmap":
        # Sales per cluster heatmap
        clusters = cluster_properties(properties_data, 10, "geographic")
        heatmap_data = []
        max_sales = max([c.count for c in clusters]) if clusters else 1
        for cluster in clusters:
            intensity_val = (cluster.count / max_sales) if max_sales > 0 else 0
            heatmap_data.append(
                {
                    "lat": cluster.center_lat,
                    "lng": cluster.center_lng,
                    "intensity": intensity_val,
                    "sales_count": cluster.count,
                }
            )
        response_data["heatmap_data"] = heatmap_data
        # Convert clusters to dict format
        response_data["clusters"] = [
            {
                "center_lat": c.center_lat,
                "center_lng": c.center_lng,
                "count": c.count,
                "properties": [
                    {
                        "id": p.id,
                        "latitude": p.latitude,
                        "longitude": p.longitude,
                        "price": int(round(p.price)) if p.price is not None else None,
                        "address": p.address,
                        "county": p.county,
                    }
                    for p in c.properties
                ],
            }
            for c in clusters
        ]

    # Heatmap polygons (grid cells with metadata)
    from api.services.heatmap import compute_heatmap_polygons

    heatmap_polygons_raw = compute_heatmap_polygons(
        properties_data, north, south, east, west, analysis_mode
    )
    response_data["heatmap_polygons"] = [
        HeatmapPolygon(**p) for p in heatmap_polygons_raw
    ]

    # Also return points for marker display
    response_data["points"] = properties_data[:]  # Limit points

    # Ensure viewport is a MapViewport dict
    response_data["viewport"] = {
        "north": north,
        "south": south,
        "east": east,
        "west": west,
    }

    return MapAnalysisResponse(**response_data)
