"""
Map routes for geographic data and clustering.
"""

from fastapi import APIRouter, Query, Depends
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from datetime import datetime, timedelta
from typing import Dict, Any
import logging


from models import PropertyModel, AddressModel, PriceHistoryModel
from api.schemas import (
    MapClustersResponse,
    MapPointsResponse,
    MapPoint,
    MapAnalysisResponse,
)
from dependencies import get_db

router = APIRouter()
logger = logging.getLogger(__name__)


def create_map_point(
    prop: PropertyModel,
    address: AddressModel,
    latest_price: Optional[float],
    latest_date: Optional[str] = None,
) -> MapPoint:
    """Create a map point from property and address."""
    return MapPoint(
        id=prop.id,
        latitude=address.latitude,
        longitude=address.longitude,
        price=latest_price,
        address=address.address,
        county=address.county,
        date=latest_date,
    )


@router.get("/points", response_model=MapPointsResponse)
async def get_map_points(
    north: float = Query(..., description="North boundary"),
    south: float = Query(..., description="South boundary"),
    east: float = Query(..., description="East boundary"),
    west: float = Query(..., description="West boundary"),
    max_points: int = Query(1000, ge=1, description="Maximum points to return"),
    county: Optional[str] = Query(None, description="Filter by county"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    has_geocoding: Optional[bool] = Query(
        None, description="Filter by geocoding status"
    ),
    has_daft_data: Optional[bool] = Query(
        None, description="Filter by Daft.ie data availability"
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """Get individual map points for a viewport (for small areas or low zoom)."""
    from sqlalchemy import func
    from api.services.property_filtering import (
        build_property_query,
        get_latest_prices_in_date_range,
    )
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

    # Get total count BEFORE limiting
    total_count = query.count()

    # Now apply limit for the actual results
    query = query.limit(max_points)

    results = query.all()
    property_ids = [prop.id for prop, _ in results]

    # Get latest prices WITHIN the date range if dates are provided
    price_map = get_latest_prices_in_date_range(
        db=db,
        property_ids=property_ids,
        start_date=start_date,
        end_date=end_date,
    )

    points = []
    for prop, address in results:
        # Get latest price and date from price_map (within date range if dates provided)
        price_data = price_map.get(prop.id)
        latest_price = price_data[0] if price_data else None
        latest_date = None
        if price_data and len(price_data) > 1:
            latest_date_value = price_data[1]
            # Convert date to string in YYYY-MM-DD format
            if latest_date_value:
                if isinstance(latest_date_value, datetime):
                    latest_date = latest_date_value.date().strftime("%Y-%m-%d")
                elif hasattr(latest_date_value, "strftime"):
                    # date object
                    latest_date = latest_date_value.strftime("%Y-%m-%d")
                elif isinstance(latest_date_value, str):
                    latest_date = latest_date_value

        points.append(create_map_point(prop, address, latest_price, latest_date))

    return MapPointsResponse(points=points, total=total_count)


@router.get("/clusters", response_model=MapClustersResponse)
async def get_map_clusters(
    north: float = Query(..., description="North boundary"),
    south: float = Query(..., description="South boundary"),
    east: float = Query(..., description="East boundary"),
    west: float = Query(..., description="West boundary"),
    zoom: int = Query(10, ge=1, le=20, description="Zoom level"),
    cluster_mode: str = Query(
        "geographic", description="Clustering mode: geographic, price, size"
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
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """Get map clusters for a viewport with different clustering modes."""
    from api.services.map_clustering import cluster_properties
    from api.services.property_filtering import (
        build_property_query,
        get_latest_prices_in_date_range,
    )
    from sqlalchemy import func
    from models import PriceHistoryModel

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
        has_geocoding=has_geocoding,
        has_daft_data=has_daft_data,
    )

    # Price filtering (requires join with price_history)
    if min_price is not None or max_price is not None:
        # Get latest price for each property
        price_subquery = (
            db.query(
                PriceHistoryModel.property_id,
                func.max(PriceHistoryModel.price).label("latest_price"),
            )
            .group_by(PriceHistoryModel.property_id)
            .subquery()
        )
        query = query.join(
            price_subquery, PropertyModel.id == price_subquery.c.property_id
        )

        if min_price is not None:
            query = query.filter(price_subquery.c.latest_price >= min_price)
        if max_price is not None:
            query = query.filter(price_subquery.c.latest_price <= max_price)

    results = query.all()
    property_ids = [prop.id for prop, _ in results]

    # Get latest prices WITHIN the date range if dates are provided
    price_map = get_latest_prices_in_date_range(
        db=db,
        property_ids=property_ids,
        start_date=start_date,
        end_date=end_date,
    )

    # Prepare data for clustering
    properties_data = []
    for prop, address in results:
        # Get latest price from price_map (within date range if dates provided)
        price_data = price_map.get(prop.id)
        latest_price = price_data[0] if price_data else None

        properties_data.append(
            {
                "id": prop.id,
                "latitude": address.latitude,
                "longitude": address.longitude,
                "price": latest_price,
                "address": address.address,
                "county": address.county,
            }
        )

    # Cluster properties
    clusters = cluster_properties(properties_data, zoom, cluster_mode)

    return MapClustersResponse(
        clusters=clusters,
        total_properties=len(properties_data),
        viewport={"north": north, "south": south, "east": east, "west": west},
    )


@router.get("/analysis", response_model=MapAnalysisResponse)
async def get_map_analysis(
    north: float = Query(..., description="North boundary"),
    south: float = Query(..., description="South boundary"),
    east: float = Query(..., description="East boundary"),
    west: float = Query(..., description="West boundary"),
    analysis_mode: str = Query(
        ...,
        description="Analysis mode: spatial-patterns, hotspots, cluster-identification, growth-decline, price-heatmap, sales-heatmap",
    ),
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

    # Limit results to prevent timeout (max 10000 properties)
    # First, get count to check if we need to limit
    count = query.count()
    logger.info(f"Found {count} properties in viewport")

    if count > 10000:
        logger.warning(f"Too many properties ({count}), limiting to 10000")
        results = query.limit(10000).all()
    else:
        results = query.all()

    # Get all property IDs
    property_ids = [prop.id for prop, _ in results]

    # Optimize: Get latest price for all properties in a single query
    # If date filters are applied, get the latest price WITHIN the date range
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

        # Query for latest prices
        latest_prices_query = db.query(
            PriceHistoryModel.property_id,
            func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
        ).filter(PriceHistoryModel.property_id.in_(property_ids))

        # Apply date filter if provided - get latest price WITHIN the date range
        if price_date_filter:
            latest_prices_query = latest_prices_query.filter(and_(*price_date_filter))

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

        # Create a dict for fast lookup
        price_map = {pid: (price, date) for pid, price, date in latest_prices}
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
                "price": latest_price,
                "date": date_str,
                "address": address.address,
                "county": address.county,
            }
        )
    logger.info(f"Processed {len(properties_data)} properties")
    # Process based on analysis mode
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
                    "price": p.price,
                    "address": p.address,
                    "county": p.county,
                }
                for p in cluster.properties
            ]

            prices = [p.price for p in cluster.properties if p.price]
            avg_price = sum(prices) / len(prices) if prices else 0

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
            avg_price = sum(prices) / len(prices) if prices else 0
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
                                            "early_avg": round(early_avg, 2),
                                            "late_avg": round(late_avg, 2),
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
                                "avg_price": avg_price,
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
                        "price": p.price,
                        "address": p.address,
                        "county": p.county,
                    }
                    for p in c.properties
                ],
            }
            for c in clusters
        ]

    # Also return points for marker display
    response_data["points"] = properties_data[:1000]  # Limit points

    # Ensure viewport is a MapViewport dict
    response_data["viewport"] = {
        "north": north,
        "south": south,
        "east": east,
        "west": west,
    }

    return MapAnalysisResponse(**response_data)
