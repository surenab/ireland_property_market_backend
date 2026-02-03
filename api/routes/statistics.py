"""
Statistics routes for property data analysis.
"""

from fastapi import APIRouter, Query, Depends
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from models import PropertyModel, AddressModel, PriceHistoryModel
from api.schemas import (
    PriceTrendsResponse,
    PriceTrendPoint,
    ClustersResponse,
    PriceCluster,
    CountyComparisonResponse,
    CountyStatistics,
    CorrelationResponse,
)
from api.services import statistics
from dependencies import get_db

router = APIRouter()


@router.get("/price-trends", response_model=PriceTrendsResponse)
async def get_price_trends(
    period: str = Query("monthly", pattern="^(monthly|quarterly|yearly)$"),
    county: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    has_geocoding: Optional[bool] = None,
    has_daft_data: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    """Get price trends over time with filters."""
    from sqlalchemy import or_
    from datetime import datetime, timedelta
    from datetime import date as date_type

    # Build base query with joins
    query = (
        db.query(PriceHistoryModel)
        .join(PropertyModel, PriceHistoryModel.property_id == PropertyModel.id)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
    )

    # Apply filters
    if county:
        query = query.filter(AddressModel.county == county)

    if has_geocoding is not None:
        if has_geocoding:
            query = query.filter(
                AddressModel.latitude.isnot(None), AddressModel.longitude.isnot(None)
            )
        else:
            query = query.filter(
                or_(AddressModel.latitude.is_(None), AddressModel.longitude.is_(None))
            )

    if has_daft_data is not None:
        if has_daft_data:
            query = query.filter(PropertyModel.daft_html.isnot(None))
        else:
            query = query.filter(PropertyModel.daft_html.is_(None))

    # Apply date filters
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            query = query.filter(PriceHistoryModel.date_of_sale >= start_dt)
        except ValueError:
            pass

    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
            query = query.filter(PriceHistoryModel.date_of_sale < end_dt)
        except ValueError:
            pass

    # Apply price filters
    if min_price is not None:
        query = query.filter(PriceHistoryModel.price >= min_price)
    if max_price is not None:
        query = query.filter(PriceHistoryModel.price <= max_price)

    price_history_records = query.all()

    # Convert to list of dicts
    # Convert date objects to strings for statistics service
    history_data = []
    for ph in price_history_records:
        date_str = ""
        if ph.date_of_sale:
            if isinstance(ph.date_of_sale, datetime):
                date_str = ph.date_of_sale.date().strftime("%d/%m/%Y")
            elif isinstance(ph.date_of_sale, date_type):
                date_str = ph.date_of_sale.strftime("%d/%m/%Y")
            elif hasattr(ph.date_of_sale, "strftime"):
                date_str = ph.date_of_sale.strftime("%d/%m/%Y")
            else:
                date_str = str(ph.date_of_sale)

        history_data.append(
            {
                "date_of_sale": date_str,
                "price": ph.price,
            }
        )

    # Calculate trends
    trends_data = statistics.calculate_price_trends(history_data, period)

    return PriceTrendsResponse(
        trends=[PriceTrendPoint(**trend) for trend in trends_data],
        period=period,
    )


@router.get("/clusters", response_model=ClustersResponse)
async def get_price_clusters(
    n_clusters: int = Query(5, ge=2, le=20),
    algorithm: str = Query("kmeans", pattern="^(kmeans|dbscan)$"),
    county: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Get price clustering analysis."""

    # Build query
    query = db.query(PriceHistoryModel)

    if county:
        query = (
            query.join(PropertyModel)
            .join(AddressModel)
            .filter(AddressModel.county == county)
        )

    price_history_records = query.all()

    # Get latest price for each property
    property_prices = {}
    for ph in price_history_records:
        prop_id = ph.property_id
        if prop_id not in property_prices:
            property_prices[prop_id] = []
        property_prices[prop_id].append((ph.date_of_sale, ph.price))

    # Get latest price per property
    prices = []
    for prop_id, price_list in property_prices.items():
        latest = max(price_list, key=lambda x: x[0])
        prices.append(latest[1])

    # Calculate clusters
    clusters_data = statistics.calculate_price_clusters(
        prices, n_clusters=n_clusters, algorithm=algorithm
    )

    return ClustersResponse(
        clusters=[PriceCluster(**cluster) for cluster in clusters_data],
        algorithm=algorithm,
        n_clusters=len(clusters_data),
    )


@router.get("/county", response_model=CountyComparisonResponse)
async def get_county_comparison(
    db: Session = Depends(get_db),
):
    """Get county-level price comparison statistics."""
    # Use a single optimized query with subqueries to avoid IN clause limits
    # Get latest price per property using joins instead of IN clauses

    # Subquery to get properties with addresses that have counties
    properties_with_addresses = (
        db.query(PropertyModel.id)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
        .filter(AddressModel.county.isnot(None))
        .subquery()
    )

    # Get latest date per property (for properties that have addresses)
    # Use join instead of IN to avoid SQLite parameter limits
    latest_dates = (
        db.query(
            PriceHistoryModel.property_id,
            func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
        )
        .join(
            properties_with_addresses,
            PriceHistoryModel.property_id == properties_with_addresses.c.id,
        )
        .group_by(PriceHistoryModel.property_id)
        .subquery()
    )

    # Get latest prices by joining with latest dates
    latest_prices = (
        db.query(PriceHistoryModel.property_id, PriceHistoryModel.price)
        .join(
            latest_dates,
            and_(
                PriceHistoryModel.property_id == latest_dates.c.property_id,
                PriceHistoryModel.date_of_sale == latest_dates.c.latest_date,
            ),
        )
        .subquery()
    )

    # Final query: get properties with addresses and their latest prices
    # Use a single query with joins instead of IN clauses
    results = (
        db.query(AddressModel.county, latest_prices.c.price)
        .join(PropertyModel, AddressModel.property_id == PropertyModel.id)
        .join(latest_prices, PropertyModel.id == latest_prices.c.property_id)
        .filter(AddressModel.county.isnot(None))
        .all()
    )

    # Build properties data list
    properties_list = []
    for county, price in results:
        if county and price is not None:
            properties_list.append(
                {
                    "county": county,
                    "price": price,
                }
            )

    # Calculate statistics
    county_stats = statistics.calculate_county_statistics(properties_list)

    # Calculate overall statistics
    all_prices = [p["price"] for p in properties_list]
    overall_average = float(sum(all_prices) / len(all_prices)) if all_prices else 0.0
    overall_median = (
        float(sorted(all_prices)[len(all_prices) // 2]) if all_prices else 0.0
    )

    return CountyComparisonResponse(
        counties=[CountyStatistics(**stat) for stat in county_stats],
        overall_average=overall_average,
        overall_median=overall_median,
    )


@router.get("/correlation", response_model=CorrelationResponse)
async def get_correlation(
    variable: str = Query("size", pattern="^(size|date)$"),
    db: Session = Depends(get_db),
):
    """Get correlation analysis between price and another variable."""
    # Get all price history
    price_history = db.query(PriceHistoryModel).all()

    prices = []
    x_values = []

    for ph in price_history:
        prices.append(ph.price)

        if variable == "size":
            # Extract size from property_size_description
            size_desc = ph.property_size_description or ""
            # Try to extract numeric size (simplified)
            size_value = None
            if "38" in size_desc and "125" in size_desc:
                size_value = 81.5  # Midpoint
            elif "less than 38" in size_desc.lower():
                size_value = 19.0  # Midpoint of < 38
            elif "greater than 125" in size_desc.lower() or "125" in size_desc:
                size_value = 150.0  # Estimate
            else:
                size_value = None

            x_values.append(size_value if size_value else float("nan"))

        elif variable == "date":
            # Use date as numeric (days since epoch)
            from datetime import datetime

            # Handle date object or string
            if isinstance(ph.date_of_sale, datetime):
                date_obj = ph.date_of_sale
            elif hasattr(ph.date_of_sale, "strftime"):
                # date object
                date_obj = datetime.combine(ph.date_of_sale, datetime.min.time())
            else:
                # Try to parse string
                date_obj = datetime.strptime(str(ph.date_of_sale), "%d/%m/%Y")

            x_values.append(date_obj.timestamp())

    # Calculate correlation
    correlation_data = statistics.calculate_correlation(x_values, prices)

    return CorrelationResponse(**correlation_data)


@router.get("/date-range")
async def get_date_range(
    db: Session = Depends(get_db),
):
    """Get the minimum and maximum dates from price history."""
    from sqlalchemy import func

    # Get min and max dates from price history
    result = (
        db.query(
            func.min(PriceHistoryModel.date_of_sale).label("min_date"),
            func.max(PriceHistoryModel.date_of_sale).label("max_date"),
        )
        .filter(PriceHistoryModel.date_of_sale.isnot(None))
        .first()
    )

    if result and result.min_date and result.max_date:
        min_year = (
            result.min_date.year
            if hasattr(result.min_date, "year")
            else int(str(result.min_date)[:4])
        )
        max_year = (
            result.max_date.year
            if hasattr(result.max_date, "year")
            else int(str(result.max_date)[:4])
        )

        return {
            "min_year": min_year,
            "max_year": max_year,
            "min_date": (
                result.min_date.strftime("%Y-%m-%d")
                if hasattr(result.min_date, "strftime")
                else str(result.min_date)
            ),
            "max_date": (
                result.max_date.strftime("%Y-%m-%d")
                if hasattr(result.max_date, "strftime")
                else str(result.max_date)
            ),
        }

    # Fallback to current year if no data
    from datetime import datetime

    current_year = datetime.now().year
    return {
        "min_year": current_year - 1,
        "max_year": current_year,
        "min_date": f"{current_year - 1}-01-01",
        "max_date": f"{current_year}-12-31",
    }
