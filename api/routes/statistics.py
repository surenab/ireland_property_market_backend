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
    CountyComparisonResponse,
    CountyStatistics,
    DatabaseStatsResponse,
    PriceDistributionResponse,
    PriceDistributionBucket,
)
from api.services import statistics
from dependencies import get_db
from api.cache import cached

router = APIRouter()


@router.get("/price-trends", response_model=PriceTrendsResponse)
@cached(ttl=300)  # Cache for 5 minutes
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
                "price": int(round(float(ph.price))) if ph.price is not None else 0,
            }
        )

    # Calculate trends
    trends_data = statistics.calculate_price_trends(history_data, period)

    return PriceTrendsResponse(
        trends=[PriceTrendPoint(**trend) for trend in trends_data],
        period=period,
    )


@router.get("/price-distribution", response_model=PriceDistributionResponse)
@cached(ttl=300)  # Cache for 5 minutes
async def get_price_distribution(
    county: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    has_geocoding: Optional[bool] = None,
    has_daft_data: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    """Get price distribution (histogram buckets) with optional filters."""
    from sqlalchemy import or_
    from datetime import datetime, timedelta

    query = (
        db.query(PriceHistoryModel.price)
        .join(PropertyModel, PriceHistoryModel.property_id == PropertyModel.id)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
    )
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
    if min_price is not None:
        query = query.filter(PriceHistoryModel.price >= min_price)
    if max_price is not None:
        query = query.filter(PriceHistoryModel.price <= max_price)

    rows = query.all()
    prices = [r[0] for r in rows if r[0] is not None]

    # Buckets: 0-50k, 50k-100k, 100k-150k, 150k-200k, 200k-250k, 250k-300k, 300k-400k, 400k-500k, 500k-750k, 750k-1M, 1M+
    edges = [
        (0, 50_000, "€0–50k"),
        (50_000, 100_000, "€50k–100k"),
        (100_000, 150_000, "€100k–150k"),
        (150_000, 200_000, "€150k–200k"),
        (200_000, 250_000, "€200k–250k"),
        (250_000, 300_000, "€250k–300k"),
        (300_000, 400_000, "€300k–400k"),
        (400_000, 500_000, "€400k–500k"),
        (500_000, 750_000, "€500k–750k"),
        (750_000, 1_000_000, "€750k–1M"),
        (1_000_000, float("inf"), "€1M+"),
    ]
    buckets = []
    for lo, hi, label in edges:
        count = sum(1 for p in prices if lo <= p < hi)
        buckets.append(
            PriceDistributionBucket(
                bucket_label=label,
                min_price=float(lo) if hi != float("inf") else lo,
                max_price=float(hi) if hi != float("inf") else 2_000_000,
                count=count,
            )
        )
    return PriceDistributionResponse(buckets=buckets)


@router.get("/county", response_model=CountyComparisonResponse)
@cached(ttl=300)  # Cache for 5 minutes
async def get_county_comparison(
    county: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    has_geocoding: Optional[bool] = None,
    has_daft_data: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    """Get county-level price comparison statistics with optional filters."""
    from sqlalchemy import or_
    from datetime import datetime, timedelta

    # Subquery: properties with addresses that have counties (and optional filters)
    properties_with_addresses = (
        db.query(PropertyModel.id)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
        .filter(AddressModel.county.isnot(None))
    )
    if county:
        properties_with_addresses = properties_with_addresses.filter(AddressModel.county == county)
    if has_geocoding is not None:
        if has_geocoding:
            properties_with_addresses = properties_with_addresses.filter(
                AddressModel.latitude.isnot(None), AddressModel.longitude.isnot(None)
            )
        else:
            properties_with_addresses = properties_with_addresses.filter(
                or_(AddressModel.latitude.is_(None), AddressModel.longitude.is_(None))
            )
    if has_daft_data is not None:
        if has_daft_data:
            properties_with_addresses = properties_with_addresses.filter(PropertyModel.daft_html.isnot(None))
        else:
            properties_with_addresses = properties_with_addresses.filter(PropertyModel.daft_html.is_(None))
    properties_with_addresses = properties_with_addresses.subquery()

    # Get latest date per property (optionally within date range)
    latest_dates_q = (
        db.query(
            PriceHistoryModel.property_id,
            func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
        )
        .join(
            properties_with_addresses,
            PriceHistoryModel.property_id == properties_with_addresses.c.id,
        )
    )
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            latest_dates_q = latest_dates_q.filter(PriceHistoryModel.date_of_sale >= start_dt)
        except ValueError:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
            latest_dates_q = latest_dates_q.filter(PriceHistoryModel.date_of_sale < end_dt)
        except ValueError:
            pass
    latest_dates = latest_dates_q.group_by(PriceHistoryModel.property_id).subquery()

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

    # Build properties data list (apply price filters)
    properties_list = []
    for county, price in results:
        if county and price is not None:
            if min_price is not None and price < min_price:
                continue
            if max_price is not None and price > max_price:
                continue
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
    overall_average = int(round(sum(all_prices) / len(all_prices))) if all_prices else 0
    overall_median = (
        int(round(sorted(all_prices)[len(all_prices) // 2])) if all_prices else 0
    )

    return CountyComparisonResponse(
        counties=[CountyStatistics(**stat) for stat in county_stats],
        overall_average=overall_average,
        overall_median=overall_median,
    )


@router.get("/db-stats", response_model=DatabaseStatsResponse)
@cached(ttl=60)  # Cache for 1 minute (stats don't change frequently)
async def get_database_stats(
    db: Session = Depends(get_db),
):
    """Get database statistics: total addresses, properties, and price history records."""
    from sqlalchemy import func

    # Count total addresses
    total_addresses = db.query(func.count(AddressModel.id)).scalar() or 0

    # Count total properties
    total_properties = db.query(func.count(PropertyModel.id)).scalar() or 0

    # Count total price history records
    total_price_history = db.query(func.count(PriceHistoryModel.id)).scalar() or 0

    return DatabaseStatsResponse(
        total_addresses=total_addresses,
        total_properties=total_properties,
        total_price_history=total_price_history,
    )
