"""
Utility functions for filtering properties by various criteria.
"""

from typing import Optional, List
from sqlalchemy.orm import Session, Query
from sqlalchemy import and_, func
from datetime import datetime, timedelta
from datetime import date as date_type

from models import PropertyModel, AddressModel, PriceHistoryModel


def filter_properties_by_date_range(
    query: Query,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Query:
    """
    Filter properties query to only include properties with sales within the date range.

    Args:
        query: SQLAlchemy query for PropertyModel and AddressModel
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (inclusive)

    Returns:
        Modified query with date filtering applied
    """
    if not start_date and not end_date:
        return query

    # Build date filter for price history
    # date_of_sale is now stored as Date, so we convert string inputs to date objects
    date_filter = []

    if start_date:
        try:
            # Parse string date to datetime object
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            date_filter.append(PriceHistoryModel.date_of_sale >= start_dt)
        except ValueError:
            pass

    if end_date:
        try:
            # Parse string date to datetime object and add one day to include the end date
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
            date_filter.append(PriceHistoryModel.date_of_sale < end_dt)
        except ValueError:
            pass

    if date_filter:
        # Find properties that have sales within the date range
        price_subquery = (
            query.session.query(PriceHistoryModel.property_id)
            .filter(and_(*date_filter))
            .distinct()
            .subquery()
        )

        # Filter main query to only include properties with sales in date range
        query = query.join(
            price_subquery, PropertyModel.id == price_subquery.c.property_id
        )

    return query


def get_latest_prices_in_date_range(
    db: Session,
    property_ids: List[int],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    Get latest prices for properties within a date range.

    Args:
        db: Database session
        property_ids: List of property IDs to get prices for
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (inclusive)

    Returns:
        Dictionary mapping property_id to (price, date) tuple
    """
    if not property_ids:
        return {}

    # Build date filter for price history if dates are provided
    # date_of_sale is now stored as Date, so we convert string inputs to date objects
    price_date_filter = []

    if start_date:
        try:
            # Parse string date to date object
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            price_date_filter.append(PriceHistoryModel.date_of_sale >= start_dt)
        except ValueError:
            pass

    if end_date:
        try:
            # Parse string date to date object and add one day to include the end date
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
            price_date_filter.append(PriceHistoryModel.date_of_sale < end_dt)
        except ValueError:
            pass

    # Batch queries to avoid SQL parameter limits (typically 1000-2000)
    # Process in chunks of 1000 to be safe
    BATCH_SIZE = 10000
    result = {}

    for i in range(0, len(property_ids), BATCH_SIZE):
        batch_ids = property_ids[i : i + BATCH_SIZE]

        # Query for latest prices for this batch
        latest_prices_query = db.query(
            PriceHistoryModel.property_id,
            func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
        ).filter(PriceHistoryModel.property_id.in_(batch_ids))

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

        # Add results to dict (coerce price to int for whole euros)
        for pid, price, sale_date in latest_prices:
            date_str = None
            if sale_date:
                if isinstance(sale_date, date_type):
                    date_str = sale_date.strftime("%Y-%m-%d")
                elif isinstance(sale_date, datetime):
                    date_str = sale_date.date().strftime("%Y-%m-%d")
                elif isinstance(sale_date, str):
                    date_str = sale_date
            price_int = int(round(float(price))) if price is not None else 0
            result[pid] = (price_int, date_str)

    return result


def build_property_query(
    db: Session,
    north: float,
    south: float,
    east: float,
    west: float,
    county: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    has_geocoding: Optional[bool] = None,
    has_daft_data: Optional[bool] = None,
    min_sales: Optional[int] = None,
) -> Query:
    """
    Build a base query for properties within a viewport with optional filters.

    Args:
        db: Database session
        north: North boundary latitude
        south: South boundary latitude
        east: East boundary longitude
        west: West boundary longitude
        county: Optional county filter
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        min_price: Optional minimum price filter
        max_price: Optional maximum price filter
        has_geocoding: Optional geocoding status filter
        has_daft_data: Optional Daft.ie data availability filter
        min_sales: Optional minimum number of price history entries (e.g. 2 = at least 2 sales)

    Returns:
        SQLAlchemy query for PropertyModel and AddressModel
    """
    from sqlalchemy import or_, func

    # Base query for properties within viewport
    query = (
        db.query(PropertyModel, AddressModel)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
        .filter(
            AddressModel.latitude.isnot(None),
            AddressModel.longitude.isnot(None),
            AddressModel.latitude >= south,
            AddressModel.latitude <= north,
            AddressModel.longitude >= west,
            AddressModel.longitude <= east,
        )
    )

    # Apply county filter
    if county:
        query = query.filter(AddressModel.county == county)

    # Apply geocoding filter
    if has_geocoding is not None:
        if has_geocoding:
            query = query.filter(
                AddressModel.latitude.isnot(None),
                AddressModel.longitude.isnot(None),
            )
        else:
            query = query.filter(
                or_(
                    AddressModel.latitude.is_(None),
                    AddressModel.longitude.is_(None),
                )
            )

    # Apply Daft.ie data filter
    if has_daft_data is not None:
        if has_daft_data:
            query = query.filter(PropertyModel.daft_html.isnot(None))
        else:
            query = query.filter(PropertyModel.daft_html.is_(None))

    # Apply date range filter
    query = filter_properties_by_date_range(query, start_date, end_date)

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

    # Min sales: properties with at least N price history entries
    if min_sales is not None and min_sales >= 1:
        sales_count_subquery = (
            db.query(PriceHistoryModel.property_id)
            .group_by(PriceHistoryModel.property_id)
            .having(func.count(PriceHistoryModel.id) >= min_sales)
            .subquery()
        )
        query = query.join(
            sales_count_subquery, PropertyModel.id == sales_count_subquery.c.property_id
        )

    return query
