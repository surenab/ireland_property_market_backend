"""
Property routes for API.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, and_, func
from datetime import datetime


from database import PropertyRepository, AddressRepository, PriceHistoryRepository
from models import (
    PropertyModel,
    AddressModel,
    PriceHistoryModel,
    generate_address_hash,
    parse_date,
)
from api.schemas import (
    PropertyResponse,
    PropertyListItem,
    PriceHistoryResponse,
    BulkUploadRequest,
    BulkUploadResponse,
    BulkUploadResult,
)
from dependencies import get_db
from api.cache import cached
from api.services.property_filtering import get_latest_prices_in_date_range

router = APIRouter()


@router.get("/", response_model=dict)
@cached(ttl=300)  # Cache for 5 minutes
async def list_properties(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    county: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    has_geocoding: Optional[bool] = None,
    has_daft_data: Optional[bool] = None,
    min_sales: Optional[int] = Query(None, ge=1, description="Minimum number of price history entries (e.g. 2 = at least 2 sales)"),
    sort: Optional[str] = Query("default", description="Sort: default (id), price_asc, price_desc, date_desc"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """List properties with pagination and filtering."""
    from datetime import timedelta

    PropertyRepository(db)
    AddressRepository(db)

    # Build base query with join to addresses
    try:
        base_query = db.query(PropertyModel).join(
            AddressModel, PropertyModel.id == AddressModel.property_id
        )
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error building base query: {e}")
        raise

    # Date range filter: properties that have at least one sale in range
    if start_date or end_date:
        date_filters = []
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
                date_filters.append(PriceHistoryModel.date_of_sale >= start_dt)
            except ValueError:
                pass
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(
                    days=1
                )
                date_filters.append(PriceHistoryModel.date_of_sale < end_dt)
            except ValueError:
                pass
        if date_filters:
            props_in_range = (
                db.query(PriceHistoryModel.property_id)
                .filter(and_(*date_filters))
                .distinct()
                .subquery()
            )
            base_query = base_query.join(
                props_in_range, PropertyModel.id == props_in_range.c.property_id
            )

    # Apply filters
    if county:
        base_query = base_query.filter(AddressModel.county == county)

    if has_geocoding is not None:
        if has_geocoding:
            base_query = base_query.filter(
                AddressModel.latitude.isnot(None), AddressModel.longitude.isnot(None)
            )
        else:
            base_query = base_query.filter(
                or_(AddressModel.latitude.is_(None), AddressModel.longitude.is_(None))
            )

    if has_daft_data is not None:
        if has_daft_data:
            base_query = base_query.filter(PropertyModel.daft_html.isnot(None))
        else:
            base_query = base_query.filter(PropertyModel.daft_html.is_(None))

    # Min sales: properties with at least N price history entries
    if min_sales is not None and min_sales >= 1:
        sales_count_subquery = (
            db.query(PriceHistoryModel.property_id)
            .group_by(PriceHistoryModel.property_id)
            .having(func.count(PriceHistoryModel.id) >= min_sales)
            .subquery()
        )
        base_query = base_query.join(
            sales_count_subquery, PropertyModel.id == sales_count_subquery.c.property_id
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
        base_query = base_query.join(
            price_subquery, PropertyModel.id == price_subquery.c.property_id
        )

        if min_price is not None:
            base_query = base_query.filter(price_subquery.c.latest_price >= min_price)
        if max_price is not None:
            base_query = base_query.filter(price_subquery.c.latest_price <= max_price)

    # Get distinct property IDs that match filters
    property_ids_query = base_query.with_entities(PropertyModel.id).distinct()

    # Get total count - use subquery to avoid issues with count() on distinct queries
    count_subquery = property_ids_query.subquery()
    total = db.query(func.count()).select_from(count_subquery).scalar() or 0

    if total == 0:
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 0,
        }

    offset = (page - 1) * page_size

    # Apply sort: join with latest price/date subquery and order
    if sort in ("price_asc", "price_desc", "date_desc"):
        latest_date_subq = (
            db.query(
                PriceHistoryModel.property_id,
                func.max(PriceHistoryModel.date_of_sale).label("latest_date"),
            )
            .group_by(PriceHistoryModel.property_id)
            .subquery()
        )
        sort_subquery = (
            db.query(
                PriceHistoryModel.property_id,
                func.max(PriceHistoryModel.price).label("latest_price"),
                latest_date_subq.c.latest_date,
            )
            .join(
                latest_date_subq,
                and_(
                    PriceHistoryModel.property_id == latest_date_subq.c.property_id,
                    PriceHistoryModel.date_of_sale == latest_date_subq.c.latest_date,
                ),
            )
            .group_by(PriceHistoryModel.property_id, latest_date_subq.c.latest_date)
            .subquery()
        )
        ordered_query = base_query.join(
            sort_subquery, PropertyModel.id == sort_subquery.c.property_id
        ).with_entities(PropertyModel.id)
        if sort == "price_asc":
            ordered_query = ordered_query.order_by(sort_subquery.c.latest_price.asc())
        elif sort == "price_desc":
            ordered_query = ordered_query.order_by(sort_subquery.c.latest_price.desc())
        else:
            ordered_query = ordered_query.order_by(sort_subquery.c.latest_date.desc())
        property_ids = ordered_query.offset(offset).limit(page_size).all()
    else:
        property_ids = (
            property_ids_query.order_by(PropertyModel.id)
            .offset(offset)
            .limit(page_size)
            .all()
        )

    property_ids_list = [pid[0] for pid in property_ids]

    # Batch-fetch latest (price, date) for all property IDs in one go (avoids N+1)
    latest_prices_map = get_latest_prices_in_date_range(
        db, property_ids_list, start_date, end_date
    )

    # Load properties with addresses (single query)
    properties = (
        db.query(PropertyModel)
        .filter(PropertyModel.id.in_(property_ids_list))
        .options(joinedload(PropertyModel.address))
        .all()
    )
    id_to_prop = {p.id: p for p in properties}

    # Build response in sort order using property_ids_list
    items = []
    for pid in property_ids_list:
        prop = id_to_prop.get(pid)
        if not prop:
            continue
        address = prop.address
        latest_price, latest_sale_date = latest_prices_map.get(pid, (None, None))
        items.append(
            PropertyListItem(
                id=prop.id,
                address=address.address if address else None,
                county=address.county if address else None,
                latitude=address.latitude if address else None,
                longitude=address.longitude if address else None,
                latest_price=int(round(latest_price)) if latest_price is not None else None,
                latest_sale_date=latest_sale_date,
            )
        )

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
    }


@router.get("/{property_id}", response_model=PropertyResponse)
@cached(ttl=300)  # Cache for 5 minutes
async def get_property(
    property_id: int,
    db: Session = Depends(get_db),
):
    """Get property details with address and price history."""
    property_repo = PropertyRepository(db)
    price_history_repo = PriceHistoryRepository(db)

    property_obj = property_repo.get_property_by_id(property_id)
    if not property_obj:
        raise HTTPException(status_code=404, detail="Property not found")

    # Get price history
    price_history = price_history_repo.get_price_history_by_property(property_id)
    price_history_list = [PriceHistoryResponse.from_orm(ph) for ph in price_history]

    # Get address
    address = None
    if property_obj.address:
        from api.schemas import AddressResponse

        address = AddressResponse(
            id=property_obj.address.id,
            address=property_obj.address.address,
            county=property_obj.address.county,
            eircode=property_obj.address.eircode,
            latitude=property_obj.address.latitude,
            longitude=property_obj.address.longitude,
            formatted_address=property_obj.address.formatted_address,
            country=property_obj.address.country,
            geocoded_at=property_obj.address.geocoded_at,
        )

    return PropertyResponse(
        id=property_obj.id,
        created_at=property_obj.created_at,
        updated_at=property_obj.updated_at,
        daft_url=property_obj.daft_url,
        daft_html=property_obj.daft_html,
        daft_title=property_obj.daft_title,
        daft_body=property_obj.daft_body,
        daft_scraped=property_obj.daft_scraped,
        daft_scraped_at=property_obj.daft_scraped_at,
        address=address,
        price_history=price_history_list,
    )


@router.get("/{property_id}/history", response_model=List[PriceHistoryResponse])
@cached(ttl=300)  # Cache for 5 minutes
async def get_property_history(
    property_id: int,
    db: Session = Depends(get_db),
):
    """Get price history for a property."""
    property_repo = PropertyRepository(db)
    price_history_repo = PriceHistoryRepository(db)

    property_obj = property_repo.get_property_by_id(property_id)
    if not property_obj:
        raise HTTPException(status_code=404, detail="Property not found")

    price_history = price_history_repo.get_price_history_by_property(property_id)

    return [
        PriceHistoryResponse.from_orm(ph)
        for ph in sorted(price_history, key=lambda x: x.date_of_sale)
    ]


@router.post("/bulk-upload", response_model=BulkUploadResponse)
async def bulk_upload_properties(
    request: BulkUploadRequest,
    db: Session = Depends(get_db),
):
    """Bulk upload properties with create or update logic.

    For each property:
    - If address/eircode exists, update the property
    - If not, create a new property
    - Always update/add price history records
    """
    property_repo = PropertyRepository(db)
    address_repo = AddressRepository(db)
    price_history_repo = PriceHistoryRepository(db)

    results = []
    created_count = 0
    updated_count = 0
    failed_count = 0

    for prop_data in request.properties:
        try:
            # Find existing address by address hash or eircode
            address_hash = generate_address_hash(
                prop_data.address.address,
                prop_data.address.county,
                prop_data.address.eircode,
            )
            existing_address = address_repo.find_by_hash(address_hash)

            if existing_address:
                # Update existing property
                property_obj = property_repo.get_property_by_id(
                    existing_address.property_id
                )
                if not property_obj:
                    results.append(
                        BulkUploadResult(
                            success=False,
                            message=f"Property not found for address {existing_address.id}",
                            address=prop_data.address.address,
                        )
                    )
                    failed_count += 1
                    continue

                # Update property daft data if provided
                if prop_data.daft_url is not None:
                    property_obj.daft_url = prop_data.daft_url
                if prop_data.daft_html is not None:
                    property_obj.daft_html = prop_data.daft_html
                if prop_data.daft_title is not None:
                    property_obj.daft_title = prop_data.daft_title
                if prop_data.daft_body is not None:
                    property_obj.daft_body = prop_data.daft_body
                if prop_data.daft_scraped is not None:
                    property_obj.daft_scraped = prop_data.daft_scraped
                    if prop_data.daft_scraped:
                        property_obj.daft_scraped_at = datetime.utcnow()

                property_obj.updated_at = datetime.utcnow()
                db.flush()

                # Update address geo data if provided
                if (
                    prop_data.address.latitude is not None
                    and prop_data.address.longitude is not None
                ):
                    address_repo.update_geo_data(
                        existing_address.id,
                        prop_data.address.latitude,
                        prop_data.address.longitude,
                        prop_data.address.formatted_address,
                        prop_data.address.country,
                    )

                property_id = property_obj.id
                updated_count += 1
                action = "updated"
            else:
                # Create new property
                property_obj = property_repo.get_or_create_property()

                # Set daft data
                if prop_data.daft_url is not None:
                    property_obj.daft_url = prop_data.daft_url
                if prop_data.daft_html is not None:
                    property_obj.daft_html = prop_data.daft_html
                if prop_data.daft_title is not None:
                    property_obj.daft_title = prop_data.daft_title
                if prop_data.daft_body is not None:
                    property_obj.daft_body = prop_data.daft_body
                property_obj.daft_scraped = prop_data.daft_scraped
                if prop_data.daft_scraped:
                    property_obj.daft_scraped_at = datetime.utcnow()

                db.flush()
                property_id = property_obj.id

                # Create address
                address_obj = address_repo.create_address(
                    property_id=property_id,
                    address=prop_data.address.address,
                    county=prop_data.address.county,
                    eircode=prop_data.address.eircode,
                    address_hash=address_hash,
                )

                # Set geo data if provided
                if (
                    prop_data.address.latitude is not None
                    and prop_data.address.longitude is not None
                ):
                    address_repo.update_geo_data(
                        address_obj.id,
                        prop_data.address.latitude,
                        prop_data.address.longitude,
                        prop_data.address.formatted_address,
                        prop_data.address.country,
                    )

                created_count += 1
                action = "created"

            # Add/update price history
            for price_data in prop_data.price_history:
                # Parse date
                sale_date = parse_date(price_data.date_of_sale)
                if not sale_date:
                    # Try parsing as YYYY-MM-DD format
                    try:
                        sale_date = datetime.strptime(
                            price_data.date_of_sale, "%Y-%m-%d"
                        ).date()
                    except ValueError:
                        continue

                # Check if price history already exists for this date
                existing_history = (
                    db.query(PriceHistoryModel)
                    .filter(
                        PriceHistoryModel.property_id == property_id,
                        PriceHistoryModel.date_of_sale == sale_date,
                    )
                    .first()
                )

                if existing_history:
                    # Update existing price history (coerce to int for DB)
                    existing_history.price = int(round(float(price_data.price))) if price_data.price is not None else 0
                    existing_history.not_full_market_price = (
                        price_data.not_full_market_price
                    )
                    existing_history.vat_exclusive = price_data.vat_exclusive
                    existing_history.description = price_data.description
                    existing_history.property_size_description = (
                        price_data.property_size_description
                    )
                else:
                    # Create new price history (coerce to int for DB)
                    price_history_repo.create_price_history(
                        property_id=property_id,
                        date_of_sale=sale_date,
                        price=int(round(float(price_data.price))) if price_data.price is not None else 0,
                        not_full_market_price=price_data.not_full_market_price,
                        vat_exclusive=price_data.vat_exclusive,
                        description=price_data.description,
                        property_size_description=price_data.property_size_description,
                    )

            db.commit()

            results.append(
                BulkUploadResult(
                    success=True,
                    property_id=property_id,
                    message=f"Property {action} successfully",
                    address=prop_data.address.address,
                )
            )
        except Exception as e:
            db.rollback()
            results.append(
                BulkUploadResult(
                    success=False,
                    message=f"Error: {str(e)}",
                    address=prop_data.address.address,
                )
            )
            failed_count += 1

    return BulkUploadResponse(
        total=len(request.properties),
        created=created_count,
        updated=updated_count,
        failed=failed_count,
        results=results,
    )
