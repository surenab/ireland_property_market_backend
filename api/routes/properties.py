"""
Property routes for API.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, func
from datetime import datetime


from database import PropertyRepository, AddressRepository, PriceHistoryRepository
from models import PropertyModel, AddressModel, PriceHistoryModel
from api.schemas import (
    PropertyResponse,
    PropertyListItem,
    PriceHistoryResponse,
)
from dependencies import get_db

router = APIRouter()


@router.get("/", response_model=dict)
async def list_properties(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    county: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    has_geocoding: Optional[bool] = None,
    has_daft_data: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    """List properties with pagination and filtering."""
    PropertyRepository(db)
    AddressRepository(db)
    price_history_repo = PriceHistoryRepository(db)

    # Build base query with join to addresses
    # Start simple: just get properties that have addresses
    try:
        base_query = db.query(PropertyModel).join(
            AddressModel, PropertyModel.id == AddressModel.property_id
        )
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error building base query: {e}")
        raise

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

    # Get the property IDs for this page
    offset = (page - 1) * page_size
    property_ids = property_ids_query.offset(offset).limit(page_size).all()
    property_ids_list = [pid[0] for pid in property_ids]

    # Now query the actual properties with addresses loaded
    # Use order_by to maintain consistent ordering
    properties = (
        db.query(PropertyModel)
        .filter(PropertyModel.id.in_(property_ids_list))
        .options(joinedload(PropertyModel.address))
        .order_by(PropertyModel.id)
        .all()
    )

    # Build response
    items = []
    for prop in properties:
        # Access address - it should be loaded via joinedload
        address = prop.address

        # Get latest price - optimize by getting it in a single query if possible
        # For now, use the repository method
        price_history = price_history_repo.get_price_history_by_property(prop.id)
        latest_price = None
        latest_sale_date = None
        if price_history:
            latest = max(price_history, key=lambda x: x.date_of_sale)
            latest_price = latest.price
            # Convert date to string for API response
            if latest.date_of_sale:
                if isinstance(latest.date_of_sale, datetime):
                    latest_sale_date = latest.date_of_sale.date().strftime("%Y-%m-%d")
                elif hasattr(latest.date_of_sale, "strftime"):
                    latest_sale_date = latest.date_of_sale.strftime("%Y-%m-%d")
                else:
                    latest_sale_date = str(latest.date_of_sale)

        items.append(
            PropertyListItem(
                id=prop.id,
                address=address.address if address else None,
                county=address.county if address else None,
                latitude=address.latitude if address else None,
                longitude=address.longitude if address else None,
                latest_price=latest_price,
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
