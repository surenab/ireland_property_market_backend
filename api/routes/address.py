"""
Address-related API routes.
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import or_


from database import PriceHistoryRepository
from models import AddressModel, PropertyModel
from api.schemas import (
    PropertyListItem,
    AddressResponse,
    SearchResponse,
)
from dependencies import get_db

router = APIRouter()


@router.get("/search", response_model=dict)
async def search_addresses(
    q: str = Query(..., min_length=1, description="Search query (address or eircode)"),
    limit: int = Query(50, ge=1, le=100, description="Maximum results"),
    db: Session = Depends(get_db),
):
    """Search properties by address or eircode."""
    price_history_repo = PriceHistoryRepository(db)

    # Normalize search query
    query_lower = q.lower().strip()

    # Search in addresses and eircodes
    query = (
        db.query(PropertyModel, AddressModel)
        .join(AddressModel, PropertyModel.id == AddressModel.property_id)
        .filter(
            or_(
                AddressModel.address.ilike(f"%{query_lower}%"),
                AddressModel.eircode.ilike(f"%{query_lower}%"),
                AddressModel.county.ilike(f"%{query_lower}%"),
            )
        )
        .limit(limit)
    )

    results = query.all()

    items = []
    for prop, address in results:
        # Get latest price
        price_history = price_history_repo.get_price_history_by_property(prop.id)
        latest_price = None
        latest_sale_date = None
        if price_history:
            latest = max(price_history, key=lambda x: x.date_of_sale)
            latest_price = latest.price
            latest_sale_date = latest.date_of_sale

        items.append(
            PropertyListItem(
                id=prop.id,
                address=address.address,
                county=address.county,
                latitude=address.latitude,
                longitude=address.longitude,
                latest_price=latest_price,
                latest_sale_date=latest_sale_date,
            )
        )

    return SearchResponse(
        properties=items,
        total=len(items),
        query=q,
    )


@router.get("/autocomplete", response_model=List[str])
async def autocomplete_addresses(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Autocomplete suggestions for addresses, eircodes, and counties (partial matching)."""
    query_lower = q.lower().strip()

    # Get unique addresses matching query (partial match)
    addresses = (
        db.query(AddressModel.address)
        .filter(AddressModel.address.ilike(f"%{query_lower}%"))
        .distinct()
        .limit(limit // 3)  # Allocate 1/3 for addresses
        .all()
    )

    # Get unique eircodes matching query (partial match)
    eircodes = (
        db.query(AddressModel.eircode)
        .filter(
            AddressModel.eircode.isnot(None),
            AddressModel.eircode.ilike(f"%{query_lower}%"),
        )
        .distinct()
        .limit(limit // 3)  # Allocate 1/3 for eircodes
        .all()
    )

    # Get unique counties matching query (partial match)
    counties = (
        db.query(AddressModel.county)
        .filter(
            AddressModel.county.isnot(None),
            AddressModel.county.ilike(f"%{query_lower}%"),
        )
        .distinct()
        .limit(limit // 3)  # Allocate 1/3 for counties
        .all()
    )

    # Combine and return unique suggestions
    suggestions = set()
    suggestions.update([addr[0] for addr in addresses if addr[0]])
    suggestions.update([eir[0] for eir in eircodes if eir[0]])
    suggestions.update([county[0] for county in counties if county[0]])

    # Return sorted list, limited to requested limit
    return sorted(list(suggestions))[:limit]


@router.get("/counties", response_model=List[str])
async def list_counties(
    db: Session = Depends(get_db),
):
    """Get list of all counties."""
    counties = (
        db.query(AddressModel.county)
        .distinct()
        .filter(AddressModel.county.isnot(None))
        .order_by(AddressModel.county)
        .all()
    )
    return [county[0] for county in counties]


@router.get("/countries", response_model=List[str])
async def list_countries(
    db: Session = Depends(get_db),
):
    """Get list of all countries."""
    countries = (
        db.query(AddressModel.country)
        .distinct()
        .filter(AddressModel.country.isnot(None))
        .order_by(AddressModel.country)
        .all()
    )
    return [country[0] for country in countries]


@router.get("/", response_model=dict)
async def list_addresses(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    county: Optional[str] = Query(None, description="Filter by county"),
    has_geocoding: Optional[bool] = Query(
        None, description="Filter by geocoding status"
    ),
    db: Session = Depends(get_db),
):
    """List addresses with pagination and filtering."""
    # Build base query
    query = db.query(AddressModel)

    # Apply filters
    if county:
        query = query.filter(AddressModel.county == county)

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

    # Get total count
    total = query.count()

    # Apply pagination
    offset = (page - 1) * page_size
    addresses = query.offset(offset).limit(page_size).all()

    items = [
        AddressResponse(
            id=addr.id,
            address=addr.address,
            county=addr.county,
            eircode=addr.eircode,
            latitude=addr.latitude,
            longitude=addr.longitude,
            formatted_address=addr.formatted_address,
            country=addr.country,
            geocoded_at=addr.geocoded_at,
        )
        for addr in addresses
    ]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
    }


@router.get("/{address_id}", response_model=AddressResponse)
async def get_address(
    address_id: int,
    db: Session = Depends(get_db),
):
    """Get address details by ID."""
    address = db.query(AddressModel).filter(AddressModel.id == address_id).first()
    if not address:
        raise HTTPException(status_code=404, detail="Address not found")

    return AddressResponse(
        id=address.id,
        address=address.address,
        county=address.county,
        eircode=address.eircode,
        latitude=address.latitude,
        longitude=address.longitude,
        formatted_address=address.formatted_address,
        country=address.country,
        geocoded_at=address.geocoded_at,
    )
