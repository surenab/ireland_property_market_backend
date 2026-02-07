"""
Address-related API routes.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List
from sqlalchemy.orm import Session


from models import AddressModel
from api.schemas import (
    AddressResponse,
)
from dependencies import get_db
from api.cache import cached

router = APIRouter()


@router.get("/counties", response_model=List[str])
@cached(ttl=300)  # Cache for 5 minutes
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
@cached(ttl=300)  # Cache for 5 minutes
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


@router.get("/{address_id}", response_model=AddressResponse)
@cached(ttl=300)  # Cache for 5 minutes
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
