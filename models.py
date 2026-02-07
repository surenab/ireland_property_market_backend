"""
Pydantic models and SQLAlchemy ORM models for Property, Address, and PriceHistory.
"""

from datetime import datetime, date
from typing import Optional
import hashlib

from pydantic import BaseModel
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    JSON,
    Date,
    Index,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# ============================================================================
# Pydantic Models
# ============================================================================


class PropertyBase(BaseModel):
    """Base property model."""

    pass


class PropertyCreate(PropertyBase):
    """Property creation model."""

    pass


class Property(PropertyBase):
    """Property model with ID."""

    id: int
    created_at: datetime
    updated_at: datetime
    daft_url: Optional[str] = None
    daft_html: Optional[str] = None
    daft_title: Optional[str] = None
    daft_body: Optional[str] = None
    daft_scraped: bool = False
    daft_scraped_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class AddressBase(BaseModel):
    """Base address model."""

    address: str
    county: str
    eircode: Optional[str] = None


class AddressCreate(AddressBase):
    """Address creation model."""

    pass


class Address(AddressBase):
    """Address model with geo data."""

    id: int
    property_id: int
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    formatted_address: Optional[str] = None
    country: Optional[str] = None
    raw_geo_data: Optional[dict] = None
    geocoded_at: Optional[datetime] = None
    address_hash: Optional[str] = None

    class Config:
        from_attributes = True


class PriceHistoryBase(BaseModel):
    """Base price history model."""

    date_of_sale: datetime
    price: float
    not_full_market_price: bool
    vat_exclusive: bool
    description: str
    property_size_description: Optional[str] = None


class PriceHistoryCreate(PriceHistoryBase):
    """Price history creation model."""

    property_id: int


class PriceHistory(PriceHistoryBase):
    """Price history model with ID."""

    id: int
    property_id: int

    class Config:
        from_attributes = True


# ============================================================================
# SQLAlchemy ORM Models
# ============================================================================


class PropertyModel(Base):
    """SQLAlchemy model for Property."""

    __tablename__ = "properties"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    daft_url = Column(String, nullable=True)
    daft_html = Column(Text, nullable=True)
    daft_title = Column(String, nullable=True)
    daft_body = Column(Text, nullable=True)
    daft_scraped = Column(Boolean, default=False, nullable=False)
    daft_scraped_at = Column(DateTime, nullable=True)

    # Relationships
    address = relationship("AddressModel", back_populates="property", uselist=False)
    price_history = relationship(
        "PriceHistoryModel", back_populates="property", cascade="all, delete-orphan"
    )


class AddressModel(Base):
    """SQLAlchemy model for Address."""

    __tablename__ = "addresses"
    __table_args__ = (
        Index("idx_lat_lng", "latitude", "longitude"),
        Index("idx_county", "county"),
    )

    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(
        Integer, ForeignKey("properties.id"), unique=True, nullable=False, index=True
    )
    address = Column(String, nullable=False, index=True)
    county = Column(String, nullable=False)
    eircode = Column(String, nullable=True, index=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    formatted_address = Column(Text, nullable=True)
    country = Column(String, nullable=True)
    raw_geo_data = Column(JSON, nullable=True)
    geocoded_at = Column(DateTime, nullable=True)
    address_hash = Column(String, nullable=True, index=True)

    # Relationships
    property = relationship("PropertyModel", back_populates="address")


class PriceHistoryModel(Base):
    """SQLAlchemy model for PriceHistory."""

    __tablename__ = "price_history"
    __table_args__ = (
        # Speeds up sort-by-price/date: GROUP BY property_id, MAX(date_of_sale) and joins on (property_id, date_of_sale)
        Index("idx_price_history_property_date", "property_id", "date_of_sale"),
    )

    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(
        Integer, ForeignKey("properties.id"), nullable=False, index=True
    )
    date_of_sale = Column(Date, nullable=False, index=True)
    price = Column(Float, nullable=False)
    not_full_market_price = Column(Boolean, nullable=False)
    vat_exclusive = Column(Boolean, nullable=False)
    description = Column(Text, nullable=False)
    property_size_description = Column(Text, nullable=True)

    # Relationships
    property = relationship("PropertyModel", back_populates="price_history")


# ============================================================================
# Utility Functions
# ============================================================================


def normalize_address(address: str) -> str:
    """Normalize address string for comparison."""
    if not address:
        return ""
    # Lowercase, strip, and remove extra spaces
    normalized = " ".join(address.lower().strip().split())
    return normalized


def generate_address_hash(
    address: str, county: str, eircode: Optional[str] = None
) -> str:
    """Generate a hash for address deduplication."""
    normalized_addr = normalize_address(address)
    normalized_county = normalize_address(county) if county else ""
    normalized_eircode = normalize_address(eircode) if eircode else ""

    hash_string = f"{normalized_addr}|{normalized_county}|{normalized_eircode}"
    return hashlib.md5(hash_string.encode()).hexdigest()


def parse_price(price_str: str) -> float:
    """Parse price string to float."""
    if not price_str:
        return 0.0

    # Remove currency symbols, commas, and whitespace
    cleaned = price_str.replace("€", "").replace("£", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_boolean(value: str) -> bool:
    """Parse Yes/No string to boolean."""
    if not value:
        return False
    return value.strip().lower() in ("yes", "true", "1", "y")


def parse_date(date_str: str) -> Optional[date]:
    """Parse date string in dd/mm/yyyy format to date object.

    Args:
        date_str: Date string in dd/mm/yyyy format

    Returns:
        date object if parsing succeeds, None otherwise
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # Try different date formats
    formats = [
        "%d/%m/%Y",  # dd/mm/yyyy (primary format)
        "%Y-%m-%d",  # yyyy-mm-dd
        "%Y/%m/%d",  # yyyy/mm/dd
        "%d-%m-%Y",  # dd-mm-yyyy
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.date()
        except ValueError:
            continue

    return None
