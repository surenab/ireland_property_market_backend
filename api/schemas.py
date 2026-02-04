"""
Pydantic schemas for API requests and responses.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ============================================================================
# Property Schemas
# ============================================================================


class AddressResponse(BaseModel):
    """Address response schema."""

    id: int
    address: str
    county: str
    eircode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    formatted_address: Optional[str] = None
    country: Optional[str] = None
    geocoded_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class PriceHistoryResponse(BaseModel):
    """Price history response schema."""

    id: int
    date_of_sale: str  # Serialized as string in YYYY-MM-DD format
    price: float
    not_full_market_price: bool
    vat_exclusive: bool
    description: str
    property_size_description: Optional[str] = None

    class Config:
        from_attributes = True

    @classmethod
    def from_orm(cls, obj):
        """Custom from_orm to handle date serialization."""
        data = {
            "id": obj.id,
            "price": obj.price,
            "not_full_market_price": obj.not_full_market_price,
            "vat_exclusive": obj.vat_exclusive,
            "description": obj.description,
            "property_size_description": obj.property_size_description,
        }

        # Convert date to string
        from datetime import date as date_type

        if hasattr(obj, "date_of_sale") and obj.date_of_sale:
            if isinstance(obj.date_of_sale, datetime):
                data["date_of_sale"] = obj.date_of_sale.date().strftime("%Y-%m-%d")
            elif isinstance(obj.date_of_sale, date_type):
                data["date_of_sale"] = obj.date_of_sale.strftime("%Y-%m-%d")
            elif hasattr(obj.date_of_sale, "strftime"):
                data["date_of_sale"] = obj.date_of_sale.strftime("%Y-%m-%d")
            else:
                data["date_of_sale"] = str(obj.date_of_sale)
        else:
            data["date_of_sale"] = ""

        return cls(**data)


class PropertyResponse(BaseModel):
    """Property response schema."""

    id: int
    created_at: datetime
    updated_at: datetime
    daft_url: Optional[str] = None
    daft_html: Optional[str] = None
    daft_title: Optional[str] = None
    daft_body: Optional[str] = None
    daft_scraped: bool = False
    daft_scraped_at: Optional[datetime] = None
    address: Optional[AddressResponse] = None
    price_history: List[PriceHistoryResponse] = []

    class Config:
        from_attributes = True


class PropertyListItem(BaseModel):
    """Property list item (simplified for list views)."""

    id: int
    address: Optional[str] = None
    county: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    latest_price: Optional[float] = None
    latest_sale_date: Optional[str] = None

    class Config:
        from_attributes = True


# ============================================================================
# Map Schemas
# ============================================================================


class MapPoint(BaseModel):
    """Map point schema."""

    id: int
    latitude: float
    longitude: float
    price: Optional[float] = None
    address: Optional[str] = None
    county: Optional[str] = None
    date: Optional[str] = None


class MapCluster(BaseModel):
    """Map cluster schema."""

    center_lat: float
    center_lng: float
    count: int
    bounds: dict  # {north, south, east, west}
    properties: List[MapPoint] = []


class MapClustersResponse(BaseModel):
    """Map clusters response."""

    clusters: List[MapCluster]
    total_properties: int
    viewport: dict  # {north, south, east, west}


class MapPointsResponse(BaseModel):
    """Map points response."""

    points: List[MapPoint]
    total: int


# ============================================================================
# Statistics Schemas
# ============================================================================


class PriceTrendPoint(BaseModel):
    """Price trend data point."""

    date: str
    average_price: float
    median_price: float
    std_deviation: float
    min_price: float
    max_price: float
    count: int


class PriceTrendsResponse(BaseModel):
    """Price trends response."""

    trends: List[PriceTrendPoint]
    period: str  # "monthly", "quarterly", "yearly"


class PriceCluster(BaseModel):
    """Price cluster schema."""

    cluster_id: int
    price_range: dict  # {min, max}
    count: int
    average_price: float
    properties: List[int] = []  # Property IDs


class ClustersResponse(BaseModel):
    """Clusters response."""

    clusters: List[PriceCluster]
    algorithm: str  # "kmeans", "dbscan"
    n_clusters: int


class CountyStatistics(BaseModel):
    """County statistics."""

    county: str
    property_count: int
    average_price: float
    median_price: float
    min_price: float
    max_price: float
    price_per_sqm: Optional[float] = None


class CountyComparisonResponse(BaseModel):
    """County comparison response."""

    counties: List[CountyStatistics]
    overall_average: float
    overall_median: float


class CorrelationResponse(BaseModel):
    """Correlation analysis response."""

    correlation_coefficient: float
    p_value: float
    sample_size: int
    interpretation: str


# ============================================================================
# Search Schemas
# ============================================================================


class SearchResponse(BaseModel):
    """Search response."""

    properties: List[PropertyListItem]
    total: int
    query: str


# ============================================================================
# Query Parameters
# ============================================================================


class PropertyFilters(BaseModel):
    """Property filter parameters."""

    county: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_latitude: Optional[float] = None
    max_latitude: Optional[float] = None
    min_longitude: Optional[float] = None
    max_longitude: Optional[float] = None
    has_geocoding: Optional[bool] = None
    has_daft_data: Optional[bool] = None


class PaginationParams(BaseModel):
    """Pagination parameters."""

    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=1000)


class MapViewport(BaseModel):
    """Map viewport parameters."""

    north: float
    south: float
    east: float
    west: float
    zoom: Optional[int] = None


class MapAnalysisResponse(BaseModel):
    """Map analysis response."""

    analysis_mode: str
    total_properties: int
    viewport: MapViewport
    heatmap_data: List[Dict[str, Any]] = []
    clusters: List[Dict[str, Any]] = []
    points: List[Dict[str, Any]] = []


# ============================================================================
# Bulk Upload Schemas
# ============================================================================


class PriceHistoryBulk(BaseModel):
    """Price history for bulk upload."""

    date_of_sale: str  # YYYY-MM-DD format
    price: float
    not_full_market_price: bool = False
    vat_exclusive: bool = False
    description: str
    property_size_description: Optional[str] = None


class AddressBulk(BaseModel):
    """Address for bulk upload."""

    address: str
    county: str
    eircode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    formatted_address: Optional[str] = None
    country: Optional[str] = None


class PropertyBulk(BaseModel):
    """Property for bulk upload."""

    address: AddressBulk
    price_history: List[PriceHistoryBulk] = []
    daft_url: Optional[str] = None
    daft_html: Optional[str] = None
    daft_title: Optional[str] = None
    daft_body: Optional[str] = None
    daft_scraped: bool = False


class BulkUploadRequest(BaseModel):
    """Bulk upload request schema."""

    properties: List[PropertyBulk]


class BulkUploadResult(BaseModel):
    """Result for a single property upload."""

    success: bool
    property_id: Optional[int] = None
    message: str
    address: str


class BulkUploadResponse(BaseModel):
    """Bulk upload response schema."""

    total: int
    created: int
    updated: int
    failed: int
    results: List[BulkUploadResult]
