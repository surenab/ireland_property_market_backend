"""
Database setup and CRUD operations for SQLite and PostgreSQL.
"""

import logging
from typing import Optional, List
from datetime import datetime

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from models import Base, PropertyModel, AddressModel, PriceHistoryModel
from config import get_db_path
from config import is_production, get_database_url

logger = logging.getLogger(__name__)


class Database:
    """Database manager for SQLite and PostgreSQL operations."""

    def __init__(
        self, db_path: Optional[str] = None, database_url: Optional[str] = None
    ):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database (used in development mode)
            database_url: PostgreSQL connection URL (used in production mode)
        """

        # Check environment variable to determine which database to use
        if is_production():
            # Production mode: use PostgreSQL
            db_url = database_url or get_database_url()
            if not db_url:
                raise ValueError(
                    "Production mode requires PostgreSQL configuration. "
                    "Please set DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME environment variables."
                )
            logger.info("Connecting to PostgreSQL database (production mode)")
            self.db_type = "postgresql"
            self.db_path = None
            self.engine = create_engine(
                db_url,
                echo=False,
                pool_pre_ping=True,  # Verify connections before using
                pool_size=5,  # Connection pool size
                max_overflow=10,  # Max overflow connections
            )
        else:
            # Development mode: use SQLite
            self.db_path = db_path or get_db_path()
            self.db_type = "sqlite"
            logger.info(
                f"Connecting to SQLite database at {self.db_path} (development mode)"
            )
            self.engine = create_engine(
                f"sqlite:///{self.db_path}",
                echo=False,
                connect_args={
                    "check_same_thread": False,
                    "timeout": 30.0,  # Increase timeout for concurrent access
                },
                pool_pre_ping=True,  # Verify connections before using
            )
            # Enable WAL mode for better concurrency (SQLite only)
            self._enable_wal_mode()

        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def _enable_wal_mode(self):
        """Enable WAL (Write-Ahead Logging) mode for better SQLite concurrency."""
        if self.db_type != "sqlite":
            return

        import sqlite3

        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "PRAGMA synchronous=NORMAL"
            )  # Balance between safety and speed
            conn.execute(
                "PRAGMA cache_size=10000"
            )  # Increase cache for better performance
            conn.execute("PRAGMA temp_store=memory")  # Use memory for temp tables
            conn.close()
            logger.debug("Enabled WAL mode for SQLite database")
        except Exception as e:
            logger.warning(f"Could not enable WAL mode: {e}")

    def create_tables(self):
        """Create all database tables and ensure all required fields exist."""
        try:
            Base.metadata.create_all(bind=self.engine)
            if self.db_type == "sqlite":
                logger.info(f"Database tables created successfully in {self.db_path}")
            else:
                logger.info("Database tables created successfully in PostgreSQL")

            # Ensure all required fields exist (for existing databases - SQLite only)
            if self.db_type == "sqlite":
                self._ensure_all_fields_exist()

            # Ensure spatial indexes exist
            self._ensure_indexes_exist()
        except SQLAlchemyError as e:
            logger.error(f"Error creating database tables: {e}")
            raise

    def _ensure_all_fields_exist(self):
        """Ensure all required fields exist in the database tables (SQLite only)."""
        if self.db_type != "sqlite":
            return

        import sqlite3

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Check properties table columns
            cursor.execute("PRAGMA table_info(properties)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]

            # Add missing fields if they don't exist
            if "daft_title" not in column_names:
                logger.info("Adding missing daft_title column...")
                cursor.execute("ALTER TABLE properties ADD COLUMN daft_title TEXT")

            if "daft_body" not in column_names:
                logger.info("Adding missing daft_body column...")
                cursor.execute("ALTER TABLE properties ADD COLUMN daft_body TEXT")

            if "daft_scraped" not in column_names:
                logger.info("Adding missing daft_scraped column...")
                cursor.execute(
                    "ALTER TABLE properties ADD COLUMN daft_scraped BOOLEAN DEFAULT 0"
                )
                # Update existing records
                cursor.execute(
                    "UPDATE properties SET daft_scraped = 0 WHERE daft_scraped IS NULL"
                )

            conn.commit()
            conn.close()

        except Exception as e:
            logger.warning(
                f"Could not ensure all fields exist (this is OK for new databases): {e}"
            )

    def _ensure_indexes_exist(self):
        """Ensure spatial indexes on addresses and sort indexes on price_history exist."""
        try:
            session = self.get_session()

            if self.db_type == "sqlite":
                import sqlite3

                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()

                # Addresses indexes
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='addresses'"
                )
                addr_indexes = [row[0] for row in cursor.fetchall()]
                if "idx_lat_lng" not in addr_indexes:
                    logger.info(
                        "Creating spatial index idx_lat_lng on addresses table..."
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_lat_lng ON addresses(latitude, longitude)"
                    )
                if "idx_county" not in addr_indexes:
                    logger.info("Creating index idx_county on addresses table...")
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_county ON addresses(county)"
                    )

                # price_history: composite index for sort-by-price/date (GROUP BY property_id, MAX(date_of_sale))
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='price_history'"
                )
                ph_indexes = [row[0] for row in cursor.fetchall()]
                if "idx_price_history_property_date" not in ph_indexes:
                    logger.info(
                        "Creating index idx_price_history_property_date on price_history table..."
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_price_history_property_date ON price_history(property_id, date_of_sale)"
                    )

                conn.commit()
                conn.close()

            elif self.db_type == "postgresql":
                # For PostgreSQL, use raw SQL to create indexes if they don't exist
                from sqlalchemy import text

                # Addresses indexes
                result = session.execute(
                    text(
                        """
                    SELECT indexname FROM pg_indexes 
                    WHERE tablename = 'addresses' 
                    AND indexname IN ('idx_lat_lng', 'idx_county')
                """
                    )
                )
                addr_indexes = [row[0] for row in result.fetchall()]
                if "idx_lat_lng" not in addr_indexes:
                    logger.info(
                        "Creating spatial index idx_lat_lng on addresses table..."
                    )
                    session.execute(
                        text(
                            "CREATE INDEX idx_lat_lng ON addresses(latitude, longitude)"
                        )
                    )
                if "idx_county" not in addr_indexes:
                    logger.info("Creating index idx_county on addresses table...")
                    session.execute(
                        text("CREATE INDEX idx_county ON addresses(county)")
                    )

                # price_history: composite index for sort-by-price/date
                result = session.execute(
                    text(
                        """
                    SELECT indexname FROM pg_indexes 
                    WHERE tablename = 'price_history' 
                    AND indexname = 'idx_price_history_property_date'
                """
                    )
                )
                if result.fetchone() is None:
                    logger.info(
                        "Creating index idx_price_history_property_date on price_history table..."
                    )
                    session.execute(
                        text(
                            "CREATE INDEX idx_price_history_property_date ON price_history(property_id, date_of_sale)"
                        )
                    )

                session.commit()

            session.close()
            logger.info("Indexes verified/created successfully")

        except Exception as e:
            logger.warning(
                f"Could not ensure indexes exist (this is OK for new databases): {e}"
            )

    def get_session(self) -> Session:
        """Get a database session."""
        return self.SessionLocal()

    def close(self):
        """Close database connection."""
        self.engine.dispose()


class PropertyRepository:
    """Repository for Property operations."""

    def __init__(self, session: Session):
        self.session = session

    def get_or_create_property(self) -> PropertyModel:
        """Get or create a property. Since properties are identified by address/eircode,
        we'll create a new property for each unique address/eircode combination."""
        property_obj = PropertyModel()
        self.session.add(property_obj)
        self.session.flush()  # Get the ID
        return property_obj

    def get_property_by_id(self, property_id: int) -> Optional[PropertyModel]:
        """Get property by ID."""
        return (
            self.session.query(PropertyModel)
            .filter(PropertyModel.id == property_id)
            .first()
        )

    def update_daft_data(
        self,
        property_id: int,
        daft_url: Optional[str] = None,
        daft_html: Optional[str] = None,
        daft_title: Optional[str] = None,
        daft_body: Optional[str] = None,
        daft_scraped: bool = True,
    ) -> bool:
        """Update Daft.ie scraping data for a property."""
        try:
            property_obj = (
                self.session.query(PropertyModel)
                .filter(PropertyModel.id == property_id)
                .first()
            )
            if not property_obj:
                return False

            if daft_url is not None:
                property_obj.daft_url = daft_url
            if daft_html is not None:
                property_obj.daft_html = daft_html
            if daft_title is not None:
                property_obj.daft_title = daft_title
            if daft_body is not None:
                property_obj.daft_body = daft_body
            property_obj.daft_scraped = daft_scraped
            property_obj.daft_scraped_at = datetime.utcnow()

            self.session.flush()
            return True
        except SQLAlchemyError as e:
            logger.error(f"Error updating Daft data for property {property_id}: {e}")
            return False

    def get_unscraped_properties(
        self, limit: Optional[int] = None
    ) -> List[PropertyModel]:
        """Get properties that haven't been scraped from Daft.ie yet (daft_scraped = False)."""
        query = self.session.query(PropertyModel).filter(
            PropertyModel.daft_scraped.is_(False)
        )
        if limit:
            query = query.limit(limit)
        return query.all()

    def count_unscraped_properties(self) -> int:
        """Count properties that haven't been scraped from Daft.ie yet (daft_scraped = False)."""
        return (
            self.session.query(PropertyModel)
            .filter(PropertyModel.daft_scraped.is_(False))
            .count()
        )


class AddressRepository:
    """Repository for Address operations."""

    def __init__(self, session: Session):
        self.session = session

    def find_by_address_or_eircode(
        self, address: str, county: str, eircode: Optional[str] = None
    ) -> Optional[AddressModel]:
        """Find address by address string or eircode."""
        from models import normalize_address

        normalized_address = normalize_address(address)
        normalized_eircode = normalize_address(eircode) if eircode else None

        # Try to find by normalized address
        query = self.session.query(AddressModel).filter(
            and_(
                AddressModel.address == normalized_address,
                AddressModel.county == county,
            )
        )
        result = query.first()
        if result:
            return result

        # Try to find by eircode if provided
        if normalized_eircode:
            query = self.session.query(AddressModel).filter(
                AddressModel.eircode == normalized_eircode
            )
            result = query.first()
            if result:
                return result

        return None

    def find_by_hash(self, address_hash: str) -> Optional[AddressModel]:
        """Find address by hash."""
        return (
            self.session.query(AddressModel)
            .filter(AddressModel.address_hash == address_hash)
            .first()
        )

    def get_ungocoded_addresses(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        min_id: Optional[int] = None,
    ) -> List[AddressModel]:
        """Get addresses that haven't been geocoded yet (both lat and lng must be None).

        Args:
            limit: Maximum number of addresses to return
            offset: Number of addresses to skip (for pagination)
            min_id: Minimum address ID to start from (for sequential processing)
        """
        query = self.session.query(AddressModel).filter(
            and_(AddressModel.latitude.is_(None), AddressModel.longitude.is_(None))
        )

        if min_id is not None:
            query = query.filter(AddressModel.id > min_id)

        query = query.order_by(AddressModel.id)

        if offset is not None:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        return query.all()

    def get_ungocoded_addresses_reverse(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        max_id: Optional[int] = None,
    ) -> List[AddressModel]:
        """Get addresses that haven't been geocoded yet, in reverse ID order (highest first).

        This is useful when running multiple geocoders in parallel to avoid conflicts.

        Args:
            limit: Maximum number of addresses to return
            offset: Number of addresses to skip (for pagination)
            max_id: Maximum address ID to start from (for reverse sequential processing)
        """
        query = self.session.query(AddressModel).filter(
            and_(AddressModel.latitude.is_(None), AddressModel.longitude.is_(None))
        )

        if max_id is not None:
            query = query.filter(AddressModel.id < max_id)

        query = query.order_by(AddressModel.id.desc())  # Reverse order

        if offset is not None:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        return query.all()

    def count_ungocoded_addresses(self) -> int:
        """Count addresses that haven't been geocoded yet (both lat and lng must be None)."""
        return (
            self.session.query(AddressModel)
            .filter(
                and_(AddressModel.latitude.is_(None), AddressModel.longitude.is_(None))
            )
            .count()
        )

    def count_total_addresses(self) -> int:
        """Count total addresses."""
        return self.session.query(AddressModel).count()

    def create_address(
        self,
        property_id: int,
        address: str,
        county: str,
        eircode: Optional[str] = None,
        address_hash: Optional[str] = None,
    ) -> AddressModel:
        """Create a new address."""
        from models import normalize_address

        normalized_address = normalize_address(address)
        address_obj = AddressModel(
            property_id=property_id,
            address=normalized_address,
            county=county,
            eircode=normalize_address(eircode) if eircode else None,
            address_hash=address_hash,
        )
        self.session.add(address_obj)
        self.session.flush()
        return address_obj

    def update_geo_data(
        self,
        address_id: int,
        latitude: float,
        longitude: float,
        formatted_address: Optional[str] = None,
        country: Optional[str] = None,
        raw_geo_data: Optional[dict] = None,
    ) -> bool:
        """Update geocoding data for an address (thread-safe with retry logic)."""
        import time

        max_retries = 3
        for attempt in range(max_retries):
            try:
                address_obj = (
                    self.session.query(AddressModel)
                    .filter(AddressModel.id == address_id)
                    .first()
                )
                if not address_obj:
                    return False

                address_obj.latitude = latitude
                address_obj.longitude = longitude
                address_obj.formatted_address = formatted_address
                address_obj.country = country
                address_obj.raw_geo_data = raw_geo_data
                address_obj.geocoded_at = datetime.utcnow()

                self.session.flush()
                return True
            except SQLAlchemyError as e:
                error_str = str(e).lower()
                if "database is locked" in error_str or "locked" in error_str:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 0.1
                        time.sleep(wait_time)
                        self.session.rollback()
                        continue
                    else:
                        logger.error(
                            f"Error updating geo data for address {address_id}: Database locked after {max_retries} attempts"
                        )
                        self.session.rollback()
                        return False
                else:
                    logger.error(
                        f"Error updating geo data for address {address_id}: {e}"
                    )
                    self.session.rollback()
                    return False
        return False


class PriceHistoryRepository:
    """Repository for PriceHistory operations."""

    def __init__(self, session: Session):
        self.session = session

    def create_price_history(
        self,
        property_id: int,
        date_of_sale: datetime,
        price: int,
        not_full_market_price: bool,
        vat_exclusive: bool,
        description: str,
        property_size_description: Optional[str] = None,
    ) -> PriceHistoryModel:
        """Create a new price history record. Price is stored as integer (whole euros)."""
        price_int = int(round(float(price))) if price is not None else 0
        price_history = PriceHistoryModel(
            property_id=property_id,
            date_of_sale=date_of_sale,
            price=price_int,
            not_full_market_price=not_full_market_price,
            vat_exclusive=vat_exclusive,
            description=description,
            property_size_description=property_size_description,
        )
        self.session.add(price_history)
        self.session.flush()
        return price_history

    def get_price_history_by_property(
        self, property_id: int
    ) -> List[PriceHistoryModel]:
        """Get all price history records for a property."""
        return (
            self.session.query(PriceHistoryModel)
            .filter(PriceHistoryModel.property_id == property_id)
            .all()
        )
