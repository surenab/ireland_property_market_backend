#!/usr/bin/env python3
"""
Script to dump data from a local database and upload it to a deployed app.

Usage:
    python dump_and_upload.py --db-path /path/to/properties.db --api-url https://your-api-url.com
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Add parent directory to path to import models
sys.path.insert(0, str(Path(__file__).parent))

from models import PropertyModel, AddressModel, PriceHistoryModel
from database import Database


def serialize_date(d: date) -> str:
    """Serialize date to YYYY-MM-DD format."""
    if isinstance(d, datetime):
        return d.date().strftime("%Y-%m-%d")
    elif isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    return str(d)


def dump_properties_from_db(
    db_path: str, batch_size: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Dump all properties from database in bulk upload format.

    Args:
        db_path: Path to the SQLite database
        batch_size: Optional batch size to process in chunks (None = all at once)

    Returns:
        List of property dictionaries in bulk upload format
    """
    print(f"Connecting to database: {db_path}")
    db = Database(db_path=db_path)
    session = db.get_session()

    try:
        # Get all properties with addresses
        query = session.query(PropertyModel).join(
            AddressModel, PropertyModel.id == AddressModel.property_id
        )

        if batch_size:
            total = query.count()
            print(f"Found {total} properties. Processing in batches of {batch_size}...")
        else:
            properties = query.all()
            total = len(properties)
            print(f"Found {total} properties. Processing all at once...")

        all_properties = []
        processed = 0

        if batch_size:
            offset = 0
            while True:
                batch = query.offset(offset).limit(batch_size).all()
                if not batch:
                    break

                for prop in batch:
                    property_data = serialize_property(prop, session)
                    if property_data:
                        all_properties.append(property_data)
                        processed += 1
                        if processed % 100 == 0:
                            print(f"Processed {processed}/{total} properties...")

                offset += batch_size
        else:
            for prop in properties:
                property_data = serialize_property(prop, session)
                if property_data:
                    all_properties.append(property_data)
                    processed += 1
                    if processed % 100 == 0:
                        print(f"Processed {processed}/{total} properties...")

        print(f"Successfully dumped {len(all_properties)} properties")
        return all_properties

    finally:
        session.close()
        db.close()


def serialize_property(
    prop: PropertyModel, session: Session
) -> Optional[Dict[str, Any]]:
    """Serialize a property to bulk upload format.

    Args:
        prop: PropertyModel instance
        session: Database session

    Returns:
        Dictionary in bulk upload format or None if property has no address
    """
    # Get address
    address = prop.address
    if not address:
        return None

    # Get price history
    price_history = (
        session.query(PriceHistoryModel)
        .filter(PriceHistoryModel.property_id == prop.id)
        .all()
    )

    # Build property data (coerce types for bulk-upload API: str, int, bool)
    def _opt_str(v):
        if v is None:
            return None
        s = (v or "").strip()
        return s if s else None

    property_data = {
        "address": {
            "address": (address.address or "").strip() or "",
            "county": (address.county or "").strip() or "",
            "eircode": _opt_str(address.eircode),
            "latitude": float(address.latitude) if address.latitude is not None else None,
            "longitude": float(address.longitude) if address.longitude is not None else None,
            "formatted_address": _opt_str(address.formatted_address),
            "country": _opt_str(address.country),
        },
        "price_history": [],
    }
    for ph in price_history:
        sale_date = ph.date_of_sale
        date_str = serialize_date(sale_date) if sale_date else None
        if not date_str or date_str == "None":
            continue
        try:
            price_val = ph.price
            price_int = int(round(float(price_val))) if price_val is not None else 0
        except (TypeError, ValueError):
            price_int = 0
        property_data["price_history"].append({
            "date_of_sale": date_str,
            "price": price_int,
            "not_full_market_price": bool(ph.not_full_market_price),
            "vat_exclusive": bool(ph.vat_exclusive),
            "description": (ph.description or "").strip() or "Unknown",
            "property_size_description": _opt_str(ph.property_size_description),
        })

    # Add daft data if available (ensure bool for daft_scraped)
    if prop.daft_url:
        property_data["daft_url"] = prop.daft_url
    if prop.daft_html:
        property_data["daft_html"] = prop.daft_html
    if prop.daft_title:
        property_data["daft_title"] = prop.daft_title
    if prop.daft_body:
        property_data["daft_body"] = prop.daft_body
    property_data["daft_scraped"] = bool(prop.daft_scraped)

    return property_data


def upload_properties(
    api_url: str,
    properties: List[Dict[str, Any]],
    batch_size: int = 1000,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Dict[str, Any]:
    """Upload properties to the API in batches.

    Args:
        api_url: Base URL of the deployed API
        properties: List of property dictionaries
        batch_size: Number of properties to upload per request
        max_retries: Maximum number of retries for failed requests
        retry_delay: Delay between retries in seconds

    Returns:
        Dictionary with upload statistics
    """
    endpoint = f"{api_url.rstrip('/')}/api/properties/bulk-upload"

    total = len(properties)
    total_created = 0
    total_updated = 0
    total_failed = 0
    batches = (total + batch_size - 1) // batch_size

    print(f"\nUploading {total} properties in {batches} batches of {batch_size}...")
    print(f"API endpoint: {endpoint}\n")

    for batch_num in range(batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total)
        batch = properties[start_idx:end_idx]

        print(f"Batch {batch_num + 1}/{batches} ({len(batch)} properties)...", end=" ")

        payload = {"properties": batch}

        # Retry logic
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=300,  # 5 minute timeout for large batches
                )
                if response.status_code == 422:
                    try:
                        err = response.json()
                        print(f"\n  Validation error (422): {err}")
                    except Exception:
                        print(f"\n  Validation error (422): {response.text[:500]}")
                response.raise_for_status()

                result = response.json()
                batch_created = result.get("created", 0)
                batch_updated = result.get("updated", 0)
                batch_failed = result.get("failed", 0)

                total_created += batch_created
                total_updated += batch_updated
                total_failed += batch_failed

                print(
                    f"✓ Created: {batch_created}, Updated: {batch_updated}, Failed: {batch_failed}"
                )
                break

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    print(f"✗ Error (attempt {attempt + 1}/{max_retries}): {e}")
                    print(f"  Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"✗ Failed after {max_retries} attempts: {e}")
                    total_failed += len(batch)

        # Small delay between batches to avoid overwhelming the server
        if batch_num < batches - 1:
            time.sleep(0.5)

    return {
        "total": total,
        "created": total_created,
        "updated": total_updated,
        "failed": total_failed,
    }


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Dump properties from local database and upload to deployed API"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        required=True,
        help="Path to the local SQLite database file",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        required=True,
        help="Base URL of the deployed API (e.g., https://your-api-url.com)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of properties to upload per batch (default: 100)",
    )
    parser.add_argument(
        "--dump-only",
        action="store_true",
        help="Only dump data to JSON file, don't upload",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file path (only used with --dump-only)",
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Input JSON file path (skip dump, upload from file)",
    )

    args = parser.parse_args()

    # Validate database path
    if not args.input and not Path(args.db_path).exists():
        print(f"Error: Database file not found: {args.db_path}")
        sys.exit(1)

    # Dump properties
    if args.input:
        print(f"Loading properties from {args.input}...")
        with open(args.input, "r") as f:
            data = json.load(f)
            properties = data.get("properties", data)
    else:
        properties = dump_properties_from_db(args.db_path)

    if not properties:
        print("No properties found to upload.")
        sys.exit(0)

    # Save to file if requested
    if args.dump_only:
        output_path = args.output or "properties_dump.json"
        print(f"\nSaving {len(properties)} properties to {output_path}...")
        with open(output_path, "w") as f:
            json.dump({"properties": properties}, f, indent=2)
        print(f"✓ Dump saved to {output_path}")
        return

    # Upload properties
    print(f"\n{'='*60}")
    print("Starting upload...")
    print(f"{'='*60}\n")

    stats = upload_properties(
        api_url=args.api_url,
        properties=properties,
        batch_size=args.batch_size,
    )

    print(f"\n{'='*60}")
    print("Upload Summary:")
    print(f"{'='*60}")
    print(f"Total properties: {stats['total']}")
    print(f"Created: {stats['created']}")
    print(f"Updated: {stats['updated']}")
    print(f"Failed: {stats['failed']}")
    print(f"{'='*60}\n")

    if stats["failed"] > 0:
        print("⚠ Warning: Some properties failed to upload.")
        sys.exit(1)
    else:
        print("✓ All properties uploaded successfully!")


if __name__ == "__main__":
    main()
