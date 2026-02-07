"""
PPR CSV parser for Property Price Register uploads.
Parses CSV, normalizes, and groups by address (one property per address, multiple price history rows).
No imports from DataScraper; uses backend models only.
"""

import io
import logging
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from models import (
    generate_address_hash,
    normalize_address,
    parse_boolean,
    parse_date,
    parse_price,
)

logger = logging.getLogger(__name__)

PRICE_COLUMN_CANONICAL = "Price (€)"

REQUIRED_COLUMNS = [
    "Date of Sale (dd/mm/yyyy)",
    "Address",
    "County",
    "Eircode",
    PRICE_COLUMN_CANONICAL,
    "Not Full Market Price",
    "VAT Exclusive",
    "Description of Property",
    "Property Size Description",
]


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required column names exist; map Price column variants."""
    df.columns = [c.strip() for c in df.columns]
    for col in list(df.columns):
        if col.startswith("Price (") and col != PRICE_COLUMN_CANONICAL:
            df = df.rename(columns={col: PRICE_COLUMN_CANONICAL})
            break
    return df


def load_csv_from_bytes(content: bytes, encoding: Optional[str] = None) -> pd.DataFrame:
    """Load CSV from bytes into a DataFrame.

    Args:
        content: Raw CSV bytes
        encoding: Optional encoding (default tries utf-8, then latin-1)

    Returns:
        DataFrame with trimmed column names
    """
    encodings = [encoding] if encoding else ["utf-8", "latin-1", "iso-8859-1", "cp1252"]
    last_error = None
    for enc in encodings:
        if not enc:
            continue
        try:
            text = content.decode(enc)
            df = pd.read_csv(
                io.StringIO(text),
                quotechar='"',
                skipinitialspace=True,
                on_bad_lines="skip",
                low_memory=False,
            )
            df = _normalize_column_names(df)
            return df
        except (UnicodeDecodeError, Exception) as e:
            last_error = e
            continue
    raise ValueError(f"Failed to decode CSV with any encoding. Last error: {last_error}")


def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Clean DataFrame and add normalized columns and address_hash."""
    df = df.fillna("")
    df.columns = df.columns.str.strip()
    df = _normalize_column_names(df)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["normalized_address"] = df["Address"].apply(
        lambda x: normalize_address(str(x)) if x else ""
    )
    df["normalized_county"] = df["County"].apply(
        lambda x: normalize_address(str(x)) if x else ""
    )
    df["normalized_eircode"] = df["Eircode"].apply(
        lambda x: normalize_address(str(x)) if pd.notna(x) and x else ""
    )
    # address_hash = MD5(normalized Address|County|Eircode); import uses this to decide new vs existing (DB lookup by hash)
    df["address_hash"] = df.apply(
        lambda row: generate_address_hash(
            row["Address"],
            row["County"],
            row["Eircode"] if pd.notna(row["Eircode"]) and row["Eircode"] else None,
        ),
        axis=1,
    )
    return df


def identify_unique_properties(df: pd.DataFrame) -> Dict[str, List[int]]:
    """Group row indices by unique property (same address or eircode)."""
    property_groups: Dict[str, List[int]] = defaultdict(list)
    property_key_to_group: Dict[str, str] = {}

    for idx, row in df.iterrows():
        normalized_addr = row["normalized_address"]
        normalized_eircode = row["normalized_eircode"]
        county = row["normalized_county"]
        found_group = None

        if normalized_addr:
            addr_key = f"addr:{normalized_addr}:{county}"
            if addr_key in property_key_to_group:
                found_group = property_key_to_group[addr_key]
        if not found_group and normalized_eircode:
            eircode_key = f"eircode:{normalized_eircode}"
            if eircode_key in property_key_to_group:
                found_group = property_key_to_group[eircode_key]

        if not found_group:
            found_group = row["address_hash"]
            property_groups[found_group] = []
            if normalized_addr:
                property_key_to_group[f"addr:{normalized_addr}:{county}"] = found_group
            if normalized_eircode:
                property_key_to_group[f"eircode:{normalized_eircode}"] = found_group

        property_groups[found_group].append(idx)

    return dict(property_groups)


def parse_price_history_row(row: pd.Series, address_hash: str) -> Optional[Dict[str, Any]]:
    """Parse a single CSV row into a price history record. Returns None if date/price invalid."""
    date_str = str(row.get("Date of Sale (dd/mm/yyyy)", "")).strip()
    date_of_sale = parse_date(date_str)
    if not date_of_sale:
        return None
    price_str = str(row.get(PRICE_COLUMN_CANONICAL, "")).strip()
    price = parse_price(price_str)
    return {
        "date_of_sale": date_of_sale,
        "price": price,
        "not_full_market_price": parse_boolean(str(row.get("Not Full Market Price", "No"))),
        "vat_exclusive": parse_boolean(str(row.get("VAT Exclusive", "No"))),
        "description": str(row.get("Description of Property", "")).strip() or "Unknown",
        "property_size_description": (
            str(row["Property Size Description"]).strip()
            if pd.notna(row.get("Property Size Description")) and row.get("Property Size Description")
            else None
        ),
        "address_hash": address_hash,
    }


def _filter_last_year_and_current_year(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows where Date of Sale is in last calendar year or current year."""
    today = date.today()
    start = date(today.year - 1, 1, 1)
    end = date(today.year, 12, 31)
    date_col = "Date of Sale (dd/mm/yyyy)"
    if date_col not in df.columns:
        return df

    def in_range(val: Any) -> bool:
        d = parse_date(str(val).strip() if val else "")
        return d is not None and start <= d <= end

    before = len(df)
    mask = df[date_col].apply(in_range)
    df = df[mask].reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.info(
            "PPR filter: keeping only last year + current year (%s–%s): %s rows kept, %s rows dropped",
            start,
            end,
            len(df),
            dropped,
        )
    return df


def parse_ppr_csv(
    content: bytes,
    encoding: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Parse PPR CSV and return new-property groups and price history records.

    Only rows from last calendar year and current year are included.
    Does not touch the database. Caller should use find_by_hash to split into
    new vs existing and then create/update accordingly.

    Args:
        content: Raw CSV file bytes
        encoding: Optional encoding

    Returns:
        (property_data_list, price_history_records)
        - property_data_list: list of dicts with address_hash, address, county, eircode, row_indices
        - price_history_records: list of dicts with date_of_sale, price, ..., address_hash
          (one per CSV row, in order; use address_hash to associate with property_data)
    """
    df = load_csv_from_bytes(content, encoding=encoding)
    df = clean_and_normalize(df)
    df = _filter_last_year_and_current_year(df)
    property_groups = identify_unique_properties(df)

    property_data_list: List[Dict[str, Any]] = []
    price_history_records: List[Dict[str, Any]] = []

    for address_hash, row_indices in property_groups.items():
        first_idx = row_indices[0]
        row = df.iloc[first_idx]
        eircode_val = row.get("Eircode")
        eircode = (
            eircode_val
            if pd.notna(eircode_val) and eircode_val
            else None
        )
        property_data_list.append({
            "address_hash": address_hash,
            "address": row["Address"],
            "county": row["County"],
            "eircode": eircode,
            "row_indices": row_indices,
        })

    for idx, row in df.iterrows():
        rec = parse_price_history_row(row, row["address_hash"])
        if rec:
            price_history_records.append(rec)

    logger.info(
        f"Parsed {len(property_data_list)} unique properties, "
        f"{len(price_history_records)} price history records from {len(df)} rows"
    )
    return property_data_list, price_history_records
