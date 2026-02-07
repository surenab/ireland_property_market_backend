"""Tests for PPR CSV parser (used by upload)."""

import sys
from pathlib import Path

import pytest

_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from api.services.ppr_csv_parser import parse_ppr_csv, _filter_last_year_and_current_year
import pandas as pd
from datetime import date


# Minimal valid PPR CSV (required columns; last year + current year for filter)
_MINIMAL_PPR_CSV_STR = (
    "Date of Sale (dd/mm/yyyy),Address,County,Eircode,Price (\u20ac),"
    "Not Full Market Price,VAT Exclusive,Description of Property,Property Size Description\n"
    "01/01/2025,1 Main St,Dublin,D01AB12,300000,No,No,Second-Hand Dwelling house /Apartment,\n"
    "15/06/2025,2 Other Rd,Cork,,250000,No,Yes,New Dwelling house /Apartment,\n"
)
MINIMAL_PPR_CSV = _MINIMAL_PPR_CSV_STR.encode("utf-8")


def test_parse_ppr_csv_returns_property_list_and_price_history():
    """parse_ppr_csv returns (property_data_list, price_history_records) with expected shape."""
    property_data_list, price_history_records = parse_ppr_csv(MINIMAL_PPR_CSV)

    assert isinstance(property_data_list, list)
    assert isinstance(price_history_records, list)
    assert len(property_data_list) >= 1
    assert len(price_history_records) >= 1

    prop = property_data_list[0]
    assert "address_hash" in prop
    assert "address" in prop
    assert "county" in prop
    assert "row_indices" in prop

    ph = price_history_records[0]
    assert "date_of_sale" in ph
    assert "price" in ph
    assert "address_hash" in ph
    assert "not_full_market_price" in ph
    assert "vat_exclusive" in ph
    assert "description" in ph


def test_parse_ppr_csv_price_is_int():
    """Parsed price history has integer price."""
    _, price_history_records = parse_ppr_csv(MINIMAL_PPR_CSV)
    assert len(price_history_records) >= 1
    for ph in price_history_records:
        assert isinstance(ph["price"], int)


def test_parse_ppr_csv_filters_last_year_and_current_year():
    """Only rows from last year and current year are included."""
    property_data_list, price_history_records = parse_ppr_csv(MINIMAL_PPR_CSV)
    today = date.today()
    start = date(today.year - 1, 1, 1)
    end = date(today.year, 12, 31)
    for ph in price_history_records:
        ds = ph["date_of_sale"]
        if isinstance(ds, str) and len(ds) == 10:
            year = int(ds[:4])
            assert start.year <= year <= end.year, f"date_of_sale {ds} outside range"


def test_parse_ppr_csv_missing_columns_raises():
    """parse_ppr_csv with missing required columns raises ValueError."""
    bad_csv = b"Address,County\n1 Main St,Dublin\n"
    with pytest.raises(ValueError):
        parse_ppr_csv(bad_csv)
