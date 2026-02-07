"""Tests for PPR download-and-import upload API."""

import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest


# Minimal valid PPR CSV (required columns; dates in current year for filter)
_MINIMAL_PPR_CSV = (
    "Date of Sale (dd/mm/yyyy),Address,County,Eircode,Price (\u20ac),"
    "Not Full Market Price,VAT Exclusive,Description of Property,Property Size Description\n"
    "01/01/2025,1 Main St,Dublin,D01AB12,300000,No,No,Second-Hand Dwelling house /Apartment,\n"
    "15/06/2025,2 Other Rd,Cork,,250000,No,Yes,New Dwelling house /Apartment,\n"
)


def _minimal_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ppr.csv", _MINIMAL_PPR_CSV.encode("utf-8"))
    return buf.getvalue()


@pytest.fixture
def mock_ppr_download():
    """Mock requests.get to return a minimal PPR zip so the background task does not hit the network."""
    zip_bytes = _minimal_zip_bytes()
    fake_resp = MagicMock()
    fake_resp.content = zip_bytes
    fake_resp.raise_for_status = MagicMock()
    with patch("api.routes.upload.requests.get", return_value=fake_resp):
        yield


@pytest.fixture
def mock_geocoder_and_daft():
    """Mock BingGeocoder and DaftScraper so import does not call external APIs."""
    with patch("api.routes.upload.BingGeocoder") as mock_bing:
        mock_bing.return_value.geocode_address.return_value = None
        with patch("api.routes.upload.DaftScraper") as mock_daft:
            mock_daft.return_value.search_bing_for_daft.return_value = None
            yield


def test_post_ppr_download_and_import_returns_202_and_job_id(client, mock_ppr_download, mock_geocoder_and_daft):
    """POST /api/admin/ppr-download-and-import returns 202 and a job_id."""
    response = client.post("/api/admin/ppr-download-and-import")
    assert response.status_code == 202
    data = response.json()
    assert "job_id" in data
    assert isinstance(data["job_id"], str)
    assert len(data["job_id"]) > 0


def test_get_ppr_import_status_unknown_job_returns_404(client):
    """GET /api/admin/ppr-import-status/{job_id} with unknown job_id returns 404."""
    response = client.get("/api/admin/ppr-import-status/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


def test_post_then_get_ppr_import_status_returns_job_status(client, mock_ppr_download, mock_geocoder_and_daft):
    """After POST, GET with returned job_id returns 200 and status (running, completed, or failed)."""
    post_response = client.post("/api/admin/ppr-download-and-import")
    assert post_response.status_code == 202
    job_id = post_response.json()["job_id"]

    # Poll briefly until background task finishes (mocked download + geocoder/daft complete quickly)
    for _ in range(25):
        get_response = client.get(f"/api/admin/ppr-import-status/{job_id}")
        assert get_response.status_code == 200
        data = get_response.json()
        assert "status" in data
        assert data["status"] in ("running", "completed", "failed")
        if data["status"] in ("completed", "failed"):
            break
        import time
        time.sleep(0.2)
    else:
        pytest.fail("Job did not complete within 5 seconds")


def test_ppr_import_status_response_shape(client, mock_ppr_download, mock_geocoder_and_daft):
    """GET status response has expected shape: status, optional result, optional error."""
    post_response = client.post("/api/admin/ppr-download-and-import")
    job_id = post_response.json()["job_id"]
    # Wait for job to finish
    for _ in range(25):
        get_response = client.get(f"/api/admin/ppr-import-status/{job_id}")
        data = get_response.json()
        if data.get("status") in ("completed", "failed"):
            break
        import time
        time.sleep(0.2)
    else:
        get_response = client.get(f"/api/admin/ppr-import-status/{job_id}")
        data = get_response.json()

    assert "status" in data
    if data["status"] == "completed":
        assert "result" in data
        result = data["result"]
        if result:
            assert "total_rows" in result
            assert "created" in result
            assert "updated" in result
    if data["status"] == "failed":
        assert "error" in data
