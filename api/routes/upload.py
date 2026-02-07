"""
PPR download and import route.
Download PPR-ALL.zip, unzip CSV, import. POST returns job_id; frontend polls GET status until completed.
"""

import io
import logging
import threading
import uuid
import zipfile
from collections import defaultdict
from typing import Any, Dict, List

import requests
import urllib3
from fastapi import APIRouter, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from api.schemas import (
    PprImportJobStartResponse,
    PprImportStatusResponse,
    PprUploadResponse,
)
from api.services.bing_geocoder import BingGeocoder
from api.services.daft_scraper import DaftScraper
from api.services.ppr_csv_parser import parse_ppr_csv
from config import get_db_instance
from database import AddressRepository, PriceHistoryRepository, PropertyRepository
from models import PriceHistoryModel

logger = logging.getLogger(__name__)

# PPR download uses verify=False due to site's SSL chain; suppress the warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

router = APIRouter()

# Max upload size 100MB
MAX_FILE_SIZE = 100 * 1024 * 1024

# Default PPR zip URL (Property Price Register Ireland)
PPR_ZIP_URL = (
    "https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/"
    "Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip"
)

# In-memory job store for async import status (job_id -> { status, result?, error? })
_import_jobs: Dict[str, Dict[str, Any]] = {}
_import_jobs_lock = threading.Lock()


def _process_ppr_content(content: bytes, db: Session) -> PprUploadResponse:
    """Parse CSV content and import (create/update properties, geocode, Daft scrape). Returns response counts."""
    logger.info(
        "Import: parsing PPR CSV (%s bytes, %.1f MB)",
        len(content),
        len(content) / (1024 * 1024),
    )
    errors: List[str] = []
    try:
        property_data_list, price_history_records = parse_ppr_csv(content)
    except ValueError as e:
        logger.warning("Import: CSV parse failed (ValueError): %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Import: CSV parse error")
        raise HTTPException(status_code=400, detail=f"Parse error: {e}")

    total_rows = sum(len(p["row_indices"]) for p in property_data_list)
    logger.info(
        "Import: parse OK — %s unique properties, %s price history rows, %s total rows",
        len(property_data_list),
        len(price_history_records),
        total_rows,
    )

    ph_by_hash: dict = defaultdict(list)
    for ph in price_history_records:
        ph_by_hash[ph["address_hash"]].append(ph)

    property_repo = PropertyRepository(db)
    address_repo = AddressRepository(db)
    price_history_repo = PriceHistoryRepository(db)
    geocoder = BingGeocoder(rate_limit_delay=0.2, timeout=10)
    daft_scraper = DaftScraper(rate_limit_delay=2.0, timeout=30)

    created = 0
    updated = 0
    skipped = 0
    geocoded = 0
    failed_geocode = 0
    daft_scraped = 0
    failed_daft = 0

    logger.info(
        "Import: starting property loop (%s properties). "
        "Matching by address_hash: found in DB → existing (update price history); not found → new (create + geocode + Daft)",
        len(property_data_list),
    )
    processed = 0
    progress_interval = 5000

    for prop in property_data_list:
        address_hash = prop["address_hash"]
        existing_address = address_repo.find_by_hash(
            address_hash
        )  # DB lookup: same hash = same property
        price_list = ph_by_hash.get(address_hash, [])

        try:
            if existing_address:
                property_id = existing_address.property_id
                updated += 1
                for ph in price_list:
                    sale_date = ph["date_of_sale"]
                    existing_ph = (
                        db.query(PriceHistoryModel)
                        .filter(
                            PriceHistoryModel.property_id == property_id,
                            PriceHistoryModel.date_of_sale == sale_date,
                        )
                        .first()
                    )
                    if existing_ph:
                        existing_ph.price = int(round(float(ph["price"]))) if ph.get("price") is not None else 0
                        existing_ph.not_full_market_price = ph["not_full_market_price"]
                        existing_ph.vat_exclusive = ph["vat_exclusive"]
                        existing_ph.description = ph["description"]
                        existing_ph.property_size_description = ph.get(
                            "property_size_description"
                        )
                    else:
                        price_history_repo.create_price_history(
                            property_id=property_id,
                            date_of_sale=sale_date,
                            price=int(round(float(ph["price"]))) if ph.get("price") is not None else 0,
                            not_full_market_price=ph["not_full_market_price"],
                            vat_exclusive=ph["vat_exclusive"],
                            description=ph["description"],
                            property_size_description=ph.get(
                                "property_size_description"
                            ),
                        )
            else:
                property_obj = property_repo.get_or_create_property()
                db.flush()
                address_repo.create_address(
                    property_id=property_obj.id,
                    address=prop["address"],
                    county=prop["county"],
                    eircode=prop.get("eircode"),
                    address_hash=address_hash,
                )
                db.flush()
                created += 1
                addr_obj = address_repo.find_by_hash(address_hash)
                if not addr_obj:
                    errors.append(f"Address not found after create: {prop['address']}")
                    db.rollback()
                    continue
                for ph in price_list:
                    sale_date = ph["date_of_sale"]
                    price_history_repo.create_price_history(
                        property_id=property_obj.id,
                        date_of_sale=sale_date,
                        price=int(round(float(ph["price"]))) if ph.get("price") is not None else 0,
                        not_full_market_price=ph["not_full_market_price"],
                        vat_exclusive=ph["vat_exclusive"],
                        description=ph["description"],
                        property_size_description=ph.get("property_size_description"),
                    )
                geo = geocoder.geocode_address(
                    prop["address"],
                    prop["county"],
                    prop.get("eircode"),
                )
                if (
                    geo
                    and geo.get("latitude") is not None
                    and geo.get("longitude") is not None
                ):
                    address_repo.update_geo_data(
                        addr_obj.id,
                        geo["latitude"],
                        geo["longitude"],
                        geo.get("formatted_address"),
                        geo.get("country"),
                    )
                    geocoded += 1
                else:
                    failed_geocode += 1
                # Daft.ie: Bing search for address + county -> daft URL, title, body
                daft_result = daft_scraper.search_bing_for_daft(
                    prop["address"],
                    prop["county"],
                )
                if daft_result and daft_result.get("href"):
                    property_repo.update_daft_data(
                        property_obj.id,
                        daft_url=daft_result["href"],
                        daft_title=daft_result.get("title") or "",
                        daft_body=daft_result.get("body") or "",
                        daft_scraped=True,
                    )
                    daft_scraped += 1
                else:
                    property_repo.update_daft_data(
                        property_obj.id,
                        daft_scraped=True,
                    )
                    failed_daft += 1
            db.commit()
        except Exception as e:
            db.rollback()
            errors.append(f"{prop['address']}: {e}")
            logger.exception("Error processing property %s", prop.get("address"))

        processed += 1
        if processed % progress_interval == 0:
            logger.info(
                "Import: progress %s/%s — new(created)=%s existing(updated)=%s geocoded=%s failed_geocode=%s daft_scraped=%s failed_daft=%s",
                processed,
                len(property_data_list),
                created,
                updated,
                geocoded,
                failed_geocode,
                daft_scraped,
                failed_daft,
            )

    logger.info(
        "Import: complete — new(created)=%s existing(updated)=%s geocoded=%s failed_geocode=%s daft_scraped=%s failed_daft=%s errors=%s",
        created,
        updated,
        geocoded,
        failed_geocode,
        daft_scraped,
        failed_daft,
        len(errors),
    )
    return PprUploadResponse(
        total_rows=total_rows,
        unique_properties=len(property_data_list),
        created=created,
        updated=updated,
        skipped=skipped,
        geocoded=geocoded,
        failed_geocode=failed_geocode,
        daft_scraped=daft_scraped,
        failed_daft=failed_daft,
        errors=errors[:50],
    )


def _run_download_and_import(job_id: str) -> None:
    """Background task: download zip, unzip CSV, run _process_ppr_content with a fresh DB session."""
    logger.info("[job %s] Background task started", job_id)
    db_instance = get_db_instance()
    session = db_instance.get_session()
    logger.info("[job %s] DB session acquired", job_id)
    try:
        logger.info("[job %s] Step 1/4: downloading zip from %s", job_id, PPR_ZIP_URL)
        resp = requests.get(
            PPR_ZIP_URL,
            timeout=300,
            headers=PPR_DOWNLOAD_HEADERS,
            verify=False,
        )
        resp.raise_for_status()
        zip_bytes = resp.content
        logger.info(
            "[job %s] Step 1/4: download OK — status=%s, size=%s bytes (%.1f MB)",
            job_id,
            resp.status_code,
            len(zip_bytes),
            len(zip_bytes) / (1024 * 1024),
        )
        if len(zip_bytes) > MAX_FILE_SIZE:
            logger.warning(
                "[job %s] Step 1/4: zip too large (%s bytes > %s), failing",
                job_id,
                len(zip_bytes),
                MAX_FILE_SIZE,
            )
            with _import_jobs_lock:
                _import_jobs[job_id] = {
                    "status": "failed",
                    "result": None,
                    "error": "Downloaded zip too large",
                }
            return
        logger.info("[job %s] Step 2/4: unzipping archive", job_id)
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            logger.info("[job %s] Step 2/4: zip has %s entries", job_id, len(names))
            csv_name = None
            for name in names:
                if name.lower().endswith(".csv"):
                    csv_name = name
                    break
            if not csv_name:
                logger.error(
                    "[job %s] Step 2/4: no .csv in zip (entries: %s)", job_id, names[:5]
                )
                with _import_jobs_lock:
                    _import_jobs[job_id] = {
                        "status": "failed",
                        "result": None,
                        "error": "No CSV file found in the zip",
                    }
                return
            logger.info("[job %s] Step 2/4: using CSV %s", job_id, csv_name)
            with zf.open(csv_name) as f:
                content = f.read()
        logger.info(
            "[job %s] Step 2/4: unzip OK — CSV size=%s bytes (%.1f MB)",
            job_id,
            len(content),
            len(content) / (1024 * 1024),
        )
        logger.info(
            "[job %s] Step 3/4: parsing and importing (create/update, geocode, Daft)",
            job_id,
        )
        result = _process_ppr_content(content, session)
        logger.info(
            "[job %s] Step 4/4: import done — total_rows=%s unique=%s created=%s updated=%s geocoded=%s daft_scraped=%s",
            job_id,
            result.total_rows,
            result.unique_properties,
            result.created,
            result.updated,
            result.geocoded,
            result.daft_scraped,
        )
        with _import_jobs_lock:
            _import_jobs[job_id] = {
                "status": "completed",
                "result": result,
                "error": None,
            }
        logger.info("[job %s] Background task completed successfully", job_id)
    except HTTPException as e:
        err_msg = e.detail if isinstance(e.detail, str) else str(e.detail)
        logger.warning("[job %s] Background task failed (HTTP): %s", job_id, err_msg)
        with _import_jobs_lock:
            _import_jobs[job_id] = {
                "status": "failed",
                "result": None,
                "error": err_msg,
            }
    except Exception as e:
        logger.exception("[job %s] Background task failed: %s", job_id, e)
        with _import_jobs_lock:
            _import_jobs[job_id] = {"status": "failed", "result": None, "error": str(e)}
    finally:
        session.close()
        logger.info("[job %s] DB session closed", job_id)


@router.get("/ppr-import-status/{job_id}", response_model=PprImportStatusResponse)
async def ppr_import_status(job_id: str) -> PprImportStatusResponse:
    """Get status of an async PPR import job. Poll until status is 'completed' or 'failed'."""
    with _import_jobs_lock:
        job = _import_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return PprImportStatusResponse(
        status=job["status"],
        result=job.get("result"),
        error=job.get("error"),
    )


def run_ppr_download_and_import_sync() -> PprUploadResponse:
    """
    Run PPR download-and-import synchronously (same flow as admin/upload).
    For use from CLI scripts. Caller must have initialized DB (set_db_instance) and loaded .env if needed.
    """
    logger.info("PPR sync: starting download from %s", PPR_ZIP_URL)
    db_instance = get_db_instance()
    logger.info("PPR sync: DB instance acquired")
    session = db_instance.get_session()
    logger.info("PPR sync: session acquired")
    try:
        resp = requests.get(
            PPR_ZIP_URL,
            timeout=300,
            headers=PPR_DOWNLOAD_HEADERS,
            verify=False,
        )
        resp.raise_for_status()
        zip_bytes = resp.content
        logger.info(
            "PPR sync: download OK — %s bytes (%.1f MB)",
            len(zip_bytes),
            len(zip_bytes) / (1024 * 1024),
        )
        if len(zip_bytes) > MAX_FILE_SIZE:
            session.close()
            raise HTTPException(status_code=400, detail="Downloaded zip too large")
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            csv_name = None
            for name in names:
                if name.lower().endswith(".csv"):
                    csv_name = name
                    break
            if not csv_name:
                session.close()
                raise HTTPException(
                    status_code=400, detail="No CSV file found in the zip"
                )
            with zf.open(csv_name) as f:
                content = f.read()
        logger.info(
            "PPR sync: unzip OK — CSV %s bytes (%.1f MB)",
            len(content),
            len(content) / (1024 * 1024),
        )
        result = _process_ppr_content(content, session)
        logger.info(
            "PPR sync: done — new(created)=%s existing(updated)=%s geocoded=%s daft_scraped=%s",
            result.created,
            result.updated,
            result.geocoded,
            result.daft_scraped,
        )
        return result
    finally:
        session.close()


# Headers and cookies for Property Price Register download (browser-like)
PPR_DOWNLOAD_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-GB,en;q=0.9,ru-RU;q=0.8,ru;q=0.7,hy-AM;q=0.6,hy;q=0.5,en-US;q=0.4",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Cookie": (
        "BIGipServerpool-HTTPS-C_169-D_170=2852134410.47873.0000; "
        "_pk_id.33.72c8=f37e07611967dfd2.1769985176.; "
        'CookieScriptConsent={"action":"accept","categories":"[\\"performance\\"]","key":"1e64d783-3d16-44ca-8a1c-f2e0b88725af"}; '
        "f5avraaaaaaaaaaaaaaaa_session_=KGIJBKFDNCEHKADKGFILOLHGCDKJOEEGKIIEPAAPDGHNJDNKFFBPCKEGPMJFECHOONEDOODLEHEDDMCLJHIAGGOJNPLOIIHEOMHNNHMANENHHKCIMABPKFGKBAPGCHFD; "
        "_pk_ref.33.72c8=%5B%22%22%2C%22%22%2C1770478712%2C%22https%3A%2F%2Fwww.google.com%2F%22%5D; "
        "_pk_ses.33.72c8=1; "
        "TSa76d061c027=08c4192abcab2000ecc7bb7249398351a25d25d067069575dd51b31fc941d835c31442c5a98b079308d137cf54113000d4eccbd67bb9686d8613160efe803105d31bc6fa8036f9b910175cb41006d3b5b23b732129747cb7f083383e4ac5382f"
    ),
    "DNT": "1",
    "Referer": "https://www.google.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


@router.post(
    "/ppr-download-and-import",
    response_model=PprImportJobStartResponse,
    status_code=202,
)
async def ppr_download_and_import(
    background_tasks: BackgroundTasks,
) -> PprImportJobStartResponse:
    """Start async download of PPR-ALL.zip and import. Returns job_id; poll GET /ppr-import-status/{job_id} for result."""
    job_id = str(uuid.uuid4())
    with _import_jobs_lock:
        _import_jobs[job_id] = {"status": "running", "result": None, "error": None}
    background_tasks.add_task(_run_download_and_import, job_id)
    logger.info(
        "PPR download-and-import: request received, job %s queued (poll /ppr-import-status/%s)",
        job_id,
        job_id,
    )
    return PprImportJobStartResponse(job_id=job_id)
