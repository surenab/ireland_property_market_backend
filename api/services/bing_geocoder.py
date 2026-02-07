"""
Bing Maps geocoding service for address -> lat/long.
Ported from DataScraper/bing_geocoder.py; no imports from DataScraper.
Uses Bing Maps overlay HTML endpoint; rate limiting and retries.
"""

import json
import logging
import re
import time
from typing import Any, Dict, Optional

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout

from models import generate_address_hash

logger = logging.getLogger(__name__)


class BingGeocoder:
    """Bing Maps geocoding (lat/long) via overlay endpoint."""

    def __init__(
        self,
        rate_limit_delay: float = 0.2,
        timeout: int = 10,
    ):
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.last_request_time = 0.0
        self.base_url = "https://www.bing.com/maps/overlaybfpr"
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def _rate_limit(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def _build_query(
        self, address: str, county: str, eircode: Optional[str] = None
    ) -> str:
        parts = [address]
        if county:
            parts.append(county)
        if eircode:
            parts.append(eircode)
        parts.append("Ireland")
        return ", ".join(filter(None, parts))

    def geocode_address(
        self,
        address: str,
        county: str,
        eircode: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Geocode a single address. Returns dict with latitude, longitude, formatted_address, country or None."""
        cache_key = generate_address_hash(address, county, eircode)
        if cache_key in self._cache:
            return self._cache[cache_key]

        query = self._build_query(address, county, eircode)
        self._rate_limit()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                params = {
                    "q": query,
                    "localMapView": "",
                    "filters": 'MapCardType:"unknown" direction_partner:"maps"',
                    "ads": "1",
                    "count": "20",
                    "ecount": "20",
                    "first": "0",
                    "efirst": "1",
                    "form": "MPSRBX",
                    "cardType": "unknown",
                    "cardWidth": "424",
                    "srs": "sb",
                    "mapsV10": "1",
                }
                headers = {
                    "accept": "*/*",
                    "accept-language": "en-US,en;q=0.9",
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                    "referer": f"https://www.bing.com/maps/search?style=r&q={requests.utils.quote(query)}",
                }
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                overlay_container = soup.find("div", class_="overlay-container")
                entity_data = None
                latitude = None
                longitude = None
                formatted_address = None
                country = "Ireland"

                if overlay_container and overlay_container.get("data-entity"):
                    try:
                        entity_data = json.loads(overlay_container["data-entity"])
                        geometry = entity_data.get("geometry", {})
                        if geometry:
                            longitude = geometry.get("x")
                            latitude = geometry.get("y")
                        entity_info = entity_data.get("entity", {}).get("entity", {})
                        if entity_info:
                            formatted_address = (
                                entity_info.get("address") or entity_info.get("title")
                            )
                            if formatted_address and "Ireland" in formatted_address:
                                country = "Ireland"
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.debug("Parse data-entity: %s", e)

                if latitude is None or longitude is None:
                    lat_long_div = soup.find("div", class_="geochainModuleLatLong")
                    if lat_long_div:
                        lat_long_text = lat_long_div.get_text(strip=True)
                        match = re.search(
                            r"(-?\d+\.?\d*),\s*(-?\d+\.?\d*)", lat_long_text
                        )
                        if match:
                            latitude = float(match.group(1))
                            longitude = float(match.group(2))

                if formatted_address is None:
                    h2 = soup.find("h2")
                    if h2:
                        formatted_address = h2.get_text(strip=True)

                if latitude is not None and longitude is not None:
                    result = {
                        "latitude": latitude,
                        "longitude": longitude,
                        "formatted_address": formatted_address or query,
                        "country": country,
                    }
                    self._cache[cache_key] = result
                    logger.debug("Geocoded %s -> (%s, %s)", query, latitude, longitude)
                    return result

                logger.warning("No coordinates in response for: %s", query)
                self._cache[cache_key] = None
                return None

            except Timeout:
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                logger.error("Geocoding timeout after %s attempts: %s", max_retries, query)
                self._cache[cache_key] = None
                return None
            except requests.exceptions.HTTPError as e:
                code = e.response.status_code if e.response else None
                if code == 429 and attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 5)
                    continue
                logger.error("HTTP error geocoding %s: %s", query, e)
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                self._cache[cache_key] = None
                return None
            except RequestException as e:
                logger.error("Request error geocoding %s: %s", query, e)
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                self._cache[cache_key] = None
                return None
            except Exception as e:
                logger.error("Unexpected error geocoding %s: %s", query, e)
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)
                    continue
                self._cache[cache_key] = None
                return None

        self._cache[cache_key] = None
        return None
