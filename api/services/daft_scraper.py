"""
Bing search for Daft.ie links (address -> daft URL, title, body).
Ported from DataScraper/daft_scraper.py; no imports from DataScraper.
"""

import base64
import logging
import random
import re
import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class DaftScraper:
    """Search Bing for address + county + daft.ie and return first Daft.ie result."""

    def __init__(self, rate_limit_delay: float = 2.0, timeout: int = 30):
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.last_request_time = 0.0
        self.session = requests.Session()
        self.user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        ]

    def _get_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-GB,en;q=0.9",
            "user-agent": random.choice(self.user_agents),
        }
        if referer:
            headers["referer"] = referer
        return headers

    def _rate_limit(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def _decode_bing_url(self, href: str) -> str:
        """Decode Bing redirect URL (u=base64) to final URL."""
        url_match = re.search(r"u=([^&]+)", href)
        if not url_match:
            return href
        try:
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            u_list = qs.get("u")
            if u_list:
                encoded_url = u_list[0]
                if encoded_url.startswith("a1"):
                    encoded_url = encoded_url[2:]
                return base64.b64decode(encoded_url).decode("utf-8")
        except Exception as e:
            logger.debug("Decode Bing URL: %s", e)
        return href

    def _is_daft_link(self, href: str, aria_label: str) -> bool:
        """True if link is to Daft.ie (by href or aria-label)."""
        if not href or href.startswith("/search"):
            return False
        resolved = self._decode_bing_url(href)
        if "daft.ie" in resolved.lower():
            return True
        if "daft" in (aria_label or "").lower():
            return True
        return False

    def search_bing_for_daft(
        self, address: str, county: str, max_results: int = 10
    ) -> Optional[Dict[str, Any]]:
        """Search Bing for address + county + daft.ie. Returns first Daft.ie result with title, href, body or None."""
        query = f"{address} {county} daft.ie"
        self._rate_limit()
        try:
            search_response = self.session.get(
                "https://www.bing.com/search",
                params={"q": query},
                headers=self._get_headers(),
                timeout=self.timeout,
            )
            search_response.raise_for_status()
            soup = BeautifulSoup(search_response.text, "html.parser")
            result_elements = soup.find_all("li", class_="b_algo")
            if not result_elements:
                result_elements = soup.select('li[class*="b_algo"]')

            for elem in result_elements[:max_results]:
                try:
                    # All links in this result (include without target="_blank")
                    links = elem.find_all("a", href=True)
                    for link in links:
                        href = link.get("href", "")
                        aria_label = (link.get("aria-label") or "").lower()
                        if not self._is_daft_link(href, aria_label):
                            continue

                        href = self._decode_bing_url(href)
                        if "daft.ie" not in href.lower():
                            continue

                        title = ""
                        h2_elem = elem.find("h2")
                        if h2_elem:
                            title_link = h2_elem.find("a")
                            if title_link:
                                title = title_link.get_text(strip=True)
                        if not title:
                            title = link.get_text(strip=True)
                        if not title and h2_elem:
                            title = h2_elem.get_text(strip=True)

                        b_caption = elem.find("div", class_="b_caption")
                        body = b_caption.get_text(strip=True) if b_caption else ""

                        return {
                            "title": title or "Daft.ie Property",
                            "href": href,
                            "body": body,
                        }
                except Exception as e:
                    logger.debug("Parse b_algo element: %s", e)
                    continue

            # Fallback: any link in the page pointing to daft.ie
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                resolved = self._decode_bing_url(href)
                if "daft.ie" in resolved.lower() and "daft.ie" in resolved:
                    title = a.get_text(strip=True) or "Daft.ie Property"
                    return {
                        "title": title[:200] if len(title) > 200 else title,
                        "href": resolved,
                        "body": "",
                    }
            return None
        except requests.exceptions.RequestException as e:
            logger.warning("Bing search error for %s: %s", query, e)
            return None
        except Exception as e:
            logger.warning("Daft search error for %s: %s", query, e)
            return None
