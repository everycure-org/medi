"""Look up PMDA per-drug review report URLs.

PMDA publishes per-drug English review reports on the master listing page:
    https://www.pmda.go.jp/english/review-services/reviews/approved-information/drugs/0001.html

Each row links to a PDF at the stable URL pattern:
    https://www.pmda.go.jp/drugs/<YEAR>/P<reviewID>/<approval_numbers>_..._A100_<n>.pdf

These are the per-product authoritative review documents — the closest PMDA
equivalent to a DailyMed SPL or an FDA NDA. We scrape the listing once and
build a name -> review URL index, cached to disk.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from medic.enrichment.cache import EnrichmentCache

logger = logging.getLogger(__name__)

LISTING_URL = (
    "https://www.pmda.go.jp/english/review-services/reviews/"
    "approved-information/drugs/0001.html"
)
CACHE_PATH = Path("cache/enrichment/pmda_review_urls.json")
LANDING_CACHE = Path("cache/downloads/pmda/0001.html")
REVIEW_REPORTS_DIR = Path("cache/downloads/pmda/review_reports")

# Polite-fetch settings for per-product PDF downloads
_USER_AGENT = (
    "MeDIC/1.0 (research-curation; https://github.com/everycure-org/medic; "
    "contact: nicolas.matentzoglu@gmail.com)"
)
_FETCH_SLEEP_SECONDS = 1.0
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0  # exponential: 2s, 4s, 8s
# Search-page fallback URL fragment that indicates we don't have a real per-
# product PDF, just a brand-name search page. We never try to download these.
_SEARCH_PAGE_FRAGMENT = "PmdaSearch/iyakuSearch/"

_safe_name_re = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename(name: str) -> str:
    """Make a filesystem-safe stem from a drug name. Lowercases + strips junk."""
    s = (name or "").strip().lower()
    s = _safe_name_re.sub("_", s)
    s = s.strip("_") or "unknown"
    return s[:120]  # cap length to be friendly to all filesystems

_index: dict[str, str] | None = None
_built = False


def _fetch_landing(force: bool = False) -> str:
    LANDING_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if LANDING_CACHE.exists() and not force:
        return LANDING_CACHE.read_text(encoding="utf-8")
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(LISTING_URL)
        resp.raise_for_status()
    LANDING_CACHE.write_text(resp.text, encoding="utf-8")
    return resp.text


def _normalize(name: str) -> str:
    """Lowercase + strip salt/hydrate noise so PMDA INNs match drug labels."""
    s = (name or "").lower().strip()
    # Drop common salt suffixes for matching
    s = re.sub(
        r"\s*(?:hydrochloride|hydrate|monohydrate|dihydrate|trihydrate|sodium|"
        r"potassium|calcium|sulfate|sulphate|tartrate|besylate|mesylate|"
        r"fumarate|disodium|maleate|tosylate|acetate|"
        r"\(genetical recombination\)|\(genetical, recombinant\)|\(recombinant\))\s*",
        " ",
        s,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


_TYPE_WORDS_RE = re.compile(
    r"\s+(?:Partial\s+Change\s+Approval|Approval|Re-?examination|Reexamination|"
    r"Note\s*\d*|\(.*?\))\s*$",
    re.IGNORECASE,
)
_PRODUCT_ID_RE = re.compile(r"/drugs/(\d{4})/(P\d+)/")


def _absolute(href: str) -> str:
    return href if href.startswith("http") else f"https://www.pmda.go.jp{href}"


def build_index(force: bool = False) -> dict[str, dict]:
    """Scrape the PMDA listing and return {normalized_drug_name: entry}.

    Each entry is {"url": EN_pdf_url, "product_id": "P20190905002", "year": 2019}.
    Indexed by both brand name and active ingredient (most recent year wins).

    The EN PDF URL is the user-facing English review report (often hosted at
    /files/<num>.pdf). The product_id is extracted from the JP URL pattern
    /drugs/<year>/P<reviewID>/, which is the stable per-product handle.
    """
    cache = EnrichmentCache(CACHE_PATH)
    cached = cache.get("__INDEX__")
    if cached and not force:
        return cached.get("map") or {}

    html = _fetch_landing(force=force)
    soup = BeautifulSoup(html, "html.parser")

    candidates: list[tuple[str, dict, int]] = []  # (name, entry, year)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        header = rows[0].get_text(" ", strip=True).lower()
        if "non-proprietary" not in header or "brand name" not in header:
            continue
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 4:
                continue
            brand_text = cells[0].get_text(" ", strip=True)
            inn_text = cells[1].get_text(" ", strip=True)
            brand_clean = _TYPE_WORDS_RE.sub("", brand_text).strip()

            # Walk anchors in the row; grab EN href and JP href separately
            en_url = ""
            jp_url = ""
            for a in row.find_all("a", href=True):
                text = a.get_text(strip=True).lower()
                href = a["href"]
                if "english" in text and not en_url:
                    en_url = _absolute(href)
                elif "japanese" in text and not jp_url:
                    jp_url = _absolute(href)

            # Extract stable product_id and year from the JP URL pattern
            product_id = ""
            year = None
            m = _PRODUCT_ID_RE.search(jp_url)
            if m:
                year = int(m.group(1))
                product_id = m.group(2)
            else:
                # Try EN url as fallback (some entries have the structured URL on EN)
                m = _PRODUCT_ID_RE.search(en_url)
                if m:
                    year = int(m.group(1))
                    product_id = m.group(2)

            preferred_url = en_url or jp_url
            if not preferred_url or year is None:
                continue

            entry = {
                "url": preferred_url,
                "product_id": product_id,
                "year": year,
            }
            # Index by brand, INN, and individual ingredients (combinations split on /, ;)
            indexable_names = {brand_clean, inn_text, brand_text}
            for ing in re.split(r"\s*[/;,]\s*|\s+and\s+", inn_text, flags=re.IGNORECASE):
                ing = ing.strip()
                if ing and len(ing) > 3:
                    indexable_names.add(ing)
            for name in indexable_names:
                key = _normalize(name)
                if not key:
                    continue
                candidates.append((key, entry, year))

    # Pick the most recent review per name
    out: dict[str, tuple[dict, int]] = {}
    for name, entry, year in candidates:
        prev = out.get(name)
        if prev is None or year > prev[1]:
            out[name] = (entry, year)
    flat = {n: e for n, (e, _) in out.items()}
    cache.put("__INDEX__", {"map": flat, "size": len(flat)})
    cache.flush()
    logger.info("PMDA review index: %d unique names", len(flat))
    return flat


def _ensure_index() -> dict[str, dict]:
    global _index, _built
    if not _built:
        try:
            _index = build_index()
        except Exception as e:
            logger.warning("PMDA review index build failed: %s", e)
            _index = {}
        _built = True
    return _index or {}


def lookup_review(drug_name: str) -> dict:
    """Return {url, product_id, year} for a drug name, or {}."""
    idx = _ensure_index()
    entry = idx.get(_normalize(drug_name)) or {}
    return entry


def lookup_review_url(drug_name: str) -> str:
    """Return the PMDA per-drug review URL, or empty string."""
    return (lookup_review(drug_name) or {}).get("url", "")


def fetch_review_report(url: str, drug_name: str) -> Path | None:
    """Download a PMDA per-product review report PDF and cache it.

    The cache lives at ``cache/downloads/pmda/review_reports/<safe_drug>.pdf``.
    Subsequent calls with the same ``drug_name`` skip the network and return the
    cached path. Search-page fallback URLs (``PmdaSearch/iyakuSearch/``) are
    refused — only per-product PDFs are downloaded.

    Args:
        url: The review-report URL from :func:`lookup_review`.
        drug_name: Drug label / brand to use for the on-disk filename. Need not
            match the index key — purely a convenience for the human reader.

    Returns:
        Path to the downloaded PDF on success, or ``None`` if the URL is the
        search-page fallback, empty, or the download failed permanently.
    """
    if not url:
        return None
    if _SEARCH_PAGE_FRAGMENT in url:
        # Search-page fallback — no per-product PDF available, skip silently.
        logger.debug("Skipping search-page fallback URL for %s: %s", drug_name, url)
        return None

    REVIEW_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = REVIEW_REPORTS_DIR / f"{_safe_filename(drug_name)}.pdf"
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    headers = {"User-Agent": _USER_AGENT}
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            # Polite: sleep before every network round-trip (not just retries)
            time.sleep(_FETCH_SLEEP_SECONDS)
            with httpx.Client(timeout=120, follow_redirects=True, headers=headers) as client:
                resp = client.get(url)
            if resp.status_code == 200:
                dest.write_bytes(resp.content)
                logger.info(
                    "PMDA review report downloaded: %s (%d bytes) -> %s",
                    url, len(resp.content), dest,
                )
                return dest
            if 500 <= resp.status_code < 600:
                # Retryable server error — exponential backoff.
                wait = _BACKOFF_BASE ** attempt
                logger.warning(
                    "PMDA review fetch %s returned %d (attempt %d/%d), retrying in %.1fs",
                    url, resp.status_code, attempt + 1, _MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue
            # 4xx — not retryable.
            logger.warning(
                "PMDA review fetch %s returned %d, giving up", url, resp.status_code,
            )
            return None
        except httpx.HTTPError as e:
            last_exc = e
            wait = _BACKOFF_BASE ** attempt
            logger.warning(
                "PMDA review fetch %s raised %s (attempt %d/%d), retrying in %.1fs",
                url, e, attempt + 1, _MAX_RETRIES, wait,
            )
            time.sleep(wait)
    logger.error("PMDA review fetch %s failed after %d attempts: %s",
                 url, _MAX_RETRIES, last_exc)
    return None
