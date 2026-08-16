"""Download and cache EMA EPAR Product Information PDFs.

EMA medicines have a dedicated landing page on the EMA website following the
pattern::

    https://www.ema.europa.eu/en/medicines/human/EPAR/<slug>

Each landing page links to a "Product information" PDF (the regulatory
document equivalent to a DailyMed SPL) at a deterministic URL keyed on the
same slug::

    https://www.ema.europa.eu/en/documents/product-information/<slug>-epar-product-information_en.pdf

This module is responsible for fetching and caching those PDFs locally.
It is deliberately polite (single-threaded, 1s sleep between fetches,
exponential backoff on 5xx, project-identifying User-Agent) because EMA
serves these directly from www.ema.europa.eu.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Public constants ---------------------------------------------------------

EPAR_PDF_URL_TEMPLATE = (
    "https://www.ema.europa.eu/en/documents/product-information/"
    "{slug}-epar-product-information_en.pdf"
)
"""Deterministic URL pattern for EMA EPAR Product Information PDFs."""

CACHE_DIR = Path("cache/downloads/ema/epar")
"""Cache directory for downloaded EPAR PDFs."""

USER_AGENT = (
    "MeDIC/0.1 (Medicines, Diseases, Indications, and Contraindications; "
    "https://github.com/everycure-org/medic; contact: medic@everycure.org)"
)
"""User-Agent header identifying this project to EMA servers."""

_FETCH_SLEEP_SECONDS = 1.0
_MAX_RETRIES = 4
_BACKOFF_BASE_SECONDS = 2.0
_REQUEST_TIMEOUT_SECONDS = 60


def _build_pdf_url(slug: str) -> str:
    """Build the EPAR PDF URL for a given EMA medicine slug."""
    return EPAR_PDF_URL_TEMPLATE.format(slug=slug.lower())


def _cache_path(slug: str) -> Path:
    """Return the local cache path for a given slug's EPAR PDF."""
    return CACHE_DIR / f"{slug.lower()}.pdf"


def fetch_epar_pdf(slug: str, force: bool = False) -> Path | None:
    """Fetch and cache the EMA EPAR Product Information PDF for a slug.

    Returns the local cached path on success, or ``None`` if the PDF cannot
    be fetched (e.g. 404 — slug does not have an EPAR document).

    Behaviour:
    - If a cached file already exists at ``cache/downloads/ema/epar/<slug>.pdf``
      and ``force`` is False, the cached path is returned without a network call.
    - On 404, returns ``None`` (the slug is not an EMA EPAR or has no PDF).
    - On 5xx, retries up to 4 times with exponential backoff (2s, 4s, 8s, 16s).
    - Sleeps 1 second after each successful network fetch (no sleep on cache hit
      or on terminal 404), to be respectful to EMA bandwidth.
    - The User-Agent header identifies this project.

    Args:
        slug: EMA medicine slug, e.g. ``"keppra"``. Case-insensitive
            (normalized to lowercase before constructing URL/path).
        force: If True, re-download even if a cached copy exists.

    Returns:
        ``Path`` to the cached PDF on success, or ``None`` on 404/empty slug.
    """
    if not slug:
        return None

    slug = slug.strip().lower()
    if not slug:
        return None

    dest = _cache_path(slug)
    if dest.exists() and not force:
        logger.debug("EPAR PDF cache hit: %s", dest)
        return dest

    url = _build_pdf_url(slug)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/pdf,*/*"}

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=_REQUEST_TIMEOUT_SECONDS,
                stream=True,
            )
        except requests.RequestException as exc:
            last_exc = exc
            wait = _BACKOFF_BASE_SECONDS ** (attempt + 1)
            logger.warning(
                "EPAR fetch network error for slug=%s (attempt %d/%d): %s; "
                "retrying in %.1fs",
                slug, attempt + 1, _MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
            continue

        status = response.status_code
        if status == 404:
            logger.info("EPAR PDF not found (404) for slug=%s url=%s", slug, url)
            response.close()
            return None
        if 500 <= status < 600:
            wait = _BACKOFF_BASE_SECONDS ** (attempt + 1)
            logger.warning(
                "EPAR fetch %d for slug=%s (attempt %d/%d); retrying in %.1fs",
                status, slug, attempt + 1, _MAX_RETRIES, wait,
            )
            response.close()
            time.sleep(wait)
            continue
        if status != 200:
            logger.warning(
                "EPAR fetch unexpected status=%d for slug=%s url=%s",
                status, slug, url,
            )
            response.close()
            return None

        # Success — stream to cache atomically.
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".part")
        try:
            with open(tmp_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        fh.write(chunk)
            tmp_path.replace(dest)
        finally:
            response.close()
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

        logger.info("EPAR PDF cached: %s (%d bytes)", dest, dest.stat().st_size)
        # Polite delay after a successful network fetch.
        time.sleep(_FETCH_SLEEP_SECONDS)
        return dest

    logger.error(
        "EPAR fetch exhausted %d retries for slug=%s; last error=%s",
        _MAX_RETRIES, slug, last_exc,
    )
    return None
