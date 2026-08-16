"""Fetch PMDA approval data PDF from the primary English review portal.

PMDA publishes a single consolidated PDF covering April 2004 to present at
https://www.pmda.go.jp/english/review-services/reviews/approved-information/drugs/0002.html
The link target changes with each update; we scrape the landing page to find
the current PDF URL.
"""

import logging
import re
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

LANDING_URL = (
    "https://www.pmda.go.jp/english/review-services/reviews/"
    "approved-information/drugs/0002.html"
)
RAW_DIR = Path("data/raw/pmda/primary")
PDF_FILENAME = "pmda_approvals.pdf"


def fetch_landing_page(force: bool = False) -> str:
    """Fetch and cache the PMDA landing HTML."""
    cache = RAW_DIR / "landing.html"
    cache.parent.mkdir(parents=True, exist_ok=True)
    if cache.exists() and not force:
        return cache.read_text(encoding="utf-8")
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(LANDING_URL)
        resp.raise_for_status()
    cache.write_text(resp.text, encoding="utf-8")
    return resp.text


def find_approval_pdf_url(html: str) -> str:
    """Extract the absolute URL of the approval PDF from the landing HTML.

    The link is typically labeled with a date range like "April 2004 to <Month> <Year>".
    """
    # Match anchor tags with PDF hrefs containing /files/<digits>.pdf
    pattern = re.compile(
        r'<a[^>]*href="(/files/\d+\.pdf)"[^>]*>([^<]*)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(html):
        href, label = m.group(1), m.group(2)
        if "april 2004" in label.lower() or "approved drug" in label.lower():
            return f"https://www.pmda.go.jp{href}"
    # Fallback: take the first /files/*.pdf link
    m = re.search(r'href="(/files/\d+\.pdf)"', html, re.IGNORECASE)
    if m:
        return f"https://www.pmda.go.jp{m.group(1)}"
    raise ValueError("No approval PDF link found on PMDA landing page")


def download_pdf(url: str, dest: Path, force: bool = False) -> Path:
    """Download a PDF if not already cached."""
    if dest.exists() and not force:
        logger.info("Using cached PMDA PDF: %s", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading PMDA PDF from %s", url)
    with httpx.Client(timeout=300, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    dest.write_bytes(resp.content)
    logger.info("Wrote %d bytes to %s", len(resp.content), dest)
    return dest


def fetch_primary_pdf(force: bool = False) -> Path:
    """Fetch the latest PMDA approval PDF. Returns path to cached file."""
    dest = RAW_DIR / PDF_FILENAME
    if dest.exists() and not force:
        return dest
    html = fetch_landing_page(force=force)
    url = find_approval_pdf_url(html)
    return download_pdf(url, dest, force=force)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    path = fetch_primary_pdf()
    print(path)
