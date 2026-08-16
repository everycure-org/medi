"""Parse EMA EPAR Product Information PDFs to extract §4.3 Contraindications.

EMA EPAR Product Information documents follow the SmPC (Summary of Product
Characteristics) structure mandated by the European Commission. Section 4.3
"Contraindications" appears in every authorized SmPC and is bounded by 4.4
"Special warnings and precautions for use".

This module uses ``pdfplumber`` (already a project dep) to extract page text
and a defensive regex to locate §4.3.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Section start: a line that is "4.3" or "4.3." followed by "Contraindications".
# Tolerates minor whitespace and case variations. The PDF text typically has
# one section header per line.
_SECTION_START_RE = re.compile(
    r"\n\s*4\s*\.\s*3\.?\s+Contraindications\b[^\n]*\n",
    re.IGNORECASE,
)
# Section end: next subsection 4.4 (Special warnings...). We use ``\b`` to
# avoid swallowing things like "4.40mg".
_SECTION_END_RE = re.compile(
    r"\n\s*4\s*\.\s*4\b",
    re.IGNORECASE,
)


def _extract_full_text(pdf_path: Path) -> str:
    """Extract all page text from a PDF using pdfplumber.

    Returns an empty string if the PDF is unreadable.
    """
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - dep is declared
        raise RuntimeError(
            "pdfplumber is required for EMA EPAR parsing. Install it via "
            "`uv sync` (already declared in pyproject.toml)."
        ) from exc

    pages_text: list[str] = []
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                try:
                    text = page.extract_text() or ""
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "pdfplumber failed on page in %s: %s", pdf_path, exc,
                    )
                    text = ""
                pages_text.append(text)
    except Exception as exc:  # noqa: BLE001
        logger.warning("pdfplumber could not open %s: %s", pdf_path, exc)
        return ""

    # Join with explicit newlines so cross-page section detection still works.
    return "\n".join(pages_text)


def _strip_running_artifacts(text: str) -> str:
    """Best-effort cleanup: collapse repeated whitespace at line boundaries.

    EMA EPARs sometimes embed page numbers / running headers like
    "Product information page 5 of 142". A surgical fix is hard without
    layout analysis, so we just normalize whitespace runs at line boundaries
    to make the regex more robust.
    """
    # Trim trailing whitespace per line, collapse 3+ blank lines to 2.
    out_lines = [line.rstrip() for line in text.splitlines()]
    text = "\n".join(out_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def extract_contraindications_text(pdf_path: Path) -> str:
    """Extract the §4.3 Contraindications section text from an EPAR PDF.

    Locates the section header ``4.3 Contraindications`` and returns the text
    up to (but not including) ``4.4`` (the next subsection — Special warnings
    and precautions for use).

    Returns an empty string if the PDF cannot be read or the section header
    is not found.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.warning("EPAR PDF does not exist: %s", pdf_path)
        return ""

    raw = _extract_full_text(pdf_path)
    if not raw:
        return ""

    cleaned = _strip_running_artifacts(raw)
    # Add a leading newline so the regex (which anchors on \n) can match a
    # header that happens to be at the very top.
    haystack = "\n" + cleaned

    start_match = _SECTION_START_RE.search(haystack)
    if not start_match:
        logger.info(
            "Could not find §4.3 Contraindications header in %s", pdf_path,
        )
        return ""

    section_start = start_match.end()
    end_match = _SECTION_END_RE.search(haystack, section_start)
    section_end = end_match.start() if end_match else len(haystack)

    section = haystack[section_start:section_end].strip()
    return section
