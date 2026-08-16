"""Parse the PMDA approvals PDF into structured records.

The PDF tabulates approvals page-by-page with consistent columns:
  - Review Category
  - Approval Date (e.g. "May 19, 2025")
  - No.
  - Brand Name (Applicant Company)
  - New Approval / Partial Change
  - Active Ingredient (underlined: new active ingredient)
  - Notes (the indication snippet — readable English text)

Each page covers approvals for a single month. We also capture the
month/year header from page text for context.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)


_MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Sept": "09", "Oct": "10", "Nov": "11", "Dec": "12",
    "January": "01", "February": "02", "March": "03", "April": "04",
    "June": "06", "July": "07", "August": "08", "September": "09",
    "October": "10", "November": "11", "December": "12",
}

_HEADER_KEYS = {
    "review category", "approval date", "no.", "brand name",
    "new approval", "active ingredient", "notes",
}


def _parse_date(date_str: str) -> str:
    """Convert PMDA dates like 'May 19, 2025' or 'Jun. 26, 2023' to YYYYMMDD."""
    s = (date_str or "").strip()
    if not s:
        return ""
    # Strip surrounding whitespace and trailing punctuation
    m = re.match(r"([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})", s)
    if not m:
        return ""
    mon = _MONTHS.get(m.group(1)) or _MONTHS.get(m.group(1)[:3])
    if not mon:
        return ""
    return f"{m.group(3)}{mon}{int(m.group(2)):02d}"


def _is_header_row(row: list[str | None]) -> bool:
    """Detect the header row at the top of each page's table."""
    text = " ".join((c or "") for c in row).lower()
    matches = sum(1 for key in _HEADER_KEYS if key in text)
    return matches >= 3


def _clean_cell(value: str | None) -> str:
    """Collapse PMDA-style soft line breaks within a cell to spaces."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value.replace("\n", " ")).strip()


def _strip_orphan_tag(text: str) -> tuple[str, bool]:
    """Strip '[Orphan drug]' marker. Returns (cleaned_text, is_orphan)."""
    is_orphan = "[orphan drug]" in text.lower()
    cleaned = re.sub(r"\[Orphan drug\]\.?", "", text, flags=re.IGNORECASE).strip()
    return cleaned, is_orphan


def _split_brand_company(brand_cell: str) -> tuple[str, str]:
    """Split 'Brand Name X mg\\n(Company Co., Ltd.)' into (brand, company)."""
    if not brand_cell:
        return "", ""
    # Company is in the trailing parenthetical
    m = re.search(r"\(([^()]+(?:Co\.|K\.K\.|Inc\.|Ltd\.|Pharma|GK)[^()]*)\)\s*$", brand_cell)
    if m:
        company = m.group(1).strip()
        brand = brand_cell[: m.start()].strip()
    else:
        # Last parenthetical fallback
        last_open = brand_cell.rfind("(")
        last_close = brand_cell.rfind(")")
        if 0 <= last_open < last_close:
            company = brand_cell[last_open + 1 : last_close].strip()
            brand = brand_cell[:last_open].strip()
        else:
            company = ""
            brand = brand_cell.strip()
    return brand, company


_PARENTHETICAL_TO_STRIP = re.compile(
    r"\s*\((?:GENETICAL\s+RECOMBINATION|GENETICAL[\s,]+RECOMBINANT|RECOMBINANT|"
    r"RECOMB\.|HYDRATE|MONOHYDRATE|DIHYDRATE|TRIHYDRATE|HYDROCHLORIDE|"
    r"SODIUM|POTASSIUM|CALCIUM|SUCROSE|TARTRATE|BESYLATE|MESYLATE|"
    r"FUMARATE|SULFATE|SULPHATE|DISODIUM|MALATE|TOSYLATE)\)\s*",
    re.IGNORECASE,
)


def _split_ingredients(ingredient_cell: str) -> list[str]:
    """Split 'A; B / C and D' into individual ingredient names (uppercase, normalized).

    Strips trailing modifiers in parentheses like '(genetical recombination)' that
    confuse downstream grounding preprocessors. The salt/hydrate forms remain
    available in the original `source_ingredients` field on the parent record.
    """
    if not ingredient_cell:
        return []
    s = ingredient_cell.upper()
    # Strip footnote markers like (1) (2) etc.
    s = re.sub(r"\(\d+\)", " ", s)
    s = re.sub(r"\d+\)", " ", s)
    # Strip salt/recombination suffixes that confuse LLM preprocessors
    s = _PARENTHETICAL_TO_STRIP.sub(" ", s)
    # Split on common delimiters
    parts = re.split(r"[;,/]| AND ", s)
    return [p.strip() for p in parts if p.strip()]


def _extract_month_header(text: str) -> str:
    """Find the 'New Drugs Approved in <Month> <Year>' header on a page."""
    m = re.search(
        r"New Drugs Approved in ([A-Za-z]+(?:\s*\d{4})?)",
        text or "",
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else ""


def parse_pmda_pdf(pdf_path: Path) -> list[dict]:
    """Extract approval records from the PMDA approvals PDF.

    Returns a list of records with one entry per drug-row. Combination drugs
    where a row lists multiple active ingredients become multiple records
    (one per ingredient) sharing the same approval_date / brand / notes.
    """
    records: list[dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            page_text = page.extract_text() or ""
            month_header = _extract_month_header(page_text)
            for table in page.extract_tables() or []:
                if not table:
                    continue
                # Find header row index
                header_row_idx = None
                for i, row in enumerate(table[:3]):
                    if row and _is_header_row(row):
                        header_row_idx = i
                        break
                if header_row_idx is None:
                    continue
                header = [(_clean_cell(c)).lower() for c in table[header_row_idx]]
                # Map header text to column indices
                col_idx: dict[str, int] = {}
                for i, h in enumerate(header):
                    if "review" in h and "category" in h:
                        col_idx["category"] = i
                    elif "approval" in h and "date" in h:
                        col_idx["date"] = i
                    elif h == "no." or h == "no":
                        col_idx["no"] = i
                    elif "brand" in h:
                        col_idx["brand"] = i
                    elif "approval" in h or "change" in h:
                        col_idx.setdefault("approval_type", i)
                    elif "ingredient" in h:
                        col_idx["ingredient"] = i
                    elif "notes" in h:
                        col_idx["notes"] = i
                if "ingredient" not in col_idx or "notes" not in col_idx:
                    continue  # not a drug table

                def cell(name: str) -> str:
                    i = col_idx.get(name, -1)
                    if 0 <= i < len(row):
                        return _clean_cell(row[i])
                    return ""

                for row in table[header_row_idx + 1 :]:
                    if not row or all((c or "").strip() == "" for c in row):
                        continue
                    ingredient_cell = cell("ingredient")
                    if not ingredient_cell:
                        continue
                    notes_cell = cell("notes")
                    snippet, is_orphan = _strip_orphan_tag(notes_cell)
                    brand_cell = cell("brand")
                    brand, company = _split_brand_company(brand_cell)
                    approval_date = _parse_date(cell("date"))
                    category = cell("category")
                    approval_type = cell("approval_type")

                    ingredients = _split_ingredients(ingredient_cell)
                    if not ingredients:
                        continue

                    for ing in ingredients:
                        records.append({
                            "source": "PMDA",
                            "source_name": ing,
                            "source_ingredients": [ingredient_cell],
                            "approval_date": approval_date,
                            "indication": snippet,
                            "snippet": snippet,
                            "brand_name": brand,
                            "applicant_company": company,
                            "review_category": category,
                            "approval_type": approval_type,
                            "is_orphan": is_orphan,
                            "month_header": month_header,
                            "page_index": page_idx,
                        })
    logger.info("Parsed %d records from %s", len(records), pdf_path)
    return records


# Heading patterns for the Contraindications section.
# English: "Contraindications" — preferred because PMDA review reports are
# bilingual and the English heading is more reliable to anchor on.
# Japanese: "禁忌" (absolute) and "原則禁忌" (relative / "in principle").
_EN_CONTRA_HEADING_RE = re.compile(
    r"(?:^|\n)\s*(?:\d+(?:[.\)]\s*\d+)*\s*[.\)]?\s*)?"
    r"Contraindications?"  # singular or plural — both occur in PMDA reports
    r"(?:\s*\([^)\n]{0,120}\))?"
    r"\s*[:：]?\s*\n",
    re.MULTILINE,
)
_JA_CONTRA_HEADING_RE = re.compile(
    r"(?:^|\n)\s*(?:[\d０-９]+(?:[.\)]\s*[\d０-９]+)*\s*[.\)]?\s*)?"
    r"(?:原則禁忌|禁忌)"
    r"(?:\s*[（(][^)）\n]{0,120}[)）])?\s*[:：]?\s*\n",
    re.MULTILINE,
)

# Boundary patterns marking the end of the contraindications section. These
# include the next major bilingual section heading (English or Japanese) and
# generic numbered headings like "\n2. Foo".
_EN_BOUNDARY_HEADINGS = (
    "Indications",
    "Indication",
    "Dosage and Administration",
    "Precautions",
    "Warnings",
    "Adverse Reactions",
    "Pharmacokinetics",
    "Clinical Studies",
    "Description",
    "Composition",
    "Important Precautions",
)
_JA_BOUNDARY_HEADINGS = (
    "効能又は効果",
    "効能・効果",
    "用法及び用量",
    "用法・用量",
    "使用上の注意",
    "重要な基本的注意",
    "副作用",
    "薬物動態",
)


def _build_boundary_re() -> re.Pattern[str]:
    en = "|".join(re.escape(h) for h in _EN_BOUNDARY_HEADINGS)
    ja = "|".join(re.escape(h) for h in _JA_BOUNDARY_HEADINGS)
    # Match next major section heading at line-start. We deliberately do NOT
    # use a generic ``\d+\.`` numbered-list pattern as a boundary: PMDA contra
    # sections themselves are usually numbered lists, so that would clip the
    # section to zero length. Instead require either an explicit known heading
    # or a numbered heading + Capitalized title-case word that itself looks
    # like a section name (e.g. "2. Adverse Reactions").
    pattern = (
        r"(?:^|\n)\s*"
        r"(?:(?:\d+(?:[.\)]\s*\d+)*\s*[.\)]?\s*)?(?:" + en + r"|" + ja + r")"
        r")"
    )
    return re.compile(pattern, re.MULTILINE)


_BOUNDARY_RE = _build_boundary_re()


def _extract_pdf_text(pdf_path: Path) -> str:
    """Concatenate page text from a PDF using the existing pdfplumber dep."""
    parts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t:
                parts.append(t)
    return "\n".join(parts)


def _slice_section(text: str, heading_match: re.Match[str]) -> str:
    """Return text from end-of-heading until the next major heading or EOF."""
    start = heading_match.end()
    boundary = _BOUNDARY_RE.search(text, pos=start)
    end = boundary.start() if boundary else len(text)
    section = text[start:end].strip()
    # Cap to avoid runaway captures if no boundary fires.
    if len(section) > 8000:
        section = section[:8000]
    return section


def extract_contraindications_from_pdf(text) -> list[dict]:
    """Extract the Contraindications section from a PMDA review-report PDF.

    The argument is permissive for callers' convenience:

    - If ``text`` is a path-like value (``Path`` or a string ending in
      ``.pdf`` that points at an existing file), the PDF is opened and its
      full text extracted via ``pdfplumber``.
    - Otherwise ``text`` is treated as already-extracted PDF text.

    The English ``Contraindications`` heading is preferred because PMDA review
    reports are bilingual and the English section is more reliable. If the
    English heading is not found, we fall back to the Japanese ``禁忌`` /
    ``原則禁忌`` headings (PMDA Japanese sections are typically followed by a
    numbered list of contraindicated conditions).

    Returns:
        Either an empty list (no section found) or a single-element list of
        the form::

            [{
                "text": "<captured section text>",
                "language": "en" | "ja",
                "header": "<heading that matched>",
                "diseases": [],   # filled in by the grounding step downstream
            }]

        ``diseases`` is intentionally left empty — disease extraction and
        grounding happen in the caller.
    """
    # Accept Path-like input by extracting full text first.
    pdf_input: Path | None = None
    if isinstance(text, Path):
        pdf_input = text
    elif isinstance(text, str) and text.lower().endswith(".pdf") and Path(text).exists():
        pdf_input = Path(text)
    if pdf_input is not None:
        try:
            text = _extract_pdf_text(pdf_input)
        except Exception as e:
            logger.warning("Failed to extract text from %s: %s", pdf_input, e)
            return []

    if not text:
        return []

    # Prefer the English heading.
    m = _EN_CONTRA_HEADING_RE.search(text)
    if m:
        section = _slice_section(text, m)
        if section:
            return [{
                "text": section,
                "language": "en",
                "header": m.group(0).strip(),
                "diseases": [],
            }]

    # Fall back to Japanese.
    m = _JA_CONTRA_HEADING_RE.search(text)
    if m:
        section = _slice_section(text, m)
        if section:
            return [{
                "text": section,
                "language": "ja",
                "header": m.group(0).strip(),
                "diseases": [],
            }]

    return []


def _approval_document_id(record: dict, name: str) -> str:
    """A stable, distinct id for one approval row in the PMDA approvals PDF.

    The PDF is a single consolidated document, so there is no per-approval URL to key on.
    Page index plus approval date identifies a row well enough to keep two approvals for the
    same ingredient apart, and is stable across reruns because both come from the PDF itself.
    """
    page = record.get("page_index")
    date = (record.get("approval_date") or "").strip()
    parts = [p for p in (str(page) if page is not None else "", date) if p]
    return f"PMDA:{name}#{'-'.join(parts)}" if parts else f"PMDA:{name}"


def deduplicate_by_ingredient(records: list[dict]) -> list[dict]:
    """Collapse to one drug record per (uppercased) source_name, keeping the earliest approval.

    The **drug** axis wants one entity per active ingredient, so that collapse is right. The
    **indication** axis does not: each source row is a separate approval, with its own date and
    its own statement of what was approved. Pipe-joining those into one ``indication`` string
    made 1,078 of 1,976 PMDA drug records carry several distinct claims in a single blob, dated
    to the ingredient's first-ever approval and truncated at 500 characters downstream.

    So the rows are also kept individually on ``approvals``, one entry per source row, each with
    its own ``approval_date``, ``snippet``, ``brand_name`` and a distinct ``document_id``. The
    pipe-joined ``indication`` / ``brand_name`` fields are retained for the drug record, which
    genuinely does describe the ingredient as a whole.
    """
    by_name: dict[str, dict] = {}
    for r in records:
        key = (r.get("source_name", "") or "").strip().upper()
        if not key:
            continue
        existing = by_name.get(key)
        if existing is None:
            existing = {
                "source": "PMDA",
                "source_name": key,
                "approval_date": r.get("approval_date", ""),
                "indication": r.get("indication", ""),
                "snippets": [],
                "brand_names": [],
                "approvals": [],
                "is_orphan": r.get("is_orphan", False),
            }
            # Carry through yj_code if the upstream parser captured one.
            # (Today the approvals PDF does not contain YJ codes, so this is
            # always empty — but the slot is plumbed defensively.)
            yj = (r.get("yj_code", "") or "").strip()
            if yj:
                existing["yj_code"] = yj
            by_name[key] = existing
        else:
            # Preserve YJ code on subsequent rows if the first row was missing it.
            if not existing.get("yj_code"):
                yj = (r.get("yj_code", "") or "").strip()
                if yj:
                    existing["yj_code"] = yj

        # Earliest approval date wins
        new_date = r.get("approval_date", "")
        if new_date and (not existing.get("approval_date") or new_date < existing["approval_date"]):
            existing["approval_date"] = new_date
            existing["indication"] = r.get("indication", existing.get("indication", ""))

        snippet = (r.get("snippet") or "").strip()
        if snippet and snippet not in existing["snippets"]:
            existing["snippets"].append(snippet)
        # One entry per source row — the per-approval grain the indication side needs.
        if snippet:
            entry = {
                "document_id": _approval_document_id(r, key),
                "approval_date": (r.get("approval_date") or "").strip(),
                "snippet": snippet,
                "brand_name": (r.get("brand_name") or "").strip(),
            }
            if r.get("approval_type"):
                entry["approval_type"] = r["approval_type"]
            if not any(a["document_id"] == entry["document_id"]
                       and a["snippet"] == entry["snippet"]
                       for a in existing["approvals"]):
                existing["approvals"].append(entry)
        brand = (r.get("brand_name") or "").strip()
        if brand and brand not in existing["brand_names"]:
            existing["brand_names"].append(brand)

        existing["is_orphan"] = existing["is_orphan"] or r.get("is_orphan", False)

    # Flatten snippets/brand lists into pipe-joined strings, keep richest indication
    out = []
    for key, rec in by_name.items():
        rec["indication"] = " | ".join(rec["snippets"]) if rec["snippets"] else rec.get("indication", "")
        rec["brand_name"] = " | ".join(rec["brand_names"])
        # Keep the first non-empty snippet as the canonical "snippet" field
        rec["snippet"] = rec["snippets"][0] if rec["snippets"] else ""
        rec["approvals"].sort(key=lambda a: (a["approval_date"], a["document_id"]))
        out.append(rec)
    return out
