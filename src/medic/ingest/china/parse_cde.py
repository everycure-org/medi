"""Parse the manually-provided CDE scrape into grounded-ready drug records.

The scrape (``background/cder_drugs_final_all.csv``) has exactly two columns:

======  ======================================================================
Column  Meaning
======  ======================================================================
drug_name       Chinese drug product name, with a formulation suffix
                (e.g. ``来那度胺胶囊`` = lenalidomide capsule).
approval_date   Approval date, predominantly ``YYYY/M/D``, with a small tail of
                other shapes (``YYYYMMDD``, ``YYYY-MM-DD``, ``YYYY年M月D日`` …).
======  ======================================================================

There is **no indication column**, so China contributes a **drug list only** —
no indications or contraindications (same as Russia).

The record's ``source_name`` holds the **verbatim Chinese name**; the shared
translation stage (``medic.translation.translate_records``, DeepL via babelon)
then translates it to English and overwrites ``source_name`` with the English
value before grounding. The verbatim Chinese name is also preserved on the record
as ``original_name_zh`` (invariant I-7 — faithful source string).
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

from medic.mention import assign_mention

logger = logging.getLogger(__name__)

# Chinese digits are not expected in the numeric portions of these dates, so a
# purely-ASCII digit extraction is sufficient after we replace the year/month/day
# ideographs with separators.
_CJK_DATE_SEP = str.maketrans({"年": "-", "月": "-", "日": ""})


def normalize_china_date(value: str) -> str:
    """Normalize a CDE approval date to ``YYYYMMDD``; empty string if unparseable.

    Handles the shapes actually seen in the scrape:
      ``2019/10/21`` / ``2019/9/10`` (year-first slash — the dominant form),
      ``20080618`` (already compact), ``2021-01-07`` (dash, possibly with a
      stray leading apostrophe ``'`` / ``‘``), ``2018.07.06`` (dot),
      ``2020年8月5日`` (CJK), and ``2002-10-.24`` (stray punctuation).
    """
    if not value:
        return ""
    s = str(value).strip()
    # Strip stray leading quote characters seen in the scrape.
    s = s.lstrip("'‘’\"").strip()
    if not s:
        return ""

    # Already compact YYYYMMDD.
    if re.fullmatch(r"\d{8}", s):
        return s

    # Normalize CJK date ideographs to separators, then unify all separators.
    s = s.translate(_CJK_DATE_SEP)
    # Split on any run of non-digit characters (handles /, -, ., and the
    # ``2002-10-.24`` stray-dot case).
    parts = [p for p in re.split(r"\D+", s) if p]
    if len(parts) != 3:
        logger.warning("Could not parse China date: %r", value)
        return ""
    year, month, day = parts
    if len(year) != 4 or not (1 <= len(month) <= 2) or not (1 <= len(day) <= 2):
        logger.warning("Could not parse China date: %r", value)
        return ""
    try:
        y, m, d = int(year), int(month), int(day)
    except ValueError:
        logger.warning("Could not parse China date: %r", value)
        return ""
    if not (1 <= m <= 12 and 1 <= d <= 31):
        logger.warning("Implausible China date: %r", value)
        return ""
    return f"{y:04d}{m:02d}{d:02d}"


def parse_cde_csv(
    csv_path: Path,
    limit: int | None = None,
) -> list[dict]:
    """Parse the CDE scrape into de-duplicated drug records (pre-translation).

    Deduplicates by the Chinese ``drug_name`` (keeping the earliest approval
    date) and returns records shaped for the translation + grounding stages. The
    Chinese name is left in ``source_name``; the caller runs
    ``medic.translation.translate_records(records, "zh")`` to translate it to
    English before grounding.

    Args:
        csv_path: Path to ``cder_drugs_final_all.csv``.
        limit: If set, only the first ``limit`` **unique** drug names are kept
            (validation aid — keeps translation volume small).

    Returns:
        List of dicts with ``source: "CHINA"``, ``source_name`` (verbatim
        Chinese, translated in place by the translation stage), ``original_name_zh``
        (verbatim Chinese, kept), and ``approval_date`` (``YYYYMMDD``).
    """
    # Aggregate by Chinese name, keeping the earliest approval date.
    aggregated: dict[str, str] = {}
    total_rows = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "drug_name" not in reader.fieldnames:
            raise ValueError(
                f"CDE scrape {csv_path} is missing the required 'drug_name' column "
                f"(found columns: {reader.fieldnames})."
            )
        for row in reader:
            name = (row.get("drug_name") or "").strip()
            if not name:
                continue
            total_rows += 1
            date = normalize_china_date(row.get("approval_date") or "")
            existing = aggregated.get(name)
            if existing is None:
                aggregated[name] = date
            elif date and (not existing or date < existing):
                aggregated[name] = date

    names = list(aggregated.keys())
    if limit is not None:
        names = names[:limit]

    logger.info(
        "Parsed %d CDE rows -> %d unique Chinese names%s",
        total_rows,
        len(aggregated),
        f" (limited to {len(names)})" if limit is not None else "",
    )

    records = []
    for zh_name in names:
        record = {
            "source": "CHINA",
            # ``source_name`` holds the verbatim Chinese; the translation stage
            # overwrites it with English. The Chinese is also kept as provenance.
            "source_name": zh_name,
            "original_name_zh": zh_name,
            "approval_date": aggregated[zh_name],
        }
        # Mint the MEDICNE id from the verbatim Chinese literal at extraction (I-9),
        # before translation overwrites ``source_name``.
        assign_mention(record, "drugs", literal=zh_name)
        records.append(record)
    return records
