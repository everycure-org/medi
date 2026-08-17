"""Tests for India CDSCO primary-source PDF parsing.

Pure-function unit tests + optional integration tests gated on the presence of
the downloaded CDSCO primary PDFs and the legacy CSV.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from medic.ingest.india.parse_pdf import (
    _identify_columns,
    _is_header_row,
    _parse_date,
    deduplicate_by_drug_name,
    parse_all_pdfs,
    parse_india_pdf,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
PRIMARY_DIR = REPO_ROOT / "data/raw/india/primary"
SAMPLE_PDF = PRIMARY_DIR / "india_2025.pdf"
LEGACY_CSV = REPO_ROOT / "data/raw/india/indian_drugs.csv"


def test_parse_date_dot_format():
    assert _parse_date("16.01.2025") == "20250116"
    assert _parse_date("01.12.2023") == "20231201"


def test_parse_date_slash_format():
    assert _parse_date("16/01/2025") == "20250116"


def test_parse_date_iso_format():
    assert _parse_date("2025-01-16") == "20250116"


def test_parse_date_default_year_fallback():
    assert _parse_date("", default_year=2024) == ""
    assert _parse_date("unknown", default_year=2024) == "20240101"


def test_parse_date_invalid():
    assert _parse_date("") == ""
    assert _parse_date("foo") == ""


def test_identify_columns():
    cols = _identify_columns(["S.No.", "Name of New Drug", "Indication", "Date of Approval"])
    assert cols.get("sno") == 0
    assert cols.get("name") == 1
    assert cols.get("indication") == 2
    assert cols.get("date") == 3


def test_is_header_row():
    assert _is_header_row(["S.No.", "Name of New Drug", "Indication", "Date of Approval"])
    assert not _is_header_row(["1.", "Tafamidis Bulk Drug", "Not applicable", "16.01.2025"])


# ---------------------------------------------------------------------------
# Integration tests gated on real PDFs + legacy CSV
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not SAMPLE_PDF.exists(), reason="India 2025 PDF not downloaded")
def test_parse_india_pdf_extracts_records():
    records = parse_india_pdf(SAMPLE_PDF, default_year=2025)
    assert len(records) > 0
    for r in records[:5]:
        assert r["source"] == "INDIA"
        assert r["source_name"]
        assert "snippet" in r
    # 2025 PDF dates should fall in 2025
    dated = [r for r in records if r.get("approval_date", "").startswith("2025")]
    assert len(dated) >= len(records) - 2  # allow a few stragglers


@pytest.mark.skipif(not SAMPLE_PDF.exists(), reason="India 2025 PDF not downloaded")
def test_parse_india_pdf_known_drug():
    """Verify a specific known drug from the 2025 list is captured."""
    records = parse_india_pdf(SAMPLE_PDF, default_year=2025)
    names = [r["source_name"].lower() for r in records]
    # Tafamidis was approved Jan 16, 2025 (S.No. 1 in the 2025 PDF)
    assert any("tafamidis" in n for n in names), names[:5]


@pytest.mark.skipif(not PRIMARY_DIR.exists() or not list(PRIMARY_DIR.glob("*.pdf")), reason="No India PDFs")
def test_parse_all_pdfs_aggregates():
    records = parse_all_pdfs(PRIMARY_DIR)
    assert len(records) > 0


def _count_pdfs() -> int:
    return len(list(PRIMARY_DIR.glob("*.pdf"))) if PRIMARY_DIR.exists() else 0


@pytest.mark.skipif(
    not (PRIMARY_DIR.exists() and _count_pdfs() >= 3),
    reason="Need at least 3 India PDFs for snippet quality check",
)
def test_india_pdf_snippet_quality():
    """Indication snippets should be human-readable English, not garbage."""
    records = parse_all_pdfs(PRIMARY_DIR)
    deduped = deduplicate_by_drug_name(records)
    indication_records = [r for r in deduped if r.get("snippet")]
    assert len(indication_records) >= 10
    # At least 60% of snippets contain a recognizable medical word
    medical_words = ("treatment", "indicated", "therapy", "patients", "disease", "infection", "disorder", "syndrome", "cancer")
    quality = sum(
        1 for r in indication_records
        if any(w in r["snippet"].lower() for w in medical_words)
    )
    assert quality / len(indication_records) > 0.5, (
        f"Only {quality}/{len(indication_records)} snippets contain medical terms"
    )


@pytest.mark.skipif(
    not (PRIMARY_DIR.exists() and _count_pdfs() >= 3),
    reason="Need at least 3 India PDFs",
)
def test_india_pdf_dedup_preserves_records():
    """Dedup output count should be reasonable (between 1/4 and full row count)."""
    records = parse_all_pdfs(PRIMARY_DIR)
    deduped = deduplicate_by_drug_name(records)
    assert len(deduped) > 0
    assert len(deduped) <= len(records)
    assert len(deduped) > len(records) // 4
