"""Tests for PMDA primary-source PDF parsing.

Two layers of testing:

1. Pure-function unit tests for date parsing, brand/company splitting, and
   ingredient splitting (no I/O).
2. Optional integration tests that compare the PDF parser output to the legacy
   `pmda_approvals.csv` golden data. These are skipped if the PDF or golden CSV
   are not present.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from medic.ingest.pmda.parse_pdf import (
    _is_header_row,
    _parse_date,
    _split_brand_company,
    _split_ingredients,
    _strip_orphan_tag,
    deduplicate_by_ingredient,
    parse_pmda_pdf,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
PRIMARY_PDF = REPO_ROOT / "data/raw/pmda/primary/pmda_approvals.pdf"


def test_parse_date_short_month():
    assert _parse_date("Jun. 26, 2023") == "20230626"
    assert _parse_date("May 19, 2025") == "20250519"
    assert _parse_date("Jan. 18, 2024") == "20240118"


def test_parse_date_long_month():
    assert _parse_date("January 18, 2024") == "20240118"
    assert _parse_date("September 5, 2024") == "20240905"


def test_parse_date_invalid_returns_empty():
    assert _parse_date("") == ""
    assert _parse_date("foo bar") == ""
    assert _parse_date("invalid date") == ""


def test_strip_orphan_tag():
    text, is_orphan = _strip_orphan_tag(
        "A drug with a new indication for X. [Orphan drug]"
    )
    assert is_orphan is True
    assert "Orphan" not in text


def test_strip_orphan_tag_no_marker():
    text, is_orphan = _strip_orphan_tag("Plain indication.")
    assert is_orphan is False
    assert text == "Plain indication."


def test_split_brand_company():
    brand, company = _split_brand_company(
        "Slinda 28 Tablets (ASKA Pharmaceutical Co., Ltd.)"
    )
    assert brand == "Slinda 28 Tablets"
    assert company == "ASKA Pharmaceutical Co., Ltd."


def test_split_brand_company_no_paren():
    brand, company = _split_brand_company("Some Brand")
    assert brand == "Some Brand"
    assert company == ""


def test_split_ingredients_simple():
    assert _split_ingredients("Aspirin") == ["ASPIRIN"]


def test_split_ingredients_multi():
    parts = _split_ingredients("Aspirin; Ibuprofen / Naproxen and Caffeine")
    assert "ASPIRIN" in parts
    assert "IBUPROFEN" in parts
    assert "NAPROXEN" in parts
    assert "CAFFEINE" in parts


def test_split_ingredients_strips_footnotes():
    parts = _split_ingredients("Drug A (1) ; Drug B (2)")
    assert any("DRUG A" in p for p in parts)
    assert any("DRUG B" in p for p in parts)
    # footnote markers stripped
    assert not any("(1)" in p or "(2)" in p for p in parts)


def test_is_header_row_positive():
    assert _is_header_row(
        ["Review Category", "Approval Date", "No.", "Brand Name",
         "New Approval", "Active Ingredient", "Notes"]
    )


def test_is_header_row_negative():
    assert not _is_header_row(["1", "May 19, 2025", "1", "Brand X", "Approval", "Drug X", "Description"])


# ---------------------------------------------------------------------------
# Integration tests against actual PMDA PDF + legacy CSV golden data
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not PRIMARY_PDF.exists(), reason="PMDA primary PDF not downloaded")
def test_parse_pmda_pdf_extracts_known_fields():
    """The PDF parser should extract well-known recent approvals with snippets."""
    records = parse_pmda_pdf(PRIMARY_PDF)
    assert len(records) > 100, f"Expected >100 raw records, got {len(records)}"
    # Every record has the required shape
    for r in records[:50]:
        assert r["source"] == "PMDA"
        assert r["source_name"]
        assert "snippet" in r
    # At least 80% of records have a non-empty snippet
    with_snippet = sum(1 for r in records if r.get("snippet"))
    assert with_snippet / len(records) > 0.8


@pytest.mark.skipif(not PRIMARY_PDF.exists(), reason="PMDA primary PDF not downloaded")
def test_parse_pmda_pdf_dedup_yields_reasonable_count():
    records = parse_pmda_pdf(PRIMARY_PDF)
    deduped = deduplicate_by_ingredient(records)
    # Legacy norm has ~1196 unique ingredients; PDF should be similar order
    assert 800 < len(deduped) < 3000


# ---------------------------------------------------------------------------
# Per-approval grain: one indication statement per PDF row, not one blob per drug
# ---------------------------------------------------------------------------


def _rows():
    """Three PDF rows for one ingredient: three separate approvals, three dates."""
    return [
        {"source": "PMDA", "source_name": "PEMBROLIZUMAB", "approval_date": "20160928",
         "snippet": "A drug with a new indication for malignant pleural mesothelioma.",
         "brand_name": "KEYTRUDA", "page_index": 3},
        {"source": "PMDA", "source_name": "PEMBROLIZUMAB", "approval_date": "20211125",
         "snippet": "A drug with a new indication for unresectable biliary tract cancer.",
         "brand_name": "KEYTRUDA", "page_index": 9},
        {"source": "PMDA", "source_name": "pembrolizumab", "approval_date": "20230324",
         "snippet": "A drug with a new indication for adjuvant treatment of renal cell carcinoma.",
         "brand_name": "KEYTRUDA", "page_index": 14},
    ]


def test_dedup_still_yields_one_drug_record_per_ingredient():
    """The drug axis is unchanged: one entity per active ingredient."""
    out = deduplicate_by_ingredient(_rows())
    assert len(out) == 1
    assert out[0]["source_name"] == "PEMBROLIZUMAB"
    assert out[0]["approval_date"] == "20160928"      # earliest, as before


def test_dedup_retains_each_approval_separately():
    """The indication axis needs the rows back: one statement per approval, own date."""
    rec = deduplicate_by_ingredient(_rows())[0]
    approvals = rec["approvals"]
    assert len(approvals) == 3
    assert [a["approval_date"] for a in approvals] == ["20160928", "20211125", "20230324"]
    for a in approvals:
        assert " | " not in a["snippet"], "an approval statement must not be a concatenation"
        assert a["document_id"].startswith("PMDA:")


def test_each_approval_carries_its_own_date_not_the_earliest():
    """The collapse used to date every statement to the drug's first-ever approval."""
    rec = deduplicate_by_ingredient(_rows())[0]
    biliary = next(a for a in rec["approvals"] if "biliary" in a["snippet"])
    assert biliary["approval_date"] == "20211125"
    assert biliary["approval_date"] != rec["approval_date"]


def test_approval_document_ids_are_distinct_and_stable():
    rec = deduplicate_by_ingredient(_rows())[0]
    ids = [a["document_id"] for a in rec["approvals"]]
    assert len(set(ids)) == 3
    assert ids == [a["document_id"] for a in deduplicate_by_ingredient(_rows())[0]["approvals"]]


def test_identical_rows_still_collapse():
    rows = _rows()[:1] * 2
    rec = deduplicate_by_ingredient(rows)[0]
    assert len(rec["approvals"]) == 1


@pytest.mark.skipif(not PRIMARY_PDF.exists(), reason="PMDA primary PDF not downloaded")
def test_no_real_approval_statement_is_a_concatenation():
    """The live defect: 1,078 of 1,976 drug records held pipe-joined statements."""
    for rec in deduplicate_by_ingredient(parse_pmda_pdf(PRIMARY_PDF)):
        for a in rec.get("approvals", []):
            assert " | " not in a["snippet"], rec["source_name"]
