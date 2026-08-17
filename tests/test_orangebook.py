"""Tests for Orange Book ingest parsing (no network calls)."""

import tempfile
from pathlib import Path

import pandas as pd

from medic.ingest.orangebook.__main__ import parse_orangebook, get_marketing_status


def test_parse_orangebook_basic():
    """Test parsing of Orange Book products.txt format."""
    data = pd.DataFrame({
        "Ingredient": ["ASPIRIN", "IBUPROFEN"],
        "Approval_Date": ["Jan 01, 1990", "Feb 15, 2000"],
        "Type": ["RX", "OTC"],
        "DF;Route": ["TABLET;ORAL", "CAPSULE;ORAL"],
        "Applicant": ["BAYER", "ADVIL"],
        "ApplNo": ["NDA000123", "NDA000456"],
    })
    with tempfile.NamedTemporaryFile(suffix=".txt", mode="w", delete=False) as f:
        data.to_csv(f.name, sep="\t", index=False)
        records = parse_orangebook(Path(f.name))

    assert len(records) == 2
    assert records[0]["source"] == "ORANGEBOOK"
    assert records[0]["source_name"] in ("ASPIRIN", "IBUPROFEN")
    assert all(r["approval_date"] for r in records)


def test_marketing_status_most_permissive():
    assert get_marketing_status(["RX", "OTC"]) == "OTC"
    assert get_marketing_status(["DISCONTINUED", "RX"]) == "RX"
    assert get_marketing_status(["DISCONTINUED"]) == "DISCN"
    assert get_marketing_status([]) == "NONE"


def test_parse_orangebook_deduplicates_by_ingredient():
    """Same ingredient with different formulations should be grouped."""
    data = pd.DataFrame({
        "Ingredient": ["ASPIRIN", "ASPIRIN"],
        "Approval_Date": ["Jan 01, 1990", "Mar 15, 1985"],
        "Type": ["RX", "OTC"],
        "DF;Route": ["TABLET;ORAL", "CAPSULE;ORAL"],
        "Applicant": ["BAYER", "BAYER"],
        "ApplNo": ["NDA000123", "NDA000456"],
    })
    with tempfile.NamedTemporaryFile(suffix=".txt", mode="w", delete=False) as f:
        data.to_csv(f.name, sep="\t", index=False)
        records = parse_orangebook(Path(f.name))

    assert len(records) == 1
    assert records[0]["source_name"] == "ASPIRIN"
    # Earliest approval date should be used
    assert records[0]["approval_date"] == "19850315"
    # Most permissive status: OTC > RX
    assert records[0]["marketing_status_usa"] == "OTC"


def test_parse_orangebook_handles_prior_to_date():
    """Test 'Approved Prior to Jan 1, 1982' dates."""
    data = pd.DataFrame({
        "Ingredient": ["OLDRUG"],
        "Approval_Date": ["Approved Prior to Jan 1, 1982"],
        "Type": ["RX"],
        "DF;Route": ["TABLET;ORAL"],
        "Applicant": ["ACME"],
        "ApplNo": ["NDA000999"],
    })
    with tempfile.NamedTemporaryFile(suffix=".txt", mode="w", delete=False) as f:
        data.to_csv(f.name, sep="\t", index=False)
        records = parse_orangebook(Path(f.name))

    assert len(records) == 1
    assert records[0]["approval_date"] == "19820101"
