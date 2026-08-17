"""Tests for common ingest utilities."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from medic.ingest.common import (
    standardize_columns,
    reformat_date,
    should_skip_expensive_calls,
    deduplicate_with_join,
    load_source_urls,
    write_grounding_report,
)


def test_should_skip_true():
    with patch.dict(os.environ, {"MEDIC_SKIP_EXPENSIVE_CALLS": "1"}):
        assert should_skip_expensive_calls() is True


def test_should_skip_false():
    with patch.dict(os.environ, {}, clear=True):
        assert should_skip_expensive_calls() is False


def test_standardize_columns():
    df = pd.DataFrame({"Ingredient": ["aspirin"], "Approval_Date": ["2020"]})
    mapping = {"Ingredient": "source_name", "Approval_Date": "approval_date"}
    result = standardize_columns(df, mapping)
    assert "source_name" in result.columns
    assert "approval_date" in result.columns


def test_reformat_date_us():
    assert reformat_date("March 14, 2025") == "20250314"


def test_reformat_date_eu():
    assert reformat_date("14/03/2025") == "20250314"


def test_reformat_date_iso():
    assert reformat_date("2025-03-14") == "20250314"


def test_reformat_date_already_formatted():
    assert reformat_date("20250314") == "20250314"


def test_reformat_date_empty():
    assert reformat_date("") == ""
    assert reformat_date(None) == ""


def test_reformat_date_prior_to():
    assert reformat_date("Approved Prior to Jan 1, 1982") == "19820101"


def test_deduplicate_with_join():
    df = pd.DataFrame({
        "name": ["aspirin", "aspirin", "ibuprofen"],
        "source": ["ob", "pb", "ob"],
        "date": ["2020", "2021", "2019"],
    })
    result = deduplicate_with_join(df, key_cols=["name"])
    assert len(result) == 2


def test_load_source_urls():
    urls = load_source_urls()
    assert isinstance(urls, dict)
    # Should have at least orangebook if conf/source_urls.yaml exists
    if urls:
        assert "orangebook" in urls


def test_write_grounding_report():
    with tempfile.TemporaryDirectory() as tmpdir:
        report = {"total_drugs": 100, "auto_accepted": 90}
        path = write_grounding_report(report, Path(tmpdir), "test")
        assert path.exists()
