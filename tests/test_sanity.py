"""Tests for source sanity: row-count floors + version/fingerprint manifest."""

from __future__ import annotations

import json

import pytest

from medic.ingest.sanity import (
    ROW_FLOORS,
    SourceSanityError,
    check_row_floor,
    record_source,
    source_fingerprint,
)


# ---------------------------------------------------------------------------
# check_row_floor
# ---------------------------------------------------------------------------
def test_passes_at_or_above_floor():
    assert check_row_floor("china", ROW_FLOORS["china"]) == ROW_FLOORS["china"]
    assert check_row_floor("china", ROW_FLOORS["china"] + 500) == ROW_FLOORS["china"] + 500


def test_raises_below_floor():
    with pytest.raises(SourceSanityError, match="truncated"):
        check_row_floor("china", 10)


def test_non_strict_warns_but_returns():
    assert check_row_floor("china", 10, strict=False) == 10  # no raise


def test_limited_run_skips_floor():
    assert check_row_floor("china", 3, limited=True) == 3


def test_env_bypass_skips_floor(monkeypatch):
    monkeypatch.setenv("MEDIC_SKIP_ROW_FLOORS", "1")
    assert check_row_floor("china", 3) == 3


def test_unknown_source_has_no_floor():
    assert check_row_floor("mystery", 1) == 1  # nothing to assert


def test_explicit_floor_override():
    with pytest.raises(SourceSanityError):
        check_row_floor("mystery", 5, floor=100)


# ---------------------------------------------------------------------------
# source_fingerprint
# ---------------------------------------------------------------------------
def test_fingerprint_is_deterministic(tmp_path):
    a = tmp_path / "a.csv"
    a.write_text("drug,date\nx,2020\n")
    fp1 = source_fingerprint(str(a))
    fp2 = source_fingerprint(str(a))
    assert fp1["sha256"] == fp2["sha256"]
    assert set(fp1) == {"file", "bytes", "sha256", "modified"}
    assert fp1["file"] == "a.csv"


def test_fingerprint_changes_with_content(tmp_path):
    a = tmp_path / "a.csv"
    a.write_text("one")
    sha_a = source_fingerprint(str(a))["sha256"]
    a.write_text("two")
    assert source_fingerprint(str(a))["sha256"] != sha_a


# ---------------------------------------------------------------------------
# record_source (manifest)
# ---------------------------------------------------------------------------
def test_record_source_writes_and_updates_manifest(tmp_path):
    src = tmp_path / "cder.csv"
    src.write_text("drug_name,approval_date\n甲,2020/1/1\n")
    manifest = str(tmp_path / "source_manifest.json")

    entry = record_source("china", str(src), 1521, manifest_path=manifest)
    assert entry["row_count"] == 1521

    data = json.loads(open(manifest).read())
    assert data["china"]["row_count"] == 1521
    assert data["china"]["file"] == "cder.csv"

    # A second source is added without clobbering the first.
    record_source("russia", str(src), 5885, manifest_path=manifest)
    data = json.loads(open(manifest).read())
    assert set(data) == {"china", "russia"}
    assert data["china"]["row_count"] == 1521
