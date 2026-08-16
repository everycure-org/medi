"""Tests for the EveryCure coverage-gap report."""

from __future__ import annotations

import yaml

from medic.coverage import (
    approved_index_from_products,
    classify_everycure,
    find_gaps,
    load_everycure,
)


def _ec(name, curie, status, atc=""):
    return {"source": "EVERYCURE", "source_name": name, "normalized_id": curie,
            "approved_usa": status, "atc_main": atc}


def _pd(curie, label, *authorities):
    """A product Drug record in the v2.0 shape (mention + approvals)."""
    drug = {"identity": {"resolved_id": curie, "resolved_label": label}}
    if authorities:
        drug["approvals"] = [{"authority": a, "status": "APPROVED"} for a in authorities]
    return drug


def test_classify_covered_duplicate_gap():
    records = [
        _ec("Aspirin", "CHEBI:1", "APPROVED"),        # same id covered -> covered
        _ec("Adalimumab", "UNII:X", "APPROVED"),       # covered under CHEBI, name matches -> duplicate
        _ec("Noveldrug", "UNII:Z", "APPROVED"),        # nowhere -> gap
        _ec("Foo", "CHEBI:9", "NOT_APPROVED"),         # not approved -> ignored
    ]
    buckets = classify_everycure(
        records, approved_curies={"CHEBI:1"}, approved_names={"adalimumab"}
    )
    assert [r["source_name"] for r in buckets["covered"]] == ["Aspirin"]
    assert [r["source_name"] for r in buckets["duplicate"]] == ["Adalimumab"]
    assert [r["source_name"] for r in buckets["gap"]] == ["Noveldrug"]


def test_find_gaps_excludes_name_duplicates():
    records = [_ec("Adalimumab", "UNII:X", "APPROVED")]
    # Covered under a different id (name match) -> not a true gap.
    assert find_gaps(records, {"CHEBI:9"}, approved_names={"adalimumab"}) == []
    # No name match -> true gap.
    assert [r["source_name"] for r in find_gaps(records, set(), approved_names=set())] == [
        "Adalimumab"
    ]


def test_approved_index_reads_ids_and_names(tmp_path):
    p = tmp_path / "drug_list.yaml"
    p.write_text(yaml.safe_dump({"drugs": [
        _pd("CHEBI:1", "Aspirin", "FDA"),
        _pd("CHEBI:2", "Metformin", "PMDA"),
        _pd("CHEBI:3", "Foo"),  # no approvals -> not in index
    ]}))
    ids, names = approved_index_from_products(str(p))
    assert ids == {"CHEBI:1", "CHEBI:2"}
    assert names == {"aspirin", "metformin"}


def test_load_everycure_roundtrip(tmp_path):
    p = tmp_path / "everycure.yaml"
    p.write_text(yaml.safe_dump([_ec("Aspirin", "CHEBI:1", "APPROVED")]))
    assert load_everycure(str(p))[0]["source_name"] == "Aspirin"


def test_end_to_end_separates_duplicate_from_gap(tmp_path):
    ec = tmp_path / "everycure.yaml"
    ec.write_text(yaml.safe_dump([
        _ec("Aspirin", "CHEBI:1", "APPROVED"),         # covered
        _ec("Adalimumab", "UNII:X", "APPROVED"),        # duplicate (CHEBI:9 same name)
        _ec("Noveldrug", "UNII:Z", "APPROVED"),         # true gap
    ]))
    dl = tmp_path / "drug_list.yaml"
    dl.write_text(yaml.safe_dump({"drugs": [
        _pd("CHEBI:1", "Aspirin", "FDA"),
        _pd("CHEBI:9", "Adalimumab", "FDA"),
    ]}))
    ids, names = approved_index_from_products(str(dl))
    b = classify_everycure(load_everycure(str(ec)), ids, names)
    assert [r["source_name"] for r in b["gap"]] == ["Noveldrug"]
    assert [r["source_name"] for r in b["duplicate"]] == ["Adalimumab"]
