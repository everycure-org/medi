"""The QC guard for approval-date provenance, on the shape that actually shipped.

`check_approval_date_authority` exists because neither I-1 gate can see this class of defect:
both compare a record's source against its jurisdiction, and an FDA row carrying a Russian
registration date is a well-formed FDA row to both of them. The guard is the only thing standing
between that and a release, so it needs its own test — including the false-positive case that
made the first version of it useless.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "build_qc", Path(__file__).resolve().parents[1] / "scripts" / "build_qc.py")
build_qc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build_qc)


def _products(status: dict, approvals: list[dict]) -> dict:
    """One drug and one indication pair carrying a single regulatory status row."""
    return {
        "drug_list": [{
            "identity": {"resolved_id": "CHEBI:10033", "resolved_label": "warfarin"},
            "approvals": approvals,
        }],
        "indication_list": [{
            "drug_id": "CHEBI:10033",
            "disease_id": "HP:0100601",
            "assertions": [{"source": "DAILYMED", "regulatory_status": [status]}],
        }],
    }


def test_a_date_another_authority_issued_is_a_violation():
    """The shipped bug: warfarin's Russian registration date on its FDA row."""
    result = build_qc.check_approval_date_authority(_products(
        {"authority": "FDA", "source": "DAILYMED", "approval_date": "20061229"},
        [{"authority": "MOH_RUSSIA", "approval_date": "20061229"}],
    ))
    assert result.status == "FAIL"
    assert result.detail["violations"] == 1
    assert "MOH_RUSSIA" in result.detail["examples"][0]


def test_a_date_the_naming_authority_issued_is_fine():
    result = build_qc.check_approval_date_authority(_products(
        {"authority": "FDA", "source": "DAILYMED", "approval_date": "20020524"},
        [{"authority": "FDA", "approval_date": "20020524"},
         {"authority": "EMA", "approval_date": "20020319"}],
    ))
    assert result.status == "PASS"


def test_a_date_no_authority_has_on_record_is_fine():
    """The false positive that made the first version of this check unusable.

    An indication document carries its own approval date, and `drug_list` records only the
    earliest per authority from the marketing registries — so a date it has never seen is the
    normal case, not a smear. Flagging it fired on 1,450 legitimate PMDA and EMA rows.
    """
    result = build_qc.check_approval_date_authority(_products(
        {"authority": "PMDA", "source": "PMDA", "approval_date": "20150824"},
        [{"authority": "PMDA", "approval_date": "20140926"}],
    ))
    assert result.status == "PASS"


@pytest.mark.parametrize("status", ["FDA", {"authority": "FDA"}, {"approval_date": "20061229"}])
def test_rows_with_nothing_to_check_are_skipped(status):
    """Legacy shorthand and half-filled rows must not crash the check."""
    result = build_qc.check_approval_date_authority(_products(
        status, [{"authority": "MOH_RUSSIA", "approval_date": "20061229"}]))
    assert result.status == "PASS"
