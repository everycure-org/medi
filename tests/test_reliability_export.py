"""Tests for the reliability-annotated statement export."""

from __future__ import annotations

from medic.reliability import ReliabilityTier, StatementType
from medic.reliability_export import COLUMNS, flatten


def test_flatten_approval_row():
    rec = {"identity": {"resolved_id": "CHEBI:1", "resolved_label": "Aspirin"},
           "approvals": [{"authority": "FDA", "status": "APPROVED"},
                         {"authority": "PMDA", "status": "APPROVED"}],
           "evidence": [{"approval_status": "APPROVED", "reference": "ob"}]}
    row = flatten(rec, StatementType.DRUG_APPROVAL, ReliabilityTier.HIGH)
    assert set(row) == set(COLUMNS)
    assert row["statement_type"] == "DRUG_APPROVAL"
    assert row["reliability"] == "HIGH"
    assert row["is_reliable"] is True
    assert row["drug_id"] == "CHEBI:1"
    assert row["jurisdictions"] == "japan,usa"       # sorted (FDA->usa, PMDA->japan)
    assert row["disease_id"] == ""


def test_flatten_indication_row():
    # v3.0 pair shape: identity at the top, provenance and evidence on the assertions.
    rec = {"relationship_type": "INDICATION",
           "drug_id": "CHEBI:6437", "drug_label": "levetiracetam",
           "disease_id": "MONDO:5027", "disease_label": "epilepsy",
           "assertions": [{"source": "EMA", "document": "EMA:keppra",
                           "evidence": {"jurisdiction": "EU", "reference": "epar"}}]}
    row = flatten(rec, StatementType.INDICATION, ReliabilityTier.MEDIUM)
    assert row["drug_id"] == "CHEBI:6437"
    assert row["disease_id"] == "MONDO:5027"
    assert row["relationship"] == "INDICATION"
    assert row["jurisdictions"] == "EU"              # falls back to evidence jurisdiction
    assert row["is_reliable"] is True                # core + MEDIUM


def test_low_and_noncore_are_not_reliable():
    rec = {"identity": {"resolved_id": "CHEBI:1"},
           "approvals": [{"authority": "FDA", "status": "APPROVED"}], "evidence": [{}]}
    assert flatten(rec, StatementType.DRUG_APPROVAL, ReliabilityTier.LOW)["is_reliable"] is False
    research = {"drug_id": "CHEBI:1", "disease_id": "MONDO:1", "evidence": [{}]}
    assert flatten(research, StatementType.RESEARCH_ASSOCIATION,
                   ReliabilityTier.HIGH)["is_reliable"] is False  # non-core excluded
