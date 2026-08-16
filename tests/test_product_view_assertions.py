"""product_view is the single read-side accessor; consumers must not learn the new shape."""

from medic import product_view as pv

PAIR = {
    "drug_id": "CHEBI:135272", "drug_label": "etifoxine",
    "disease_id": "MONDO:0011918", "disease_label": "anxiety",
    "relationship_type": "INDICATION",
    "assertions": [
        {"source": "CDSCO", "document": "CDSCO:2024",
         "evidence": {"jurisdiction": "INDIA", "approval_status": "APPROVED"},
         "regulatory_status": {"authority": "CDSCO", "status": "APPROVED"}},
        {"source": "GRLS", "document": "GRLS:x",
         "evidence": {"jurisdiction": "RUSSIA", "approval_status": "APPROVED"},
         "regulatory_status": {"authority": "MOH_RUSSIA", "status": "APPROVED"}},
    ],
}


def test_pair_level_accessors_read_the_top_level():
    assert pv.assoc_drug_id(PAIR) == "CHEBI:135272"
    assert pv.assoc_drug_label(PAIR) == "etifoxine"
    assert pv.assoc_disease_id(PAIR) == "MONDO:0011918"
    assert pv.assoc_disease_label(PAIR) == "anxiety"


def test_evidence_is_flattened_in_assertion_order():
    assert [e["jurisdiction"] for e in pv.assoc_evidence(PAIR)] == ["INDIA", "RUSSIA"]


def test_authorities_come_from_every_assertion():
    assert pv.assoc_authorities(PAIR) == {"CDSCO", "MOH_RUSSIA"}


def test_jurisdictions_come_from_every_assertion():
    assert pv.assoc_jurisdictions(PAIR) == {"INDIA", "RUSSIA"}


def test_a_partially_populated_record_returns_empties_rather_than_raising():
    assert pv.assoc_evidence({}) == []
    assert pv.assoc_authorities({}) == set()
    assert pv.assoc_jurisdictions({}) == set()
    assert pv.assoc_assertions({}) == []
    assert pv.assoc_drug_id({"assertions": []}) == ""


def test_a_malformed_assertion_entry_is_skipped_not_crashed_on():
    bad = {"assertions": [None, "nonsense", {"source": "X", "evidence": {"jurisdiction": "USA"}}]}
    assert pv.assoc_jurisdictions(bad) == {"USA"}
