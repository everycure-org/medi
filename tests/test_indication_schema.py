"""The two-level association: one pair, N single-source assertions (design spec D1)."""

from linkml_runtime import SchemaView

SCHEMA = "src/medic/schema/indication.yaml"


def _sv():
    return SchemaView(SCHEMA)


def test_source_assertion_shape():
    slots = set(_sv().class_slots("SourceAssertion"))
    assert {"source", "jurisdiction", "document", "spans", "drug", "disease",
            "assertion", "evidence", "regulatory_status"} <= slots


def test_evidence_and_regulatory_status_are_singular_on_an_assertion():
    """One document attests one thing; the lists live only at pair level as derived views."""
    sv = _sv()
    assert sv.induced_slot("evidence", "SourceAssertion").multivalued is not True
    assert sv.induced_slot("regulatory_status", "SourceAssertion").multivalued is not True


def test_spans_are_multivalued_on_an_assertion():
    assert _sv().induced_slot("spans", "SourceAssertion").multivalued is True


def test_source_and_document_are_required():
    """The document is half the record key; an optional one would re-collapse assertions."""
    sv = _sv()
    assert sv.induced_slot("source", "SourceAssertion").required is True
    assert sv.induced_slot("document", "SourceAssertion").required is True


def test_the_association_holds_assertions_and_a_pair_confidence():
    sv = _sv()
    slots = set(sv.class_slots("IndicationAssociation"))
    assert {"drug_id", "disease_id", "relationship_type", "reliability",
            "confidence", "assertions"} <= slots
    assertions = sv.induced_slot("assertions", "IndicationAssociation")
    assert assertions.range == "SourceAssertion"
    assert assertions.multivalued is True
    assert sv.induced_slot("confidence", "IndicationAssociation").range == "PairConfidence"


def test_provenance_no_longer_hangs_off_the_association_itself():
    """drug/disease/evidence move down into assertions; leaving them here invites the old bug."""
    slots = set(_sv().class_slots("IndicationAssociation"))
    for gone in ("drug", "disease", "evidence"):
        assert gone not in slots, f"{gone} should live on SourceAssertion now"
