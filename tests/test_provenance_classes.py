"""Classes and slots added for source-scoped association provenance (design spec §5).

Additive only. The flat Assertion confidence slots (subject_confidence / object_confidence /
relationship_confidence) are removed in Plan 3, when the merge is rewritten to populate the
nested ConfidenceBreakdown instead; removing them here would break the working merge.
"""

from linkml_runtime import SchemaView

PROVENANCE_SCHEMA = "src/medic/schema/provenance.yaml"


def _sv():
    return SchemaView(PROVENANCE_SCHEMA)


def _slots(sv, cls):
    return set(sv.class_slots(cls))


def test_text_span_role_enum_values():
    vals = set(_sv().get_enum("TextSpanRoleEnum").permissible_values)
    assert vals == {
        "SECTION_HEADER", "SECTION_TEXT", "SUBSECTION_HEADER", "SUBSECTION_TEXT",
        "LIMITATION_STATEMENT", "TABLE_HEADER", "TABLE_CELL", "LIST_ITEM",
        "DOCUMENT_TITLE", "STRUCTURED_FIELD", "FULL_DOCUMENT", "UNKNOWN",
    }


def test_text_span_carries_a_required_role_and_a_document():
    sv = _sv()
    assert {"role", "document"} <= _slots(sv, "TextSpan")
    role = sv.induced_slot("role", "TextSpan")
    assert role.required is True
    assert role.range == "TextSpanRoleEnum"


def test_every_step_requires_confidence_and_basis():
    sv = _sv()
    assert {"confidence", "confidence_basis"} <= _slots(sv, "TransformationStep")
    basis = sv.induced_slot("confidence_basis", "TransformationStep")
    assert basis.range == "ConfidenceBasis"
    assert basis.required is True
    assert sv.induced_slot("confidence", "TransformationStep").required is True


def test_extraction_step_carries_span_anchoring_and_co_mentions():
    sv = _sv()
    slots = _slots(sv, "ExtractionStep")
    assert {"span_role", "span_index", "char_start", "char_end",
            "mention_index", "mention_total", "co_mentions"} <= slots
    co = sv.induced_slot("co_mentions", "ExtractionStep")
    assert co.range == "CoMention"
    assert co.multivalued is True


def test_co_mention_shape():
    sv = _sv()
    slots = _slots(sv, "CoMention")
    assert {"value", "entity_type", "mention_id", "char_start", "char_end"} <= slots
    assert sv.induced_slot("value", "CoMention").required is True


def test_confidence_breakdown_requires_all_four_components_and_a_basis():
    sv = _sv()
    slots = _slots(sv, "ConfidenceBreakdown")
    assert {"subject", "object", "relationship", "overall", "basis"} == slots
    for name in ("subject", "object", "relationship", "overall", "basis"):
        assert sv.induced_slot(name, "ConfidenceBreakdown").required is True, name


def test_pair_confidence_shape():
    assert {"overall", "method", "n_assertions"} <= _slots(_sv(), "PairConfidence")


def test_assertion_gains_span_index_and_negation_scope():
    sv = _sv()
    slots = _slots(sv, "Assertion")
    assert {"span_index", "negation_scope"} <= slots
    assert sv.induced_slot("negation_scope", "Assertion").multivalued is True


def test_the_flat_confidence_slots_are_gone():
    """Superseded by ConfidenceBreakdown; kept through Plans 1-2 to avoid a broken tree."""
    sv = _sv()
    slots = set(sv.class_slots("Assertion"))
    for gone in ("subject_confidence", "object_confidence", "relationship_confidence"):
        assert gone not in slots, gone
    assert sv.induced_slot("confidence", "Assertion").range == "ConfidenceBreakdown"


def test_source_reference_is_gone_from_text_span():
    assert "source_reference" not in set(_sv().class_slots("TextSpan"))
