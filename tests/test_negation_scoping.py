"""Negation is scoped to the span the extraction read (design spec §4.3).

The live bug this replaces: `check_text = " ".join([snippet, section])` put a
"Limitations of Use" sentence in scope for the *positive* claim's negation check.
"""

from medic.merge.on_label_merge import _build_disease_provenance

UBRELVY = (
    "UBRELVY is indicated for the acute treatment of migraine with or without aura in "
    "adults. Limitations of Use UBRELVY is not indicated for the preventive treatment of "
    "migraine."
)


def _assoc(text, disease="migraine with aura"):
    record = {"source": "DAILYMED", "indications_text": text, "set_id": "fd9f9458"}
    assoc = {"relationship_type": "INDICATION",
             "evidence": [{"original_disease_label": disease, "snippet": text,
                           "setid": "fd9f9458"}]}
    return _build_disease_provenance(record, assoc, "MONDO:0005475", disease)


def test_a_limitations_clause_does_not_negate_the_positive_claim():
    _mention, assertion = _assoc(UBRELVY)
    assert "negated_inversion" not in (assertion.get("flags") or [])


def test_the_limitation_span_is_excluded_from_negation_scope():
    mention, assertion = _assoc(UBRELVY)
    roles = [s["role"] for s in mention["source_spans"]]
    limitation = roles.index("LIMITATION_STATEMENT")
    assert limitation not in assertion["negation_scope"]
    assert assertion["span_index"] in assertion["negation_scope"]


def test_a_genuinely_negated_claim_is_still_flagged():
    _mention, assertion = _assoc(
        "PRODUCT is not indicated for the treatment of migraine with aura.")
    assert "negated_inversion" in (assertion.get("flags") or [])


def test_the_limitation_text_is_retained_not_discarded():
    mention, _assertion = _assoc(UBRELVY)
    texts = [s["text"] for s in mention["source_spans"]]
    assert any("preventive treatment" in t for t in texts)


def test_the_extraction_reads_the_indication_span_not_the_header():
    mention, _assertion = _assoc(UBRELVY)
    step = mention["resolution"]["pipeline"][0]
    assert step["span_role"] == "SECTION_TEXT"
    assert "Limitations of Use" not in step["input_value"]


def test_spans_are_typed_and_carry_the_document():
    mention, _assertion = _assoc(UBRELVY)
    for span in mention["source_spans"]:
        assert span["role"]
        assert span["document"] == "DailyMed:fd9f9458"


def test_a_source_without_dailymed_structure_still_gets_one_typed_span():
    record = {"source": "INDIA", "indications_text": "Indicated for anxiety"}
    assoc = {"relationship_type": "INDICATION",
             "evidence": [{"original_disease_label": "anxiety",
                           "snippet": "Indicated for anxiety"}]}
    mention, assertion = _build_disease_provenance(
        record, assoc, "MONDO:0011918", "anxiety")
    assert [s["role"] for s in mention["source_spans"]] == ["TABLE_CELL"]
    assert assertion["span_index"] == 0
