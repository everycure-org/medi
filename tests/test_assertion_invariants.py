"""I-8b, I-10, I-11, I-12, I-13 (design spec §7)."""

from medic.provenance_build import validate_pair, validate_source_assertion


def _mention(source, literal="anxiety", resolved="MONDO:0011918", span_index=None):
    return {
        "id": "MEDICNE:x", "original_literal": literal, "entity_type": "disease",
        "mention_source": source, "resolved_id": resolved,
        "resolution": {
            "input_value": literal, "output_value": resolved, "confidence": 1.0,
            "pipeline": [
                {"category": "EXTRACTION", "input_value": literal, "output_value": literal,
                 "confidence": 1.0, "confidence_basis": "DETERMINISTIC",
                 # A structured-field read anchors to no span, so span_index is absent.
                 **({"span_index": span_index} if span_index is not None else {})},
                {"category": "GROUNDING", "input_value": literal, "output_value": resolved,
                 "confidence": 1.0, "confidence_basis": "MEASURED"},
                {"category": "NORMALIZATION", "input_value": resolved,
                 "output_value": resolved, "quality": "identity",
                 "confidence": 1.0, "confidence_basis": "DETERMINISTIC"},
            ],
        },
    }


def _assertion(**kw):
    a = {
        "source": "CDSCO", "document": "CDSCO:2024",
        "spans": [{"role": "TABLE_CELL", "text": "anxiety", "document": "CDSCO:2024"}],
        "drug": _mention("CDSCO", "Etifoxine", "CHEBI:135272"),
        "disease": _mention("CDSCO", span_index=0),
        "assertion": {"confidence": {"subject": 1.0, "object": 1.0, "relationship": 1.0,
                                     "overall": 1.0, "basis": "MEASURED"}},
    }
    a.update(kw)
    return a


def test_a_consistent_assertion_has_no_violations():
    assert validate_source_assertion(_assertion()) == []


def test_i10_a_mixed_source_assertion_is_caught():
    """The defect that started this: India disease, Russia drug."""
    bad = _assertion(drug=_mention("GRLS", "Этифоксин", "CHEBI:135272"))
    assert any("mention_source" in p for p in validate_source_assertion(bad))


def test_i10_a_span_from_another_document_is_caught():
    bad = _assertion(spans=[{"role": "TABLE_CELL", "text": "anxiety", "document": "OTHER:1"}])
    assert any("document" in p for p in validate_source_assertion(bad))


def test_i8b_the_extraction_input_must_equal_its_span():
    bad = _assertion(spans=[{"role": "TABLE_CELL", "text": "something else",
                             "document": "CDSCO:2024"}])
    assert any("I-8b" in p for p in validate_source_assertion(bad))


def test_i11_a_step_without_a_basis_is_caught():
    bad = _assertion()
    del bad["disease"]["resolution"]["pipeline"][1]["confidence_basis"]
    assert any("confidence_basis" in p for p in validate_source_assertion(bad))


def test_i11_a_step_without_a_confidence_is_caught():
    bad = _assertion()
    del bad["disease"]["resolution"]["pipeline"][1]["confidence"]
    assert any("has no confidence" in p for p in validate_source_assertion(bad))


def test_i11_the_overall_must_be_the_product():
    bad = _assertion()
    bad["assertion"]["confidence"]["overall"] = 0.1
    assert any("overall" in p for p in validate_source_assertion(bad))


def test_i12_a_chain_ending_in_a_curie_needs_a_normalization_step():
    bad = _assertion()
    bad["disease"]["resolution"]["pipeline"].pop()
    assert any("I-12" in p for p in validate_source_assertion(bad))


def test_i13_the_pair_confidence_must_match_the_aggregate():
    pair = {"assertions": [_assertion()],
            "confidence": {"overall": 0.5, "method": "NOISY_OR", "n_assertions": 1}}
    assert any("corroboration" in p for p in validate_pair(pair))


def test_i13_a_wrong_assertion_count_is_caught():
    pair = {"assertions": [_assertion()],
            "confidence": {"overall": 1.0, "method": "NOISY_OR", "n_assertions": 7}}
    assert any("n_assertions" in p for p in validate_pair(pair))


def _claim(overall):
    return {"confidence": {"subject": overall, "object": 1.0, "relationship": 1.0,
                           "overall": overall, "basis": "MEASURED"}}


def test_i13_a_flat_noisy_or_over_one_source_is_a_violation():
    """The pre-fix arithmetic: two documents from one source OR'd as if independent.

    Flat, [0.8, 0.8] gives 0.96. Grouped by source it stays 0.8 — one regulator saying
    the same thing twice is not a second opinion.
    """
    a = _assertion(assertion=_claim(0.8))
    b = _assertion(assertion=_claim(0.8), document="CDSCO:2025")
    pair = {"assertions": [a, b],
            "confidence": {"overall": 0.96, "method": "NOISY_OR",
                           "n_assertions": 2, "n_sources": 1}}
    assert any("corroboration" in p for p in validate_pair(pair))


def test_i13_two_sources_at_the_same_confidence_still_corroborate():
    a = _assertion(assertion=_claim(0.8))
    b = _assertion(assertion=_claim(0.8), source="EMA", document="EMA:keppra")
    b["drug"]["mention_source"] = b["disease"]["mention_source"] = "EMA"
    b["spans"][0]["document"] = "EMA:keppra"
    pair = {"assertions": [a, b],
            "confidence": {"overall": 0.96, "method": "NOISY_OR",
                           "n_assertions": 2, "n_sources": 2}}
    assert validate_pair(pair) == []


def test_i13_a_wrong_source_count_is_caught():
    pair = {"assertions": [_assertion()],
            "confidence": {"overall": 1.0, "method": "NOISY_OR",
                           "n_assertions": 1, "n_sources": 4}}
    assert any("n_sources" in p for p in validate_pair(pair))


def test_a_clean_pair_has_no_violations():
    pair = {"assertions": [_assertion()],
            "confidence": {"overall": 1.0, "method": "NOISY_OR",
                           "n_assertions": 1, "n_sources": 1}}
    assert validate_pair(pair) == []
