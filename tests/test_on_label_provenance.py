"""The per-source SourceAssertion: two inlined Mentions + the Assertion, and reliability."""

from medic.merge.on_label_merge import _build_source_assertions, _finalize_pair
from medic.provenance_build import validate_mention_chain, validate_source_assertion
from medic.reliability import ReliabilityTier, score_reliability


def _record():
    return {
        "final_normalized_drug_id": "CHEBI:12777",
        "final_normalized_drug_label": "vitamin A",
        "final_normalized_disease_id": "MONDO:0004425",
        "final_normalized_disease_label": "hyperthyroidism",
        "relationship_type": "INDICATION",
        "source": "DAILYMED",
        "indications_text": (
            "BACMIN is indicated for prophylactic or therapeutic nutritional "
            "supplementation in physiologically stressful conditions. These include "
            "conditions causing depletion, such as chronic alcoholism and hyperthyroidism."
        ),
        "disease_grounding": {
            "original_string": "hyperthyroidism", "grounded_id": "MONDO:0004425",
            "grounded_label": "hyperthyroidism", "grounding_quality": "lexical_exact",
            "confidence": 1.0},
        "disease_normalization": {
            "original_id": "MONDO:0004425", "normalized_id": "MONDO:0004425",
            "normalized_label": "hyperthyroidism", "normalization_quality": "none"},
        "evidence": [{
            "source_type": "REGULATORY", "jurisdiction": "USA",
            "original_disease_label": "hyperthyroidism",
            "original_drug_label": "VITAMIN A",
            "snippet": "conditions causing depletion ... hyperthyroidism",
            "setid": "34beae32", "approval_status": "APPROVED",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=34beae32"}],
    }


def _pair(record=None, relationship="INDICATION"):
    """Wrap one assertion in its pair, the way the merge does."""
    record = record or _record()
    assertion = _build_source_assertions(record)[0]
    pair = {
        "drug_id": record["final_normalized_drug_id"],
        "drug_label": record["final_normalized_drug_label"],
        "disease_id": record["final_normalized_disease_id"],
        "disease_label": record["final_normalized_disease_label"],
        "relationship_type": relationship,
        "assertions": [assertion],
    }
    _finalize_pair(pair)
    return pair, assertion


def test_both_entities_are_inlined_mentions_from_this_document():
    _p, a = _pair()
    assert a["source"] == "DAILYMED"
    assert a["document"] == "DailyMed:34beae32"
    # the drug's literal is THIS document's string, not the canonical label (I-7)
    assert a["drug"]["original_literal"] == "VITAMIN A"
    assert a["drug"]["resolved_id"] == "CHEBI:12777"
    assert a["drug"]["mention_source"] == "DAILYMED"
    d = a["disease"]
    assert d["id"].startswith("MEDICNE:")
    assert d["original_literal"] == "hyperthyroidism"
    assert d["resolved_id"] == "MONDO:0004425"
    assert [s["category"] for s in d["resolution"]["pipeline"]] == [
        "EXTRACTION", "GROUNDING", "NORMALIZATION"]
    ext = d["resolution"]["pipeline"][0]
    # the mention is relation-agnostic: recognition confidence only, no claim info
    assert ext["confidence"] > 0.0
    assert "asserted_relationship" not in ext and "entailment_score" not in ext


def test_spans_are_typed_and_live_on_the_assertion():
    _p, a = _pair()
    assert a["spans"][0]["section_code"] == "LOINC:34067-9"
    assert a["spans"][0]["document"] == "DailyMed:34beae32"
    assert a["spans"][0]["role"] in ("SECTION_TEXT", "SECTION_HEADER")
    # the Mention no longer carries them — they belong to the assertion, not one entity
    assert "source_spans" not in a["disease"]


def test_the_assertion_is_internally_single_source():
    _p, a = _pair()
    assert validate_source_assertion(a) == []


def test_assertion_holds_the_claim_not_the_mention():
    _p, a = _pair()
    assertion = a["assertion"]
    assert assertion["input_value"]
    assert assertion["confidence"]["overall"] > 0.0   # the word IS in the text…
    assert assertion["flags"] == []                   # …an over_extraction would go HERE
    assert "relationship_type" not in assertion


def test_the_confidence_breakdown_is_complete_and_multiplies_out():
    _p, a = _pair()
    conf = a["assertion"]["confidence"]
    assert set(conf) == {"subject", "object", "relationship", "overall", "basis"}
    assert conf["overall"] == round(
        conf["subject"] * conf["object"] * conf["relationship"], 6)


def test_merge_time_xref_normalization_is_recorded_as_a_step():
    from medic.merge.on_label_merge import PRE_XREF_KEY, _normalize_disease_id

    rec = _record()
    rec["final_normalized_disease_id"] = "HP:0000822"
    rec["disease_grounding"]["grounded_id"] = "HP:0000822"
    rec["disease_normalization"] = {"original_id": "HP:0000822", "normalized_id": "HP:0000822",
                                    "normalization_quality": "none"}
    _normalize_disease_id(rec, {"HP:0000822": "MONDO:0005044"})
    assert rec[PRE_XREF_KEY] == "HP:0000822"
    assert rec["final_normalized_disease_id"] == "MONDO:0005044"

    a = _build_source_assertions(rec)[0]
    pl = a["disease"]["resolution"]["pipeline"]
    assert [s["category"] for s in pl][-2:] == ["NORMALIZATION", "NORMALIZATION"]
    hop = pl[-1]
    assert hop["input_value"] == "HP:0000822"
    assert hop["output_value"] == "MONDO:0005044"
    assert hop["quality"] == "asserted_exact"
    assert validate_mention_chain(a["disease"]) == []


def test_source_asserted_id_is_recorded_not_silently_asserted():
    rec = _record()
    rec.pop("disease_grounding")
    rec.pop("disease_normalization")
    pair, a = _pair(rec)
    g = next(s for s in a["disease"]["resolution"]["pipeline"]
             if s["category"] == "GROUNDING")
    assert g["quality"] == "source_asserted"
    assert g["method"] == "SOURCE_ASSERTED"
    assert g["output_value"] == "MONDO:0004425"
    assert validate_mention_chain(a["disease"]) == []
    assert score_reliability(pair) != ReliabilityTier.HIGH


def test_reliability_is_computable_on_the_pair():
    pair, _a = _pair()
    assert score_reliability(pair).value in {"HIGH", "MEDIUM", "LOW", "EXCLUDED"}


def test_negated_indication_sets_negation_flag_and_excludes():
    rec = _record()
    rec["final_normalized_disease_label"] = "gout"
    rec["indications_text"] = "PROBENECID is not indicated for the treatment of gout."
    rec["disease_grounding"]["original_string"] = "gout"
    rec["evidence"][0]["original_disease_label"] = "gout"
    rec["evidence"][0]["snippet"] = "PROBENECID is not indicated for the treatment of gout."
    pair, a = _pair(rec)
    # negation is a property of the CLAIM, so the flag lands on the assertion...
    assert "negated_inversion" in a["assertion"]["flags"]
    # ...not on the disease mention, whose recognition of "gout" was perfectly correct
    ext = a["disease"]["resolution"]["pipeline"][0]
    assert "negated_inversion" not in ext.get("flags", [])
    assert score_reliability(pair).value == "EXCLUDED"
