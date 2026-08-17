"""Tests for building Mention dicts (with ordered steps) from the legacy stage objects."""

from medic.provenance_build import build_assertion, build_mention


def _validate(mention: dict) -> None:
    """The built Mention must validate against the master schema (target class Mention)."""
    import subprocess
    import tempfile
    import yaml

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
        yaml.safe_dump(mention, fh, allow_unicode=True)
        path = fh.name
    out = subprocess.run(
        ["uv", "run", "linkml-validate", "--schema", "src/medic/schema/medic.yaml",
         "--target-class", "Mention", path],
        capture_output=True, text=True,
    )
    assert out.returncode == 0, f"validate failed: {out.stdout}\n{out.stderr}"


def test_two_step_english_drug():
    # Orange Book ASPIRIN: grounding + identity normalization, no translation.
    m = build_mention(
        original_literal="ASPIRIN",
        entity_type="drug",
        mention_id="MEDICNE:aspirin",
        source="ORANGEBOOK",
        grounding={"original_string": "ASPIRIN", "grounded_id": "CHEBI:15365",
                   "grounded_label": "acetylsalicylic acid", "grounding_quality": "lexical_exact",
                   "confidence": 1.0},
        normalization={"original_id": "CHEBI:15365", "normalized_id": "CHEBI:15365",
                       "normalized_label": "acetylsalicylic acid", "normalization_quality": "none"},
    )
    assert m["id"] == "MEDICNE:aspirin"
    assert m["resolved_id"] == "CHEBI:15365"
    cats = [s["category"] for s in m["resolution"]["pipeline"]]
    assert cats == ["GROUNDING", "NORMALIZATION"]
    g = m["resolution"]["pipeline"][0]
    assert g["input_value"] == "ASPIRIN" and g["output_value"] == "CHEBI:15365"
    assert g["quality"] == "lexical_exact"
    assert g["source_vocabulary"] == "CHEBI"


def test_all_four_steps_chinese_drug():
    m = build_mention(
        original_literal="坎地沙坦酯片",
        entity_type="drug",
        mention_id="MEDICNE:cand",
        source="CHINA",
        source_language="zh",
        translation={"source_value": "坎地沙坦酯片", "translation_value": "Candesartan Cilexetil Tablets",
                     "source_language": "zh", "translation_language": "en",
                     "translator": "wikidata:Q116709136", "translator_expertise": "ALGORITHM",
                     "translation_status": "CANDIDATE"},
        grounding={"original_string": "坎地沙坦酯片", "grounded_id": "CHEBI:3348",
                   "grounded_label": "Candesartan cilexetil", "grounding_quality": "lexical_exact_surgery",
                   "confidence": 0.8},
        normalization={"original_id": "CHEBI:3348", "normalized_id": "CHEBI:3348",
                       "normalized_label": "Candesartan cilexetil", "normalization_quality": "none"},
    )
    cats = [s["category"] for s in m["resolution"]["pipeline"]]
    assert cats == ["TRANSLATION", "GROUNDING", "NORMALIZATION"]
    t = m["resolution"]["pipeline"][0]
    assert t["input_value"] == "坎地沙坦酯片"
    assert t["output_value"] == "Candesartan Cilexetil Tablets"
    assert t["method"] == "API"
    assert t["status"] == "CANDIDATE"
    assert "unreviewed_machine" in t["flags"]
    # the grounder sees the English translation, not the Chinese literal
    assert m["resolution"]["pipeline"][1]["input_value"] == "Candesartan Cilexetil Tablets"


def test_disease_mention_carries_recognition_only():
    # The mention records recognising the entity: plain `confidence`, recognition flags only.
    # Relation-level signals passed in are NOT smuggled onto the extraction step.
    m = build_mention(
        original_literal="hyperthyroidism",
        entity_type="disease",
        mention_id="MEDICNE:hyper",
        source="DailyMed",
        source_spans=[{"text": "... conditions causing depletion ... hyperthyroidism ...",
                       "source_reference": "DailyMed:abc", "section_code": "LOINC:34067-9"}],
        extraction={"supporting_quote": "... hyperthyroidism ...", "output_value": "hyperthyroidism",
                    "confidence": 1.0, "flags": ["over_extraction", "truncated_snippet"]},
        grounding={"original_string": "hyperthyroidism", "grounded_id": "MONDO:0004425",
                   "grounded_label": "hyperthyroidism", "grounding_quality": "lexical_exact", "confidence": 1.0},
    )
    cats = [s["category"] for s in m["resolution"]["pipeline"]]
    assert cats == ["EXTRACTION", "GROUNDING"]
    e = m["resolution"]["pipeline"][0]
    assert e["confidence"] == 1.0
    assert "asserted_relationship" not in e and "entailment_score" not in e
    # the claim-level flag is dropped from the step; only the recognition flag survives
    assert e["flags"] == ["truncated_snippet"]
    assert m["source_spans"][0]["section_code"] == "LOINC:34067-9"


def test_build_assertion_holds_the_claim():
    a = build_assertion(
        supporting_quote="BACMIN is indicated for nutritional supplementation ... hyperthyroidism",
        relationship="INDICATION",
        subject_confidence=1.0, object_confidence=1.0, relationship_confidence=1.0,
        negated=False, flags=["over_extraction"],
    )
    assert a["input_value"].startswith("BACMIN")
    assert a["relationship"] == "INDICATION"
    assert a["confidence"]["overall"] == 1.0
    assert a["flags"] == ["over_extraction"]
    # the relation is named by the owning record, not repeated here
    assert "relationship_type" not in a and "asserted_relationship" not in a


def test_assertion_confidence_is_the_product_of_three_explicit_inputs():
    a = build_assertion(
        supporting_quote="X is indicated for the treatment of Y",
        relationship="INDICATION",
        subject_confidence=0.8, object_confidence=0.75, relationship_confidence=1.0,
    )
    # all three inputs are on the record, redundantly, so the arithmetic is auditable
    a = a["confidence"]          # nested ConfidenceBreakdown since Plan 3 (I-11)
    assert a["subject"] == 0.8
    assert a["object"] == 0.75
    assert a["relationship"] == 1.0
    assert a["overall"] == 0.6           # 0.8 * 0.75 * 1.0


def test_trigger_span_is_extractive_and_verifiable():
    quote = "Vfend is indicated for the treatment of invasive aspergillosis in adults."
    a = build_assertion(supporting_quote=quote, relationship="INDICATION")
    # the rationale is a VERBATIM substring of the source — checkable, not generated prose
    assert a["trigger_span"] == "is indicated for the treatment of"
    assert a["trigger_span"] in quote
    assert a["trigger_cue"] == "indication_phrase"


def test_trigger_cue_distinguishes_contraindication():
    a = build_assertion(
        supporting_quote="PRODUCT is contraindicated in patients with severe renal impairment.",
        relationship="CONTRAINDICATION")
    assert a["trigger_span"] == "is contraindicated in"
    assert a["trigger_cue"] == "contraindication_phrase"


def test_agreeing_cue_wins_over_a_limitations_of_use_clause():
    # Real false-positive class: the quote contains BOTH an indication phrase and a
    # "Limitations of Use" contraindication phrase. The cue that matches the asserted
    # relation governs — otherwise correct records get flagged as inversions.
    quote = ("Nateglinide tablets are indicated as an adjunct to diet and exercise to improve "
             "glycemic control in adults with type 2 diabetes mellitus. Limitations of Use : "
             "Nateglinide tablets should not be used in patients with type 1 diabetes.")
    a = build_assertion(supporting_quote=quote, relationship="INDICATION")
    assert a["trigger_cue"] == "indication_phrase"
    assert a["trigger_span"] == "are indicated as an adjunct to"
    assert "wrong_section" not in a["flags"]
    # the same text read as a CONTRAINDICATION legitimately finds the other cue
    b = build_assertion(supporting_quote=quote, relationship="CONTRAINDICATION")
    assert b["trigger_cue"] == "contraindication_phrase"


def test_contradicting_sole_cue_is_flagged_for_review():
    # Only a contraindication cue present, yet an INDICATION is asserted -> review signal.
    a = build_assertion(
        supporting_quote="PRODUCT is contraindicated in patients with hepatic impairment.",
        relationship="INDICATION")
    assert a["trigger_cue"] == "contraindication_phrase"
    assert "wrong_section" in a["flags"]


def test_section_warrant_licenses_a_cueless_claim():
    # A bare disease list from a section that by construction holds only indications.
    a = build_assertion(supporting_quote="Epilepsy; myoclonic seizures",
                        relationship="INDICATION", section_warrant="EMA")
    assert a["trigger_cue"] == "section_header"
    assert a["section_warrant"] == "EMA"
    assert "trigger_span" not in a          # structural provenance, not a phrase


def test_missing_trigger_is_reported_not_invented():
    a = build_assertion(supporting_quote="Associated symptoms included hypotonia.",
                        relationship="INDICATION")
    assert a["trigger_cue"] == "none_found"
    assert "trigger_span" not in a          # nothing verifiable -> nothing asserted


def test_every_step_is_version_stamped():
    # Provenance must say WHAT ran each step, with a version — otherwise a record cannot be
    # compared against a re-run (FAILURE_MODES 13.1).
    m = build_mention(
        original_literal="坎地沙坦酯片", entity_type="drug", mention_id="MEDICNE:c",
        extraction={"method": "STRUCTURED_FIELD", "tool": "medic-ingest-china"},
        translation={"source_value": "坎地沙坦酯片", "translation_value": "Candesartan",
                     "translator": "wikidata:Q116709136", "translator_expertise": "ALGORITHM",
                     "translation_status": "CANDIDATE"},
        grounding={"original_string": "坎地沙坦酯片", "grounded_id": "CHEBI:3348",
                   "grounding_quality": "lexical_exact_surgery", "confidence": 0.8},
        normalization={"original_id": "CHEBI:3348", "normalized_id": "CHEBI:3348",
                       "normalization_quality": "none", "tool": "medic-normalizer/1"},
    )
    for step in m["resolution"]["pipeline"]:
        assert step.get("tool"), f"{step['category']} has no tool"
        assert step.get("tool_version"), f"{step['category']} has no tool_version"
    by_cat = {s["category"]: s for s in m["resolution"]["pipeline"]}
    assert by_cat["GROUNDING"]["tool"] == "medic-lexical-grounder"
    assert by_cat["NORMALIZATION"]["tool_version"] == "1"
    assert by_cat["TRANSLATION"]["tool"] == "babelon"
    # the machine-translation engine is the agent
    assert by_cat["TRANSLATION"]["agent"]["agent_name"] == "DeepL"


def test_llm_steps_pin_the_model_id():
    m = build_mention(
        original_literal="gout", entity_type="disease", mention_id="MEDICNE:g",
        extraction={"supporting_quote": "indicated for gout", "output_value": "gout",
                    "method": "LLM", "confidence": 1.0},
        grounding={"original_string": "gout", "grounded_id": "MONDO:1",
                   "grounding_quality": "lexical_exact", "confidence": 1.0},
    )
    ext = m["resolution"]["pipeline"][0]
    assert ext["agent"]["agent_type"] == "AI_AGENT"
    assert ext["agent"]["agent_version"]            # the dated model id
    assert ext["tool"] == "medic-extractor"

    a = build_assertion(supporting_quote="indicated for gout", relationship_confidence=1.0)
    assert a["tool"] == "medic-extractor" and a["tool_version"]
    assert a["agent"]["agent_version"] == ext["agent"]["agent_version"]


def test_build_assertion_negation():
    a = build_assertion(supporting_quote="not indicated for gout",
                        relationship_confidence=1.0,
                        negated=True, negation_cue="not indicated")
    assert "negated_inversion" in a["flags"]
    assert a["negation_cue"] == "not indicated"


def test_grounding_step_carries_applied_rules():
    # The Stage-1 preprocessing rules that fired (from the SSSOM store) must reach the
    # funneled GroundingStep — I-8 completeness.
    m = build_mention(
        original_literal="Ferrous Sulphate 150mg Sustained Release",
        entity_type="drug", mention_id="MEDICNE:fes",
        grounding={"original_string": "Ferrous Sulphate 150mg Sustained Release",
                   "grounded_id": "CHEBI:75832", "grounded_label": "iron(2+) sulfate",
                   "grounding_quality": "lexical_exact_normalized", "confidence": 0.9},
        applied_rules=["formulation_strip"],
    )
    g = next(s for s in m["resolution"]["pipeline"] if s["category"] == "GROUNDING")
    assert g["applied_rules"] == ["formulation_strip"]


def test_drug_mention_opens_with_structured_extraction():
    # Every drug mention should open with a STRUCTURED_FIELD ExtractionStep (spec Example 3),
    # quoting the verbatim literal, before grounding.
    m = build_mention(
        original_literal="ASPIRIN", entity_type="drug", mention_id="MEDICNE:aspirin",
        source="ORANGEBOOK",
        extraction={"method": "STRUCTURED_FIELD"},
        grounding={"original_string": "ASPIRIN", "grounded_id": "CHEBI:15365",
                   "grounded_label": "acetylsalicylic acid", "grounding_quality": "lexical_exact",
                   "confidence": 1.0},
        normalization={"original_id": "CHEBI:15365", "normalized_id": "CHEBI:15365",
                       "normalized_label": "acetylsalicylic acid", "normalization_quality": "none"},
    )
    cats = [s["category"] for s in m["resolution"]["pipeline"]]
    assert cats == ["EXTRACTION", "GROUNDING", "NORMALIZATION"]
    e = m["resolution"]["pipeline"][0]
    assert e["method"] == "STRUCTURED_FIELD"
    assert e["input_value"] == "ASPIRIN" and e["output_value"] == "ASPIRIN"
    assert e["quality"] == "verbatim"


def test_builds_validate_against_schema():
    m = build_mention(
        original_literal="ASPIRIN", entity_type="drug", mention_id="MEDICNE:aspirin",
        source="ORANGEBOOK",
        extraction={"method": "STRUCTURED_FIELD"},
        grounding={"original_string": "ASPIRIN", "grounded_id": "CHEBI:15365",
                   "grounded_label": "acetylsalicylic acid", "grounding_quality": "lexical_exact",
                   "confidence": 1.0},
        applied_rules=["formulation_strip"],
    )
    _validate(m)
