"""Tests for the standalone transformation-provenance schema (plan #1 foundation)."""

from linkml_runtime.utils.schemaview import SchemaView

SCHEMA = "src/medic/schema/provenance.yaml"


def test_schema_loads_and_is_standalone():
    sv = SchemaView(SCHEMA)
    # standalone namespace, no medic coupling
    assert sv.schema.id == "https://w3id.org/monarch-initiative/transformation-provenance"
    assert "medic" not in (sv.schema.default_prefix or "")


def test_discriminator_and_method_enums():
    sv = SchemaView(SCHEMA)
    assert set(sv.get_enum("TransformationCategory").permissible_values) == {
        "EXTRACTION", "TRANSLATION", "GROUNDING", "NORMALIZATION"}
    assert set(sv.get_enum("TransformationMethod").permissible_values) == {
        "LLM", "DETERMINISTIC_RULE", "LEXICAL_MATCH", "TRANSLITERATION",
        "API", "STRUCTURED_FIELD", "SOURCE_ASSERTED", "HUMAN"}
    assert set(sv.get_enum("StepStatus").permissible_values) == {
        "MACHINE", "CANDIDATE", "UNDER_REVIEW", "CONFIRMED", "REJECTED"}
    assert set(sv.get_enum("AgentTypeEnum").permissible_values) == {"HUMAN", "AI_AGENT"}


def test_quality_and_flag_enums():
    sv = SchemaView(SCHEMA)
    assert set(sv.get_enum("ExtractionQuality").permissible_values) == {
        "verbatim", "canonicalized", "synonym", "not_stated"}
    assert set(sv.get_enum("TranslationPrecision").permissible_values) == {
        "exact", "broader", "narrower", "close"}
    # ExtractionFlag = recognition failures ONLY (is the entity really here?)
    assert set(sv.get_enum("ExtractionFlag").permissible_values) == {
        "hallucination", "truncated_snippet", "coreference_ambiguity", "scope_narrowed"}
    # AssertionFlag = relation/claim failures (entity may be recognised perfectly)
    assert set(sv.get_enum("AssertionFlag").permissible_values) == {
        "negated_inversion", "over_extraction", "wrong_section", "wrong_pairing"}
    assert set(sv.get_enum("TranslationFlag").permissible_values) == {
        "unreviewed_machine", "trade_name_source"}
    assert set(sv.get_enum("GroundingFlag").permissible_values) == {
        "fuzzy", "ambiguous_resolved", "broadened", "isotope_risk",
        "formulation_stripped", "rxnorm_proposed", "script_transliteration"}
    assert set(sv.get_enum("NormalizationFlag").permissible_values) == {
        "no_target_xref", "deprecated_replacement"}


def test_moved_enums_present_here():
    sv = SchemaView(SCHEMA)
    assert set(sv.get_enum("GroundingQualityEnum").permissible_values) == {
        "curated", "lexical_exact", "lexical_exact_normalized",
        "lexical_exact_surgery", "rxnorm_proposed", "source_asserted",
                    "unresolved"}
    assert set(sv.get_enum("NormalizationQualityEnum").permissible_values) == {
        "curated", "asserted_exact", "deprecated_replacement", "identity"}
    assert "deepl_translation" in sv.get_enum("PreprocessingRuleEnum").permissible_values
    assert "ALGORITHM" in sv.get_enum("TranslatorExpertiseEnum").permissible_values


def test_step_class_hierarchy():
    sv = SchemaView(SCHEMA)
    assert sv.get_class("TransformationStep").abstract is True
    for sub in ["ExtractionStep", "TranslationStep", "GroundingStep", "NormalizationStep"]:
        assert sv.get_class(sub).is_a == "TransformationStep"
    base_slots = set(sv.class_slots("TransformationStep"))
    assert {"category", "input_value", "output_value", "method"} <= base_slots


def test_subclass_slot_usage_narrows_ranges():
    sv = SchemaView(SCHEMA)

    def rng(cls, slot):
        return sv.induced_slot(slot, cls).range

    assert rng("ExtractionStep", "quality") == "ExtractionQuality"
    assert rng("ExtractionStep", "flags") == "ExtractionFlag"
    assert rng("TranslationStep", "quality") == "TranslationPrecision"
    assert rng("TranslationStep", "flags") == "TranslationFlag"
    assert rng("GroundingStep", "quality") == "GroundingQualityEnum"
    assert rng("GroundingStep", "flags") == "GroundingFlag"
    assert rng("GroundingStep", "applied_rules") == "PreprocessingRuleEnum"
    assert rng("NormalizationStep", "quality") == "NormalizationQualityEnum"
    assert rng("NormalizationStep", "flags") == "NormalizationFlag"


def test_mention_and_textspan():
    sv = SchemaView(SCHEMA)
    mslots = set(sv.class_slots("Mention"))
    assert {"id", "original_literal", "source_spans", "resolution", "resolved_id"} <= mslots
    assert "mention_id" not in mslots and "steps" not in mslots
    assert sv.induced_slot("resolution", "Mention").range == "Resolution"
    assert sv.induced_slot("source_spans", "Mention").range == "TextSpan"
    # `document` replaced `source_reference`, and `role` became required, when spans
    # became typed (Plan 2/3, D6).
    assert {"text", "role", "document", "section_code"} <= set(sv.class_slots("TextSpan"))


def test_extraction_step_carries_no_relation_info():
    # An entity mention must say nothing about the claim it participates in — that is the
    # Assertion's job (a disease Mention is reusable and relation-agnostic).
    sv = SchemaView(SCHEMA)
    es = set(sv.class_slots("ExtractionStep"))
    assert not ({"asserted_relationship", "entailment_score", "negation_cue"} & es)
    assert "confidence" in es          # plain confidence replaces entailment_score


def test_assertion_class():
    sv = SchemaView(SCHEMA)
    aslots = set(sv.class_slots("Assertion"))
    assert {"input_value", "confidence", "flags", "negation_cue", "method"} <= aslots
    assert sv.induced_slot("flags", "Assertion").range == "AssertionFlag"
    # the relation itself is named by the owning record, never repeated on the Assertion
    assert "relationship_type" not in aslots


def test_resolution_container():
    sv = SchemaView(SCHEMA)
    rslots = set(sv.class_slots("Resolution"))
    assert {"input_value", "output_value", "confidence", "pipeline"} <= rslots
    assert sv.induced_slot("pipeline", "Resolution").multivalued is True
    assert sv.induced_slot("pipeline", "Resolution").range == "TransformationStep"
    # uniform output_label + split tool/tool_version on every step
    base = set(sv.class_slots("TransformationStep"))
    assert {"output_label", "tool", "tool_version"} <= base


def test_agent_is_vendored_fresh():
    sv = SchemaView(SCHEMA)
    aslots = set(sv.class_slots("ProvenanceAgent"))
    assert {"agent_id", "agent_type", "agent_name"} <= aslots
    assert sv.induced_slot("agent_type", "ProvenanceAgent").range == "AgentTypeEnum"
