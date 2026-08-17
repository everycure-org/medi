from linkml_runtime.utils.schemaview import SchemaView

# The grounding *classes* still live in grounding.yaml, but the three grounding enums
# (Grounding/Normalization quality + PreprocessingRuleEnum) and TranslatorExpertiseEnum
# were moved to the standalone provenance.yaml. The legacy Grounding class only fully
# resolves its enum ranges through the master schema, so class-slot checks load that.
GROUNDING_SCHEMA = "src/medic/schema/grounding.yaml"
PROVENANCE_SCHEMA = "src/medic/schema/provenance.yaml"
MASTER_SCHEMA = "src/medic/schema/medic.yaml"


def test_grounding_class_slots():
    sv = SchemaView(MASTER_SCHEMA)
    slots = set(sv.class_slots("Grounding"))
    assert {"original_string", "grounded_id", "grounded_label",
            "grounding_quality", "confidence"} <= slots


def test_grounding_quality_enum_values():
    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("GroundingQualityEnum").permissible_values)
    assert vals == {"curated", "lexical_exact", "lexical_exact_normalized",
                    "lexical_exact_surgery", "rxnorm_proposed", "source_asserted",
                    "unresolved"}


def test_normalization_quality_enum_values():
    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("NormalizationQualityEnum").permissible_values)
    assert vals == {"curated", "asserted_exact", "deprecated_replacement", "identity"}


def test_preprocessing_rule_enum_documents_all_transforms():
    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("PreprocessingRuleEnum").permissible_values)
    # existing + the new fine-grained levers (#1 INN spelling, #2 fuzzy, #3 translation)
    assert {"salt_ester_strip", "qualifier_strip", "combination_split",
            "disease_to_disorder", "arabic_to_roman", "brit_to_am",
            "inn_suffix_in_to_ine", "inn_z_to_s", "inn_ti_to_thi",
            "fuzzy_edit1_unique", "translation_dictionary", "translation_llm"} <= vals


def test_every_rule_has_certainty_and_family():
    sv = SchemaView(PROVENANCE_SCHEMA)
    pvs = sv.get_enum("PreprocessingRuleEnum").permissible_values
    for name, pv in pvs.items():
        ann = pv.annotations
        assert "certainty" in ann, f"{name} missing certainty"
        c = float(ann["certainty"].value)
        assert 0.0 <= c <= 1.0, f"{name} certainty out of range: {c}"
        assert "rule_family" in ann, f"{name} missing rule_family"


def test_code_rule_maps_match_schema():
    # RULE_CERTAINTY / RULE_PREDICATE in code must mirror the schema annotations exactly.
    from medic.grounding.lexical.preprocess import RULE_CERTAINTY, RULE_PREDICATE
    sv = SchemaView(PROVENANCE_SCHEMA)
    pvs = sv.get_enum("PreprocessingRuleEnum").permissible_values
    assert set(pvs) == set(RULE_CERTAINTY) == set(RULE_PREDICATE)
    for name, pv in pvs.items():
        assert float(pv.annotations["certainty"].value) == RULE_CERTAINTY[name], name
        assert pv.annotations["predicate"].value == RULE_PREDICATE[name], name


def test_rule_families_group_as_expected():
    sv = SchemaView(PROVENANCE_SCHEMA)
    pvs = sv.get_enum("PreprocessingRuleEnum").permissible_values
    fam = {}
    for name, pv in pvs.items():
        fam.setdefault(pv.annotations["rule_family"].value, set()).add(name)
    assert fam["spelling_inn"] == {"inn_suffix_in_to_ine", "inn_z_to_s", "inn_ph_to_f",
                                   "inn_ti_to_thi", "inn_ae_oe_to_e"}
    assert fam["fuzzy"] == {"fuzzy_edit1_unique"}
    assert fam["translation"] == {"translation_dictionary", "translation_llm",
                                   "deepl_translation"}
