from medic.grounding.lexical.preprocess import (
    base_normalize,
    formulation_variants,
    generate_variants,
    strip_formulation,
)


def _vset(s):
    return {v.string for v in generate_variants(s)}


def test_base_normalize_whitespace_case():
    assert base_normalize("  Marfan  Syndrome ") == "marfan syndrome"


def test_base_normalize_diacritics_and_brackets():
    assert base_normalize("Sjögren’s [SS]") == "sjogren's"


def test_disease_to_disorder():
    assert "heart disorder" in _vset("heart disease")


def test_disorder_to_disease():
    assert "heart disease" in _vset("heart disorder")


def test_arabic_to_roman_with_indicator():
    assert "diabetes type i" in _vset("diabetes type 1")


def test_roman_to_arabic_with_indicator():
    assert "diabetes type 1" in _vset("diabetes type i")


def test_no_roman_conversion_without_indicator():
    # "trisomy 21" must NOT become a roman numeral (no indicator prefix)
    assert not any("xxi" in v or "trisomy i" in v for v in _vset("trisomy 21"))


def test_comma_drop_type():
    assert "diabetes mellitus type 2" in _vset("diabetes mellitus, type 2")


def test_hyphen_type_to_space():
    assert "glycogen storage disease type 2" in _vset("glycogen storage disease type-2")


def test_brit_to_am_spelling():
    assert "brain tumor" in _vset("brain tumour")


def test_cell_hyphen_to_space():
    assert "t cell leukemia" in _vset("t-cell leukemia")


def test_strip_leading_other_is_broad():
    variants = generate_variants("other bacterial infection")
    match = [v for v in variants if v.string == "bacterial infection"]
    assert match and match[0].scope == "broad"


def test_variants_exclude_input_and_dedupe():
    variants = generate_variants("marfan syndrome")
    assert all(v.string != "marfan syndrome" for v in variants)
    assert len({v.string for v in variants}) == len(variants)


# --- formulation_strip -----------------------------------------------------------------

def _fstrip(s):
    return base_normalize(strip_formulation(s))


def test_formulation_strip_strength_and_form():
    assert _fstrip("Ferrous Sulphate 150mg Sustained Release") == "ferrous sulphate"


def test_formulation_strip_pharmacopoeia_tag():
    assert _fstrip("Hydroxyurea USP 500mg Capsule") == "hydroxyurea"


def test_formulation_strip_dose_range_and_paren_metadata():
    assert _fstrip("Ruxolitinib Tablet 5mg/15mg/20mg (Additional indication)") == "ruxolitinib"


def test_formulation_strip_percent_concentration():
    assert _fstrip("Feracrylum 3% solution") == "feracrylum"


def test_formulation_strip_release_abbreviation():
    assert _fstrip("Lamotrigine ER Tablet 25mg/50mg/100mg/200 mg") == "lamotrigine"


def test_formulation_strip_keeps_combination_separator():
    # residue stays a combination so the matcher can split it downstream
    out = _fstrip("Netupitant 300 mg + Palonosetron 0.5 mg Capsule")
    assert "netupitant" in out and "palonosetron" in out and "+" in out


# --- guards ----------------------------------------------------------------------------

def test_guard_preserves_vitamin_d3_digit():
    # token-internal digit must survive (not read as a dose)
    assert "vitamin d3" in _fstrip(
        "Vitamin D3 (Cholecalciferol) Orally disintegrating strips 2000 IU")


def test_guard_preserves_b12():
    assert strip_formulation("Vitamin B12 500mcg Tablet").lower().replace(" ", "") \
        .startswith("vitaminb12")


def test_guard_preserves_omega3_hyphenated_numeric():
    assert "omega-3" in strip_formulation("Omega-3 Acid 1000mg Capsule").lower()


def test_guard_word_boundary_no_substring_clip():
    # 'ip' (pharmacopoeia) must not clip inside 'Apixaban'; 'sr' must not clip a real token
    assert _fstrip("Apixaban Tablets 2.5/5mg") == "apixaban"


def test_guard_refuses_empty_residue():
    # a string that strips to nothing meaningful yields no variant (no false strip)
    assert formulation_variants("500mg Tablet") == []
    assert formulation_variants("10mg Capsule USP") == []


def test_guard_no_variant_when_nothing_removed():
    assert formulation_variants("aspirin") == []


def test_formulation_variant_provenance():
    vs = formulation_variants("Ibuprofen 200mg Tablet")
    assert len(vs) == 1
    assert vs[0].applied == ["formulation_strip"]
    assert vs[0].scope == "close"
    assert base_normalize(vs[0].string) == "ibuprofen"
