"""Tests for enrichment pipeline."""


from medic.enrichment.atc_smiles import decompose_atc
from medic.enrichment.drug_tags import classify_from_atc


def test_decompose_atc():
    levels = decompose_atc("N02AB03")
    assert levels["atc_main"] == "N"
    assert levels["atc_level1"] == "N02"
    assert levels["atc_level2"] == "N02A"
    assert levels["atc_level3"] == "N02AB"
    assert levels["atc_level4"] == "N02AB03"


def test_decompose_atc_empty():
    assert decompose_atc("") == {}


def test_classify_steroid():
    tags = classify_from_atc(["H02AB01"])
    assert tags["is_steroid"] is True
    assert tags["is_antimicrobial"] is False


def test_classify_antimicrobial():
    tags = classify_from_atc(["J01CA04"])
    assert tags["is_antimicrobial"] is True
    assert tags["is_steroid"] is False


def test_classify_no_atc():
    tags = classify_from_atc([])
    assert all(v is False for v in tags.values())


def test_classify_multiple_atc():
    tags = classify_from_atc(["L01XA01", "V09IX01"])
    assert tags["is_chemotherapy"] is True
    assert tags["is_cancer_drug"] is True
    assert tags["is_radioisotope_or_diagnostic_agent"] is True
