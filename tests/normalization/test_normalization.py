from medic.normalization.normalizer import Normalizer
from medic.normalization.store import NormalizationDecision, NormalizationMappingStore


def _norm(tmp_path, mi):
    store = NormalizationMappingStore(str(tmp_path / "disease_normalization.sssom.tsv"), "diseases")
    store.load()
    return Normalizer("diseases", mi, store, "MONDO", "medic-normalizer/0")


def test_asserted_exact(tmp_path):
    mi = {"UMLS:C0024796": ("MONDO:0007947", "asserted_exact", "skos:exactMatch")}
    d = _norm(tmp_path, mi).normalize("UMLS:C0024796", "Marfan")
    assert d.object_id == "MONDO:0007947" and d.normalization_quality == "asserted_exact"


def test_none_when_no_mapping(tmp_path):
    d = _norm(tmp_path, {}).normalize("UMLS:C9999999", "x")
    assert d.object_id == "UMLS:C9999999" and d.normalization_quality == "identity"


def test_store_manual_wins(tmp_path):
    p = tmp_path / "disease_normalization.sssom.tsv"
    s = NormalizationMappingStore(str(p), "diseases")
    s.load()
    m = NormalizationDecision("ICD10CM:E11.9", "skos:exactMatch", "MONDO:0005148", "y",
                              "curated", "semapv:ManualMappingCuration", "hand")
    s.record(m)
    s.save()
    s2 = NormalizationMappingStore(str(p), "diseases")
    s2.load()
    s2.record(NormalizationDecision("ICD10CM:E11.9", "skos:exactMatch", "MONDO:9999999", "z",
                                    "asserted_exact", "semapv:UnspecifiedMatching", "auto"))
    s2.save()
    s3 = NormalizationMappingStore(str(p), "diseases")
    s3.load()
    assert s3.lookup("ICD10CM:E11.9").object_id == "MONDO:0005148"
