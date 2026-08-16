from medic.grounding.store import GroundingDecision, LiteralMappingStore


def _auto(label, obj, prep=None, match_string=None):
    return GroundingDecision(
        subject_label=label, entity_type="diseases", predicate_id="skos:exactMatch",
        object_id=obj, object_label="x", object_match_field="rdfs:label",
        mapping_justification="semapv:LexicalMatching", subject_preprocessing=prep or [],
        match_string=match_string if match_string is not None else label, confidence=1.0)


def test_roundtrip_and_key_normalization(tmp_path):
    p = tmp_path / "disease_grounding.sssom.tsv"
    s = LiteralMappingStore(str(p), "diseases")
    s.load()
    s.record_subject("Marfan syndrome", [_auto("Marfan syndrome", "MONDO:0007947")])
    s.save()
    s2 = LiteralMappingStore(str(p), "diseases")
    s2.load()
    rows = s2.lookup("marfan   syndrome")
    assert len(rows) == 1 and rows[0].object_id == "MONDO:0007947"


def test_manual_row_preserved_over_auto(tmp_path):
    p = tmp_path / "disease_grounding.sssom.tsv"
    s = LiteralMappingStore(str(p), "diseases")
    s.load()
    m = _auto("Hunter syndrome", "MONDO:0010674")
    m.mapping_justification = "semapv:ManualMappingCuration"
    s.record_subject("Hunter syndrome", [m])
    s.save()
    s2 = LiteralMappingStore(str(p), "diseases")
    s2.load()
    s2.record_subject("Hunter syndrome", [_auto("Hunter syndrome", "MONDO:9999999")])
    s2.save()
    s3 = LiteralMappingStore(str(p), "diseases")
    s3.load()
    assert s3.lookup("Hunter syndrome")[0].object_id == "MONDO:0010674"


def test_rxnorm_proposal_preserved_over_auto_and_locked(tmp_path):
    # An RxNorm proposal (justification RXNORM) is locked: it survives a later auto write
    # and is reported by locked_rows so the matcher can short-circuit on it offline.
    p = tmp_path / "drug_grounding.sssom.tsv"
    s = LiteralMappingStore(str(p), "drugs")
    s.load()
    prop = _auto("Ephedrine Sulphide 75mg", "CHEBI:15407")
    prop.entity_type = "drugs"
    prop.mapping_justification = "RXNORM"
    prop.subject_preprocessing = ["rxnorm_resolve"]
    s.record_subject("Ephedrine Sulphide 75mg", [prop])
    s.save()

    s2 = LiteralMappingStore(str(p), "drugs")
    s2.load()
    # Simulate a fresh grounding run trying to overwrite with an auto (NoTermFound-style) row.
    s2.record_subject("Ephedrine Sulphide 75mg", [_auto("Ephedrine Sulphide 75mg", "CHEBI:99999")])
    s2.save()

    s3 = LiteralMappingStore(str(p), "drugs")
    s3.load()
    rows = s3.lookup("Ephedrine Sulphide 75mg")
    assert len(rows) == 1 and rows[0].object_id == "CHEBI:15407"
    assert rows[0].mapping_justification == "RXNORM"
    assert s3.locked_rows("Ephedrine Sulphide 75mg")  # matcher short-circuits on these
    assert not s3.manual_rows("Ephedrine Sulphide 75mg")  # but not classed as hand-curated


def test_multiple_rows_per_subject(tmp_path):
    p = tmp_path / "drug_grounding.sssom.tsv"
    s = LiteralMappingStore(str(p), "drugs")
    s.load()
    combo = "amoxicillin and clavulanate"
    s.record_subject(combo, [_auto(combo, "CHEBI:2676"), _auto(combo, "CHEBI:3729")])
    s.save()
    s2 = LiteralMappingStore(str(p), "drugs")
    s2.load()
    assert {r.object_id for r in s2.lookup(combo)} == {"CHEBI:2676", "CHEBI:3729"}


def test_grounding_quality_from_match_string():
    assert _auto("Marfan syndrome", "MONDO:1", match_string="Marfan syndrome").grounding_quality \
        == "lexical_exact"
    assert _auto("Marfan Syndrome", "MONDO:1", match_string="marfan syndrome").grounding_quality \
        == "lexical_exact_normalized"
    assert _auto("heart disease", "MONDO:1", prep=["disease_to_disorder"]).grounding_quality \
        == "lexical_exact_surgery"
