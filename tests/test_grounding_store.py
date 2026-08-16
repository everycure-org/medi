"""Tests for the grounding SSSOM store — MEDICNE ``subject_id`` anchoring (I-9)."""

from __future__ import annotations

from medic.grounding.store import GroundingDecision, LiteralMappingStore
from medic.mention import mint_mention_id


def _decision(object_id="CHEBI:15365", label="aspirin"):
    return GroundingDecision(
        subject_label="aspirin", entity_type="drugs",
        predicate_id="skos:exactMatch", object_id=object_id, object_label=label,
        object_match_field="label", mapping_justification="semapv:LexicalMatching",
        subject_preprocessing=["base_normalization"], match_string="aspirin", confidence=1.0,
    )


def test_record_subject_stamps_mention_id(tmp_path):
    store = LiteralMappingStore(str(tmp_path / "drug_grounding.sssom.tsv"), "drugs")
    mid = mint_mention_id("aspirin", "drugs")
    store.record_subject("aspirin", [_decision()], subject_id=mid)
    assert store.lookup("aspirin")[0].subject_id == mid


def test_subject_id_round_trips_to_tsv(tmp_path):
    path = str(tmp_path / "drug_grounding.sssom.tsv")
    store = LiteralMappingStore(path, "drugs")
    mid = mint_mention_id("aspirin", "drugs")
    store.record_subject("aspirin", [_decision()], subject_id=mid)
    store.save()

    # The MEDICNE id lands in the SSSOM subject_id column (previously always empty).
    body = open(path).read()
    assert mid in body

    reloaded = LiteralMappingStore(path, "drugs")
    reloaded.load()
    assert reloaded.lookup("aspirin")[0].subject_id == mid


def test_combination_decisions_share_one_mention_id(tmp_path):
    store = LiteralMappingStore(str(tmp_path / "s.tsv"), "drugs")
    mid = mint_mention_id("a and b", "drugs")
    decisions = [_decision("CHEBI:1", "a"), _decision("CHEBI:2", "b")]
    store.record_subject("a and b", decisions, subject_id=mid)
    ids = {d.subject_id for d in store.lookup("a and b")}
    assert ids == {mid}  # both components of one mention share its id
