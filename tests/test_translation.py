"""Tests for the translation stage — MEDICNE minting + Babelon store.

Fully offline: no DeepL is constructed (the store is pre-seeded or skip is set).
"""

from __future__ import annotations

from medic.mention import mint_mention_id
from medic.translation import TranslationService, TranslationStore


# ---------------------------------------------------------------------------
# MEDICNE minting
# ---------------------------------------------------------------------------
def test_mint_is_deterministic_and_normalized():
    a = mint_mention_id("Абакавир", "drugs")
    b = mint_mention_id("  абакавир ", "drugs")  # whitespace + case collapse
    assert a == b
    assert a.startswith("MEDICNE:")


def test_mint_separates_entity_types():
    assert mint_mention_id("cold", "drugs") != mint_mention_id("cold", "diseases")


# ---------------------------------------------------------------------------
# Babelon store
# ---------------------------------------------------------------------------
def test_store_round_trip(tmp_path):
    path = str(tmp_path / "drug_translation.babelon.tsv")
    store = TranslationStore(path)
    mid = mint_mention_id("来那度胺胶囊", "drugs")
    store.upsert_source(mid, "来那度胺胶囊", "zh")
    store.set_translation(mid, "Lenalidomide Capsules")
    store.save()

    reloaded = TranslationStore(path)
    reloaded.load()
    assert reloaded.translation_value(mid) == "Lenalidomide Capsules"
    assert reloaded.get(mid)["source_value"] == "来那度胺胶囊"
    assert reloaded.untranslated_ids() == []


def test_store_upsert_never_overwrites(tmp_path):
    store = TranslationStore(str(tmp_path / "s.tsv"))
    mid = mint_mention_id("x", "drugs")
    store.upsert_source(mid, "x", "ru")
    store.set_translation(mid, "translated")
    store.upsert_source(mid, "x", "ru")  # must not wipe the translation
    assert store.translation_value(mid) == "translated"


def test_untranslated_ids_tracks_empty(tmp_path):
    store = TranslationStore(str(tmp_path / "s.tsv"))
    a = mint_mention_id("a", "drugs")
    b = mint_mention_id("b", "drugs")
    store.upsert_source(a, "a", "ru")
    store.upsert_source(b, "b", "ru")
    store.set_translation(a, "AAA")
    assert store.untranslated_ids() == [b]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------
def test_service_mention_id_matches_minter(tmp_path):
    svc = TranslationService(str(tmp_path / "s.tsv"), "ru")
    assert svc.mention_id("Абакавир") == mint_mention_id("Абакавир", "drugs")


def test_service_seeded_lookup(tmp_path):
    path = str(tmp_path / "s.tsv")
    svc = TranslationService(path, "ru")
    mid = svc.mention_id("Абакавир")
    svc.store.upsert_source(mid, "Абакавир", "ru")
    svc.store.set_translation(mid, "abacavir")

    assert svc.translated("Абакавир") == "abacavir"
    obj = svc.translation_object("Абакавир")
    assert obj["translation_value"] == "abacavir"
    assert obj["source_value"] == "Абакавир"
    assert obj["subject_id"] == mid
