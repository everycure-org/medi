"""Tests for the RxNorm substance-level resolver (Phase 4 formulation grounding).

The RxNav REST call is mocked throughout — these tests never hit the network.
"""

from types import SimpleNamespace


from medic.enrichment.cache import EnrichmentCache
from medic.enrichment import rxnorm_resolve as rr


# --- false-positive guard ---------------------------------------------------


def test_guard_accepts_substring_ingredient():
    assert rr.ingredient_supported_by_source("Ephedrine Sulphide 75mg Capsule", "ephedrine")


def test_guard_accepts_stem_match():
    # "Cefaclor Monohydrate 250mg" -> ingredient "cefaclor"
    assert rr.ingredient_supported_by_source("Cefaclor Monohydrate 250mg Capsule", "cefaclor")


def test_guard_rejects_unrelated_ingredient():
    # RxNav approximate-matches "menthol" for a sheep pox vaccine; must be dropped.
    assert not rr.ingredient_supported_by_source("Sheep Pox Vaccine (For Veterinary)", "menthol")
    assert not rr.ingredient_supported_by_source(
        "Buparvaquone 5% w/v Solution for Injection", "nicardipine"
    )


def test_guard_empty_inputs():
    assert not rr.ingredient_supported_by_source("", "aspirin")
    assert not rr.ingredient_supported_by_source("aspirin 500mg", "")


# --- rxnav_resolve caching (mocked network) ---------------------------------


def test_rxnav_resolve_keeps_only_guarded_ingredients(monkeypatch, tmp_path):
    calls = []

    def fake_best(term):
        calls.append(term)
        return ("999", 20.0)

    def fake_ings(rxcui):
        return ["ephedrine", "menthol"]  # menthol is a spurious approximate match

    monkeypatch.setattr(rr, "_approximate_best_rxcui", fake_best)
    monkeypatch.setattr(rr, "_ingredient_names", fake_ings)

    cache = EnrichmentCache(tmp_path / "rx.json")
    out = rr.rxnav_resolve("Ephedrine Sulphide 75mg Capsule", cache)

    assert out["kept"] == ["ephedrine"]  # menthol dropped by the guard
    assert out["ingredients"] == ["ephedrine", "menthol"]
    assert len(calls) == 1


def test_rxnav_resolve_is_cached(monkeypatch, tmp_path):
    calls = []

    def fake_best(term):
        calls.append(term)
        return ("1", 30.0)

    monkeypatch.setattr(rr, "_approximate_best_rxcui", fake_best)
    monkeypatch.setattr(rr, "_ingredient_names", lambda r: ["aspirin"])

    cache = EnrichmentCache(tmp_path / "rx.json")
    rr.rxnav_resolve("aspirin 500mg tablet", cache)
    rr.rxnav_resolve("aspirin 500mg tablet", cache)  # served from cache

    assert len(calls) == 1  # network hit only once


def test_rxnav_resolve_network_error_cached_empty(monkeypatch, tmp_path):
    def boom(term):
        raise RuntimeError("network down")

    monkeypatch.setattr(rr, "_approximate_best_rxcui", boom)
    cache = EnrichmentCache(tmp_path / "rx.json")
    out = rr.rxnav_resolve("some drug", cache)
    assert out["kept"] == []
    assert "error" in out


# --- resolve_residue: end-to-end proposal writing (mocked network) ----------


def _grounder(mapping):
    """Fake ground_drug: returns [GroundingResult-like] for names in mapping."""

    def ground(name):
        hit = mapping.get(name.lower())
        if hit is None:
            return []
        return [SimpleNamespace(id=hit[0], label=hit[1], score=1.0)]

    return ground


def test_resolve_residue_proposes_rows_into_store(monkeypatch, tmp_path):
    # Two residue strings: one resolves cleanly, one is a spurious RxNav match.
    def fake_resolve(term, cache):
        data = {
            "Ephedrine Sulphide 75mg Capsule": {
                "rxcui": "372021", "score": 10.4,
                "ingredients": ["ephedrine"], "kept": ["ephedrine"],
            },
            "Sheep Pox Vaccine": {
                "rxcui": "993827", "score": 12.7,
                "ingredients": ["menthol"], "kept": [],  # guard already dropped it
            },
        }
        return data[term]

    monkeypatch.setattr(rr, "rxnav_resolve", fake_resolve)

    store_path = tmp_path / "drug_grounding.sssom.tsv"
    from medic.grounding.store import LiteralMappingStore

    store = LiteralMappingStore(str(store_path), "drugs")
    store.load()

    ground = _grounder({"ephedrine": ("CHEBI:15407", "ephedrine")})

    report = rr.resolve_residue(
        ["Ephedrine Sulphide 75mg Capsule", "Sheep Pox Vaccine"],
        ground,
        store=store,
        cache=EnrichmentCache(tmp_path / "rx.json"),
        rate_limit_s=0.0,
    )

    assert report["residue"] == 2
    assert report["rxnav_ingredient_hits"] == 1
    assert report["proposed"] == 1
    assert report["proposed_all_chebi"] == 1

    rows = store.lookup("Ephedrine Sulphide 75mg Capsule")
    assert len(rows) == 1
    row = rows[0]
    assert row.object_id == "CHEBI:15407"
    assert row.mapping_justification == rr.RXNORM_JUSTIFICATION
    assert row.subject_preprocessing == [rr.RXNORM_PREPROCESS]
    assert row.predicate_id == "skos:closeMatch"
    # spurious match left the store untouched
    assert store.lookup("Sheep Pox Vaccine") == []


def test_resolve_residue_skips_partial_combinations(monkeypatch, tmp_path):
    # A two-ingredient product where only one ingredient re-grounds -> no proposal.
    def fake_resolve(term, cache):
        return {"rxcui": "1", "score": 20.0,
                "ingredients": ["drugA", "drugB"], "kept": ["drugA", "drugB"]}

    monkeypatch.setattr(rr, "rxnav_resolve", fake_resolve)

    from medic.grounding.store import LiteralMappingStore

    store = LiteralMappingStore(str(tmp_path / "s.tsv"), "drugs")
    store.load()
    ground = _grounder({"druga": ("CHEBI:1", "drugA")})  # drugB missing

    report = rr.resolve_residue(
        ["drugA and drugB tablet"], ground, store=store,
        cache=EnrichmentCache(tmp_path / "rx.json"), rate_limit_s=0.0,
    )
    assert report["proposed"] == 0
    assert store.lookup("drugA and drugB tablet") == []


def test_enrich_honours_skip_flag(monkeypatch):
    monkeypatch.setenv("MEDIC_SKIP_EXPENSIVE_CALLS", "1")
    report = rr.enrich_rxnorm_resolve(["anything"], _grounder({}))
    assert report["skipped"] is True
    assert report["proposed"] == 0
