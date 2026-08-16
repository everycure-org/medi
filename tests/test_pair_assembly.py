"""Pair-level assembly: ordering, noisy-OR, dedup (design spec D1, D8, I-13)."""

import pytest

from medic.merge.on_label_merge import _append_assertion, _finalize_pair


def _assertion(source, document, overall):
    return {"source": source, "document": document,
            "assertion": {"confidence": {"subject": 1.0, "object": overall,
                                         "relationship": 1.0, "overall": overall,
                                         "basis": "MEASURED"}}}


def _pair():
    return {"drug_id": "CHEBI:135272", "disease_id": "MONDO:0011918",
            "relationship_type": "INDICATION", "assertions": []}


def test_noisy_or_across_two_sources():
    pair = _pair()
    _append_assertion(pair, _assertion("CDSCO", "d1", 0.72))
    _append_assertion(pair, _assertion("GRLS", "d2", 0.620))
    _finalize_pair(pair)
    assert pair["confidence"]["overall"] == pytest.approx(0.8936, abs=1e-4)
    assert pair["confidence"]["method"] == "NOISY_OR"
    assert pair["confidence"]["n_assertions"] == 2
    assert pair["confidence"]["n_sources"] == 2


def test_assertions_are_ordered_deterministically():
    """Reruns must be byte-identical, so insertion order must not leak into the product."""
    a, b = _pair(), _pair()
    for src, doc in [("GRLS", "z"), ("CDSCO", "a"), ("CDSCO", "b")]:
        _append_assertion(a, _assertion(src, doc, 0.5))
    for src, doc in [("CDSCO", "b"), ("GRLS", "z"), ("CDSCO", "a")]:
        _append_assertion(b, _assertion(src, doc, 0.5))
    _finalize_pair(a)
    _finalize_pair(b)
    assert [(x["source"], x["document"]) for x in a["assertions"]] == \
           [(x["source"], x["document"]) for x in b["assertions"]]
    assert [x["document"] for x in a["assertions"]] == ["a", "b", "z"]


def test_the_same_document_is_never_appended_twice():
    pair = _pair()
    _append_assertion(pair, _assertion("CDSCO", "d1", 0.72))
    _append_assertion(pair, _assertion("CDSCO", "d1", 0.72))
    assert len(pair["assertions"]) == 1


def test_two_documents_from_one_source_both_survive():
    """The ~3,000 attestations the old merge dropped."""
    pair = _pair()
    _append_assertion(pair, _assertion("DAILYMED", "DailyMed:aaa", 0.8))
    _append_assertion(pair, _assertion("DAILYMED", "DailyMed:bbb", 0.8))
    _finalize_pair(pair)
    assert len(pair["assertions"]) == 2


def test_one_source_saying_it_twice_does_not_raise_the_confidence():
    """Two SPLs of one generic are the same sentence twice, not a second opinion.

    Flat noisy-OR read them as corroboration and took 0.8 to 0.96 — and, at the 24
    relabellings of hydrochlorothiazide, to exactly 1.0.
    """
    pair = _pair()
    _append_assertion(pair, _assertion("DAILYMED", "DailyMed:aaa", 0.8))
    _append_assertion(pair, _assertion("DAILYMED", "DailyMed:bbb", 0.8))
    _finalize_pair(pair)
    assert pair["confidence"]["overall"] == pytest.approx(0.8)
    assert pair["confidence"]["n_assertions"] == 2
    assert pair["confidence"]["n_sources"] == 1


def test_a_source_contributes_its_best_attestation():
    """Within a source the cleanest resolution stands; the messier restatements do not drag it."""
    pair = _pair()
    _append_assertion(pair, _assertion("DAILYMED", "DailyMed:aaa", 0.4))
    _append_assertion(pair, _assertion("DAILYMED", "DailyMed:bbb", 0.9))
    _finalize_pair(pair)
    assert pair["confidence"]["overall"] == pytest.approx(0.9)


def test_twenty_four_labels_from_one_source_never_reach_certainty():
    """The regression that motivated the change: 24 DailyMed SPLs -> confidence 1.0."""
    pair = _pair()
    for i in range(24):
        _append_assertion(pair, _assertion("DAILYMED", f"DailyMed:{i}", 0.81))
    _finalize_pair(pair)
    assert pair["confidence"]["overall"] == pytest.approx(0.81)
    assert pair["confidence"]["n_assertions"] == 24
    assert pair["confidence"]["n_sources"] == 1


def test_a_second_regulator_still_corroborates():
    """Grouping by source must not flatten genuine cross-source agreement."""
    pair = _pair()
    for i in range(24):
        _append_assertion(pair, _assertion("DAILYMED", f"DailyMed:{i}", 0.81))
    _append_assertion(pair, _assertion("EMA", "EMA:keppra", 0.81))
    _finalize_pair(pair)
    assert pair["confidence"]["overall"] == pytest.approx(1 - 0.19 * 0.19)
    assert pair["confidence"]["n_sources"] == 2


def test_a_pair_with_one_assertion_keeps_that_confidence():
    pair = _pair()
    _append_assertion(pair, _assertion("EMA", "EMA:keppra", 0.9))
    _finalize_pair(pair)
    assert pair["confidence"]["overall"] == pytest.approx(0.9)


def test_assertions_with_no_confidence_do_not_crash_the_aggregate():
    pair = _pair()
    pair["assertions"].append({"source": "GRLS", "document": "d"})
    _finalize_pair(pair)
    assert pair["confidence"]["n_assertions"] == 1
    assert 0.0 <= pair["confidence"]["overall"] <= 1.0
