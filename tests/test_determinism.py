"""Reruns must be byte-identical (SPEC §I, "deterministic and offline").

The expensive form of this is building twice and hashing the products — that is `just
determinism`, which takes minutes. These tests cover the cheap half: the places where
non-determinism would actually get in, which is anywhere the product's *order* is decided by
something other than the data.

The specific risk is real. `_finalize_pair` sorts assertions explicitly because otherwise the
order kb/ files happen to be walked in would leak into the product, and two machines walking
`kb/indications/` in different order would produce different bytes from identical inputs.
"""

import hashlib

import yaml

from medic.merge.on_label_merge import _append_assertion, _finalize_pair


def _assertion(source, document, overall=0.5):
    return {"source": source, "document": document,
            "assertion": {"confidence": {"subject": 1.0, "object": overall,
                                         "relationship": 1.0, "overall": overall,
                                         "basis": "MEASURED"}}}


def _pair(rows):
    p = {"drug_id": "CHEBI:1", "disease_id": "MONDO:1",
         "relationship_type": "INDICATION", "assertions": []}
    for src, doc in rows:
        _append_assertion(p, _assertion(src, doc))
    _finalize_pair(p)
    return p


ROWS = [("PMDA", "PMDA:z#1-20200101"), ("DAILYMED", "DailyMed:aaa"),
        ("EMA", "EMA:keppra"), ("DAILYMED", "DailyMed:bbb")]


def test_assertion_order_does_not_depend_on_insertion_order():
    a = _pair(ROWS)
    b = _pair(list(reversed(ROWS)))
    assert [(x["source"], x["document"]) for x in a["assertions"]] == \
           [(x["source"], x["document"]) for x in b["assertions"]]


def test_a_pair_serialises_to_identical_bytes_regardless_of_input_order():
    """The property that actually matters: same inputs, same file."""
    def dump(p):
        return hashlib.sha256(
            yaml.dump(p, default_flow_style=False, allow_unicode=True,
                      sort_keys=True).encode()).hexdigest()

    assert dump(_pair(ROWS)) == dump(_pair(list(reversed(ROWS))))


def test_the_aggregate_does_not_depend_on_order_either():
    a, b = _pair(ROWS), _pair(list(reversed(ROWS)))
    assert a["confidence"] == b["confidence"]


def test_mention_ids_are_stable_across_runs():
    """MEDICNE ids are uuid5 of the surface form — no counter, no randomness (I-9)."""
    from medic.mention import mint_mention_id

    assert mint_mention_id("Абакавир", "drugs") == mint_mention_id(" абакавир ", "drugs")
    assert mint_mention_id("ALBUTEROL", "drugs") == \
        "MEDICNE:" + mint_mention_id("ALBUTEROL", "drugs").split(":", 1)[1]


def test_document_ids_are_derived_from_data_not_iteration():
    from medic.merge.on_label_merge import _document_for

    rec = {"source": "DAILYMED", "set_id": "abc"}
    assert _document_for(rec, {}) == _document_for(dict(rec), {})


def test_confidence_priors_are_read_in_a_stable_order():
    """A dict-ordering change here would silently reorder minted priors in the config."""
    from medic.confidence import index_priors, load_priors

    a, b = index_priors(load_priors()), index_priors(load_priors())
    assert a == b
    assert list(a) == list(b)
