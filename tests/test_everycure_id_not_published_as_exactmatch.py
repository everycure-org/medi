"""EveryCure's own pre-grounded id must never become a published `skos:exactMatch`.

`ingest/everycure_drugs` states plainly that MeDIC trusts EveryCure's *label* but not its
id — "UNII biologics fragment from the CHEBI ids the regulatory sources ground to" — and so
grounds the name instead. It then folded that same untrusted id into `alternate_ids`, and
`export/sssom.py` turns every `alternate_ids` entry into `skos:exactMatch`, the strongest
identity assertion SSSOM has.

The result was 569 published identity claims MeDIC had already decided not to believe,
including 119 CHEBI->CHEBI rows where both sides are real but different molecules:

    CHEBI:749610 ofatumumab      skos:exactMatch  CHEBI:28887  dimethyl ether
    CHEBI:749606 romiplostim     skos:exactMatch  CHEBI:91083  semaxanib
    CHEBI:757435 velmanase alfa  skos:exactMatch  CHEBI:29105  zinc(2+)

An anti-CD20 monoclonal antibody declared identical to a solvent. This is the one finding in
the review that publishes *false machine-readable claims* rather than a policy mismatch, and a
wrong `exactMatch` merges two entities irreversibly in a downstream KG.

Note this is deliberately narrow: a CHEBI->CHEBI row is not wrong in general. The
pre-normalization id is a legitimate alternate — same entity, the id Stage 1 rested on before
Stage 2 canonicalised it. Only the EveryCure id is excluded, and only because the record
itself says it is untrusted. It stays on the record as `everycure_id`, which is what "kept as
provenance" means.
"""

from __future__ import annotations

from medic.merge.drug_merge import _merge_group


def _record(**kw) -> dict:
    base = {
        "source": "EVERYCURE",
        "source_name": "Ofatumumab",
        "original_literal": "Ofatumumab",
        "everycure_id": "CHEBI:28887",
        "normalized_id": "CHEBI:749610",
        "normalized_label": "ofatumumab",
        "alternate_ids": ["CHEBI:28887", "DRUGBANK:DB06650"],
    }
    return {**base, **kw}


def _alternates(rec: dict) -> list[str]:
    merged = _merge_group("CHEBI:749610", [rec])
    return list(merged.get("alternate_ids") or [])


def test_the_untrusted_everycure_id_is_not_an_alternate():
    assert "CHEBI:28887" not in _alternates(_record())


def test_a_genuine_cross_reference_still_survives():
    """The filter is targeted, not a blanket drop of EveryCure provenance."""
    assert "DRUGBANK:DB06650" in _alternates(_record())


def test_a_different_chebi_that_is_not_the_everycure_id_is_kept():
    """A pre-normalization CHEBI id is a real alternate for the same entity."""
    rec = _record(alternate_ids=["CHEBI:28887", "CHEBI:999999"])
    alts = _alternates(rec)
    assert "CHEBI:999999" in alts
    assert "CHEBI:28887" not in alts


def test_a_record_with_no_everycure_id_is_unaffected():
    rec = _record(everycure_id="", alternate_ids=["CHEBI:28887"])
    assert "CHEBI:28887" in _alternates(rec)


def test_the_ingester_no_longer_folds_the_id_in_at_source():
    """Belt and braces: the read-side filter exists because kb/ was already written with the
    id folded in, but new ingest runs must not reintroduce it."""
    import inspect

    from medic.ingest.everycure_drugs import __main__ as ec

    src = inspect.getsource(ec)
    marker = "provenance_ids: list[str] = []"
    tail = src.split(marker, 1)[1].split("record: dict", 1)[0]
    assert "provenance_ids.append(everycure_id)" not in tail
