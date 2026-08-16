"""Invariant I-1: a source may only speak for its own jurisdiction.

The rule is stated in `SPEC.md` §4 and `docs/source-isolation.md`, and it is the reason the
redesign exists — a Russian drug string once ended up on an Indian indication. This module is
the single machine-readable statement of it, so the merge, the QC report and the KGX export
all check the same thing against the same table instead of three drifting copies.

Enforcement lives in two places, deliberately:

* at the **export boundary** (`medic.export.kgx.validate`), which is the last chance before a
  consumer sees an edge, and
* over the **products** (`scripts/build_qc.py`), which catches a breach that never reaches the
  export at all.

Neither is redundant. A DailyMed row relabelled `jurisdiction: EU` passes `linkml-validate`
and every enum check, because `EU` is a perfectly legal `JurisdictionEnum` value — the error
is in the *combination*, which only a cross-check can see.
"""

from __future__ import annotations

#: The jurisdiction each ingester is allowed to speak for.
#:
#: Keyed by the value written to a record's ``source`` — the ingester name — and, where they
#: differ, by the regulatory authority as well. Keying only one of the two is how the KGX
#: gate came to exempt every India edge: the map said ``CDSCO`` while the exporter wrote
#: ``INDIA``, and an unrecognised source silently skipped the check.
SOURCE_JURISDICTION: dict[str, str] = {
    "DAILYMED": "USA",
    "ORANGEBOOK": "USA",
    "PURPLEBOOK": "USA",
    "PVLENS": "USA",
    "FAERS": "USA",
    "FDA": "USA",
    "EMA": "EU",
    "EMA_EPAR": "EU",
    "PMDA": "JAPAN",
    "INDIA": "INDIA",
    "CDSCO": "INDIA",
    "RUSSIA": "RUSSIA",
    "GRLS": "RUSSIA",
    "MOH_RUSSIA": "RUSSIA",
    "CHINA": "CHINA",
    "CDE_CHINA": "CHINA",
    "NMPA_CHINA": "CHINA",
}

#: Sources that legitimately speak for no single jurisdiction — literature and curated
#: research are not a regulator's word. Listed rather than defaulted, so an unrecognised
#: source is still an error.
JURISDICTION_FREE: frozenset[str] = frozenset({"PUBMED", "CUREID", "MATRIX", "EVERYCURE"})


def expected_jurisdiction(source: str) -> str | None:
    """The jurisdiction ``source`` may speak for, or ``None`` if it speaks for none."""
    return SOURCE_JURISDICTION.get((source or "").upper())


def is_known(source: str) -> bool:
    """Whether the source is one MeDIC recognises at all.

    Callers must treat an unknown source as an error rather than skipping it. "I do not
    recognise this source" is not "there is nothing to check" — that conflation is what let
    132 India edges through a gate that reported success.
    """
    key = (source or "").upper()
    return key in SOURCE_JURISDICTION or key in JURISDICTION_FREE


def violation(source: str, jurisdiction: str) -> str | None:
    """Describe how this (source, jurisdiction) pair breaks I-1, or ``None`` if it holds."""
    key = (source or "").upper()
    if not key:
        return None
    if not is_known(key):
        return f"source '{key}' is not in SOURCE_JURISDICTION, so I-1 cannot be checked for it"
    expected = expected_jurisdiction(key)
    actual = (jurisdiction or "").upper()
    if expected is None or not actual:
        return None
    if actual != expected:
        return f"source '{key}' speaks for {expected} but the record claims '{actual}'"
    return None
