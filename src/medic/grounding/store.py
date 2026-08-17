"""Grounding decision store — SSSOM literal-mappings profile.

One hand-editable TSV per entity type under ``mappings/``. Rows are grouped by the
base-normalized subject literal; a literal may have several rows (combination drugs map
one string to several ids). Rows tagged ``semapv:ManualMappingCuration`` are the
authoritative cache: if a subject has any manual row, its whole row-set is preserved on
regeneration and wins over auto (``semapv:LexicalMatching``) rows. Unresolved decisions
are written too — as ``object_id: sssom:NoTermFound`` against a real predicate — so the
store is a complete audit that a standard SSSOM reader can still read.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field

from medic.grounding.lexical.preprocess import base_normalize

MANUAL = "semapv:ManualMappingCuration"
LEXICAL = "semapv:LexicalMatching"
NO_TERM = "sssom:NoTermFound"
TOOL = "medic-lexical-grounder"

# RxNorm substance-level resolver proposals (Phase 4 formulation grounding). These are
# network-derived, curator-reviewable proposals written by
# ``medic.enrichment.rxnorm_resolve``. They are *locked* like manual rows so an offline
# regrounding run (a) reads them deterministically instead of re-emitting NoTermFound and
# (b) does not overwrite them — but they remain distinct from ``MANUAL`` so a curator can
# still tell an auto-proposal from a hand-curated decision.
RXNORM = "RXNORM"

#: What an RxNorm proposal's ``mapping_justification`` is actually written as.
#:
#: It used to be the bare string ``RXNORM``, which is not a ``semapv:`` term and therefore not
#: a legal ``mapping_justification`` — ``sssom.validate`` raised 460 errors on it, all
#: "'RXNORM' does not match '^semapv:...'". The information was never lost: every one of these
#: rows already carries ``rxnorm_resolve`` in ``subject_preprocessing``, which is where a
#: MeDIC-specific marker belongs. An SSSOM enum slot is not the place to keep a local flag.
UNSPECIFIED = "semapv:UnspecifiedMatching"

#: ``subject_preprocessing`` entry that marks a row as an RxNorm resolver proposal.
RXNORM_RULE = "rxnorm_resolve"

#: The predicate written on an unresolved row. SSSOM expresses "we looked and found nothing"
#: as ``object_id: sssom:NoTermFound`` against a real predicate — the predicate says what was
#: sought, the object says it was not found. Writing ``NoTermFound`` into ``predicate_id`` with
#: an empty ``object_id`` (what this store used to do) is malformed, and every SSSOM reader
#: drops the row: ``parse_sssom_table`` took 21,186 rows of the drug store and returned 11,301.
#: The 12,444 unresolved decisions are exactly the rows I-4 exists to expose to a curator.
UNRESOLVED_PREDICATE = "skos:exactMatch"


def is_locked(d: "GroundingDecision") -> bool:
    """Does a curated/proposed source own this row?

    Locked rows survive regeneration and short-circuit the live matcher. Keyed on the
    preprocessing rule rather than the justification, because the justification now has to be
    a legal ``semapv:`` term and can no longer carry a MeDIC-specific marker.
    """
    return (d.mapping_justification == MANUAL
            or RXNORM_RULE in (d.subject_preprocessing or ()))

COLUMNS = [
    "subject_type", "subject_label", "subject_id", "predicate_id",
    "object_id", "object_label", "object_match_field", "mapping_justification",
    "subject_preprocessing", "match_string", "confidence", "mapping_tool",
]


def _key(s: str) -> str:
    return base_normalize(s)


@dataclass
class GroundingDecision:
    subject_label: str
    entity_type: str
    predicate_id: str
    object_id: str | None
    object_label: str | None
    object_match_field: str | None
    mapping_justification: str
    subject_preprocessing: list[str] = field(default_factory=list)
    match_string: str | None = None
    confidence: float | None = None
    # MEDICNE id of the source mention (I-9). Fills the SSSOM ``subject_id`` column,
    # turning the literal profile into an id-anchored table (the mention id is the
    # single identifier of the original literal, carried from extraction).
    subject_id: str | None = None

    @property
    def grounding_quality(self) -> str:
        from medic.grounding.lexical.matcher import quality_of
        return quality_of(self)


def _license_header() -> list[str]:
    """The licence lines for a decision store.

    Declared **CC BY 4.0, not CC0**, for the same reason as the SSSOM export: this file
    carries ``subject_label`` and ``match_string``, which reproduce verbatim source strings
    (``\"Golden Star\" Balm``, EMA and PMDA label text). MeDIC cannot waive rights it never
    held, and a blanket CC0 tells a consumer attribution is optional when EMA and PMDA both
    require it. The mapping decisions themselves — MeDIC's actual contribution — are still
    offered without conditions, and the comment says so.
    """
    from medic import release_assets

    license_url = "https://creativecommons.org/licenses/by/4.0/"
    assertions_url = "https://creativecommons.org/publicdomain/zero/1.0/"
    passthrough = ""
    try:
        lic = release_assets.load().license
        if lic:
            license_url = lic.medic_contribution or license_url
            assertions_url = lic.medic_assertions_offered_as or assertions_url
            passthrough = lic.passthrough
    except (OSError, ValueError):  # never fail a grounding run over the manifest
        pass

    comment = (
        f"The grounding decisions in this file are MeDIC's own contribution and are offered "
        f"as {assertions_url}. The set is declared {license_url} because subject_label and "
        f"match_string reproduce verbatim source strings. {passthrough}"
    ).strip()
    return [
        f"# license: {license_url}\n",
        f"# comment: {comment}\n",
    ]


class LiteralMappingStore:
    def __init__(self, path: str, entity_type: str):
        self.path = path
        self.entity_type = entity_type
        self._rows: dict[str, list[GroundingDecision]] = {}

    def load(self) -> None:
        self._rows.clear()
        if not os.path.exists(self.path):
            return
        with open(self.path, newline="") as fh:
            reader = csv.DictReader((ln for ln in fh if not ln.startswith("#")), delimiter="\t")
            for r in reader:
                object_id = r["object_id"] or None
                predicate_id = r["predicate_id"]
                # `sssom:NoTermFound` in `object_id` is the on-disk spelling of "unresolved".
                # In memory that stays `object_id=None` + `predicate_id=NO_TERM`, so every
                # consumer of a decision is unchanged by the file-format fix. The second arm
                # reads stores written before it.
                if object_id == NO_TERM or predicate_id == NO_TERM:
                    object_id, predicate_id = None, NO_TERM
                justification = r["mapping_justification"]
                preprocessing = [s for s in r["subject_preprocessing"].split("|") if s]
                # Back-compat: rows written when the RxNorm marker lived in the justification.
                if justification == RXNORM:
                    justification = UNSPECIFIED
                    if RXNORM_RULE not in preprocessing:
                        preprocessing.append(RXNORM_RULE)
                d = GroundingDecision(
                    subject_label=r["subject_label"], entity_type=self.entity_type,
                    predicate_id=predicate_id, object_id=object_id,
                    object_label=r["object_label"] or None,
                    object_match_field=r["object_match_field"] or None,
                    mapping_justification=justification,
                    subject_preprocessing=preprocessing,
                    match_string=r["match_string"] or None,
                    confidence=float(r["confidence"]) if r["confidence"] else None,
                    subject_id=r.get("subject_id") or None,
                )
                self._rows.setdefault(_key(d.subject_label), []).append(d)

    def lookup(self, subject_label: str) -> list[GroundingDecision]:
        """All decision rows for a subject (empty list if none)."""
        return self._rows.get(_key(subject_label), [])

    def all_rows(self) -> list[GroundingDecision]:
        """Every decision row in the store, flattened (order not significant)."""
        return [d for rows in self._rows.values() for d in rows]

    def manual_rows(self, subject_label: str) -> list[GroundingDecision]:
        return [d for d in self.lookup(subject_label) if d.mapping_justification == MANUAL]

    def locked_rows(self, subject_label: str) -> list[GroundingDecision]:
        """Rows a curated/proposed source owns for a subject (manual + RxNorm proposals).

        The live matcher short-circuits on these so offline runs read them deterministically
        instead of re-grounding; ``record_subject`` refuses to overwrite them.
        """
        return [d for d in self.lookup(subject_label) if is_locked(d)]

    def record_subject(
        self, subject_label: str, decisions: list[GroundingDecision],
        subject_id: str | None = None,
    ) -> None:
        """Replace the auto row-set for a subject.

        A subject that already carries any *locked* row (manual curation or an RxNorm
        proposal) is owned by that source and is never overwritten by auto rows.
        ``subject_id`` is the mention's MEDICNE id (I-9); it is stamped onto every
        decision row so the store is anchored on the original literal's id.

        **A caller that does not know the mention id cannot erase one.** When ``subject_id``
        is absent the id already on file is carried forward, because the store is keyed by
        literal and the last writer wins: DailyMed grounds the same drug names as the
        drug-list ingesters but through ``ground_drug_best``, which supplies no id, so a
        plain replacement blanked 1,285 rows that ``build-drug-list`` had just stamped —
        visible only on a full build, where both run. I-9 makes that id the anchor the whole
        transformation chain joins on; losing it leaves the literal as the only join key,
        which is the fragile thing mention ids replaced.
        """
        k = _key(subject_label)
        existing = self._rows.get(k)
        if existing and any(is_locked(d) for d in existing):
            return  # curator / proposer owns this subject
        inherited = next((d.subject_id for d in existing or [] if d.subject_id), "")
        for d in decisions:
            if subject_id:
                d.subject_id = subject_id
            elif inherited and not d.subject_id:
                d.subject_id = inherited
        self._rows[k] = list(decisions)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "w", newline="") as fh:
            fh.write(f"# mapping_set_id: https://w3id.org/medic/mappings/{self.entity_type}_grounding\n")
            fh.write(f"# mapping_tool: {TOOL}\n")
            for line in _license_header():
                fh.write(line)
            writer = csv.DictWriter(fh, fieldnames=COLUMNS, delimiter="\t")
            writer.writeheader()
            for k in sorted(self._rows):
                for d in sorted(self._rows[k], key=lambda x: x.object_id or ""):
                    unresolved = d.object_id is None or d.predicate_id == NO_TERM
                    writer.writerow({
                        "subject_type": "rdfs literal", "subject_label": d.subject_label,
                        "subject_id": d.subject_id or "",
                        "predicate_id": UNRESOLVED_PREDICATE if unresolved else d.predicate_id,
                        "object_id": NO_TERM if unresolved else (d.object_id or ""),
                        "object_label": d.object_label or "",
                        "object_match_field": d.object_match_field or "",
                        "mapping_justification": d.mapping_justification,
                        "subject_preprocessing": "|".join(d.subject_preprocessing),
                        "match_string": d.match_string or "",
                        "confidence": "" if d.confidence is None else f"{d.confidence:.4f}",
                        "mapping_tool": TOOL,
                    })
