"""Read-side view over a Stage-1 grounding SSSOM decision store.

The git-tracked stores under ``mappings/`` are the authoritative record of every string→ID
decision (invariant I-4). The product's ``GroundingStep`` is a *view* of that record — so when
a source record reaches the merge without its structured ``grounding`` object (older ingest
runs, sources that only carried a final id), the decision is still recoverable here rather
than lost.

Reading a git-tracked TSV keeps the merge offline and deterministic (I-2). Both
``merge/drug_merge.py`` and ``merge/on_label_merge.py`` use this so the two sides cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from medic.grounding.lexical.preprocess import base_normalize
from medic.grounding.store import LiteralMappingStore

#: Predicates that indicate the match broadened the claim (a curator-reviewable event).
_BROADENING_PREDICATES = frozenset({"skos:broadMatch"})

#: Preprocessing rule -> the GroundingFlag it implies.
RULE_FLAGS = {
    "fuzzy_edit1_unique": "fuzzy",
    "formulation_strip": "formulation_stripped",
    "cyrillic_transliteration": "script_transliteration",
    "rxnorm_resolve": "rxnorm_proposed",
    "qualifier_strip": "broadened",
    "strip_leading_other": "broadened",
}


@dataclass(frozen=True)
class GroundingDecision:
    """One recorded string→ID decision, as the store holds it."""

    subject_id: str = ""
    subject_label: str = ""
    object_id: str = ""
    object_label: str = ""
    predicate_id: str = ""
    confidence: float | None = None
    applied_rules: tuple[str, ...] = field(default_factory=tuple)
    #: The string the index actually matched. Dropping this column is what made `quality()`
    #: unable to tell `lexical_exact` from `lexical_exact_normalized` — see `quality()`.
    match_string: str = ""
    mapping_justification: str = ""

    @property
    def subject_preprocessing(self) -> tuple[str, ...]:
        """Alias so `matcher.quality_of` can read this view's decision unchanged."""
        return self.applied_rules

    def flags(self) -> list[str]:
        """GroundingFlags implied by this decision's rules and predicate."""
        out: list[str] = []
        for rule in self.applied_rules:
            flag = RULE_FLAGS.get(rule)
            if flag and flag not in out:
                out.append(flag)
        if self.predicate_id in _BROADENING_PREDICATES and "broadened" not in out:
            out.append("broadened")
        return out

    def quality(self) -> str:
        """`GroundingQualityEnum` value for this decision.

        Delegates to `matcher.quality_of`, the same function the grounder itself uses, so a
        decision read back from the store is classified exactly as it was when made.

        It used to re-derive the value from `applied_rules` alone, because this view dropped
        the store's `match_string` column. That cannot distinguish `lexical_exact` ("the
        string was unchanged") from `lexical_exact_normalized` ("it matched only after
        normalization") — the distinction *is* a comparison of `match_string` against the
        trimmed subject. With no rules recorded it answered `lexical_exact`, so 10,656 drug
        steps in the on-label products asserted no transform had happened when one had:
        `VORICONAZOLE` matched `voriconazole` and published as "string unchanged". The
        on-label products reported zero `lexical_exact_normalized` for drugs while
        `drug_list.yaml`, which goes through `quality_of`, reported 2,200.
        """
        from medic.grounding.lexical.matcher import quality_of
        return quality_of(self)

    def as_grounding(self, *, original_string: str = "") -> dict:
        """Render as the legacy ``grounding`` object shape ``build_mention`` consumes."""
        return {
            "subject_id": self.subject_id,
            "original_string": original_string or self.subject_label,
            "grounded_id": self.object_id,
            "grounded_label": self.object_label,
            "grounding_quality": self.quality(),
            "confidence": self.confidence,
        }


class GroundingStoreView:
    """Lookup over a grounding store, by mention id or by source literal."""

    def __init__(self, path: str, entity_type: str):
        self.path = path
        self.entity_type = entity_type
        self._by_subject: dict[str, GroundingDecision] = {}
        self._by_label: dict[str, GroundingDecision] = {}
        self._label_by_object: dict[str, str] = {}

    def load(self) -> GroundingStoreView:
        try:
            store = LiteralMappingStore(self.path, self.entity_type)
            store.load()
            rows = store.all_rows()
        except Exception:
            return self
        for d in rows:
            if not getattr(d, "object_id", ""):
                continue  # unresolved (sssom:NoTermFound) — no decision to funnel
            try:
                conf = float(d.confidence) if d.confidence is not None else None
            except (TypeError, ValueError):
                conf = None
            dec = GroundingDecision(
                subject_id=d.subject_id or "",
                subject_label=d.subject_label or "",
                object_id=d.object_id,
                object_label=getattr(d, "object_label", "") or "",
                predicate_id=getattr(d, "predicate_id", "") or "",
                confidence=conf,
                applied_rules=tuple(d.subject_preprocessing or ()),
                # Both are needed by `quality_of`: without `mapping_justification` every
                # decision reads as `curated`, and without `match_string` an exact hit cannot
                # be told from one that only matched after normalization.
                match_string=getattr(d, "match_string", "") or "",
                mapping_justification=getattr(d, "mapping_justification", "") or "",
            )
            if dec.subject_id:
                self._by_subject.setdefault(dec.subject_id, dec)
            if dec.subject_label:
                self._by_label.setdefault(base_normalize(dec.subject_label), dec)
            self._label_by_object.setdefault(dec.object_id, dec.object_label)
        return self

    def __len__(self) -> int:
        return len(self._by_subject)

    def decision_for(
        self, *, mention_id: str = "", literal: str = "", object_id: str = "",
    ) -> GroundingDecision | None:
        """The recorded decision for a mention — by id, else by (normalized) literal.

        When ``object_id`` is given, a candidate whose ``object_id`` disagrees is rejected, so
        a stale store row can never silently re-point a record at a different entity.
        """
        for dec in (self._by_subject.get(mention_id) if mention_id else None,
                    self._by_label.get(base_normalize(literal)) if literal else None):
            if dec and (not object_id or dec.object_id == object_id):
                return dec
        return None

    def label_for(self, object_id: str) -> str | None:
        """The label the store publishes for ``object_id``; ``""`` when it publishes none.

        The empty string and ``None`` mean different things and callers depend on the
        difference. ``""`` is a decision: every atom naming this concept came from a
        vocabulary I-14 rule 2 forbids publishing, so it ships unnamed. ``None`` means the
        store has never seen the id and has no opinion. Collapsing the two is how restricted
        term text leaks — a caller that treats "unnamed" as "unknown" falls back to whatever
        label the ingest run happened to attach, which is the restricted string.
        """
        return self._label_by_object.get(object_id)

    def applied_rules_for(
        self, *, mention_id: str = "", literal: str = "", object_id: str = "",
    ) -> list[str]:
        dec = self.decision_for(mention_id=mention_id, literal=literal, object_id=object_id)
        return list(dec.applied_rules) if dec else []
