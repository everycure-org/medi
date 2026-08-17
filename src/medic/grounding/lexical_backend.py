"""Deterministic lexical grounding backend (Stage 1), behind the GroundingService interface."""

from __future__ import annotations

import os

from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.lexical.build import open_index
from medic.grounding.lexical.matcher import Matcher
from medic.grounding.store import LiteralMappingStore


class LexicalCascadeGrounding(GroundingService):
    def __init__(self, disease_db: str, drug_db: str | None = None, store_dir: str = "mappings",
                 translation_conf: str = "conf/grounding_translation.yaml"):
        self._matchers = {}
        self._stores = {}
        translation = {}
        if os.path.exists(translation_conf):
            from medic.grounding.lexical.preprocess import load_translation_dict
            translation = load_translation_dict(translation_conf)
        stem = {"diseases": "disease", "drugs": "drug"}
        for etype, db in (("diseases", disease_db), ("drugs", drug_db)):
            if not db:
                continue  # entity type intentionally not wired (e.g. drug-only run)
            if not os.path.exists(db):
                raise FileNotFoundError(
                    f"Lexical grounding index for {etype} not found at {db!r}. "
                    f"Build it with `just build-grounding-index` (or `just setup-grounding`)."
                )
            path = os.path.join(store_dir, f"{stem[etype]}_grounding.sssom.tsv")
            store = LiteralMappingStore(path, etype)
            store.load()
            self._stores[etype] = store
            self._matchers[etype] = Matcher(open_index(etype, db), store, translation)

    def _ground(self, etype: str, name: str, mention_id: str | None = None) -> list[GroundingResult]:
        matcher = self._matchers.get(etype)
        if matcher is None:
            return []
        decisions = matcher.ground(name)
        self._stores[etype].record_subject(name, decisions, subject_id=mention_id)
        results = [GroundingResult(id=d.object_id, label=d.object_label or "",
                                   score=d.confidence or 0.0, source_name=name, service="lexical")
                   for d in decisions if d.object_id is not None]
        return sorted(results, key=lambda r: r.score, reverse=True)

    def ground_disease(self, name: str, limit: int = 5,
                       mention_id: str | None = None) -> list[GroundingResult]:
        return self._ground("diseases", name, mention_id)

    def ground_drug(self, name: str, limit: int = 5,
                    mention_id: str | None = None) -> list[GroundingResult]:
        return self._ground("drugs", name, mention_id)

    def normalize(self, curie: str) -> GroundingResult | None:
        return None  # Stage-2 normalization is a separate service

    def last_decision(self, etype: str, name: str):
        """The recorded decision rows for a subject (list; >1 for combinations)."""
        return self._stores[etype].lookup(name)

    def flush(self) -> None:
        for store in self._stores.values():
            store.save()
