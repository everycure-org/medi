"""Stage-2 normalizer: grounded id -> canonical target id (target-namespace-supported only)."""

from __future__ import annotations

from medic.normalization.store import NormalizationDecision, NormalizationMappingStore

AUTO = "semapv:UnspecifiedMatching"


class Normalizer:
    def __init__(self, entity_type: str, mapping_index: dict, store: NormalizationMappingStore,
                 target_prefix: str, tool: str):
        self.entity_type = entity_type
        self.mi = mapping_index
        self.store = store
        self.target_prefix = target_prefix
        self.tool = tool

    def normalize(self, grounded_id: str, grounded_label: str | None) -> NormalizationDecision:
        cur = self.store.lookup(grounded_id)
        if cur is not None and cur.mapping_justification != AUTO:
            return cur
        mapped = self.mi.get(grounded_id)
        if mapped:
            target_id, quality, predicate = mapped
            d = NormalizationDecision(grounded_id, predicate, target_id, None, quality, AUTO, self.tool)
        else:
            d = NormalizationDecision(grounded_id, "skos:exactMatch", grounded_id,
                                      grounded_label, "identity", AUTO, self.tool)
        self.store.record(d)
        return d
