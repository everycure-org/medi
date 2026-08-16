"""Two-stage resolution orchestration: attach Grounding + Normalization to records.

This is the reusable helper the ingesters will call (wiring into each ingester is a
separate, reviewed change). It preserves the source string verbatim (invariant I-7) and
delegates all decision persistence to the SSSOM stores owned by the backend/normalizer.
"""

from __future__ import annotations

from collections.abc import Iterable

from medic.grounding.lexical_backend import LexicalCascadeGrounding
from medic.normalization.normalizer import Normalizer


def attach_grounding(
    records: Iterable[dict],
    label_key: str,
    backend: LexicalCascadeGrounding,
    normalizer: Normalizer | None,
    entity_type: str = "diseases",
) -> list[dict]:
    """Attach ``grounding`` and ``normalization`` dicts to each record in place.

    ``label_key`` is the field holding the verbatim source string. The record's
    ``grounding.original_string`` is copied from it unchanged.
    """
    ground = backend.ground_disease if entity_type == "diseases" else backend.ground_drug
    out = []
    for rec in records:
        raw = rec.get(label_key) or ""
        results = ground(raw)
        decisions = backend.last_decision(entity_type, raw)
        resolved = [d for d in decisions if d.object_id is not None]
        primary = resolved[0] if resolved else (decisions[0] if decisions else None)
        gid = results[0].id if results else None
        glabel = results[0].label if results else None
        score = results[0].score if results else 0.0
        rec["grounding"] = {
            "original_string": raw,
            "grounded_id": gid,
            "grounded_label": glabel,
            "grounding_quality": primary.grounding_quality if primary else "unresolved",
            "confidence": score,
        }
        if len(resolved) > 1:  # combination: one literal -> several ids
            rec["grounding"]["components"] = [d.object_id for d in resolved]
        if gid and normalizer is not None:
            nd = normalizer.normalize(gid, glabel)
            rec["normalization"] = {
                "original_id": gid,
                "normalized_id": nd.object_id,
                "normalized_label": nd.object_label,
                "normalization_quality": nd.normalization_quality,
                "tool": nd.tool,
            }
        out.append(rec)
    backend.flush()
    if normalizer is not None:
        normalizer.store.save()
    return out
