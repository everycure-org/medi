"""Normalization decision store — SSSOM term<->term profile.

One hand-editable TSV per entity type under ``mappings/``. Keyed by ``subject_id``
(the grounded id). Manual rows win and are preserved on regeneration, exactly like the
grounding store.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass

from medic.curie_utils import get_prefix
from medic.curie_utils import MEDIC_W3ID_ROOT

MANUAL = "semapv:ManualMappingCuration"
COLUMNS = ["subject_id", "predicate_id", "object_id", "object_label",
           "mapping_justification", "mapping_tool", "comment"]


@dataclass
class NormalizationDecision:
    subject_id: str
    predicate_id: str
    object_id: str
    object_label: str | None
    normalization_quality: str
    mapping_justification: str
    tool: str


def _quality_from_comment(comment: str | None) -> str:
    """Map a store row's comment to a NormalizationQualityEnum value.

    `none` is the pre-2026-08-09 spelling of `identity`. The stores were migrated, but a curator
    on an older branch can reintroduce it, so it is accepted on read and normalized.
    """
    value = (comment or "").strip()
    if not value or value == "none":
        return "identity"
    return value


class NormalizationMappingStore:
    def __init__(self, path: str, entity_type: str):
        self.path = path
        self.entity_type = entity_type
        self._rows: dict[str, NormalizationDecision] = {}

    def load(self) -> None:
        self._rows.clear()
        if not os.path.exists(self.path):
            return
        with open(self.path, newline="") as fh:
            reader = csv.DictReader((ln for ln in fh if not ln.startswith("#")), delimiter="\t")
            for r in reader:
                self._rows[r["subject_id"]] = NormalizationDecision(
                    subject_id=r["subject_id"], predicate_id=r["predicate_id"],
                    object_id=r["object_id"], object_label=r["object_label"] or None,
                    normalization_quality=_quality_from_comment(r.get("comment")),
                    mapping_justification=r["mapping_justification"], tool=r["mapping_tool"],
                )

    def lookup(self, subject_id: str) -> NormalizationDecision | None:
        return self._rows.get(subject_id)

    def record(self, d: NormalizationDecision) -> None:
        assert get_prefix(d.subject_id), f"invalid CURIE {d.subject_id}"
        existing = self._rows.get(d.subject_id)
        if existing is not None and existing.mapping_justification == MANUAL:
            return
        self._rows[d.subject_id] = d

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "w", newline="") as fh:
            fh.write(
                f"# mapping_set_id: {MEDIC_W3ID_ROOT}/mappings/{self.entity_type}_normalization\n"
            )
            writer = csv.DictWriter(fh, fieldnames=COLUMNS, delimiter="\t")
            writer.writeheader()
            for sid in sorted(self._rows):
                d = self._rows[sid]
                writer.writerow({
                    "subject_id": d.subject_id, "predicate_id": d.predicate_id,
                    "object_id": d.object_id, "object_label": d.object_label or "",
                    "mapping_justification": d.mapping_justification,
                    "mapping_tool": d.tool, "comment": d.normalization_quality,
                })
