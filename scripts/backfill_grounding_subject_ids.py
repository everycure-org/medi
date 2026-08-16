"""Backfill the MEDICNE subject_id on grounding store rows written before it was recorded.

8,883 of 21,179 drug rows and 2,138 of 7,273 disease rows carried a blank subject_id.
``GroundingStoreView.decision_for`` masks it with a normalized-literal fallback, but the store
is the authoritative record (I-4) and a row that cannot be joined by id is a hole in it.

Two traps this script exists to avoid, both of which corrupt silently:

**1. The id is pinned to the ORIGINAL literal, not to `subject_label`.** For a translated drug
the grounder saw the English string, so `subject_label` is English — but the row's `subject_id`
is the id of the *foreign* source literal, so the whole trail joins up. 7,080 of the drug
store's populated ids differ from ``mint(subject_label)`` for exactly this reason, and that is
correct. So: look the label up in the Babelon translation store first, and only mint when it is
genuinely the original. The disease store has no translated rows (0 mismatches) and mints
cleanly.

**2. The stores are CSV-quoted.** A label containing a quote or a tab is written by
``csv.writer``; splitting on ``\\t`` and rejoining would mangle it. Read and write with ``csv``.

The mint is deterministic (uuid5 of entity_type + base-normalized label), so this is
idempotent: rerunning changes nothing. Note the singular/plural trap — ``mint_mention_id`` keys
its uuid5 on the entity type and the ingest convention is the PLURAL form.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from medic.mention import mint_mention_id

STORES = [
    ("mappings/drug_grounding.sssom.tsv", "drugs", "mappings/drug_translation.babelon.tsv"),
    ("mappings/disease_grounding.sssom.tsv", "diseases", None),
]


def _translation_index(path: str | None) -> dict[str, str]:
    """Map an English translation_value -> the MEDICNE id of the original foreign literal."""
    if not path or not Path(path).exists():
        return {}
    out: dict[str, str] = {}
    with open(path, newline="") as fh:
        for row in csv.DictReader(
            (ln for ln in fh if not ln.startswith("#")), delimiter="\t"
        ):
            value = (row.get("translation_value") or "").strip()
            subject = (row.get("subject_id") or "").strip()
            if value and subject:
                out.setdefault(value, subject)
    return out


def backfill(path: str, entity_type: str, translations: str | None) -> dict[str, int]:
    """Fill blank subject_id cells in one store."""
    p = Path(path)
    text = p.read_text()
    comments = [ln for ln in text.splitlines(keepends=True) if ln.startswith("#")]
    with open(path, newline="") as fh:
        reader = csv.DictReader(
            (ln for ln in text.splitlines(keepends=True) if not ln.startswith("#")),
            delimiter="\t",
        )
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    index = _translation_index(translations)
    stats = {"from_translation": 0, "minted": 0, "skipped": 0, "already": 0}
    for row in rows:
        if (row.get("subject_id") or "").strip():
            stats["already"] += 1
            continue
        label = (row.get("subject_label") or "").strip()
        if not label:
            stats["skipped"] += 1
            continue
        if label in index:
            row["subject_id"] = index[label]
            stats["from_translation"] += 1
        else:
            row["subject_id"] = mint_mention_id(label, entity_type)
            stats["minted"] += 1

    with open(path, "w", newline="") as fh:
        fh.writelines(comments)
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t",
                                lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return stats


def main() -> int:
    for path, entity_type, translations in STORES:
        stats = backfill(path, entity_type, translations)
        print(f"{path}: from_translation={stats['from_translation']} "
              f"minted={stats['minted']} skipped={stats['skipped']} "
              f"already_populated={stats['already']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
