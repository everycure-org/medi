"""Summary stats over a grounding SSSOM decision store."""

from __future__ import annotations

import csv
from collections import Counter


def report(store_path: str) -> dict:
    by_pred: Counter = Counter()
    total = unresolved = 0
    with open(store_path, newline="") as fh:
        reader = csv.DictReader((ln for ln in fh if not ln.startswith("#")), delimiter="\t")
        for r in reader:
            total += 1
            by_pred[r["predicate_id"]] += 1
            if r["predicate_id"] == "sssom:NoTermFound":
                unresolved += 1
    return {"total": total, "unresolved": unresolved, "resolved": total - unresolved,
            "by_predicate": dict(by_pred)}
