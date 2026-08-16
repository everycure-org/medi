"""Refresh `object_label` in a grounding decision store from the current lexical index.

The store's *decisions* — which literal resolved to which id, by which rule, at what
confidence — are the authoritative, hand-editable record (I-4) and this script never touches
them. `object_label` is the one derived column: it is whatever the index happened to call the
target when the decision was made.

That distinction is why this exists. Changing which vocabulary may supply a published label
(I-14) changes every stored label without changing a single decision, and re-running the whole
grounding pipeline to propagate a derived field would be both slow and a much larger blast
radius. Matching keys on `norm_value`, which this does not affect, so a full re-ground would
produce byte-identical decisions anyway.

Every store that carries an `object_label` is refreshed, not just the grounding one. Scoping this
to `disease_grounding` alone is what let I-14 rule 2 ship broken: the grounding store correctly
blanked 240 MedDRA-only concepts, `disease_normalization` was never regenerated, kept its labels
for 238 of them, and the merge read the label from there — so restricted term text reached the
products and the KGX export anyway. A label policy has to be applied everywhere a label is stored.

Usage:
    uv run python scripts/refresh_grounding_labels.py                 # every store, in place
    uv run python scripts/refresh_grounding_labels.py --dry-run       # report only
    uv run python scripts/refresh_grounding_labels.py --store PATH    # just this one
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

DISEASE_INDEX = Path("cache/grounding/lexical_index/diseases.db")
DRUG_INDEX = Path("cache/grounding/lexical_index/drugs.db")

# Every store whose `object_label` is derived from the index, with the index that names it.
STORES: dict[Path, Path] = {
    Path("mappings/disease_grounding.sssom.tsv"): DISEASE_INDEX,
    Path("mappings/disease_normalization.sssom.tsv"): DISEASE_INDEX,
    Path("mappings/drug_grounding.sssom.tsv"): DRUG_INDEX,
    Path("mappings/drug_normalization.sssom.tsv"): DRUG_INDEX,
}


def index_labels(db_path: Path, ids: set[str]) -> dict[str, str]:
    """Current published label per object id, for the ids the store actually uses.

    One sequential scan, filtered in Python. The index carries no key on ``object_id`` (it is
    built for lookup by ``norm_value``), so a per-id query degrades to a full scan each time —
    4,034 ids against 3.7M rows. Streaming once is seconds instead of minutes.
    """
    if not db_path.exists():
        sys.exit(f"lexical index not found at {db_path} — run `just build-grounding-index` first")
    con = sqlite3.connect(db_path)
    found: dict[str, str] = {}
    for object_id, label in con.execute("SELECT object_id, object_label FROM lex"):
        if object_id in ids and object_id not in found:
            found[object_id] = label or ""
    con.close()
    return found


def refresh(store: Path, index: Path, dry_run: bool) -> None:
    # newline="" on both read and write: the store is written by `csv` and therefore uses
    # CRLF. Universal-newline mode would translate those to LF on read and never put them
    # back, rewriting all 21,187 rows of a store to change one derived column — and the next
    # `store.save()` would churn them straight back.
    with open(store, newline="") as fh:
        lines = fh.readlines()
    header_at = next(i for i, ln in enumerate(lines) if not ln.startswith("#"))
    columns = lines[header_at].rstrip("\r\n").split("\t")
    if "object_label" not in columns:
        print(f"{store}: no object_label column, nothing to refresh")
        return
    oid, olabel = columns.index("object_id"), columns.index("object_label")

    body = lines[header_at + 1:]
    ids = set()
    for ln in body:
        fields = ln.rstrip("\r\n").split("\t")
        if len(fields) > oid and fields[oid]:
            ids.add(fields[oid])
    labels = index_labels(index, ids)

    changed = emptied = out_lines = 0
    rewritten: list[str] = []
    for ln in body:
        stripped = ln.rstrip("\r\n")
        terminator = ln[len(stripped):]  # preserve this row's exact line ending
        fields = stripped.split("\t")
        if len(fields) <= max(oid, olabel) or not fields[oid]:
            rewritten.append(ln)
            continue
        out_lines += 1
        new = labels.get(fields[oid])
        if new is not None and new != fields[olabel]:
            changed += 1
            if not new:
                emptied += 1
            fields[olabel] = new
            ln = "\t".join(fields) + terminator
        rewritten.append(ln)

    print(f"{store}: {out_lines} decision rows; {changed} label(s) changed, {emptied} now unnamed")
    if dry_run:
        print("  dry run — nothing written")
        return
    with open(store, "w", newline="") as fh:
        fh.write("".join(lines[:header_at + 1]) + "".join(rewritten))
    print(f"  wrote {store}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store", type=Path, help="refresh only this store (default: all of them)")
    ap.add_argument("--index", type=Path, help="index to read labels from (with --store)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.store:
        index = args.index or STORES.get(args.store)
        if index is None:
            sys.exit(f"no index known for {args.store} — pass --index")
        refresh(args.store, index, args.dry_run)
        return
    for store, index in STORES.items():
        if store.exists():
            refresh(store, index, args.dry_run)


if __name__ == "__main__":
    main()
