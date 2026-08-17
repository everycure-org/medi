"""Pick next N uncurated diseases from the research priority queue.

Reads background/research_queue.tsv (auto-seeds from the priority TSV if
missing), filters out diseases that already have kb/research/MONDO_*.yaml,
and prints the first N as TSV rows: mondo_id<TAB>label

Used by the medic-research-batch skill to decide which diseases to process
in the next batch run.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from medic.research.curate import PRIORITY_DISEASES_PATH

QUEUE_PATH = Path("background/research_queue.tsv")
PRIORITY_PATH = PRIORITY_DISEASES_PATH
KB_RESEARCH_DIR = Path("kb/research")


def ensure_queue_exists() -> bool:
    """Seed the queue file from the priority TSV if it is missing.

    Returns True if a fresh seed was performed, False if the queue already
    existed (and is left untouched).
    """
    if QUEUE_PATH.exists():
        return False
    if not PRIORITY_PATH.exists():
        raise FileNotFoundError(f"Priority TSV missing: {PRIORITY_PATH}")
    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(PRIORITY_PATH, QUEUE_PATH)
    return True


def is_curated(mondo_id: str) -> bool:
    """Return True iff kb/research/<safe_id>.yaml exists.

    `safe_id` is the MONDO id with ':' replaced by '_'
    (e.g., MONDO:0001234 -> kb/research/MONDO_0001234.yaml).
    """
    safe = mondo_id.replace(":", "_")
    return (KB_RESEARCH_DIR / f"{safe}.yaml").exists()


def read_queue() -> list[tuple[str, str]]:
    """Parse the queue TSV and return [(mondo_id, label), ...] in file order.

    Skips:
      - the header row (first non-blank line that starts with 'mondo id')
      - blank lines
      - comment lines (start with '#')
      - rows with fewer than 2 tab-separated columns

    Raises FileNotFoundError if QUEUE_PATH does not exist; call
    ensure_queue_exists() first.
    """
    rows: list[tuple[str, str]] = []
    header_seen = False
    with QUEUE_PATH.open() as f:
        for line in f:
            stripped = line.rstrip("\n")
            if not stripped.strip():
                continue
            if stripped.lstrip().startswith("#"):
                continue
            if not header_seen and stripped.lower().startswith("mondo id"):
                header_seen = True
                continue
            parts = stripped.split("\t")
            if len(parts) < 2:
                continue
            mondo_id = parts[0].strip()
            label = parts[1].strip()
            if mondo_id and label:
                rows.append((mondo_id, label))
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Pick next N uncurated diseases from the research priority queue."
    )
    ap.add_argument(
        "--count",
        type=int,
        default=20,
        help="Number of diseases to pick (default: 20).",
    )
    args = ap.parse_args(argv)

    seeded = ensure_queue_exists()
    if seeded:
        print(
            f"# Seeded queue from {PRIORITY_PATH} -> {QUEUE_PATH}",
            file=sys.stderr,
        )

    queue = read_queue()
    uncurated = [(mid, lab) for (mid, lab) in queue if not is_curated(mid)]
    picked = uncurated[: args.count]

    for mid, lab in picked:
        print(f"{mid}\t{lab}")

    print(
        f"# picked {len(picked)} of {args.count} requested; "
        f"{len(uncurated) - len(picked)} remaining uncurated; "
        f"{len(queue) - len(uncurated)} already curated",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
