"""One-shot enrichment: add `bla_number` to existing kb/drugs/purplebook/purplebook.yaml.

Uses the cached raw FDA Purple Book CSV at cache/downloads/purplebook/purplebook.csv
to look up BLA numbers by Proper Name without needing to re-ground the entire
Purple Book corpus.

This avoids the long-running grounding step in the full ingest pipeline while
still attaching the BLA identifier needed for deep-linked
`https://purplebooksearch.fda.gov/results?query=<BLA>` URLs.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from medic.ingest.purplebook.__main__ import parse_purplebook_raw

logger = logging.getLogger(__name__)

KB_PATH = Path("kb/drugs/purplebook/purplebook.yaml")
CSV_PATH = Path("cache/downloads/purplebook/purplebook.csv")


def _normalize_name(name: str) -> str:
    return (name or "").strip().lower()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    if not KB_PATH.exists():
        raise FileNotFoundError(f"{KB_PATH} not found — run purplebook ingest first")
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"{CSV_PATH} not found — fetch the Purple Book CSV first")

    # Build name -> BLA lookup from the raw CSV
    raw_records = parse_purplebook_raw(CSV_PATH)
    bla_lookup: dict[str, str] = {}
    for r in raw_records:
        bla = r.get("bla_number", "")
        name = _normalize_name(r.get("source_name", ""))
        if name and bla:
            bla_lookup[name] = bla
    logger.info("Built BLA lookup with %d entries", len(bla_lookup))

    # Load existing kb yaml
    with open(KB_PATH) as f:
        records = yaml.safe_load(f) or []
    if not isinstance(records, list):
        raise ValueError("Expected kb/drugs/purplebook/purplebook.yaml to be a list")

    enriched = 0
    for rec in records:
        if rec.get("bla_number"):
            continue
        # Try matching on source_name first, then normalized_label
        candidates = [rec.get("source_name", ""), rec.get("normalized_label", "")]
        for c in candidates:
            bla = bla_lookup.get(_normalize_name(c))
            if bla:
                rec["bla_number"] = bla
                enriched += 1
                break

    logger.info("Enriched %d / %d records with BLA numbers", enriched, len(records))

    # Write back atomically
    tmp = KB_PATH.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(records, f, default_flow_style=False, allow_unicode=True, width=1000)
    tmp.replace(KB_PATH)
    logger.info("Wrote %s", KB_PATH)


if __name__ == "__main__":
    main()
