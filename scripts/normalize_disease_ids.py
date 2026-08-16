#!/usr/bin/env python3
"""Normalize disease IDs in on-label products to canonical MONDO identifiers.

Reads products/indication_list.yaml and products/contraindication_list.yaml,
re-grounds non-MONDO disease IDs (UMLS, HP, EFO, NCIT, DOID) via the
grounding pipeline, and writes updated files.

Usage:
    python scripts/normalize_disease_ids.py
    python scripts/normalize_disease_ids.py --dry-run
    python scripts/normalize_disease_ids.py --grounding-backend lexical
"""

import argparse
import logging
from pathlib import Path

import yaml

from medic.grounding import get_grounding_service
from medic.grounding.cache import GroundingCache
from medic.ingest.common import write_grounding_report
from medic.ingest.grounding import ground_disease_records

logger = logging.getLogger(__name__)

INDICATION_PATH = Path("products/indication_list.yaml")
CONTRAINDICATION_PATH = Path("products/contraindication_list.yaml")
CACHE_DIR = Path("cache/grounding")
REPORT_DIR = Path("kb/indications")


def normalize_file(
    yaml_path: Path,
    grounding_service,
    cache: GroundingCache,
    source_name: str,
    dry_run: bool = False,
) -> dict:
    """Normalize disease IDs in a single YAML file."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    associations = data.get("associations", [])
    if not associations:
        logger.info("No associations in %s", yaml_path)
        return {}

    # Count non-MONDO before
    non_mondo_before = sum(
        1 for a in associations
        if not a.get("final_normalized_disease_id", "").startswith("MONDO:")
    )
    logger.info(
        "%s: %d associations, %d non-MONDO disease IDs",
        yaml_path.name, len(associations), non_mondo_before,
    )

    if non_mondo_before == 0:
        logger.info("All disease IDs are already MONDO, nothing to do")
        return {"total": len(associations), "non_mondo_before": 0, "non_mondo_after": 0}

    # Run disease grounding
    grounded, report = ground_disease_records(
        associations,
        grounding_service,
        cache,
        source_name,
        disease_name_key="final_normalized_disease_label",
        disease_id_key="final_normalized_disease_id",
    )

    # Update drug_disease compound key for re-grounded records
    for assoc in grounded:
        drug_id = assoc.get("final_normalized_drug_id", "")
        disease_id = assoc.get("final_normalized_disease_id", "")
        assoc["drug_disease"] = f"{drug_id}|{disease_id}"

    # Count non-MONDO after
    non_mondo_after = sum(
        1 for a in grounded
        if not a.get("final_normalized_disease_id", "").startswith("MONDO:")
    )

    logger.info(
        "Results: %d already MONDO, %d re-grounded to MONDO, %d newly grounded, "
        "%d review recommended, %d unresolved (non-MONDO: %d -> %d)",
        report["already_mondo"], report["regrounded_to_mondo"],
        report["newly_grounded"], report["review_recommended"],
        report["unresolved"], non_mondo_before, non_mondo_after,
    )

    report["non_mondo_before"] = non_mondo_before
    report["non_mondo_after"] = non_mondo_after

    if not dry_run:
        data["associations"] = grounded
        content = yaml.dump(data, default_flow_style=False, allow_unicode=True, width=1000)
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        with open(yaml_path, "w") as f:
            f.write(content)
        logger.info("Updated %s", yaml_path)

        # Write grounding report
        write_grounding_report(report, REPORT_DIR, f"disease_grounding_{source_name}")

    return report


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Normalize disease IDs to MONDO")
    parser.add_argument("--grounding-backend", default="lexical", help="Grounding backend to use")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    args = parser.parse_args()

    grounding_service = get_grounding_service(args.grounding_backend)
    cache = GroundingCache(CACHE_DIR)

    print("=== Normalizing on-label indications ===")
    if INDICATION_PATH.exists():
        ind_report = normalize_file(INDICATION_PATH, grounding_service, cache, "on_label_diseases", args.dry_run)
    else:
        print(f"  {INDICATION_PATH} not found")
        ind_report = {}

    print("\n=== Normalizing contraindications ===")
    if CONTRAINDICATION_PATH.exists():
        ci_report = normalize_file(CONTRAINDICATION_PATH, grounding_service, cache, "contraindication_diseases", args.dry_run)
    else:
        print(f"  {CONTRAINDICATION_PATH} not found")
        ci_report = {}

    # Summary
    print("\n=== Summary ===")
    for name, report in [("Indications", ind_report), ("Contraindications", ci_report)]:
        if report:
            before = report.get("non_mondo_before", 0)
            after = report.get("non_mondo_after", 0)
            resolved = before - after
            print(f"  {name}: {before} non-MONDO -> {after} remaining ({resolved} resolved)")


if __name__ == "__main__":
    main()
