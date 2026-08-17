#!/usr/bin/env python3
"""Generate review tables for disease grounding results.

Reads the on-label products and grounding cache to produce TSV files for
expert review of:
1. Unresolvable disease IDs (no MONDO match found)
2. Review-recommended matches (moderate confidence, needs expert verification)

Usage:
    python scripts/generate_disease_grounding_review.py
"""

import json
import logging
from pathlib import Path

import pandas as pd
from medic.research.curate import PRIORITY_DISEASES_PATH

logger = logging.getLogger(__name__)

INDICATION_PATH = Path("products/indication_list.yaml")
CONTRAINDICATION_PATH = Path("products/contraindication_list.yaml")
OLD_INDICATIONS_XLSX = Path("medi/indications/data/03_primary/matrix_indication_list.xlsx")
OLD_CONTRAINDICATIONS_XLSX = Path("medi/indications/data/03_primary/matrix_contraindication_list.xlsx")
CACHE_DIR = Path("cache/grounding")
OUTPUT_DIR = Path("analysis/changes_refactor")
PRIORITY_TSV = PRIORITY_DISEASES_PATH


def load_priority_ids() -> set[str]:
    ids = set()
    with open(PRIORITY_TSV) as f:
        import csv
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ids.add(row["mondo id"].strip())
    return ids


def load_grounding_cache(source_name: str) -> dict[str, dict]:
    """Load grounding cache for a source. Returns key -> entry dict."""
    cache_file = CACHE_DIR / f"{source_name}.json"
    if not cache_file.exists():
        return {}
    with open(cache_file) as f:
        return json.load(f)


def load_old_indications() -> pd.DataFrame:
    """Load original indications with all metadata."""
    return pd.read_excel(OLD_INDICATIONS_XLSX)


def load_old_contraindications() -> pd.DataFrame:
    return pd.read_excel(OLD_CONTRAINDICATIONS_XLSX)


def build_review_tables(
    old_df: pd.DataFrame,
    cache: dict[str, dict],
    priority_ids: set[str],
    disease_id_col: str = "final normalized disease id",
    disease_label_col: str = "final normalized disease label",
    drug_id_col: str = "final normalized drug id",
    drug_label_col: str = "final normalized drug label",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build unresolved and review-recommended tables.

    Returns (unresolved_df, review_df).
    """
    # Get non-MONDO rows from original data
    non_mondo = old_df[~old_df[disease_id_col].astype(str).str.startswith("MONDO:")].copy()

    unresolved_rows = []
    review_rows = []

    for _, row in non_mondo.iterrows():
        original_id = str(row[disease_id_col]).strip()
        original_label = str(row[disease_label_col]).strip()
        drug_id = str(row.get(drug_id_col, "")).strip()
        drug_label = str(row.get(drug_label_col, "")).strip()
        prefix = original_id.split(":")[0] if ":" in original_id else "UNKNOWN"

        # Check source flags
        fda = bool(row.get("FDA") == 1.0) if "FDA" in row.index and pd.notna(row.get("FDA")) else False
        ema = bool(row.get("EMA") == 1.0) if "EMA" in row.index and pd.notna(row.get("EMA")) else False
        pmda = bool(row.get("PMDA") == 1.0) if "PMDA" in row.index and pd.notna(row.get("PMDA")) else False

        # Look up in grounding cache
        # Cache keys are normalized: lowercased, colons/spaces flattened
        cache_key = f"disease:{original_label}:{original_id}".lower().replace(":", " ")
        cached = cache.get(cache_key, {})
        if not cached or not isinstance(cached, dict):
            # Try without the ID suffix
            cache_key2 = f"disease {original_label} {original_id}".lower().replace(":", " ")
            cached = cache.get(cache_key2, {})
        if not cached or not isinstance(cached, dict):
            cache_key3 = f"disease {original_label}".lower()
            cached = cache.get(cache_key3, {})

        grounded_id = cached.get("normalized_id", "")
        grounded_label = cached.get("normalized_label", "")
        confidence = cached.get("grounding_confidence", 0.0)
        status = cached.get("grounding_status", "")
        service = cached.get("grounding_service", "")

        base_row = {
            "original_disease_id": original_id,
            "original_disease_label": original_label,
            "original_prefix": prefix,
            "drug_id": drug_id,
            "drug_label": drug_label,
            "fda": fda,
            "ema": ema,
            "pmda": pmda,
            "grounded_mondo_id": grounded_id,
            "grounded_mondo_label": grounded_label,
            "grounding_confidence": confidence,
            "grounding_service": service,
            "grounding_status": status,
            "rapid": original_id in priority_ids or grounded_id in priority_ids,
        }

        if not grounded_id or not grounded_id.startswith("MONDO:"):
            unresolved_rows.append(base_row)
        elif status == "review_recommended":
            review_rows.append(base_row)

    unresolved_df = pd.DataFrame(unresolved_rows)
    review_df = pd.DataFrame(review_rows)

    return unresolved_df, review_df


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    priority_ids = load_priority_ids()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load caches
    ind_cache = load_grounding_cache("on_label_diseases")
    ci_cache = load_grounding_cache("contraindication_diseases")

    # Indications
    print("=== Indications ===")
    old_ind = load_old_indications()
    ind_unresolved, ind_review = build_review_tables(old_ind, ind_cache, priority_ids)

    # Deduplicate by disease ID for cleaner review
    if not ind_unresolved.empty:
        ind_unresolved_dedup = ind_unresolved.drop_duplicates(subset=["original_disease_id"]).sort_values("original_disease_label")
        ind_unresolved.sort_values(["original_disease_label", "drug_label"]).to_csv(
            OUTPUT_DIR / "indications_unresolved_full.tsv", sep="\t", index=False,
        )
        ind_unresolved_dedup.to_csv(
            OUTPUT_DIR / "indications_unresolved_diseases.tsv", sep="\t", index=False,
        )
        print(f"  Unresolved: {len(ind_unresolved)} rows ({len(ind_unresolved_dedup)} unique diseases)")
        print("    -> indications_unresolved_full.tsv (all drug-disease pairs)")
        print("    -> indications_unresolved_diseases.tsv (unique diseases)")
    else:
        print("  No unresolved indications")

    if not ind_review.empty:
        ind_review_dedup = ind_review.drop_duplicates(subset=["original_disease_id", "grounded_mondo_id"]).sort_values("grounding_confidence")
        ind_review.sort_values(["grounding_confidence", "original_disease_label"]).to_csv(
            OUTPUT_DIR / "indications_review_recommended_full.tsv", sep="\t", index=False,
        )
        ind_review_dedup.to_csv(
            OUTPUT_DIR / "indications_review_recommended_diseases.tsv", sep="\t", index=False,
        )
        print(f"  Review recommended: {len(ind_review)} rows ({len(ind_review_dedup)} unique mappings)")
        print("    -> indications_review_recommended_full.tsv")
        print("    -> indications_review_recommended_diseases.tsv")
    else:
        print("  No review-recommended indications")

    # Contraindications
    print("\n=== Contraindications ===")
    old_ci = load_old_contraindications()
    ci_unresolved, ci_review = build_review_tables(
        old_ci, ci_cache, priority_ids,
        drug_id_col="final normalized drug id",
        drug_label_col="final normalized drug label",
    )

    if not ci_unresolved.empty:
        ci_unresolved_dedup = ci_unresolved.drop_duplicates(subset=["original_disease_id"]).sort_values("original_disease_label")
        ci_unresolved.sort_values(["original_disease_label", "drug_label"]).to_csv(
            OUTPUT_DIR / "contraindications_unresolved_full.tsv", sep="\t", index=False,
        )
        ci_unresolved_dedup.to_csv(
            OUTPUT_DIR / "contraindications_unresolved_diseases.tsv", sep="\t", index=False,
        )
        print(f"  Unresolved: {len(ci_unresolved)} rows ({len(ci_unresolved_dedup)} unique diseases)")
    else:
        print("  No unresolved contraindications")

    if not ci_review.empty:
        ci_review_dedup = ci_review.drop_duplicates(subset=["original_disease_id", "grounded_mondo_id"]).sort_values("grounding_confidence")
        ci_review.sort_values(["grounding_confidence", "original_disease_label"]).to_csv(
            OUTPUT_DIR / "contraindications_review_recommended_full.tsv", sep="\t", index=False,
        )
        ci_review_dedup.to_csv(
            OUTPUT_DIR / "contraindications_review_recommended_diseases.tsv", sep="\t", index=False,
        )
        print(f"  Review recommended: {len(ci_review)} rows ({len(ci_review_dedup)} unique mappings)")
    else:
        print("  No review-recommended contraindications")

    # Summary
    print("\n=== Summary ===")
    total_unresolved = len(ind_unresolved) + len(ci_unresolved)
    total_review = len(ind_review) + len(ci_review)
    print(f"  Total unresolved: {total_unresolved}")
    print(f"  Total review recommended: {total_review}")
    print(f"  Files written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
