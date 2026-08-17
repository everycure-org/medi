"""Legacy CSV/XLSX export matching the v1.0.0 release format.

Generates:
- drug_list_flexible.csv
- drug_list_stringent.csv
- orangebook.xlsx, purplebook.xlsx, ema.xlsx, pmda.xlsx
- russia.csv, india.csv
"""

import logging
from pathlib import Path

import pandas as pd
import yaml

from medic import product_view as pv

logger = logging.getLogger(__name__)

PRODUCTS_DIR = Path("products")
EXPORTS_DIR = Path("exports")

# Exact column order matching v1.0.0 drug_list_flexible.csv (31 columns)
DRUG_LIST_COLUMNS = [
    "curie",
    "curie_label",
    "source_ingredients",
    "approved_usa",
    "marketing_status_usa",
    "approved_europe",
    "approved_japan",
    "approved_india",
    "approved_russia",
    "is_combination_therapy",
    "combination_therapy_ingredients",
    "combination_therapy_ingredients_curies",
    "is_steroid",
    "is_antimicrobial",
    "is_chemotherapy",
    "is_glucose_regulator",
    "is_vaccine_or_antigen",
    "is_no_therapeutic_value",
    "is_metallic_salt",
    "is_allergen",
    "is_radioisotope_or_diagnostic_agent",
    "is_cancer_drug",
    "alternate_ids",
    "atc_codes",
    "atc_main",
    "atc_level1",
    "atc_level2",
    "atc_level3",
    "atc_level4",
    "atc_level5",
    "smiles",
]

# Stringent list: only USA, Europe, or Japan approved
# Also excludes the approved_india and approved_russia columns
STRINGENT_COLUMNS = [c for c in DRUG_LIST_COLUMNS if c not in ("approved_india", "approved_russia")]

# Source KB directories and their export format
SOURCE_EXPORTS = {
    "orangebook": {"format": "xlsx", "extra_cols": ["marketing_status_usa", "approval_date"]},
    "purplebook": {"format": "xlsx", "extra_cols": ["marketing_status_usa", "approval_date"]},
    "ema": {"format": "xlsx", "extra_cols": ["atc_code", "indication", "approval_date"]},
    "pmda": {"format": "xlsx", "extra_cols": ["indication", "approval_date"]},
    "russia": {"format": "csv", "extra_cols": ["approval_date"]},
    "india": {"format": "csv", "extra_cols": ["indication", "approval_date"]},
}

KB_DRUGS_DIR = Path("kb/drugs")


def export_legacy() -> None:
    """Export products in legacy CSV/XLSX format."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    drug_list_path = PRODUCTS_DIR / "drug_list.yaml"
    if drug_list_path.exists():
        _export_drug_lists(drug_list_path)
    else:
        logger.warning("No drug list found at %s", drug_list_path)

    # Copy per-source intermediate files as release artifacts
    _export_source_files()


def _flatten_drug(drug: dict) -> dict:
    """Project a v2.0 Drug (mention + approvals) into the flat legacy column shape.

    Identity comes from ``mention.resolved_id/label``; the ``approved_<jurisdiction>``
    booleans + ``marketing_status_usa`` are derived from the ``approvals`` list. All other
    columns (tags, ATC, alternate_ids, smiles, combination_*) are still top-level on Drug.
    """
    row = dict(drug)  # keep top-level metadata columns as-is
    row.pop("mention", None)
    row.pop("approvals", None)
    row["curie"] = pv.drug_id(drug)
    row["curie_label"] = pv.drug_label(drug)
    juris = pv.approved_jurisdictions(drug)
    for j in ("usa", "europe", "japan", "india", "russia", "china"):
        row[f"approved_{j}"] = j in juris
    # MarketingStatusEnum.NONE ("no marketing status available") is the legacy default
    # for drugs with no US marketing status (non-US-approved, or US-approved w/o a status).
    row["marketing_status_usa"] = pv.marketing_status_usa(drug) or "NONE"
    return row


def _export_drug_lists(drug_list_path: Path) -> None:
    """Export drug list as CSV files matching v1.0.0 format."""
    with open(drug_list_path) as f:
        data = yaml.safe_load(f)

    drugs = data.get("drugs", []) if isinstance(data, dict) else data
    if not drugs:
        logger.warning("Empty drug list")
        return

    df = pd.DataFrame(_flatten_drug(d) for d in drugs)

    # Ensure all expected columns exist
    for col in DRUG_LIST_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Sort by curie to match reference ordering
    df = df.sort_values("curie", na_position="last").reset_index(drop=True)

    # Convert list columns to pipe-separated strings for CSV compatibility
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: "| ".join(str(i) for i in x) if isinstance(x, list) else (str(x) if pd.notna(x) and x != "" else "")
        )

    # Export flexible list (all drugs, all columns)
    df_flex = df[DRUG_LIST_COLUMNS].copy()
    df_flex.to_csv(EXPORTS_DIR / "drug_list_flexible.csv", index=False)
    logger.info("Exported %d drugs to drug_list_flexible.csv", len(df_flex))

    # Stringent list: only drugs approved in USA, Europe, or Japan
    # Filter by approval in at least one stringent jurisdiction
    mask = pd.Series(False, index=df.index)
    for col in ["approved_usa", "approved_europe", "approved_japan"]:
        mask |= df[col].apply(lambda x: bool(x) and x != 0 and str(x).lower() not in ("false", "nan", ""))

    # Keep only stringent columns (drop india/russia)
    available_stringent = [c for c in STRINGENT_COLUMNS if c in df.columns]
    df_stringent = df.loc[mask, available_stringent].copy()
    df_stringent.to_csv(EXPORTS_DIR / "drug_list_stringent.csv", index=False)
    logger.info("Exported %d drugs to drug_list_stringent.csv", len(df_stringent))


def _export_source_files() -> None:
    """Generate per-source export files from KB YAML data."""
    for source_name, config in SOURCE_EXPORTS.items():
        yaml_path = KB_DRUGS_DIR / source_name / f"{source_name}.yaml"
        if not yaml_path.exists():
            logger.warning("No KB data for %s at %s", source_name, yaml_path)
            continue

        with open(yaml_path) as f:
            records = yaml.safe_load(f)
        if not records:
            continue

        # Map new field names to old column names
        rows = []
        for rec in records:
            row = {
                "source_ingredients": rec.get("source_name", ""),
                "corrected_curie_norm": rec.get("normalized_id", ""),
                "corrected_curie_norm_label": rec.get("normalized_label", ""),
                "alternate_ids": str(rec.get("alternate_ids", [])),
            }
            # Add approval flag
            approval_col = {
                "orangebook": "approved_usa", "purplebook": "approved_usa",
                "ema": "approved_europe", "pmda": "approved_japan",
                "russia": "approved_russia", "india": "approved_india",
            }.get(source_name)
            if approval_col:
                row[approval_col] = True
            # Add source-specific columns
            for col in config.get("extra_cols", []):
                row[col] = rec.get(col, "")
            rows.append(row)

        df = pd.DataFrame(rows)
        fmt = config["format"]
        if fmt == "xlsx":
            dest = EXPORTS_DIR / f"{source_name}.xlsx"
            df.to_excel(dest, index=False)
        else:
            dest = EXPORTS_DIR / f"{source_name}.csv"
            df.to_csv(dest, index=False)
        logger.info("Exported %d records to %s", len(df), dest)


def main():
    logging.basicConfig(level=logging.INFO)
    export_legacy()


if __name__ == "__main__":
    main()
