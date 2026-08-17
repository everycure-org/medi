"""CLI entry point for EveryCure drug list ingest.

Downloads the EveryCure curated drug list from HuggingFace and converts
it to DrugSource YAML records. We trust EveryCure's **labels** but not its
**ids**: EveryCure keys biologics by UNII (which fragments from the CHEBI ids
the regulatory sources ground to), so we ground the drug NAME through the shared
lexical grounder like every other source and keep EveryCure's ``translator_id``
only as provenance in ``alternate_ids``.

Single acquisition path: the HuggingFace dataset ``everycure/drug-list`` (via
the ``datasets`` library), or an explicitly-provided local file via
``input_path``. If neither is available, ingest fails loudly rather than
silently degrading to any legacy local table.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from medic.ingest.common import write_drug_source_yaml

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("kb/drugs/everycure")
SOURCE_NAME = "everycure"

# Boolean flag columns present in the HF dataset
_BOOLEAN_TAG_COLUMNS = [
    "is_antipsychotic",
    "is_sedative",
    "is_antimicrobial",
    "is_antifungal",
    "is_antiviral",
    "is_antiparasitic",
    "is_immunosuppressant",
    "is_chemotherapy",
    "is_hormone",
    "is_biologic",
    "is_small_molecule",
    "is_repurposed",
]


def _clean(val) -> str:
    if pd.isna(val):
        return ""
    return str(val).strip()


def _ensure_prefix(val: str, prefix: str) -> str:
    """Ensure a CURIE has the given prefix."""
    if not val:
        return val
    if not val.upper().startswith(prefix.upper()):
        return f"{prefix}{val}"
    return val


def _load_from_huggingface() -> pd.DataFrame | None:
    """Load the drug list via the datasets library."""
    try:
        from datasets import load_dataset

        logger.info("Loading everycure/drug-list from HuggingFace datasets...")
        ds = load_dataset("everycure/drug-list", split="train")
        df = ds.to_pandas()
        logger.info("Loaded %d rows from HuggingFace (everycure/drug-list)", len(df))
        return df
    except ImportError as e:
        raise RuntimeError(
            "The `datasets` library is required to load the EveryCure drug list "
            "from HuggingFace (everycure/drug-list) but is not installed. Install "
            "project dependencies (e.g. `uv sync`) and re-run."
        ) from e
    except Exception as e:
        raise RuntimeError(
            "Failed to load the EveryCure drug list from HuggingFace "
            "(everycure/drug-list). Check network access and dataset "
            f"availability, then re-run. Underlying error: {e!r}"
        ) from e


def ingest_everycure(
    input_path: Path | None = None, grounding_backend: str = "lexical"
) -> list[dict]:
    """Ingest EveryCure drug list and write DrugSource YAML.

    Args:
        input_path: Path to a local EveryCure drug list TSV/CSV. If provided,
                    it is read directly. If None, the list is loaded from the
                    HuggingFace everycure/drug-list dataset (failing loudly if
                    unavailable).

    Returns:
        List of DrugSource records written.
    """
    # Resolve source DataFrame
    if input_path is not None:
        if not input_path.exists():
            raise FileNotFoundError(
                f"EveryCure input path does not exist: {input_path}"
            )
        logger.info("Using provided input path: %s", input_path)
        sep = "\t" if input_path.suffix.lower() == ".tsv" else ","
        df = pd.read_csv(input_path, sep=sep, dtype=str)
    else:
        df = _load_from_huggingface()

    logger.info("Processing %d rows (columns: %s)", len(df), list(df.columns))

    # Filter out deleted drugs
    if "deleted" in df.columns:
        before = len(df)
        df = df[df["deleted"] != True]  # noqa: E712
        logger.info("Filtered deleted drugs: %d -> %d rows", before, len(df))

    records = []
    for _, row in df.iterrows():
        name = _clean(row.get("name", ""))
        if not name:
            continue

        # EveryCure's translator_id is their pre-grounded id — UNII for biologics, CHEBI
        # for small molecules. We trust EveryCure's LABEL but not its id (UNII biologics
        # fragment from the CHEBI ids the regulatory sources ground to), so we ground the
        # NAME below and keep translator_id + DrugBank only as provenance / alternate ids.
        everycure_id = _clean(row.get("translator_id", ""))

        drugbank_raw = _clean(row.get("drugbank_id", ""))
        drugbank = _ensure_prefix(drugbank_raw, "DRUGBANK:") if drugbank_raw else ""

        # `everycure_id` is deliberately NOT a provenance id here. `alternate_ids` becomes
        # `skos:exactMatch` in the SSSOM export — the strongest identity claim SSSOM has — and
        # this id is the one the comment above says we do not trust. Publishing it asserted
        # `CHEBI:749610 (ofatumumab) skos:exactMatch CHEBI:28887 (dimethyl ether)`, an anti-CD20
        # monoclonal antibody declared identical to a solvent, across 119 CHEBI->CHEBI rows.
        # It stays on the record as `everycure_id`, which is what "kept as provenance" means.
        provenance_ids: list[str] = []
        if drugbank and drugbank != everycure_id:
            provenance_ids.append(drugbank)

        record: dict = {
            "source": "EVERYCURE",
            "source_name": name,             # grounded by the shared lexical grounder below
            "everycure_id": everycure_id,    # untrusted pre-grounded id, kept as provenance
            "_provenance_ids": provenance_ids,
            "approval_date": "",
        }

        # Optional metadata fields
        for field in ("drug_class", "therapeutic_area", "drug_function", "drug_target"):
            val = _clean(row.get(field, ""))
            if val:
                record[field] = val

        # ATC fields
        atc_main = _clean(row.get("atc_main", ""))
        if atc_main:
            record["atc_main"] = atc_main

        for level in range(1, 6):
            for col_suffix in (f"atc_level_{level}", f"l{level}_label"):
                val = _clean(row.get(col_suffix, ""))
                if val:
                    record[col_suffix] = val

        # Boolean tag columns
        for tag_col in _BOOLEAN_TAG_COLUMNS:
            if tag_col in df.columns:
                tag_val = row.get(tag_col)
                if pd.notna(tag_val):
                    record[tag_col] = bool(tag_val)

        # Synonyms (stored as numpy array/list in HF dataset, or semicolon-separated string in TSV)
        synonyms_val = row.get("synonyms", None)
        if synonyms_val is not None:
            if isinstance(synonyms_val, (list, np.ndarray)):
                synonyms = [str(s).strip() for s in synonyms_val if s is not None and str(s).strip()]
            elif isinstance(synonyms_val, float):
                # NaN represented as float
                synonyms = []
            else:
                raw = _clean(synonyms_val)
                synonyms = [s.strip() for s in raw.split(";") if s.strip()] if raw else []
            if synonyms:
                record["synonyms"] = synonyms

        # Approval status
        approval_status = _clean(row.get("approved_usa", ""))
        if approval_status:
            record["approved_usa"] = approval_status

        records.append(record)

    if not records:
        logger.warning("No EveryCure drug records produced")
        return records

    # Ground by NAME through the shared lexical pipeline (same as every other source),
    # so EveryCure drugs land on the same CHEBI ids the regulatory sources ground to and
    # MERGE with them — instead of fragmenting under EveryCure's UNII ids. Stage-2
    # normalization runs inside ground_records.
    from medic.grounding.cache import GroundingCache
    from medic.grounding.factory import get_grounding_service
    from medic.ingest.common import write_grounding_report
    from medic.ingest.grounding import ground_records

    grounding_service = get_grounding_service(grounding_backend)
    grounded, report = ground_records(records, grounding_service, GroundingCache(), SOURCE_NAME)

    # Fold EveryCure's provenance ids into alternate_ids (ground_records set its own).
    for rec in grounded:
        alts = rec.setdefault("alternate_ids", [])
        for pid in rec.pop("_provenance_ids", []):
            if pid and pid not in alts:
                alts.append(pid)

    write_drug_source_yaml(grounded, OUTPUT_DIR, SOURCE_NAME)
    write_grounding_report(report, OUTPUT_DIR, SOURCE_NAME)
    logger.info(
        "Wrote %d EveryCure drug records (grounded by name; %d unresolved)",
        len(grounded), report.get("unresolved", 0),
    )
    return grounded


def main():
    logging.basicConfig(level=logging.INFO)
    ingest_everycure()


if __name__ == "__main__":
    main()
