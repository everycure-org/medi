"""Disease list ingest from HuggingFace everycure/disease-list dataset.

Reads the disease list and converts it to kb/diseases/disease_list.yaml
in the MeDIC schema format.

Single acquisition path: the HuggingFace dataset ``everycure/disease-list``
(via the ``datasets`` library). If it is unavailable, ingest fails loudly
rather than silently degrading to any legacy local table.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path("kb/diseases/disease_list.yaml")


def _strip_control(s) -> str:
    """Strip control characters from a scalar string-ish value."""
    text = str(s)
    return "".join(c for c in text if c == "\n" or c == "\t" or ord(c) >= 32)


def _is_scalar_na(val) -> bool:
    """True only when ``val`` is a scalar missing value.

    ``pd.isna`` returns an element-wise array for array-like input, which is
    ambiguous in a boolean context; only trust it when it yields a plain bool.
    """
    if val is None:
        return True
    missing = pd.isna(val)
    return bool(missing) if isinstance(missing, bool) else False


def _clean(val) -> str:
    """Clean a scalar cell to a string; empty string for missing/NaN."""
    if _is_scalar_na(val):
        return ""
    return _strip_control(val)


def _split_semi(val: str) -> list[str]:
    """Split a semicolon-separated string into a list, stripping whitespace."""
    if not val:
        return []
    return [s.strip() for s in val.split(";") if s.strip()]


def _to_str_list(val) -> list[str]:
    """Normalize a cell into a list of clean, non-empty strings.

    Upstream ``everycure/disease-list`` returns some columns (e.g. ``synonyms``)
    as native sequences and others (``subsets``, ``crossreferences``) as
    semicolon-separated strings; handle both so an upstream shape change on any
    of them does not crash the ingest.
    """
    if val is None:
        return []
    if isinstance(val, (list, tuple, set, np.ndarray)):
        return [c for c in (_clean(x) for x in val) if c]
    if _is_scalar_na(val):
        return []
    return _split_semi(_strip_control(val))


# Boolean flag columns expected in the HF dataset
_HF_BOOLEAN_COLUMNS = [
    "is_clingen",
    "is_cancer_or_benign_tumor",
    "is_rare",
    "is_gard_rare",
    "is_nord_rare",
    "is_ordo_subtype",
    "is_hereditary_disease",
    "is_chromosomal_disorder",
    "is_disorder_of_development",
    "is_musculoskeletal",
]


def _load_from_huggingface() -> pd.DataFrame | None:
    """Load the disease list via the datasets library."""
    try:
        from datasets import load_dataset

        logger.info("Loading everycure/disease-list from HuggingFace datasets...")
        ds = load_dataset("everycure/disease-list", split="train")
        df = ds.to_pandas()
        logger.info("Loaded %d rows from HuggingFace (everycure/disease-list)", len(df))
        return df
    except ImportError as e:
        raise RuntimeError(
            "The `datasets` library is required to load the disease list from "
            "HuggingFace (everycure/disease-list) but is not installed. Install "
            "project dependencies (e.g. `uv sync`) and re-run."
        ) from e
    except Exception as e:
        raise RuntimeError(
            "Failed to load the disease list from HuggingFace "
            "(everycure/disease-list). Check network access and dataset "
            f"availability, then re-run. Underlying error: {e!r}"
        ) from e


def _build_disease_from_hf_row(row: pd.Series, df_columns: list[str]) -> dict | None:
    """Build a disease record from a HuggingFace dataset row.

    The HF dataset uses 'id' and 'name' columns (vs. 'category_class' and 'label'
    in the legacy TSV). Maps accordingly.
    """
    # Primary ID: 'id' column in HF dataset (MONDO CURIE)
    category_class = _clean(row.get("id", ""))
    if not category_class:
        # Fallback to legacy column name if somehow mixed
        category_class = _clean(row.get("category_class", ""))
    if not category_class:
        return None

    # Label: 'name' column in HF dataset
    label = ""
    if "name" in df_columns:
        label = _clean(row.get("name", ""))
    if not label and "label" in df_columns:
        label = _clean(row.get("label", ""))

    definition = _clean(row.get("definition", ""))

    # Synonyms / subsets / crossreferences: upstream returns these as either a
    # native list/array (synonyms, as of 2026-08) or a semicolon-separated
    # string; _to_str_list handles both shapes.
    synonyms = _to_str_list(row.get("synonyms", ""))
    subsets = _to_str_list(row.get("subsets", ""))
    crossreferences = _to_str_list(row.get("crossreferences", ""))

    disease: dict = {
        "category_class": category_class,
        "label": label,
        "definition": definition,
        "synonyms": synonyms,
        "subsets": subsets,
        "crossreferences": crossreferences,
    }

    # Boolean flag columns from HF dataset
    for flag_col in _HF_BOOLEAN_COLUMNS:
        if flag_col in df_columns:
            flag_val = row.get(flag_col)
            if pd.notna(flag_val):
                disease[flag_col] = bool(flag_val)

    return disease


def ingest_disease_list() -> Path:
    """Ingest the disease list from the HuggingFace everycure/disease-list dataset."""
    df = _load_from_huggingface()

    logger.info("Processing %d diseases (columns: %s)", len(df), list(df.columns))

    df_columns = list(df.columns)
    diseases = []

    for _, row in df.iterrows():
        disease = _build_disease_from_hf_row(row, df_columns)
        if disease is not None:
            diseases.append(disease)

    # Disease-list entries arrive pre-grounded with canonical MONDO ids. They are the
    # canonical target namespace already, so Stage-2 normalization would be a no-op
    # (MONDO->MONDO). We deliberately do NOT attach a ``normalization`` object here
    # because ``disease_list.yaml`` is schema-validated as ``DiseaseList`` and the
    # ``Disease`` class has no such slot. Obsolete-MONDO replacement, if ever needed,
    # belongs in a dedicated MONDO-obsolescence pass, not here.
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.dump(
        {"diseases": diseases},
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    )
    content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
    with open(OUTPUT_PATH, "w") as f:
        f.write(content)

    logger.info("Wrote %d diseases to %s", len(diseases), OUTPUT_PATH)
    return OUTPUT_PATH


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_disease_list()
