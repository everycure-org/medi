"""PVLens adverse event ingest.

PVLens (https://github.com/GSK-Global-Safety/pvlens) extracts adverse events,
indications, and black box warnings from FDA SPL labels. It outputs SQL files
with MedDRA-coded adverse events and RxNorm-coded drugs.

PVLens key tables:
- PRODUCT_AE: (PRODUCT_ID, MEDDRA_ID, LABEL_DATE, WARNING, BLACKBOX, EXACT_MATCH)
- MEDDRA: (CODE, TERM, TTY, AUI, CUI)
- NDC_CODE: (NDC_CODE, PRODUCT_NAME)

Prerequisites: Run PVLens separately, place CSV exports in data/pvlens/.
"""

import logging
from pathlib import Path

import pandas as pd
import yaml

from medic.grounding import get_grounding_service
from medic.mention import mint_mention_id

logger = logging.getLogger(__name__)

PVLENS_DATA_DIR = Path("data/pvlens")
OUTPUT_DIR = Path("kb/adverse_events/pvlens")


def _clean(val) -> str:
    if pd.isna(val):
        return ""
    return "".join(c for c in str(val) if c == "\n" or c == "\t" or ord(c) >= 32)


def ingest_pvlens_csv(ae_file=None, grounding_backend="lexical"):
    if ae_file is None:
        ae_file = PVLENS_DATA_DIR / "product_ae.csv"
    if not ae_file.exists():
        logger.warning("PVLens data not found at %s. Run PVLens first.", ae_file)
        return

    df = pd.read_csv(ae_file)
    logger.info("Read %d PVLens AE records", len(df))
    grounding = get_grounding_service(grounding_backend)
    records = []
    drug_cache = {}

    for _, row in df.iterrows():
        drug_name = _clean(row.get("product_name", ""))
        meddra_term = _clean(row.get("meddra_term", ""))
        meddra_code = _clean(row.get("meddra_code", ""))
        if not drug_name or not meddra_term:
            continue
        if drug_name not in drug_cache:
            result = grounding.ground_drug_best(
                drug_name, mention_id=mint_mention_id(drug_name, "drugs"))
            drug_cache[drug_name] = (result.id, result.label) if result else ("", drug_name)
        drug_id, drug_label = drug_cache[drug_name]

        is_blackbox = bool(row.get("blackbox", False))
        is_warning = bool(row.get("warning", False))
        section = "BLACK_BOX_WARNING" if is_blackbox else ("WARNINGS_AND_PRECAUTIONS" if is_warning else "ADVERSE_REACTIONS")

        records.append({
            "source": "PVLENS",
            "source_drug_name": drug_name,
            "normalized_drug_id": drug_id,
            "normalized_drug_label": drug_label,
            "adverse_event_term": meddra_term,
            "adverse_event_meddra_id": f"MedDRA:{meddra_code}" if meddra_code else "",
            "label_section": section,
            "evidence": [{"source_type": "REGULATORY", "jurisdiction": "USA",
                          "reference": f"DailyMed:{_clean(row.get('spl_setid', ''))}",
                          "support": "SUPPORT", "confidence": "HIGH"}],
        })

    if records:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / "pvlens_ae.yaml"
        content = yaml.dump(records, default_flow_style=False, allow_unicode=True, width=1000)
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        with open(output_path, "w") as f:
            f.write(content)
        logger.info("Wrote %d PVLens AE records to %s", len(records), output_path)


def main():
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="PVLens adverse event ingest")
    parser.add_argument("--grounding-backend", default="lexical")
    parser.add_argument("--ae-file", type=Path, default=None)
    args = parser.parse_args()
    ingest_pvlens_csv(ae_file=args.ae_file, grounding_backend=args.grounding_backend)


if __name__ == "__main__":
    main()
