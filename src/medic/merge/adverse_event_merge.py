"""Adverse event merge: combines AE source records into a unified list.

Reads all kb/adverse_events/<source>/*.yaml files, deduplicates,
and writes products/adverse_event_list.yaml.
"""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

KB_AE_DIR = Path("kb/adverse_events")
OUTPUT_PATH = Path("products/adverse_event_list.yaml")


def merge_adverse_events() -> None:
    """Merge adverse event source records from all sources."""
    associations: dict[str, dict] = {}

    for source_dir in sorted(KB_AE_DIR.iterdir()):
        if not source_dir.is_dir():
            continue
        for yaml_file in sorted(source_dir.glob("*.yaml")):
            try:
                with open(yaml_file) as f:
                    records = yaml.safe_load(f)
                if not records:
                    continue
                if isinstance(records, dict):
                    records = [records]
                for record in records:
                    key = _make_key(record)
                    if not key:
                        continue
                    if key not in associations:
                        associations[key] = _init_association(record)
                    else:
                        _merge_into(associations[key], record)
            except Exception:
                logger.warning("Failed to read %s", yaml_file)

    ae_list = sorted(associations.values(), key=lambda a: a.get("drug_id", ""))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        yaml.dump({"associations": ae_list}, f, default_flow_style=False, allow_unicode=True)

    logger.info("Merged %d adverse event associations -> %s", len(ae_list), OUTPUT_PATH)


def _make_key(record: dict) -> str | None:
    drug_id = record.get("normalized_drug_id", "")
    ae_term = record.get("adverse_event_term", "")
    if not drug_id or not ae_term:
        return None
    return f"{drug_id}|{ae_term}"


def _init_association(record: dict) -> dict:
    return {
        "drug_id": record.get("normalized_drug_id", ""),
        "drug_label": record.get("normalized_drug_label", ""),
        "adverse_event_id": record.get("adverse_event_meddra_id", ""),
        "adverse_event_label": record.get("adverse_event_term", ""),
        "adverse_event_hpo_id": record.get("adverse_event_hpo_id", ""),
        "adverse_event_hpo_label": record.get("adverse_event_hpo_label", ""),
        "label_section": record.get("label_section", ""),
        "frequency": record.get("frequency", ""),
        "sources": [record.get("source", "")],
        "evidence": record.get("evidence", []),
    }


def _merge_into(existing: dict, record: dict) -> None:
    source = record.get("source", "")
    if source and source not in existing.get("sources", []):
        existing.setdefault("sources", []).append(source)
    existing.setdefault("evidence", []).extend(record.get("evidence", []))


def main():
    logging.basicConfig(level=logging.INFO)
    merge_adverse_events()


if __name__ == "__main__":
    main()
