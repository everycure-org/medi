#!/usr/bin/env python3
"""Parse all deep research markdown reports into kb/research/ YAML files.

Scans the research/ directory for deep research reports, matches them to
diseases in the priority list, and writes structured ResearchAssociation
YAML files to kb/research/.

Usage:
    python scripts/parse_all_deep_research.py
"""

import csv
import logging
import re
from pathlib import Path

import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent))
from parse_deep_research import parse_deep_research_file
from medic.research.curate import PRIORITY_DISEASES_PATH

logger = logging.getLogger(__name__)

RESEARCH_DIR = Path("research")
KB_RESEARCH_DIR = Path("kb/research")
PRIORITY_DISEASES_TSV = PRIORITY_DISEASES_PATH


def load_priority_diseases() -> dict[str, str]:
    """Load disease_id -> disease_label mapping from priority TSV."""
    diseases = {}
    with open(PRIORITY_DISEASES_TSV, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            disease_id = row["mondo id"].strip()
            disease_label = row["mondo label"].strip()
            if disease_id:
                diseases[disease_id] = disease_label
    return diseases


def find_disease_for_report(md_path: Path, diseases: dict[str, str]) -> tuple[str, str] | None:
    """Match a deep research markdown file to a disease.

    Checks the YAML frontmatter for mondo_id, or matches by filename.
    """
    text = md_path.read_text()

    # Check frontmatter for mondo_id
    m = re.search(r"mondo_id:\s*(\S+)", text)
    if m:
        mondo_id = m.group(1).strip()
        if mondo_id in diseases:
            return mondo_id, diseases[mondo_id]

    # Match by filename -> disease label
    # Filename pattern: Disease_Name-deep-research-provider.md
    stem = md_path.stem  # e.g., "hemophilia_A-deep-research-perplexity"
    name_part = re.sub(r"-deep-research-\w+$", "", stem)
    name_normalized = name_part.replace("_", " ").replace("-", " ").lower()

    for disease_id, disease_label in diseases.items():
        label_normalized = disease_label.replace(",", " ").replace("/", " ").lower()
        # Fuzzy match: check if significant words overlap
        name_words = set(name_normalized.split())
        label_words = set(label_normalized.split())
        if name_words and label_words:
            overlap = name_words & label_words
            if len(overlap) >= min(2, len(label_words)):
                return disease_id, disease_label

    return None


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if not PRIORITY_DISEASES_TSV.exists():
        logger.error("Priority disease file not found: %s", PRIORITY_DISEASES_TSV)
        return

    diseases = load_priority_diseases()
    logger.info("Loaded %d priority diseases", len(diseases))

    # Find all deep research reports
    md_files = sorted(RESEARCH_DIR.glob("*-deep-research-*.md"))
    md_files = [f for f in md_files if not f.name.endswith(".citations.md")]

    if not md_files:
        logger.info("No deep research reports found in %s", RESEARCH_DIR)
        return

    logger.info("Found %d deep research reports", len(md_files))

    # Group by disease (multiple providers per disease)
    disease_reports: dict[str, list[Path]] = {}
    unmatched = []

    for md_path in md_files:
        match = find_disease_for_report(md_path, diseases)
        if match:
            disease_id, _ = match
            disease_reports.setdefault(disease_id, []).append(md_path)
        else:
            unmatched.append(md_path)

    if unmatched:
        logger.warning("Could not match %d reports to diseases:", len(unmatched))
        for p in unmatched:
            logger.warning("  %s", p.name)

    # Parse and write YAML for each disease
    KB_RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    total_associations = 0

    for disease_id, reports in sorted(disease_reports.items()):
        disease_label = diseases[disease_id]
        all_associations = []
        seen_drugs: set[str] = set()

        for md_path in reports:
            associations = parse_deep_research_file(md_path, disease_id, disease_label)
            for assoc in associations:
                drug_key = assoc["drug_label"].lower()
                if drug_key not in seen_drugs:
                    seen_drugs.add(drug_key)
                    all_associations.append(assoc)
                else:
                    # Merge evidence from additional provider
                    for existing in all_associations:
                        if existing["drug_label"].lower() == drug_key:
                            existing["evidence"].extend(assoc["evidence"])
                            break

        if not all_associations:
            continue

        filename = f"{disease_id.replace(':', '_')}.yaml"
        output_path = KB_RESEARCH_DIR / filename
        content = yaml.dump(
            {"associations": all_associations},
            default_flow_style=False, allow_unicode=True, width=120,
        )
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        output_path.write_text(content)

        total_associations += len(all_associations)
        logger.info(
            "%s (%s): %d drugs from %d reports -> %s",
            disease_label, disease_id, len(all_associations), len(reports), output_path,
        )

    logger.info("Total: %d associations across %d diseases", total_associations, len(disease_reports))


if __name__ == "__main__":
    main()
