"""Compile research pipeline output into the merged product.

Reads all kb/research/*.yaml files and compiles into a unified list.
"""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

KB_RESEARCH_DIR = Path("kb/research")
OUTPUT_PATH = Path("products/research_list.yaml")


def compile_research() -> None:
    """Compile all research association YAML files."""
    associations = []

    for yaml_file in sorted(KB_RESEARCH_DIR.glob("*.yaml")):
        try:
            with open(yaml_file) as f:
                records = yaml.safe_load(f)
            if not records:
                continue
            if isinstance(records, dict):
                records = records.get("associations", [records])
            associations.extend(records)
        except Exception:
            logger.warning("Failed to read %s", yaml_file)

    if associations:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w") as f:
            yaml.dump(
                {"associations": associations},
                f,
                default_flow_style=False,
                allow_unicode=True,
            )
        logger.info("Compiled %d research associations -> %s", len(associations), OUTPUT_PATH)
    else:
        logger.info("No research associations found")


def main():
    logging.basicConfig(level=logging.INFO)
    compile_research()


if __name__ == "__main__":
    main()
