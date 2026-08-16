"""Disease merge: reads kb/diseases/ and writes products/disease_list.yaml.

Currently there is only one source (HuggingFace everycure/disease-list),
so this is a pass-through that copies the data into products/ where it
gets validated by the standard schema/term validation steps.
"""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

KB_DIR = Path("kb/diseases")
OUTPUT_PATH = Path("products/disease_list.yaml")


def merge_diseases(
    kb_dir: Path = KB_DIR,
    output_path: Path = OUTPUT_PATH,
) -> list[dict]:
    """Merge disease records from kb/ into a single product file.

    Args:
        kb_dir: Directory containing disease YAML files.
        output_path: Path to write the merged disease list.

    Returns:
        List of merged disease records.
    """
    all_diseases: list[dict] = []

    for yaml_file in sorted(kb_dir.glob("*.yaml")):
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
            if not data:
                continue
            if isinstance(data, dict) and "diseases" in data:
                all_diseases.extend(data["diseases"])
            elif isinstance(data, list):
                all_diseases.extend(data)
        except Exception:
            logger.warning("Failed to read %s", yaml_file)

    logger.info("Loaded %d diseases from %s", len(all_diseases), kb_dir)

    # Deduplicate by category_class (primary ID)
    seen: dict[str, dict] = {}
    for disease in all_diseases:
        did = disease.get("category_class", "")
        if did and did not in seen:
            seen[did] = disease
    diseases = sorted(seen.values(), key=lambda d: d.get("category_class", ""))

    logger.info("Deduplicated to %d unique diseases", len(diseases))

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.dump(
        {"diseases": diseases},
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    )
    content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
    with open(output_path, "w") as f:
        f.write(content)

    logger.info("Merged %d diseases -> %s", len(diseases), output_path)
    return diseases


def main():
    logging.basicConfig(level=logging.INFO)
    merge_diseases()


if __name__ == "__main__":
    main()
