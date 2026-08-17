#!/usr/bin/env python3
"""Batch deep research for MeDIC priority diseases.

Reads the priority disease list, checks which diseases already have deep research
outputs, and runs deep-research-client for uncovered diseases.

Usage:
    python scripts/batch_deep_research.py --provider falcon --count 5
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

import pandas as pd
from medic.research.curate import PRIORITY_DISEASES_PATH

logger = logging.getLogger(__name__)

PRIORITY_DISEASES_TSV = PRIORITY_DISEASES_PATH


def load_priority_diseases(tsv_path: Path) -> list[dict]:
    """Load priority diseases from TSV."""
    df = pd.read_csv(tsv_path, sep="\t")
    diseases = []
    for _, row in df.iterrows():
        mondo_id = str(row.get("mondo id", "")).strip()
        label = str(row.get("mondo label", "")).strip()
        if mondo_id and label:
            diseases.append({"mondo_id": mondo_id, "label": label})
    return diseases


def get_existing_research(research_dir: Path, provider: str) -> set[str]:
    """Get set of disease labels that already have research for this provider."""
    existing = set()
    if not research_dir.exists():
        return existing
    for f in research_dir.glob(f"*-deep-research-{provider}.md"):
        # Filename: Disease_Name-deep-research-provider.md
        name = f.name.replace(f"-deep-research-{provider}.md", "")
        existing.add(name.replace("_", " ").lower())
    return existing


def run_research(
    disease_name: str,
    mondo_id: str,
    provider: str,
    research_dir: Path,
    templates_dir: Path,
    extra_args: list[str],
) -> bool:
    """Run deep-research-client for a single disease. Returns True on success."""
    safe_name = disease_name.replace(" ", "_")
    output_file = research_dir / f"{safe_name}-deep-research-{provider}.md"

    provider_arg = ["--use-cborg"] if provider == "cborg" else ["--provider", provider]

    cmd = [
        "uv", "run", "--group", "research",
        "deep-research-client", "research",
        "--template", str(templates_dir / "drug_disease_research.md"),
        "--var", f"disease_name={disease_name}",
        "--var", f"mondo_id={mondo_id}",
        *provider_arg,
        "--output", str(output_file),
        "--separate-citations", f"{output_file}.citations.md",
        *extra_args,
    ]

    logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            logger.info("Success: %s -> %s", disease_name, output_file)
            return True
        else:
            logger.warning("Failed: %s (exit %d): %s", disease_name, result.returncode, result.stderr[:200])
            return False
    except subprocess.TimeoutExpired:
        logger.warning("Timeout: %s", disease_name)
        return False


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Batch deep research for MeDIC diseases")
    parser.add_argument("--provider", required=True, help="Research provider (falcon, perplexity, cyberian, etc.)")
    parser.add_argument("--count", type=int, default=5, help="Number of diseases to process")
    parser.add_argument("--research-dir", type=Path, default=Path("research"))
    parser.add_argument("--templates-dir", type=Path, default=Path("templates"))
    parser.add_argument("--only", nargs="*", help="Process only these disease labels")
    parser.add_argument("--include-existing", action="store_true", help="Re-run even if output exists")
    args, extra_args = parser.parse_known_args()

    if not PRIORITY_DISEASES_TSV.exists():
        logger.error("Priority disease file not found: %s", PRIORITY_DISEASES_TSV)
        sys.exit(1)

    diseases = load_priority_diseases(PRIORITY_DISEASES_TSV)
    logger.info("Loaded %d priority diseases", len(diseases))

    existing = set() if args.include_existing else get_existing_research(args.research_dir, args.provider)
    if existing:
        logger.info("Found %d diseases with existing %s research", len(existing), args.provider)

    # Filter to diseases that need processing
    to_process = []
    for d in diseases:
        if args.only and d["label"] not in args.only:
            continue
        if d["label"].lower() in existing:
            continue
        to_process.append(d)
        if len(to_process) >= args.count:
            break

    logger.info("Will process %d diseases with provider: %s", len(to_process), args.provider)

    success = 0
    failed = 0
    for i, d in enumerate(to_process):
        logger.info("=== Disease %d/%d: %s (%s) ===", i + 1, len(to_process), d["label"], d["mondo_id"])
        ok = run_research(
            disease_name=d["label"],
            mondo_id=d["mondo_id"],
            provider=args.provider,
            research_dir=args.research_dir,
            templates_dir=args.templates_dir,
            extra_args=extra_args,
        )
        if ok:
            success += 1
        else:
            failed += 1

    logger.info("Batch complete: %d success, %d failed out of %d", success, failed, len(to_process))


if __name__ == "__main__":
    main()
