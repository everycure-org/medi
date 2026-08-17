"""Batch research pipeline for processing multiple diseases.

Usage:
    python -m medic.research.batch --count 10
"""

import logging

from medic.research.curate import (
    _get_next_disease,
    _load_progress,
    _save_progress,
    curate_disease,
    write_research_yaml,
)

logger = logging.getLogger(__name__)


def run_batch(count: int = 10, max_results: int = 20) -> None:
    """Run curation for multiple diseases in sequence.

    Args:
        count: Number of diseases to process.
        max_results: Max PubMed results per disease.
    """
    curated = _load_progress()
    processed = 0

    for i in range(count):
        d = _get_next_disease(curated)
        if d is None:
            logger.info("No more uncurated diseases in priority list.")
            break

        disease_id = d["mondo_id"]
        disease_label = d["label"]

        logger.info("=== Disease %d/%d: %s (%s) ===", i + 1, count, disease_label, disease_id)

        associations = curate_disease(
            disease_id, disease_label, max_pubmed_results=max_results
        )

        if associations:
            write_research_yaml(disease_id, associations)

        curated.add(disease_id)
        _save_progress(curated)
        processed += 1

    logger.info("Batch complete: processed %d diseases", processed)


def main():
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="MeDIC batch research curation")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of diseases to process"
    )
    parser.add_argument(
        "--max-results", type=int, default=20, help="Max PubMed results per disease"
    )
    args = parser.parse_args()

    run_batch(count=args.count, max_results=args.max_results)


if __name__ == "__main__":
    main()
