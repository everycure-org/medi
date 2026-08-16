"""China drug ingest — CDE (Center for Drug Evaluation) / NMPA approvals.

Reads the manually-provided CDE scrape at the stable path
``background/cder_drugs_final_all.csv`` (2 columns: Chinese ``drug_name`` +
``approval_date``), translates each unique Chinese name to English via the
shared translation stage (DeepL through the ``babelon`` translator service,
cached in ``mappings/drug_translation.babelon.tsv``), and grounds the English
name through the shared grounding pipeline so China drugs resolve to canonical
ChEBI CURIEs exactly like every other source.

The scrape carries **no indication text**, so China contributes a **drug list
only** (``source: CHINA``) — no indications or contraindications (same as
Russia). See ``README.md``.

Source isolation (invariant I-1): China emits evidence only for the CHINA
jurisdiction. The scrape carries no cross-jurisdiction flag columns; none are
synthesised.
"""

import logging
from pathlib import Path

import typer

from medic.grounding.cache import GroundingCache
from medic.grounding.factory import get_grounding_service
from medic.ingest.china.locate_source import CDE_CSV_PATH, locate_cde_csv
from medic.ingest.china.parse_cde import parse_cde_csv
from medic.ingest.common import (
    should_skip_expensive_calls,
    write_drug_source_yaml,
    write_grounding_report,
)
from medic.ingest.grounding import ground_records
from medic.ingest.sanity import check_row_floor, record_source
from medic.translation import translate_records

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def main(
    grounding_backend: str = typer.Option("lexical", help="Grounding backend to use"),
    cde_csv: Path = typer.Option(
        CDE_CSV_PATH,
        help="Path to the manually-provided CDE scrape CSV.",
    ),
    limit: int = typer.Option(
        0,
        "--limit",
        help="If >0, only ingest the first N unique Chinese drug names "
        "(validation aid — keeps LLM translation volume small).",
    ),
    force_download: bool = typer.Option(
        False, "--force-download", help="Unused for China (manual-acquisition source)."
    ),
) -> None:
    """Ingest China CDE drug data: locate scrape, translate, ground, write output."""
    logging.basicConfig(level=logging.INFO)

    # "Fetch" for China is just locating the manually-provided scrape; this
    # raises a clear, actionable error if the file is missing.
    csv_path = locate_cde_csv(cde_csv)

    if should_skip_expensive_calls():
        logger.warning(
            "MEDIC_SKIP_EXPENSIVE_CALLS set: Chinese names will NOT be translated "
            "to English and therefore will NOT ground — use this only to "
            "validate parsing, not to produce the real drug list."
        )

    records = parse_cde_csv(csv_path, limit=limit or None)

    # Sanity: refuse a truncated/stale CDE export before the expensive translation,
    # and stamp the source fingerprint into the manifest for provenance.
    check_row_floor("china", len(records), limited=bool(limit))
    record_source("china", str(csv_path), len(records))

    # Stage-0 translation: Chinese -> English via DeepL (babelon), cached in the
    # Babelon store. Overwrites ``source_name`` with English and attaches the
    # ``translation`` object + MEDICNE ``mention_id`` to each record.
    translate_records(records, "zh")

    # Ground the (translated) English name through the shared pipeline — same
    # call as every other source.
    grounding_service = get_grounding_service(grounding_backend)
    cache = GroundingCache()
    grounded_records, report = ground_records(
        records, grounding_service, cache, source_name="china"
    )

    output_dir = Path("kb/drugs/china")
    write_drug_source_yaml(grounded_records, output_dir, "china")
    write_grounding_report(report, output_dir, "china")

    logger.info(
        "China ingest complete: %d drugs, %d auto-accepted, %d unresolved",
        report["total_drugs"],
        report["auto_accepted"],
        report["unresolved"],
    )


if __name__ == "__main__":
    app()
