"""Orange Book drug ingest — reference implementation for all drug sources."""

import logging
from pathlib import Path

import pandas as pd
import typer

from medic.grounding.cache import GroundingCache
from medic.grounding.factory import get_grounding_service
from medic.ingest.common import (
    download_and_extract_zip,
    load_source_urls,
    reformat_date,
    write_drug_source_yaml,
    write_grounding_report,
)
from medic.ingest.grounding import ground_records

logger = logging.getLogger(__name__)

app = typer.Typer()

# Marketing status priority: higher index = more permissive
_STATUS_PRIORITY = {"DISCONTINUED": 0, "RX": 1, "OTC": 2}


def get_marketing_status(statuses: list[str]) -> str:
    """Return the most permissive marketing status from a list.

    Priority: OTC > RX > DISCONTINUED.
    Maps DISCONTINUED -> DISCN for schema compliance.
    Returns 'NONE' if the list is empty.
    """
    if not statuses:
        return "NONE"

    best = max(statuses, key=lambda s: _STATUS_PRIORITY.get(s.upper(), -1))
    best_upper = best.upper()

    if best_upper == "DISCONTINUED":
        return "DISCN"
    return best_upper


def parse_orangebook(raw_path: Path) -> list[dict]:
    """Parse Orange Book products.txt and return standardized records.

    Reads a tab-delimited file, groups by ingredient, and computes
    earliest approval date and most permissive marketing status per ingredient.

    Args:
        raw_path: Path to the products.txt file.

    Returns:
        List of dicts with keys: source, source_name, approval_date, marketing_status_usa.
    """
    # FDA Orange Book uses ~ as delimiter
    df = pd.read_csv(raw_path, sep="~", dtype=str)
    # Fall back to tab if ~ didn't produce expected columns
    if "Ingredient" not in df.columns:
        df = pd.read_csv(raw_path, sep="\t", dtype=str)

    # Ensure required columns exist
    required = {"Ingredient", "Approval_Date", "Type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Reformat all dates
    df["approval_date_fmt"] = df["Approval_Date"].apply(reformat_date)

    # Group by ingredient
    grouped = df.groupby("Ingredient", sort=False)

    records = []
    for ingredient, group in grouped:
        # Earliest approval date (lexicographic sort of YYYYMMDD works)
        dates = [d for d in group["approval_date_fmt"] if d]
        earliest_date = min(dates) if dates else ""

        # Most permissive marketing status
        statuses = group["Type"].dropna().tolist()
        marketing_status = get_marketing_status(statuses)

        # NDA/ANDA application numbers (sorted, pipe-joined)
        appl_nos = sorted(set(
            str(a).strip() for a in group.get("Appl_No", []) if pd.notna(a) and str(a).strip()
        ))
        application_number = "|".join(appl_nos) if appl_nos else ""

        records.append({
            "source": "ORANGEBOOK",
            "source_name": str(ingredient),
            "approval_date": earliest_date,
            "marketing_status_usa": marketing_status,
            "application_number": application_number,
        })

    logger.info("Parsed %d unique ingredients from Orange Book", len(records))
    return records


@app.command()
def main(
    grounding_backend: str = typer.Option("lexical", help="Grounding backend to use"),
    force_download: bool = typer.Option(False, "--force-download", help="Force re-download of source data"),
) -> None:
    """Ingest FDA Orange Book data: download, parse, ground, and write output."""
    logging.basicConfig(level=logging.INFO)

    # Load URL config
    source_urls = load_source_urls()
    ob_config = source_urls.get("orangebook", {})
    url = ob_config.get("url", "https://www.fda.gov/media/76860/download")
    target_file = ob_config.get("target_file", "products.txt")

    # Download and extract
    dest_dir = Path("data/raw/orangebook")
    raw_path = download_and_extract_zip(url, dest_dir, target_file, force=force_download)

    # Parse raw data
    records = parse_orangebook(raw_path)

    # Ground records
    grounding_service = get_grounding_service(grounding_backend)
    cache = GroundingCache()
    grounded_records, report = ground_records(
        records, grounding_service, cache, source_name="orangebook"
    )

    # Write outputs
    output_dir = Path("kb/drugs/orangebook")
    write_drug_source_yaml(grounded_records, output_dir, "orangebook")
    write_grounding_report(report, output_dir, "orangebook")

    logger.info(
        "Orange Book ingest complete: %d drugs, %d auto-accepted, %d unresolved",
        report["total_drugs"],
        report["auto_accepted"],
        report["unresolved"],
    )


if __name__ == "__main__":
    app()
