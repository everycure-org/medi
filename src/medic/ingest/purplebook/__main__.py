"""Purple Book drug ingest — FDA biologics license database.

Single acquisition path: download the raw CSV from FDA and ground via the
grounding cascade. If the download fails, ingest fails loudly rather than
silently degrading to any legacy pre-grounded table.
"""

import logging
from pathlib import Path

import pandas as pd
import typer

from medic.grounding.cache import GroundingCache
from medic.grounding.factory import get_grounding_service
from medic.ingest.common import (
    download_file,
    load_source_urls,
    reformat_date,
    standardize_columns,
    write_drug_source_yaml,
    write_grounding_report,
)
from medic.ingest.grounding import ground_records

logger = logging.getLogger(__name__)

app = typer.Typer()

# Marketing status priority: higher index = more permissive
_STATUS_PRIORITY = {"DISCONTINUED": 0, "DISCN": 0, "RX": 1, "OTC": 2}


def get_marketing_status(statuses: list[str]) -> str:
    if not statuses:
        return "NONE"
    best = max(statuses, key=lambda s: _STATUS_PRIORITY.get(s.upper(), -1))
    best_upper = best.upper()
    if best_upper in ("DISCONTINUED", "DISCN"):
        return "DISCN"
    return best_upper


def parse_purplebook_raw(raw_path: Path) -> list[dict]:
    """Parse raw Purple Book CSV from FDA.

    The FDA Purple Book CSV has a few preamble rows before the real header.
    We auto-detect the header row by scanning for "Proper Name". Captures the
    `BLA Number` column so downstream consumers can build deep-linked URLs.
    """
    # Auto-detect header row: try a few skiprows values
    df = None
    for skip in (0, 1, 2, 3, 4, 5):
        try:
            candidate = pd.read_csv(raw_path, dtype=str, skiprows=skip)
        except Exception:
            continue
        if "Proper Name" in candidate.columns:
            df = candidate
            break
    if df is None:
        raise ValueError("Could not find Purple Book header row containing 'Proper Name'")

    col_mapping = {
        "Proper Name": "source_name",
        "Approval Date": "approval_date",
        "Marketing Status": "marketing_status_raw",
        "BLA Number": "bla_number",
    }
    # Only "Proper Name" is strictly required — others are optional
    if "Proper Name" not in df.columns:
        raise ValueError("Missing required column: Proper Name")

    df = standardize_columns(df, col_mapping)
    if "approval_date" in df.columns:
        df["approval_date"] = df["approval_date"].apply(reformat_date)
    if "bla_number" not in df.columns:
        df["bla_number"] = ""
    df = df.dropna(subset=["source_name"])
    df = df[df["source_name"].str.strip() != ""]

    grouped = df.groupby("source_name", sort=False)
    records = []
    for drug_name, group in grouped:
        dates = [d for d in group.get("approval_date", []) if d] if "approval_date" in group.columns else []
        earliest_date = min(dates) if dates else ""
        statuses = (
            group["marketing_status_raw"].dropna().tolist()
            if "marketing_status_raw" in group.columns else []
        )
        marketing_status = get_marketing_status(statuses)
        # Pipe-join distinct BLA numbers (Purple Book often has multiple per ingredient)
        blas = sorted({
            str(b).strip() for b in group["bla_number"].dropna().tolist()
            if str(b).strip() and str(b).strip().lower() != "nan"
        })
        bla_number = "|".join(blas)
        records.append({
            "source": "PURPLEBOOK",
            "source_name": str(drug_name),
            "approval_date": earliest_date,
            "marketing_status_usa": marketing_status,
            "bla_number": bla_number,
        })
    logger.info("Parsed %d unique drugs from Purple Book (raw)", len(records))
    return records


@app.command()
def main(
    grounding_backend: str = typer.Option("lexical", help="Grounding backend to use"),
    force_download: bool = typer.Option(False, "--force-download", help="Force re-download"),
) -> None:
    """Ingest FDA Purple Book data.

    The FDA Purple Book CSV is the single acquisition path. If it cannot be
    downloaded, ingest fails loudly — it does not degrade to any legacy
    pre-grounded table.
    """
    logging.basicConfig(level=logging.INFO)

    source_urls = load_source_urls()
    pb_config = source_urls.get("purplebook", {})
    url = pb_config.get("url", "")
    if not url:
        raise RuntimeError(
            "Purple Book source URL is not configured. Set `purplebook.url` in "
            "conf/source_urls.yaml to the FDA Purple Book CSV download URL."
        )

    dest_path = Path("cache/downloads/purplebook/purplebook.csv")
    try:
        raw_downloaded = download_file(url, dest_path, force=force_download)
    except Exception as e:
        raise RuntimeError(
            "Purple Book raw CSV could not be downloaded from "
            f"{url}. Fix the URL in conf/source_urls.yaml or check network "
            f"access, then re-run. Underlying error: {e!r}"
        ) from e

    if not (raw_downloaded and raw_downloaded.exists()):
        raise RuntimeError(
            f"Purple Book download reported success but no file is present at "
            f"{dest_path}. Re-run with --force-download or check the source URL "
            "in conf/source_urls.yaml."
        )

    records = parse_purplebook_raw(raw_downloaded)
    grounding_service = get_grounding_service(grounding_backend)
    cache = GroundingCache()
    grounded_records, report = ground_records(
        records, grounding_service, cache, source_name="purplebook"
    )

    output_dir = Path("kb/drugs/purplebook")
    write_drug_source_yaml(grounded_records, output_dir, "purplebook")
    write_grounding_report(report, output_dir, "purplebook")
    logger.info("Purple Book ingest complete: %d drugs", report["total_drugs"])


if __name__ == "__main__":
    app()
