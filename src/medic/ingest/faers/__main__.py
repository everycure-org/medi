"""FAERS (FDA Adverse Event Reporting System) ingest.

Downloads and parses FAERS quarterly data files (dollar-sign delimited),
aggregates drug-event pairs, computes PRR, writes to kb/adverse_events/faers/.

DRUG file columns: primaryid, caseid, drug_seq, role_cod, drugname, prod_ai, ...
REAC file columns: primaryid, caseid, pt (MedDRA preferred term), ...

See: https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html
"""

import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import yaml

from medic.grounding import get_grounding_service
from medic.mention import mint_mention_id

logger = logging.getLogger(__name__)

FAERS_DATA_DIR = Path("data/faers")
OUTPUT_DIR = Path("kb/adverse_events/faers")
FAERS_BASE_URL = "https://fis.fda.gov/content/Exports/"


def _clean(val) -> str:
    if pd.isna(val):
        return ""
    return "".join(c for c in str(val) if c == "\n" or c == "\t" or ord(c) >= 32)


def download_quarter(year, quarter, dest_dir=None):
    import httpx
    if dest_dir is None:
        dest_dir = FAERS_DATA_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = f"faers_ascii_{year}Q{quarter}.zip"
    dest_path = dest_dir / filename
    if dest_path.exists():
        logger.info("Already downloaded: %s", dest_path)
        return dest_path
    url = f"{FAERS_BASE_URL}{filename}"
    logger.info("Downloading %s", url)
    try:
        with httpx.Client(timeout=300.0, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            dest_path.write_bytes(response.content)
        logger.info("Downloaded %s (%d bytes)", dest_path, len(response.content))
        return dest_path
    except Exception:
        logger.warning("Failed to download %s", url)
        return None


def parse_faers_zip(zip_path):
    drug_df = reac_df = None
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            upper = name.upper()
            if "DRUG" in upper and upper.endswith(".TXT"):
                with zf.open(name) as f:
                    drug_df = pd.read_csv(io.TextIOWrapper(f, encoding="latin-1"),
                                          sep="$", on_bad_lines="skip", low_memory=False)
                logger.info("Parsed DRUG: %d rows from %s", len(drug_df), name)
            elif "REAC" in upper and upper.endswith(".TXT"):
                with zf.open(name) as f:
                    reac_df = pd.read_csv(io.TextIOWrapper(f, encoding="latin-1"),
                                          sep="$", on_bad_lines="skip", low_memory=False)
                logger.info("Parsed REAC: %d rows from %s", len(reac_df), name)
    return drug_df, reac_df


def aggregate_drug_event_pairs(drug_df, reac_df):
    drug_df.columns = [c.strip().lower() for c in drug_df.columns]
    reac_df.columns = [c.strip().lower() for c in reac_df.columns]
    drug_col = "prod_ai" if "prod_ai" in drug_df.columns else "drugname"
    if "role_cod" in drug_df.columns:
        drug_df = drug_df[drug_df["role_cod"].str.strip().str.upper() == "PS"]
    id_col = "primaryid" if "primaryid" in drug_df.columns else "isr"
    merged = drug_df[[id_col, drug_col]].merge(reac_df[[id_col, "pt"]], on=id_col, how="inner")
    pairs = merged.groupby([drug_col, "pt"]).size().reset_index(name="report_count")
    pairs.columns = ["drug_name", "adverse_event", "report_count"]

    total = merged[id_col].nunique()
    drug_totals = merged.groupby(drug_col)[id_col].nunique().to_dict()
    event_totals = merged.groupby("pt")[id_col].nunique().to_dict()
    prr_values = []
    for _, row in pairs.iterrows():
        a = row["report_count"]
        b = drug_totals.get(row["drug_name"], 0) - a
        c = event_totals.get(row["adverse_event"], 0) - a
        d = total - a - b - c
        denom = (c / (c + d)) if (c + d) > 0 else 0
        prr = (a / (a + b)) / denom if (a + b) > 0 and denom > 0 else 0
        prr_values.append(round(prr, 2))
    pairs["prr"] = prr_values
    return pairs


def ingest_faers(grounding_backend="lexical", min_reports=3, min_prr=2.0):
    existing = sorted(FAERS_DATA_DIR.glob("faers_ascii_*.zip")) if FAERS_DATA_DIR.exists() else []
    if not existing:
        logger.warning("No FAERS data in %s. Download from FDA first.", FAERS_DATA_DIR)
        return

    all_pairs = []
    for zp in existing:
        logger.info("Processing %s", zp.name)
        drug_df, reac_df = parse_faers_zip(zp)
        if drug_df is not None and reac_df is not None:
            all_pairs.append(aggregate_drug_event_pairs(drug_df, reac_df))
    if not all_pairs:
        return

    combined = pd.concat(all_pairs, ignore_index=True)
    combined = combined.groupby(["drug_name", "adverse_event"]).agg(
        {"report_count": "sum", "prr": "max"}).reset_index()
    signals = combined[(combined["report_count"] >= min_reports) & (combined["prr"] >= min_prr)]
    logger.info("Found %d drug-AE signals", len(signals))

    grounding = get_grounding_service(grounding_backend)
    records = []
    drug_cache = {}
    for _, row in signals.iterrows():
        dn = _clean(row["drug_name"])
        if not dn:
            continue
        if dn not in drug_cache:
            r = grounding.ground_drug_best(dn, mention_id=mint_mention_id(dn, "drugs"))
            drug_cache[dn] = (r.id, r.label) if r else ("", dn)
        did, dl = drug_cache[dn]
        records.append({
            "source": "FAERS", "source_drug_name": dn,
            "normalized_drug_id": did, "normalized_drug_label": dl,
            "adverse_event_term": _clean(row["adverse_event"]),
            "report_count": int(row["report_count"]), "prr": float(row["prr"]),
            "evidence": [{"source_type": "POST_MARKET", "jurisdiction": "USA",
                          "reference": "FAERS", "support": "SUPPORT", "confidence": "MEDIUM"}],
        })

    if records:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / "faers_signals.yaml"
        content = yaml.dump(records, default_flow_style=False, allow_unicode=True, width=1000)
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        with open(output_path, "w") as f:
            f.write(content)
        logger.info("Wrote %d FAERS signals to %s", len(records), output_path)


def main():
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="FAERS adverse event ingest")
    parser.add_argument("--grounding-backend", default="lexical")
    parser.add_argument("--download", nargs=2, type=int, metavar=("YEAR", "QUARTER"))
    parser.add_argument("--min-reports", type=int, default=3)
    parser.add_argument("--min-prr", type=float, default=2.0)
    args = parser.parse_args()
    if args.download:
        download_quarter(*args.download)
    else:
        ingest_faers(grounding_backend=args.grounding_backend,
                     min_reports=args.min_reports, min_prr=args.min_prr)


if __name__ == "__main__":
    main()
