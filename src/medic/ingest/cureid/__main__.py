"""CURE-ID drug repurposing case report ingest.

Downloads the CURE-ID open data TSV from NCATS, filters to drug-disease
treatment associations, aggregates by (drug, disease) pair, and writes
ResearchAssociationList YAML to kb/research/.
"""

import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from medic.ingest.common import download_file

logger = logging.getLogger(__name__)

CUREID_URL = "https://opendata.ncats.nih.gov/public/cureid/cureid_data.tsv"
RAW_PATH = Path("data/raw/cureid/cureid_data.tsv")
OUTPUT_PATH = Path("kb/research/cureid_associations.yaml")

# Columns we use from the TSV
_SUBJECT_CURIE = "subject_final_curie"
_SUBJECT_LABEL = "subject_final_label"
_SUBJECT_TYPE = "subject_type"
_OBJECT_CURIE = "object_final_curie"
_OBJECT_LABEL = "object_final_label"
_OBJECT_TYPE = "object_type"
_PREDICATE = "biolink_predicate"
_REPORT_ID = "report_id"
_PMID = "pmid"
_LINK = "link"
_OUTCOME = "outcome"


def outcome_to_confidence(outcome: str | None) -> str:
    """Map CURE-ID outcome text to ConfidenceEnum value."""
    if not outcome:
        return "LOW"
    outcome_lower = outcome.strip().lower()
    if "improved" in outcome_lower or "recovered" in outcome_lower:
        return "MEDIUM"
    return "LOW"


def outcome_to_support(outcome: str | None) -> str:
    """Map CURE-ID outcome text to EvidenceSupportEnum value."""
    if not outcome:
        return "SUPPORT"
    outcome_lower = outcome.strip().lower()
    if "improved" in outcome_lower or "recovered" in outcome_lower:
        return "SUPPORT"
    return "PARTIAL"


def parse_cureid_tsv(tsv_path: Path) -> list[dict]:
    """Parse CURE-ID TSV, filter to drug treatment edges.

    Returns list of dicts with keys: drug_curie, drug_label, disease_curie,
    disease_label, object_type, report_id, pmid, link, outcome.
    """
    df = pd.read_csv(tsv_path, sep="\t", dtype=str).fillna("")

    # Filter: subject is Drug, predicate is applied_to_treat
    mask = (df[_SUBJECT_TYPE] == "Drug") & (
        df[_PREDICATE] == "biolink:applied_to_treat"
    )
    df = df[mask]

    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "drug_curie": row[_SUBJECT_CURIE],
                "drug_label": row[_SUBJECT_LABEL],
                "disease_curie": row[_OBJECT_CURIE],
                "disease_label": row[_OBJECT_LABEL],
                "object_type": row[_OBJECT_TYPE],
                "report_id": row[_REPORT_ID],
                "pmid": row[_PMID],
                "link": row[_LINK],
                "outcome": row[_OUTCOME],
            }
        )

    logger.info(
        "Parsed %d drug treatment edges from %d total rows", len(records), len(df)
    )
    return records


def _build_evidence_item(record: dict, source_type: str) -> dict:
    """Build a single evidence item from a case record."""
    outcome = record.get("outcome", "")
    item = {
        "source_type": source_type,
        "confidence": outcome_to_confidence(outcome),
        "support": outcome_to_support(outcome),
        "evidence_source": "HUMAN_CLINICAL",
        "approval_status": "OFF_LABEL",
        "max_research_phase": "CASE_REPORT",
    }
    if source_type == "DATABASE":
        item["reference"] = CUREID_URL
        item["reference_title"] = "CURE-ID Open Data Portal (FDA/NCATS)"
        item["jurisdiction"] = "USA"
        report_id = record.get("report_id", "")
        outcome_text = outcome if outcome else "not reported"
        item["explanation"] = (
            f"Case report from CURE-ID (report_id: {report_id}). "
            f"Outcome: {outcome_text}"
        )
    elif source_type == "LITERATURE":
        pmid = record.get("pmid", "")
        item["reference"] = f"PMID:{pmid}"
    return item


def aggregate_associations(records: list[dict]) -> list[dict]:
    """Aggregate parsed records into ResearchAssociation dicts.

    Groups by (drug_curie, disease_curie) for Disease objects.
    Phenotype treatment edges sharing a report with a disease edge
    are folded into the disease association's notes.
    """
    today = date.today().isoformat()

    # Separate disease vs phenotype records
    disease_records = [r for r in records if r["object_type"] == "Disease"]
    pheno_records = [r for r in records if r["object_type"] == "PhenotypicFeature"]

    # Build a set of (drug_curie, report_id) that have a disease edge
    disease_report_keys = {
        (r["drug_curie"], r["report_id"]) for r in disease_records
    }

    # Phenotype records that share a report with a disease edge -> fold into notes
    folded_phenos: dict[tuple[str, str], list[dict]] = defaultdict(list)
    orphan_phenos: list[dict] = []

    for r in pheno_records:
        key = (r["drug_curie"], r["report_id"])
        if key in disease_report_keys:
            for dr in disease_records:
                if dr["drug_curie"] == r["drug_curie"] and dr["report_id"] == r["report_id"]:
                    pair_key = (dr["drug_curie"], dr["disease_curie"])
                    folded_phenos[pair_key].append(r)
                    break
        else:
            orphan_phenos.append(r)

    # Group disease records by (drug, disease)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in disease_records:
        groups[(r["drug_curie"], r["disease_curie"])].append(r)

    # Orphan phenotype records also become groups
    for r in orphan_phenos:
        groups[(r["drug_curie"], r["disease_curie"])].append(r)

    associations = []
    for (drug_curie, disease_curie), group in sorted(groups.items()):
        first = group[0]

        # Build evidence: one DATABASE item per unique report_id, plus LITERATURE per unique PMID
        seen_reports: set[str] = set()
        seen_pmids: set[str] = set()
        evidence = []

        for rec in group:
            rid = rec["report_id"]
            if rid not in seen_reports:
                seen_reports.add(rid)
                evidence.append(_build_evidence_item(rec, "DATABASE"))

            pmid = rec.get("pmid", "")
            if pmid and pmid not in seen_pmids:
                seen_pmids.add(pmid)
                evidence.append(_build_evidence_item(rec, "LITERATURE"))

        # Build notes with folded phenotype info
        notes = "Drug repurposing case report(s) from CURE-ID"
        pair_key = (drug_curie, disease_curie)
        if pair_key in folded_phenos:
            pheno_labels = sorted(
                {r["disease_label"] for r in folded_phenos[pair_key] if r["disease_label"]}
            )
            if pheno_labels:
                notes += f". Also treated symptoms: {', '.join(pheno_labels)}"

        associations.append(
            {
                "drug_id": drug_curie,
                "drug_label": first["drug_label"],
                "disease_id": disease_curie,
                "disease_label": first["disease_label"],
                "curation_status": "VALIDATED",
                "curation_date": today,
                "curator": "cureid",
                "search_query": "CURE-ID open data portal",
                "deep_research_used": False,
                "notes": notes,
                "evidence": evidence,
            }
        )

    logger.info("Aggregated %d associations from %d records", len(associations), len(records))
    return associations


def ingest_cureid(force_download: bool = False) -> Path:
    """Download CURE-ID TSV, parse, aggregate, and write research associations."""
    # Download
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    download_file(CUREID_URL, RAW_PATH, force=force_download)

    # Parse and filter
    records = parse_cureid_tsv(RAW_PATH)
    if not records:
        logger.warning("No drug treatment records found in CURE-ID data")
        return OUTPUT_PATH

    # Aggregate into associations
    associations = aggregate_associations(records)

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.dump(
        {"associations": associations},
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    )
    with open(OUTPUT_PATH, "w") as f:
        f.write(content)

    logger.info(
        "CURE-ID ingest complete: %d associations from %d treatment edges -> %s",
        len(associations),
        len(records),
        OUTPUT_PATH,
    )
    return OUTPUT_PATH


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_cureid()
