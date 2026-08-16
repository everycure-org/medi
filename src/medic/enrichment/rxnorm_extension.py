"""Enrichment: RxNorm Extension mappings from Athena CSV data."""

import csv
import logging
import sqlite3
from pathlib import Path

from medic import product_view as pv
from medic.ingest.common import should_skip_expensive_calls

logger = logging.getLogger(__name__)

RXNORM_CACHE_DIR = Path("cache/rxnorm_extension")


def _load_into_sqlite(cache_dir: Path) -> sqlite3.Connection | None:
    """Load CONCEPT and CONCEPT_RELATIONSHIP CSVs into an in-memory SQLite DB."""
    concept_file = cache_dir / "CONCEPT.csv"
    relationship_file = cache_dir / "CONCEPT_RELATIONSHIP.csv"

    if not concept_file.exists() or not relationship_file.exists():
        return None

    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE concept (
            concept_id INTEGER,
            concept_name TEXT,
            domain_id TEXT,
            vocabulary_id TEXT,
            concept_class_id TEXT,
            standard_concept TEXT,
            concept_code TEXT,
            valid_start_date TEXT,
            valid_end_date TEXT,
            invalid_reason TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE concept_relationship (
            concept_id_1 INTEGER,
            concept_id_2 INTEGER,
            relationship_id TEXT,
            valid_start_date TEXT,
            valid_end_date TEXT,
            invalid_reason TEXT
        )
    """)

    # Load CONCEPT
    try:
        with open(concept_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            next(reader, None)  # skip header row
            for row in reader:
                if len(row) >= 10:
                    cursor.execute(
                        "INSERT INTO concept VALUES (?,?,?,?,?,?,?,?,?,?)",
                        row[:10],
                    )
    except Exception:
        logger.warning("Failed to load CONCEPT.csv")
        conn.close()
        return None

    # Load CONCEPT_RELATIONSHIP
    try:
        with open(relationship_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            next(reader, None)  # skip header row
            for row in reader:
                if len(row) >= 6:
                    cursor.execute(
                        "INSERT INTO concept_relationship VALUES (?,?,?,?,?,?)",
                        row[:6],
                    )
    except Exception:
        logger.warning("Failed to load CONCEPT_RELATIONSHIP.csv")
        conn.close()
        return None

    # Create indexes
    cursor.execute("CREATE INDEX idx_concept_name ON concept(concept_name)")
    cursor.execute("CREATE INDEX idx_concept_code ON concept(concept_code)")
    cursor.execute(
        "CREATE INDEX idx_rel_id1 ON concept_relationship(concept_id_1)"
    )
    conn.commit()

    return conn


def enrich_rxnorm_extension(drugs: list[dict]) -> None:
    """Enrich drugs with RxNorm Extension / OMOP concept IDs.

    Checks cache/rxnorm_extension/ for Athena CSV data.
    If not present, logs info and returns.
    If present, loads into SQLite and looks up each drug.
    """
    if not RXNORM_CACHE_DIR.exists():
        if should_skip_expensive_calls():
            return
        logger.info("RxNorm Extension data not available, skipping")
        return

    concept_file = RXNORM_CACHE_DIR / "CONCEPT.csv"
    if not concept_file.exists():
        logger.info("RxNorm Extension data not available, skipping")
        return

    conn = _load_into_sqlite(RXNORM_CACHE_DIR)
    if conn is None:
        logger.info("RxNorm Extension data not available, skipping")
        return

    try:
        cursor = conn.cursor()
        for drug in drugs:
            drug_label = pv.drug_label(drug)
            if not drug_label:
                continue

            # Look up by concept name
            cursor.execute(
                "SELECT concept_id FROM concept WHERE concept_name = ? COLLATE NOCASE",
                (drug_label,),
            )
            rows = cursor.fetchall()

            alt_ids = set(drug.get("alternate_ids", []))
            for row in rows:
                concept_id = row[0]
                alt_ids.add(f"OMOP:{concept_id}")

            if alt_ids != set(drug.get("alternate_ids", [])):
                drug["alternate_ids"] = sorted(alt_ids)
    finally:
        conn.close()
