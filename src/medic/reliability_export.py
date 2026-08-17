"""Flatten MeDIC products into reliability-annotated statement exports.

Materialises the soft-launch contract: every statement as one row carrying its
``statement_type`` and ``reliability`` tier, plus a pre-filtered reliable subset — so a
downstream consumer selects "the trustworthy part of MeDIC" with a column filter, without
running the scorer or understanding any source's idiosyncrasies.

Writes two TSVs:
* ``medic_statements.tsv`` — every statement, annotated (the full, filterable table).
* ``medic_reliable.tsv``   — the default soft-launch subset (core types, HIGH/MEDIUM).

Run: ``just export-reliability``.
"""

from __future__ import annotations

import csv
import glob

import typer

from medic import product_view as pv
from medic.reliability import (
    RELIABLE_TIERS,
    CORE_TYPES,
    StatementReviewStore,
    StatementType,
    _load,
    classify_statement,
    score_reliability,
)

app = typer.Typer(add_completion=False)

PRODUCT_GLOBS = [
    "products/drug_list.yaml",
    "products/indication_list.yaml",
    "products/contraindication_list.yaml",
    "products/research_list.yaml",
]

COLUMNS = [
    "statement_type", "reliability", "is_reliable",
    "drug_id", "drug_label", "disease_id", "disease_label", "relationship",
    "jurisdictions", "approval_status", "reference",
]


def _first_evidence(record: dict) -> dict:
    """The first evidence row backing a record.

    Reads through ``product_view`` so this survived the flat-``evidence`` -> per-assertion
    move: evidence now lives on ``assertions[].evidence``, one row per source document.
    """
    ev = pv.assoc_evidence(record)
    return ev[0] if ev else {}


def _jurisdictions(record: dict) -> str:
    """Comma-joined jurisdiction slugs for a statement.

    A Drug record's jurisdictions come from its ``approvals`` (authority -> jurisdiction).
    An association's jurisdiction is the one that supports *that indication* — the
    supporting evidence item's ``jurisdiction`` — not the drug's full approval footprint,
    so a drug approved in China/Russia (drug-list-only) does not leak onto an EU indication.
    """
    if record.get("identity") is not None or record.get("approvals"):
        return ",".join(sorted(pv.approved_jurisdictions(record)))
    # Every jurisdiction that attests THIS pair, one per source assertion. Previously only the
    # first evidence row was reported, because the merge collapsed all sources into one record
    # and there was no way to tell which jurisdictions genuinely backed the pair.
    return ",".join(sorted(pv.assoc_jurisdictions(record)))


def flatten(record: dict, st: StatementType, tier) -> dict:
    """One export row for a statement (uniform columns across statement types)."""
    ev = _first_evidence(record)
    return {
        "statement_type": st.value,
        "reliability": tier.value,
        "is_reliable": (st in CORE_TYPES and tier in RELIABLE_TIERS),
        "drug_id": pv.drug_id(record) or pv.assoc_drug_id(record)
        or record.get("drug_id", ""),
        "drug_label": pv.drug_label(record) or pv.assoc_drug_label(record)
        or record.get("drug_label", ""),
        "disease_id": pv.assoc_disease_id(record) or record.get("disease_id", ""),
        "disease_label": pv.assoc_disease_label(record) or record.get("disease_label", ""),
        "relationship": record.get("relationship_type", ""),
        "jurisdictions": _jurisdictions(record),
        "approval_status": ev.get("approval_status", ""),
        "reference": ev.get("reference") or ev.get("source_document_url", ""),
    }


def build_rows(paths: list[str], review: StatementReviewStore) -> list[dict]:
    rows = []
    for path in paths:
        for rec in _load(path):
            st = classify_statement(rec)
            tier = score_reliability(rec, st, review_status=review.status(rec))
            rows.append(flatten(rec, st, tier))
    return rows


def _write(path: str, rows: list[dict]) -> None:
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


@app.command()
def main(
    out_all: str = typer.Option("exports/medic_statements.tsv"),
    out_reliable: str = typer.Option("exports/medic_reliable.tsv"),
) -> None:
    """Write the annotated statement table + the reliable subset."""
    paths = [p for pattern in PRODUCT_GLOBS for p in sorted(glob.glob(pattern))]
    review = StatementReviewStore().load()
    rows = build_rows(paths, review)
    reliable = [r for r in rows if r["is_reliable"]]

    _write(out_all, rows)
    _write(out_reliable, reliable)
    typer.echo(
        f"{len(rows)} statements -> {out_all}\n"
        f"{len(reliable)} reliable (core, HIGH/MEDIUM) -> {out_reliable} "
        f"({100 * len(reliable) / len(rows):.1f}%)"
    )


if __name__ == "__main__":
    app()
