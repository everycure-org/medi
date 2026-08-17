"""EveryCure coverage-gap report — regulatory approvals MeDIC is missing.

EveryCure's ``approved_usa`` column is **not** a source of truth for MeDIC approvals
(those come only from regulatory primaries — Orange/Purple Book, DailyMed, EMA, PMDA, …).
It is used here purely as an external **reference set for completeness QA**: a drug a
trusted curated list calls FDA-approved that MeDIC approves *nowhere* is a coverage gap —
a drug we should be ingesting/grounding but aren't. The first thing this surfaces is where
our regulatory ingest is weakest (today: biologics / Purple Book).

Run: ``just coverage-gaps`` (or ``python -m medic.coverage``).
"""

from __future__ import annotations

import csv
from collections import Counter

import typer
import yaml

from medic import product_view as pv

app = typer.Typer(add_completion=False)

EVERYCURE_KB = "kb/drugs/everycure/everycure.yaml"
DRUG_LIST = "products/drug_list.yaml"
DEFAULT_OUT = "background/everycure_coverage_gaps.tsv"

# ATC level-1 anatomical groups (first letter of the ATC code).
_ATC_L1 = {
    "A": "Alimentary/metabolism", "B": "Blood", "C": "Cardiovascular",
    "D": "Dermatologicals", "G": "Genitourinary", "H": "Systemic hormones",
    "J": "Anti-infectives", "L": "Antineoplastic/immunomodulating",
    "M": "Musculoskeletal", "N": "Nervous system", "P": "Antiparasitic",
    "R": "Respiratory", "S": "Sensory", "V": "Various",
}


def _atc_l1(record: dict) -> str:
    code = (record.get("atc_main") or "").strip()
    return code[0] if code else "?"


def classify_everycure(
    everycure_records: list[dict], approved_curies: set[str], approved_names: set[str]
) -> dict[str, list[dict]]:
    """Split EveryCure APPROVED drugs into covered / duplicate / gap.

    Matching on the primary id alone is misleading: EveryCure keys biologics by UNII while
    MeDIC grounds them to CHEBI, so the *same* covered drug looks uncovered. We therefore
    also match on name:

    * ``covered``   — id is in MeDIC's approved set.
    * ``duplicate`` — id is not, but a MeDIC-approved drug has the **same name**. The drug
      IS covered under a different id — this is an unmerged UNII/CHEBI duplicate (a
      *normalization* problem, not a coverage gap).
    * ``gap``       — no MeDIC-approved drug of that id or name anywhere. The real gap.
    """
    out: dict[str, list[dict]] = {"covered": [], "duplicate": [], "gap": []}
    for rec in everycure_records:
        if (rec.get("approved_usa") or "").upper() != "APPROVED":
            continue
        curie = (rec.get("normalized_id") or "").strip()
        name = (rec.get("source_name") or "").strip().lower()
        if curie and curie in approved_curies:
            out["covered"].append(rec)
        elif name and name in approved_names:
            out["duplicate"].append(rec)
        else:
            out["gap"].append(rec)
    return out


def find_gaps(
    everycure_records: list[dict], approved_curies: set[str],
    approved_names: set[str] | None = None,
) -> list[dict]:
    """True coverage gaps (uncovered by both id and name). See :func:`classify_everycure`."""
    return classify_everycure(everycure_records, approved_curies, approved_names or set())["gap"]


def load_everycure(path: str = EVERYCURE_KB) -> list[dict]:
    with open(path) as fh:
        return [r for r in (yaml.safe_load(fh) or []) if isinstance(r, dict)]


def approved_index_from_products(path: str = DRUG_LIST) -> tuple[set[str], set[str]]:
    """(ids, names) of drugs MeDIC has a regulatory approval for (any ``approved_*`` flag)."""
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    drugs = data.get("drugs", data) if isinstance(data, dict) else data
    ids, names = set(), set()
    for d in drugs:
        if isinstance(d, dict) and pv.is_approved_anywhere(d):
            ids.add(pv.drug_id(d))
            names.add(pv.drug_label(d).strip().lower())
    return ids, names


def approved_curies_from_products(path: str = DRUG_LIST) -> set[str]:
    """Backwards-compatible: just the approved id set."""
    return approved_index_from_products(path)[0]


def _write_tsv(path: str, records: list[dict]) -> None:
    cols = ["drug_name", "id", "alt_id", "atc_l1", "therapeutic_area"]
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(cols)
        for r in sorted(records, key=lambda x: (x.get("source_name") or "").lower()):
            alts = r.get("alternate_ids") or []
            w.writerow([
                r.get("source_name", ""), r.get("normalized_id", ""),
                alts[0] if alts else "", _atc_l1(r), r.get("therapeutic_area", ""),
            ])


@app.command()
def main(
    everycure: str = typer.Option(EVERYCURE_KB),
    drug_list: str = typer.Option(DRUG_LIST),
    out: str = typer.Option(DEFAULT_OUT, help="TSV path for the true-gap list."),
) -> None:
    """Report EveryCure FDA-approved drugs that no MeDIC regulatory source covers.

    Separates real coverage gaps from UNII/CHEBI duplicates (covered under a different id).
    """
    records = load_everycure(everycure)
    ids, names = approved_index_from_products(drug_list)
    buckets = classify_everycure(records, ids, names)
    covered, dup, gap = buckets["covered"], buckets["duplicate"], buckets["gap"]
    total = len(covered) + len(dup) + len(gap)

    typer.echo(
        f"EveryCure APPROVED: {total}\n"
        f"  covered (same id):                 {len(covered)}\n"
        f"  duplicate (covered under another id — UNII/CHEBI fragmentation, a normalization\n"
        f"             problem, NOT a coverage gap): {len(dup)}\n"
        f"  TRUE GAP (covered nowhere):        {len(gap)}"
    )

    non_chebi = sum(1 for r in gap if not (r.get("normalized_id") or "").startswith("CHEBI"))
    typer.echo(
        f"\nTrue gap by ATC level-1 ({non_chebi}/{len(gap)} biologic/non-CHEBI ids):"
    )
    for atc, n in Counter(_atc_l1(r) for r in gap).most_common():
        label = "(no ATC code)" if atc == "?" else _ATC_L1.get(atc, atc)
        typer.echo(f"  {n:4}  {atc}  {label}")

    _write_tsv(out, gap)
    dup_out = out.replace(".tsv", "_duplicates.tsv")
    _write_tsv(dup_out, dup)
    typer.echo(f"\nWrote {len(gap)} true gaps -> {out}")
    typer.echo(f"Wrote {len(dup)} UNII/CHEBI duplicates -> {dup_out}")


if __name__ == "__main__":
    app()
