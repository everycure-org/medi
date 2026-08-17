"""Build-level QC: reconcile the products against their sources and a recorded baseline.

Four defects in the source-scoped provenance work reached a fully green test suite and were
caught only by rebuilding and reading the output — 11,268 invalid enum values, 118 missing
normalization steps, 3,799 fabricated assertions, and 5,027 spans with no document id. Unit
tests check the code; nothing checked the *artefact*. This does.

Checks, each independently reported:

1. **reconciliation** — every source row is either kept or attributed to a named reason. An
   unattributed drop is the bug condition; a legitimate dedup that doubles overnight is the
   thing a bare count would hide.
2. **counts** — pairs/assertions/drugs against ``conf/qc_baseline.yaml`` with tolerance bands.
3. **enums** — every enum-ranged field in the products holds a permissible value.
4. **invariants** — I-8/I-8b/I-10/I-11/I-12/I-13 via ``validate_pair``.
5. **distributions** — span roles, confidence bases, reliability tiers. `UNKNOWN` span roles
   and a rising `PRIOR` share are early warnings, not errors.
6. **priors** — auto-minted confidence priors, which enter the products without review.
7. **stores** — blank MEDICNE subject ids in the grounding stores (invariant I-4).
8. **fidelity** — `original_literal` equal to the canonical label while the source said
   something else (invariant I-7). This was 462 before the redesign.

Writes ``reports/build_qc.yaml`` — commit it, so a PR diff shows "PMDA -40%" instead of
requiring someone to go looking. Exits non-zero if any check FAILs.

Usage:
    uv run python scripts/build_qc.py                 # everything the products allow
    uv run python scripts/build_qc.py --no-products   # config/store checks only (CI)
"""

from __future__ import annotations

import argparse
import collections
import csv
import sys
from pathlib import Path

import yaml

REPORT = Path("reports/build_qc.yaml")
BASELINE = Path("conf/qc_baseline.yaml")
PRODUCTS = {
    "indication_list": Path("products/indication_list.yaml"),
    "contraindication_list": Path("products/contraindication_list.yaml"),
    "drug_list": Path("products/drug_list.yaml"),
}
KB_INDICATIONS = Path("kb/indications")
GROUNDING_STORES = ["mappings/drug_grounding.sssom.tsv",
                    "mappings/disease_grounding.sssom.tsv"]


class Result:
    """One check's outcome. `status` is PASS, WARN or FAIL."""

    def __init__(self, name: str):
        self.name = name
        self.status = "PASS"
        self.detail: dict = {}
        self.messages: list[str] = []

    def fail(self, msg: str) -> None:
        self.status = "FAIL"
        self.messages.append(msg)

    def warn(self, msg: str) -> None:
        if self.status != "FAIL":
            self.status = "WARN"
        self.messages.append(msg)

    def as_dict(self) -> dict:
        out = {"status": self.status, **self.detail}
        if self.messages:
            out["messages"] = self.messages
        return out


def _load(path: Path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def _assertions(pairs: list[dict]):
    for p in pairs:
        for a in p.get("assertions") or []:
            if isinstance(a, dict):
                yield p, a



# --- 9. source isolation ------------------------------------------------------------------
def check_source_isolation(products: dict) -> Result:
    """Invariant I-1 over the products: does every record's jurisdiction match its source?

    The enum check cannot see this. A DailyMed row relabelled `jurisdiction: EU` holds a
    perfectly legal `JurisdictionEnum` value and passes `linkml-validate` and every other gate
    here — the error is in the *combination*. Relabelling 500 DailyMed rows that way used to
    leave all eight checks green.
    """
    from medic.source_isolation import violation

    r = Result("source_isolation")
    counts: collections.Counter = collections.Counter()
    examples: list[str] = []
    checked = 0
    for name, pairs in products.items():
        if name == "drug_list":
            continue
        for pair, assertion in _assertions(pairs):
            source = assertion.get("source") or ""
            evidence = assertion.get("evidence") or {}
            for jurisdiction in (assertion.get("jurisdiction"), evidence.get("jurisdiction")):
                if not jurisdiction:
                    continue
                checked += 1
                problem = violation(source, jurisdiction)
                if problem:
                    counts[problem] += 1
                    if len(examples) < 5:
                        examples.append(
                            f"{pair.get('drug_id')} -> {pair.get('disease_id')}: {problem}")
    r.detail = {"checked": checked, "violations": sum(counts.values())}
    if counts:
        r.detail["examples"] = examples
        for problem, n in counts.most_common():
            r.fail(f"{n} record(s): {problem}")
    return r


def check_approval_date_authority(products: dict) -> Result:
    """Does every approval date on a regulatory status come from the authority naming it?

    A `RegulatoryStatus` row names one authority, so the date on it has to be a date that
    authority issued. Neither I-1 gate can see a breach here: both compare the record's source
    against its jurisdiction and never read the row's content, so an FDA row carrying a Russian
    registration date is, to them, a well-formed FDA row.

    That is exactly what shipped. The merge filled a missing date from the drug's earliest
    approval date across *every* jurisdiction, which put `20061229` — warfarin's Russian
    registration — onto its FDA/DailyMed row, and the same smear onto 2,194 edges at
    reliability HIGH. Cross-checking against `drug_list.yaml` catches the class, not just the
    one instance: a date is a violation when the drug has approvals recorded for that authority
    and this date is not among them.
    """
    from medic import product_view as pv

    r = Result("approval_date_authority")

    dates_by_drug: dict[str, dict[str, set[str]]] = {}
    for drug in products.get("drug_list", []) or []:
        curie = pv.drug_id(drug)
        if not curie:
            continue
        for approval in pv.approvals(drug):
            authority = (approval.get("authority") or "").strip()
            date_val = (approval.get("approval_date") or "").strip()
            if authority and date_val:
                dates_by_drug.setdefault(curie, {}).setdefault(authority, set()).add(date_val)

    checked = violations = 0
    examples: list[str] = []
    for name, pairs in products.items():
        if name == "drug_list":
            continue
        for pair, assertion in _assertions(pairs):
            drug_id = pair.get("drug_id") or ""
            known = dates_by_drug.get(drug_id, {})
            statuses = assertion.get("regulatory_status") or []
            if isinstance(statuses, dict):  # single row, not yet a list
                statuses = [statuses]
            for status in statuses:
                if not isinstance(status, dict):
                    continue  # legacy shorthand: an authority name with no date to check
                authority = (status.get("authority") or "").strip()
                date_val = (status.get("approval_date") or "").strip()
                if not authority or not date_val:
                    continue
                checked += 1
                if date_val in known.get(authority, ()):
                    continue  # this authority really did issue this date
                # A date drug_list has not recorded for *any* authority is not evidence of a
                # smear: an indication document carries its own approval date, and drug_list
                # only records the earliest per authority from the marketing registries. The
                # signature of the defect is narrower and unambiguous — this date is one
                # another authority issued, so it cannot also be this one's.
                elsewhere = sorted(
                    a for a, dates in known.items() if date_val in dates and a != authority)
                if not elsewhere:
                    continue
                violations += 1
                if len(examples) < 5:
                    examples.append(
                        f"{drug_id} -> {pair.get('disease_id')}: {authority} row dated "
                        f"{date_val}, which belongs to {elsewhere}")
    r.detail = {"checked": checked, "violations": violations}
    if violations:
        r.detail["examples"] = examples
        r.fail(f"{violations} regulatory status row(s) dated by another authority")
    return r


# --- 1. reconciliation ---------------------------------------------------------------------
def check_reconciliation(products: dict) -> Result:
    """Every source row kept, or attributed to a named reason."""
    from medic.merge.on_label_merge import _get_disease_id, _get_drug_id, _make_key

    r = Result("reconciliation")
    per_source: dict[str, collections.Counter] = {}
    for path in sorted(KB_INDICATIONS.glob("*/*.yaml")):
        if path.name not in ("indications.yaml", "contraindications.yaml"):
            continue
        records = _load(path) or []
        if not isinstance(records, list):
            continue
        source = path.parent.name.upper()
        key = f"{source}/{path.stem}"
        counts = collections.Counter()
        seen: set[str] = set()
        for rec in records:
            rec.setdefault("source", source)
            if not _get_drug_id(rec):
                counts["dropped_no_drug_id"] += 1
                continue
            if not _get_disease_id(rec):
                counts["dropped_no_disease_id"] += 1
                continue
            if "Error" in _get_drug_id(rec) or "Error" in _get_disease_id(rec):
                counts["dropped_error_id"] += 1
                continue
            k = _make_key(rec)
            if k is None:
                counts["dropped_unkeyable"] += 1
                continue
            if k in seen:
                counts["dropped_duplicate_document"] += 1
                continue
            seen.add(k)
            counts["kept"] += 1
        counts["rows_in"] = len(records)
        per_source[key] = counts

    for key, counts in per_source.items():
        attributed = sum(v for k, v in counts.items()
                         if k.startswith("dropped_")) + counts["kept"]
        if attributed != counts["rows_in"]:
            r.fail(f"{key}: {counts['rows_in'] - attributed} rows neither kept nor attributed")
        r.detail.setdefault("sources", {})[key] = dict(sorted(counts.items()))

    out_total = sum(len(p.get("assertions") or [])
                    for name in ("indication_list", "contraindication_list")
                    for p in products.get(name, []))
    kept_total = sum(c["kept"] for c in per_source.values())
    r.detail["kept_total"] = kept_total
    r.detail["assertions_total"] = out_total
    # The merge also rewrites disease ids to MONDO, which can collapse two previously-distinct
    # pairs onto one document key, so a small shortfall is expected rather than wrong.
    if out_total > kept_total:
        r.fail(f"more assertions ({out_total}) than kept source rows ({kept_total})")
    elif kept_total - out_total > max(50, kept_total * 0.01):
        r.warn(f"{kept_total - out_total} kept rows produced no assertion")
    return r


# --- 2. counts vs baseline -----------------------------------------------------------------
def check_counts(products: dict, baseline: dict) -> Result:
    r = Result("counts")
    observed = {}
    for name in ("indication_list", "contraindication_list"):
        pairs = products.get(name, [])
        observed[f"{name}.pairs"] = len(pairs)
        observed[f"{name}.assertions"] = sum(len(p.get("assertions") or []) for p in pairs)
        by_source = collections.Counter(a["source"] for _p, a in _assertions(pairs))
        for src, n in by_source.items():
            observed[f"{name}.by_source.{src}"] = n
    observed["drug_list.drugs"] = len(products.get("drug_list", []))

    expected = (baseline or {}).get("counts") or {}
    tol = (baseline or {}).get("tolerance") or {}
    warn_pct, fail_pct = tol.get("warn_pct", 5), tol.get("fail_pct", 20)
    for key, value in sorted(observed.items()):
        exp = expected.get(key)
        if exp is None:
            r.warn(f"{key}={value} has no baseline entry")
            continue
        drift = 100.0 * (value - exp) / exp if exp else 0.0
        if abs(drift) >= fail_pct:
            r.fail(f"{key}: {value} vs baseline {exp} ({drift:+.1f}%)")
        elif abs(drift) >= warn_pct:
            r.warn(f"{key}: {value} vs baseline {exp} ({drift:+.1f}%)")
    for key in expected:
        if key not in observed:
            r.fail(f"{key} is in the baseline but absent from the build")
    r.detail["observed"] = observed
    return r


# --- 3. enum validity ----------------------------------------------------------------------
def _enum_maps():
    """(global slot->values, per-step-category slot->values) from the schemas."""
    from linkml_runtime import SchemaView

    sv = SchemaView("src/medic/schema/provenance.yaml")
    enums = {name: set(e.permissible_values) for name, e in sv.all_enums().items()}
    step_classes = {"EXTRACTION": "ExtractionStep", "TRANSLATION": "TranslationStep",
                    "GROUNDING": "GroundingStep", "NORMALIZATION": "NormalizationStep"}
    per_category: dict[str, dict[str, set]] = {}
    for category, cls in step_classes.items():
        m = {}
        for slot in ("quality", "flags", "applied_rules", "method", "category",
                     "confidence_basis", "status"):
            try:
                rng = sv.induced_slot(slot, cls).range
            except Exception:
                continue
            if rng in enums:
                m[slot] = enums[rng]
        per_category[category] = m
    glob = {}
    for slot in ("role", "span_role", "basis", "confidence_basis", "entity_type"):
        for cls in ("TextSpan", "ExtractionStep", "ConfidenceBreakdown", "CoMention"):
            try:
                rng = sv.induced_slot(slot, cls).range
            except Exception:
                continue
            if rng in enums:
                glob[slot] = enums[rng]
                break
    return glob, per_category


def check_enums(products: dict) -> Result:
    r = Result("enums")
    glob, per_category = _enum_maps()
    bad: collections.Counter = collections.Counter()

    def walk(node, category=None):
        if isinstance(node, dict):
            cat = node.get("category") if node.get("category") in per_category else category
            for k, v in node.items():
                table = per_category.get(cat, {}).get(k) or glob.get(k)
                if table is not None:
                    for item in (v if isinstance(v, list) else [v]):
                        if isinstance(item, str) and item not in table:
                            bad[f"{k}={item!r}"] += 1
                walk(v, cat)
        elif isinstance(node, list):
            for item in node:
                walk(item, category)

    for name, recs in products.items():
        walk(recs)
    if bad:
        for label, n in bad.most_common(10):
            r.fail(f"{n} occurrences of invalid enum value {label}")
    r.detail["invalid_values"] = dict(bad)
    return r


# --- 4. invariants -------------------------------------------------------------------------
def check_invariants(products: dict) -> Result:
    from medic.provenance_build import validate_pair

    r = Result("invariants")
    problems: list[str] = []
    for name in ("indication_list", "contraindication_list"):
        for pair in products.get(name, []):
            problems.extend(validate_pair(pair))
    r.detail["violations"] = len(problems)
    if problems:
        r.fail(f"{len(problems)} invariant violations")
        r.detail["examples"] = problems[:10]
    return r


# --- 5. distributions ----------------------------------------------------------------------
def check_distributions(products: dict) -> Result:
    r = Result("distributions")
    for name in ("indication_list", "contraindication_list"):
        pairs = products.get(name, [])
        roles = collections.Counter(
            s.get("role") for _p, a in _assertions(pairs) for s in a.get("spans") or [])
        bases = collections.Counter(
            st.get("confidence_basis")
            for _p, a in _assertions(pairs) for m in ("drug", "disease")
            for st in ((a.get(m) or {}).get("resolution") or {}).get("pipeline") or [])
        tiers = collections.Counter(p.get("reliability") for p in pairs)
        r.detail[name] = {"span_roles": dict(roles), "confidence_basis": dict(bases),
                          "reliability": dict(tiers)}
        if roles.get("UNKNOWN"):
            r.warn(f"{name}: {roles['UNKNOWN']} spans have role UNKNOWN")
        total_steps = sum(bases.values())
        if total_steps and bases.get("PRIOR", 0) / total_steps > 0.10:
            r.warn(f"{name}: {bases['PRIOR']}/{total_steps} steps rest on an assumed prior")
    return r


# --- 6. auto-minted priors -----------------------------------------------------------------
def check_priors() -> Result:
    from medic.confidence import load_priors

    r = Result("priors")
    priors = load_priors()
    minted = [p for p in priors if p.get("auto_generated")]
    uncalibrated = [p for p in priors if not p.get("calibrated")]
    r.detail["total"] = len(priors)
    r.detail["auto_generated"] = [
        f"{p.get('agent_name') or p.get('tool')} {p['category']}/{p['method']}={p['value']}"
        for p in minted]
    r.detail["uncalibrated"] = len(uncalibrated)
    if minted:
        r.warn(f"{len(minted)} prior(s) auto-minted from a family default — review them")
    return r


# --- 7. store integrity --------------------------------------------------------------------
def check_stores() -> Result:
    r = Result("stores")
    for path in GROUNDING_STORES:
        p = Path(path)
        if not p.exists():
            r.fail(f"{path} is missing")
            continue
        blanks = total = 0
        with open(p, newline="") as fh:
            for row in csv.DictReader(
                (ln for ln in fh if not ln.startswith("#")), delimiter="\t"
            ):
                total += 1
                if not (row.get("subject_id") or "").strip():
                    blanks += 1
        r.detail[path] = {"rows": total, "blank_subject_id": blanks}
        if blanks:
            r.fail(f"{path}: {blanks}/{total} rows have no MEDICNE subject_id (I-4)")
    return r


# --- 8. verbatim fidelity ------------------------------------------------------------------
def check_fidelity(products: dict) -> Result:
    """I-7: original_literal is the source string, never the canonical label written back."""
    r = Result("fidelity")
    offenders = 0
    for name in ("indication_list", "contraindication_list"):
        for _p, a in _assertions(products.get(name, [])):
            drug = a.get("drug") or {}
            ev = a.get("evidence") or {}
            source_said = (ev.get("original_drug_label") or "").strip()
            if (source_said and drug.get("original_literal") == drug.get("resolved_label")
                    and source_said != drug.get("resolved_label")):
                offenders += 1
    r.detail["canonical_label_writebacks"] = offenders
    if offenders:
        r.fail(f"{offenders} mentions record the canonical label as the source literal (I-7)")
    return r


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-products", action="store_true",
                    help="skip checks that need built products (for CI)")
    ap.add_argument("--out", default=str(REPORT))
    args = ap.parse_args()

    products: dict = {}
    if not args.no_products:
        missing = [str(p) for p in PRODUCTS.values() if not p.exists()]
        if missing:
            print(f"ERROR: products missing: {', '.join(missing)}. Build first, or pass "
                  f"--no-products.", file=sys.stderr)
            return 1
        for name, path in PRODUCTS.items():
            data = _load(path)
            products[name] = data.get("associations") or data.get("drugs") or []

    baseline = _load(BASELINE) if BASELINE.exists() else {}

    results = [check_priors(), check_stores()]
    if products:
        results = [
            check_reconciliation(products),
            check_counts(products, baseline),
            check_enums(products),
            check_invariants(products),
            check_source_isolation(products),
            check_approval_date_authority(products),
            check_distributions(products),
            *results,
            check_fidelity(products),
        ]

    report = {"checks": {r.name: r.as_dict() for r in results}}
    report["summary"] = collections.Counter(r.status for r in results)
    report["summary"] = dict(report["summary"])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as fh:
        yaml.dump(report, fh, default_flow_style=False, sort_keys=True, allow_unicode=True)

    width = max(len(r.name) for r in results)
    for r in results:
        print(f"  {r.status:4s}  {r.name:{width}s}  " +
              ("; ".join(r.messages[:2]) if r.messages else ""))
    print(f"\nwrote {out}")
    failed = [r.name for r in results if r.status == "FAIL"]
    if failed:
        print(f"\nQC FAILED: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
