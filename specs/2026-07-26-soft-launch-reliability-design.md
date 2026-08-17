# Soft-launch: statement-type enum + source-uniform reliability score — Design Spec

**Status:** draft for review · **Date:** 2026-07-26 · **Issue:** `issues/issue_soft_launch_reliable_subset.md`
· **Reference prototype:** `src/medic/reliability.py` (working; `just reliability-report`)

## 1. Goal

Let a downstream consumer import, in one line, the part of MeDIC that is *already fairly
trustworthy* — without needing to understand any source's idiosyncrasies. Two orthogonal knobs:

- **What kind of claim** it is (`StatementType`), so a consumer can pick MeDIC's regulatory core.
- **How trustworthy** it is (`ReliabilityTier`), scored the **same way for every source**.

The importable subset = `statement_type ∈ CORE_TYPES` **and** `reliability ∈ {HIGH, MEDIUM}`.

This is deliberately *simple*: a small enum + a four-gate score, both pure functions of fields the
pipeline already produces. No per-source special-casing, no ML, no new heavy machinery.

## 2. `StatementType` (the "what")

A statement is one claim MeDIC makes. Five kinds; the first three are the regulatory backbone:

| Type | Meaning | Source(s) | Core? |
|---|---|---|---|
| `DRUG_APPROVAL` | a drug is approved/marketed in a jurisdiction | Orange/Purple Book, EMA, PMDA, GRLS, CDE, DailyMed | ✅ |
| `INDICATION` | drug approved to treat a disease | DailyMed, EMA, PMDA, India | ✅ |
| `CONTRAINDICATION` | drug contraindicated in a disease | DailyMed (FDA only today) | ✅ |
| `ADVERSE_EVENT` | drug associated with an adverse event | PVLens/FAERS (stub) | ❌ |
| `RESEARCH_ASSOCIATION` | drug↔disease from literature / trials | CURE-ID, deep research, PubMed | ❌ |

`CORE_TYPES = {DRUG_APPROVAL, INDICATION, CONTRAINDICATION}`. Classification is a pure function of
record shape (`classify_statement`): `relationship_type` → indication/contraindication; a `curie` +
any `approved_*` → approval; `drug_id`+`disease_id` / research lifecycle fields → research; MedDRA →
adverse event.

## 3. `ReliabilityTier` (the "how trustworthy")

`HIGH > MEDIUM > LOW > EXCLUDED`. A record's tier is the **worst (most conservative) of four
independent gates**, each mapping one failure-mode family (FAILURE_MODES.md) to a tier. A gate returns
*not-applicable* when a dimension doesn't apply to a statement (a structured approval has no extraction
gate), so the identical function scores every statement — that is what makes it source-uniform.

| Gate | Reads (normalized per-record signal) | HIGH | MEDIUM | LOW | EXCLUDED |
|---|---|---|---|---|---|
| **Grounding** (FM entity-res) | `grounding` / `disease_grounding` quality + confidence | exact / normalized / curated | inexact, conf ≥ 0.7 (salt, formulation) | inexact, conf < 0.7 (fuzzy) | unresolved / no id |
| **Extraction** (FM §4–5) | `original_disease_label` vs `snippet`+section (entailment + negation) | entailed (≥0.5), not negated | partial (0<score<0.5) | not stated (score 0) | negated indication (inversion) |
| **Translation** (FM §7) | `translation.translation_status` | none / OFFICIAL | machine CANDIDATE | — | (untranslated → LOW) |
| **Provenance** (FM §8) | snippet / reference / URL / application id | any verifiable provenance | — | no provenance at all | — |
| **Approval** (DRUG_APPROVAL only) | `approved_*` / evidence `approval_status` | (pass) | — | — | not actually approved |

Overall `reliability = min(applicable gates)`; `EXCLUDED` if any gate excludes.

**Two fairness invariants (non-negotiable):**

1. *Every statement can reach HIGH on its own merits.* No gate caps a whole source below HIGH for a
   reason a good record can't overcome. In particular the provenance gate rewards **any** verifiable
   provenance and only penalises its absence — it does **not** require a direct document deep link,
   because whether a source publishes per-record links (DailyMed/EMA do; Orange Book/GRLS/CDE don't) is
   a publishing convention, not a reliability signal.
2. *Human review mitigates all concerns.* A curator marking a statement `review_status: CONFIRMED`
   forces HIGH, overriding every automated gate — the statement-level twin of the per-decision hatches
   already honoured (grounding `curated`, translation `OFFICIAL`). So no record is ever *stuck* below
   HIGH; review is always a path up.

The score reacts to curation automatically: promote a grounding, mark a translation `OFFICIAL`, confirm
a statement, or drop an inversion (REVIEW.md), and the tier rises on the next rebuild.

## 4. What the prototype produces today

`just reliability-report` over the current `products/*` (pre-drop build):

```
statement_type              HIGH    MEDIUM       LOW  EXCLUDED     total
DRUG_APPROVAL               2955       229       115       951      4250
INDICATION                  5733       345       247        10      6335
CONTRAINDICATION            1983       266       128         0      2377
RESEARCH_ASSOCIATION         164         0         0         0       164  (non-core)
Total: 13126 · Reliable core (HIGH+MEDIUM): 11511 (87.7%)
```

Reading it: ~88% of core statements are importable, and every source can reach HIGH — a well-grounded
Orange Book approval with a registry reference is HIGH even though FDA publishes no per-NDA PDF (the
earlier design wrongly capped these at MEDIUM). The **951 EXCLUDED approvals** are drug-list entries
with no approval flag and no provenance (unapproved EveryCure drugs) — correctly removed, where the
earlier design defaulted them to HIGH. The **10 EXCLUDED indications** are the negation inversions the
extractor drops at ingest (defense-in-depth). The LOW rows are fuzzy grounding / synonym-only snippets —
the REVIEW.md worklist that, once curated (or confirmed), promotes.

## 5. Consumer contract

- `medic.reliability.is_reliable(record)` → the default soft-launch predicate (core + HIGH/MEDIUM).
- `classify_statement(record)` / `score_reliability(record)` for consumers wanting their own threshold
  (e.g. HIGH-only, or including research).
- Every statement in the released products carries `statement_type` and `reliability` (Task 3), so the
  filter is a column select, not a code dependency.

## 6. Implementation plan

The prototype (Tasks 1–2) is **done and committed**; the rest wires it into the release.

- **Task 1 — enums + scorer (DONE).** `src/medic/reliability.py`: `StatementType`, `ReliabilityTier`,
  `classify_statement`, `score_reliability`, `is_reliable`; 16 tests.
- **Task 2 — report (DONE).** `just reliability-report` (type × tier tally over products).
- **Task 3 — stamp the products.** In the merge/export stages, write `statement_type` + `reliability`
  onto each released record (drug_list, indication_list, contraindication_list, research_list). Add both
  as slots to the schemas (`drug.yaml`, `indication.yaml`, plus the `StatementType`/`ReliabilityTier`
  enums in a shared schema module). Regenerate the datamodel.
- **Task 4 — a first-class reliable export.** `exports/medic_core_reliable.{tsv,jsonl}` = the
  `is_reliable` subset, so a consumer imports one file. KGX edges gain the two properties for graph
  consumers.
- **Task 5 — release gate + docs.** Add the reliability distribution to the release gate (REVIEW.md §9);
  fail a soft-launch release if the reliable-core fraction drops below a floor. Document the consumer
  contract in `docs/architecture.md` and the README.
- **Task 6 (optional) — calibration.** Spot-check ~50 records per tier against the source to confirm the
  gates are calibrated (esp. the HIGH bar); tune thresholds once, in one place.

## 7. Decisions made (flagged for review)

1. **Gates, min-combine + a human-confirmation override.** Simplest defensible rubric; each gate = one
   failure-mode family. Weighted numeric score rejected as harder to explain/act on than an ordinal tier.
2. **Fairness invariants enforced (§3).** No source-format ceiling (provenance gate does not require a
   deep link → Orange Book/GRLS/CDE can reach HIGH), and `review_status: CONFIRMED` always forces HIGH.
   This replaced the earlier design that capped linkless approvals at MEDIUM and defaulted
   provenance-less records to HIGH. *Open:* where does `review_status` live — a slot on the evidence
   item, or a small curated review store keyed by the statement's ids? (Task 3 decision.)
3. **Core = the three regulatory types.** Research/AE are non-core (opt-in). *Open:* should
   OFFICIAL-curated research be promotable into the default subset?
4. **Grounding confidence thresholds (0.9 / 0.7).** Chosen so exact→HIGH, salt/formulation→MEDIUM,
   fuzzy→LOW. Calibrate in Task 6.
5. **Score is computed, not stored, in the prototype.** Task 3 persists it so consumers don't re-run the
   scorer. Entailment/negation are recomputed from the snippet each run (cheap, deterministic).
