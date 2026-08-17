# Integrating hyperrelational indication context into the redesign

*Date: 2026-08-13* · **Status:** proposed, not implemented · Tracks
[monarch-initiative/medic#9](https://github.com/monarch-initiative/medic/issues/9)

Issue #9 asks what `medi/indications/data/03_primary/matrix_indication_list_hyperrelational.xlsx`
is, and Nico's answer sets the task: *"The format is not important. But the pipeline is."* This
records what that pipeline does, why it matters, and how to land it in the redesign — written
before deleting `medi/`, so the knowledge survives the deletion.

## What the pipeline actually is

A Kedro pipeline (`medi/src/medi/pipelines/on_label/`) that ran a **second LLM pass over DailyMed
indication text**, on top of the disease-extraction pass the redesign already reproduces:

1. `mine_fda_indications.py` pulls indication text out of SPL XML.
2. `extract_named_diseases` — Gemini batch, disease list per indication text. **The redesign
   already does this** (`extract_diseases_from_text`).
3. `extract-hyperrelations-fda` — Gemini batch with `indications_hyperrelations_prompt`,
   producing per-claim clinical context. **The redesign does not do this at all.**

Output: 10,223 rows, every one carrying a `hyperrelations` JSON payload.

The prompt is the asset worth keeping. It enforces a **closed context vocabulary** and is
explicitly extractive — *"Do not infer any of these relationships and do not add them if they are
not specified"* — which is the same discipline the redesign applies elsewhere:

> From the following indications text, extract each indication in JSON format along with the
> hyperrelational context required to understand the complete medical intent of the indication.
> […] The options for hyperrelational context are: `previous_history`, `patient_type`, `stage`,
> `coadministration`, `mutation` / `mutation_type`, `additional_details`. Do not infer any of
> these relationships and do not add them if they are not specified. For `patient_type`, relevant
> types are `not_specified`, `adult`, `pediatric`, `pregnant`, or `of childbearing age`.
> `coadministration` does not refer to specifying whether the therapy is a first-line or
> adjunctive therapy but rather listing coadministered drugs […] Do not include any indications
> for allergy tests, diagnostic agents, or surgical procedures.

Payload shape, from a real row:

```json
[{"drug": "Renese", "disease": "edema",
  "context": [{"coadministration": "corticosteroid and estrogen therapy",
               "additional_details": "adjunctive therapy"},
              {"previous_history": "congestive heart failure"},
              {"previous_history": "Nephrotic syndrome"}]}]
```

## Why this is a correctness problem, not enrichment

That Renese row is the argument. The label says polythiazide is indicated *as adjunctive therapy*
for **edema** in patients with a history of congestive heart failure, hepatic cirrhosis or
nephrotic syndrome. Flat extraction turns one qualified claim into several unqualified ones —
the same spreadsheet carries `CHEBI:8327|MONDO:0005009` (congestive heart failure),
`|MONDO:0005155` (liver cirrhosis) and `|MONDO:0005377` (nephrotic syndrome) as separate
indications.

**MeDIC would be asserting that polythiazide treats liver cirrhosis.** It does not; it treats
oedema in patients who have it. A drug-repurposing consumer cannot tell the difference from the
flat row, and the qualifier is exactly the clinically load-bearing part.

This is [#24](https://github.com/monarch-initiative/medic/issues/24) (clinical qualifiers dropped,
distinct claims collapse) with a working extractor already built for it.

## The gap in the redesign

Two distinct problems, and only one is wiring.

**1. Nothing populates it.** `hyperrelations` is carried through `SourceAssertion` and the merge
reads `record.get("hyperrelations")`, but **zero of the 12,694 assertions in the current build
have one**. The slot exists; no ingester fills it.

**2. The schema models a different thing.** `Hyperrelation` in `indication.yaml` is
*symptom-level*: `target_symptom`, `relationship` ∈ {REDUCES, PREVENTS, TREATS,
CONTRAINDICATES_IN, WORSENS}, `specificity_text` — "reduces tremor in Parkinson's". The pipeline
produces *claim qualifiers*: who the patient is, what they were treated with before, what stage,
what it is co-administered with. Neither subsumes the other, and forcing the payload into the
existing class would lose the closed vocabulary that makes it queryable.

## Plan

**Stage 1 — model the qualifiers properly.** Add a `ClinicalQualifier` class with the closed
vocabulary as an enum (`QualifierTypeEnum`: `PREVIOUS_HISTORY`, `PATIENT_TYPE`, `STAGE`,
`COADMINISTRATION`, `MUTATION`, `ADDITIONAL_DETAILS`), a `value` string, and — where the value is
itself an entity, as `previous_history` and `coadministration` usually are — an optional resolved
`Mention` so a qualifier can be grounded like anything else. Keep `Hyperrelation` for symptom
relations; these are different axes. Per the transformation-traceability rule, the enum lands
before any code emits a value.

**Stage 2 — port the extractor.** Reimplement `extract-hyperrelations-fda` as a second pass in
`medic.ingest.dailymed`, reusing the existing cache and `versions.llm_agent` pinning so the dated
model id is recorded. The Gemini prompt transfers verbatim — it is model-agnostic and its
extractive discipline is the reason it produces usable output. Emit the result as an
`ExtractionStep` with its own `co_mentions`, not as an opaque JSON blob.

**Stage 3 — attach at the right level.** A qualifier belongs to the **assertion**, not the pair:
it is what *this document* said about *this claim*, so it rides on `SourceAssertion` beside
`spans` and `evidence`. Two sources qualifying the same pair differently is real signal, and the
source-scoped model already keeps them apart.

**Stage 4 — make the collapse visible.** Once qualifiers exist, a pair whose disease is only
mentioned as a `previous_history` qualifier is a candidate mis-extraction. Add a QC check
counting those, and an `AssertionFlag` for it. That is what turns this from enrichment into the
fix for #24.

**Stage 5 — decide on the 10,223-row asset.** The spreadsheet is a *cached output* of a pipeline
we are re-running, not a source. Re-extracting from current DailyMed is preferable — the SPL set
has moved since. Keep the xlsx only if it is wanted as a regression fixture, and if so put it in
`sources/` with a fingerprint, not in a resurrected `medi/`.

## Not doing

Reviving Kedro, or keeping `medi/` alive as a parallel pipeline. The extraction *method* is worth
porting; the orchestration around it is superseded. Two pipelines producing indications is how the
provenance model got confused in the first place.

## Deleting `medi/`

Everything load-bearing is captured above: the prompt verbatim, the closed vocabulary, the payload
shape, the worked example, and the pipeline's three stages. The tree remains in git history at
`39a1b38` and its parents if a detail is needed later.
