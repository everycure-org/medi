# Source Isolation

## Rule

**Each source ingester emits evidence rows only for the jurisdiction and source it itself originates.**

No ingester may fabricate evidence for a different source or a different jurisdiction, even when the upstream raw data carries cross-jurisdictional flags or hints.

## Rationale

Evidence rows are read by reviewers who treat the `jurisdiction` and `explanation` fields as factual claims about provenance. When DailyMed emits a row that says `jurisdiction: EU` and `explanation: "EMA-approved indication from European public assessment report"`, a reasonable reviewer infers the row was sourced from the EPAR system. If the row was actually constructed from a DailyMed cross-jurisdiction column, that inference is wrong — the row is misattributed.

Misattribution corrupts:
- **Audit**: `original_drug_label` / `original_disease_label` no longer reflect what *that source* actually wrote.
- **URL provenance**: a fabricated `?search_api_fulltext=` URL pretends the EMA site published the link, but EMA never saw the query.
- **Confidence semantics**: HIGH/MEDIUM/LOW confidence per source becomes meaningless when sources cross-pollinate.
- **Coverage stats**: per-jurisdiction counts in `products/indication_list.yaml` answer the wrong question — they conflate true EMA approvals with DailyMed-spreadsheet flags labelled "EMA".

## What this means in practice

| Ingester | Allowed jurisdictions | Allowed `source_type: REGULATORY` evidence |
|---|---|---|
| `src/medic/ingest/dailymed/__main__.py` | `USA` only | DailyMed SPL labels |
| `src/medic/ingest/orangebook/__main__.py` | `USA` only | Drugs@FDA NDA approvals |
| `src/medic/ingest/purplebook/__main__.py` | `USA` only | Purple Book BLA approvals |
| `src/medic/ingest/ema/__main__.py` | `EU` only | EMA EPAR records |
| `src/medic/ingest/pmda/__main__.py` | `JAPAN` only | PMDA review reports + tabulation |
| `src/medic/ingest/india/__main__.py` | `INDIA` only | CDSCO records |
| `src/medic/ingest/russia/__main__.py` | `RUSSIA` only | GRLS records |
| `src/medic/ingest/china/__main__.py` | `CHINA` only | NMPA / CDE records |

The merger (`src/medic/merge/on_label_merge.py`) is allowed — and required — to combine evidence from multiple sources into a single association. That is **not** bleeding. It's the merger's job to deduplicate, dedup-prefer-PRIMARY, and assemble the per-association evidence list. The rule prohibits cross-source emission at *ingest*, not assembly at *merge*.

## What is and isn't bleeding

**Not bleeding (allowed):**
- DailyMed and Orange Book both produce USA evidence rows for the same drug-disease pair. The merger keeps both — they describe different artifacts (label record vs marketing-authorisation record).
- The merger injects an Orange Book FDA artifact row alongside a DailyMed evidence row, drawing identifiers from `kb/drugs/orangebook/orangebook.yaml`. Both rows are sourced from their respective ingesters; the merger is just placing them.

**Bleeding (forbidden):**
- DailyMed reading a `EMA: 1.0` flag from its own spreadsheet and emitting an `evidence` row with `jurisdiction: EU`. (This was the historical bug fixed in this revision.)
- Any ingester referencing another ingester's identifier scheme as if it were its own — e.g. an Orange Book ingester looking up a DailyMed setid as if NDA records carried setids.
- An ingester emitting an `original_drug_id` whose value comes from a different source's identifier system (e.g. a Russia ingester emitting an SPL setid because the same drug also has a DailyMed entry).

## Enforcement

This is a code-level invariant. There is currently no automated check, but reviewers should reject any PR that:

1. Adds `jurisdiction: <X>` evidence emission inside an ingester whose source is not the canonical authority for `<X>`.
2. Imports identifier-lookup utilities from another source's ingest module (`from medic.ingest.dailymed.setid_lookup import …` from anywhere except the DailyMed ingester is a code smell).
3. Reads cross-jurisdictional flag columns from a single-source raw file and uses them to set association-level `fda` / `ema` / `pmda` booleans.

## Historical context

The pre-`redesign` v1.0 Kedro pipeline (`medi/indications/`) processed each source separately into per-source dataframes (`fda_list`, `ema_list`, `pmda_list`) and then unioned them with `join_lists` (in `medi/indications/src/matrix_indication_list/pipelines/indications_list/nodes.py`). Each row in the unioned table carried a boolean column — `FDA`, `EMA`, `PMDA` — set to `True` if that row originated in that source's list. After a `groupby('drug|disease')` collapse, one row per drug-disease pair held a multi-flag attribution: "this association is approved by FDA *and* EMA *and* PMDA", or some subset.

In v1.0 these flags were **legitimate source-attribution metadata**, not bleeding. They recorded which per-source pipeline had produced each pre-union row.

The bleeding entered at the v1.0 → v2.0 migration boundary. The `redesign` rewrite reads the v1.0 matrix (`matrix_indication_list.xlsx`) as a fallback in `dailymed/__main__.py:_fallback_indications` and translates each row into evidence. The migration author had a choice:

- **Option A (correct)**: emit only a USA evidence row from DailyMed; let `src/medic/ingest/{ema,pmda}/__main__.py` emit the EMA/PMDA evidence for the same drug-disease pair.
- **Option B (what was done)**: emit a USA row *plus* an EMA row *plus* a PMDA row from DailyMed's fallback whenever the v1.0 flags showed multi-jurisdiction approval. URLs and explanations were fabricated client-side because the EPAR / PMDA documents themselves weren't being read in this code path.

Option B was likely chosen to preserve the v1.0 multi-jurisdiction coverage during migration, before the per-source EMA/PMDA ingesters (`src/medic/ingest/{ema,pmda}/__main__.py`) were complete. Once the per-source ingesters landed, the fallback's EMA/PMDA emission became *redundant* with the per-source path — and worse than redundant, because the fabricated rows out-numbered the true per-source EPAR rows for many drug-disease pairs.

This was removed (see `_build_regulatory_evidence` in `dailymed/__main__.py` and the source-isolation note in `_fallback_indications`). The `is_ema` / `is_pmda` arguments are accepted for back-compat and explicitly ignored.

The dead module `src/medic/ingest/on_label_ingest.py` contains the same anti-pattern but is no longer invoked by the build (`justfile`'s `build-on-label-list` does not call it). It can be removed in a future cleanup; it's left in place to avoid mixing concerns in the source-isolation revision.

## Related

- [Architecture](./architecture.md)
- [`src/medic/merge/on_label_merge.py`](../src/medic/merge/on_label_merge.py) — `_dedup_evidence_prefer_primary` enforces PRIMARY-over-INTERMEDIARY at merge time, partial safety net for any ingester that violates the rule.
