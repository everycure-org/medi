# MeDIC — work queue (priority order, 1 = highest)

The deterministic grounding rework, all source rebuilds (Russia/DailyMed/China), the LLM-stack
fixes, and the grounding-metadata funnel are **done and committed** (10 logical commits on
`redesign`). Regenerated data (`kb/`, `mappings/`, `cache/enrichment/`) is left uncommitted for
review. `products/` and `exports/` are gitignored release artifacts. Remaining work, ordered:

1. **Formulation string → active-ingredient CHEBI** — [issues/issue_formulation_grounding.md](issues/issue_formulation_grounding.md) — *in progress (subagent)*. Biggest drug-recall gap (India 0% grounded); deterministic formulation stripper composing with the existing ladder, an LLM active-moiety tail fallback, and an honest CHEBI-coverage boundary.
2. ~~Commit & review the changeset~~ — **DONE** (10 commits). Review of the uncommitted `kb/`/`mappings/`/`cache/` data remains yours.
3. **China full-scale ingest run** — [issues/issue_china_full_run.md](issues/issue_china_full_run.md) — ~8k Chinese→INN translations (cached/resumable); China has 0 records in products until run.
4. **Adverse events (PVLens/FAERS)** — [issues/issue_adverse_events.md](issues/issue_adverse_events.md) — whole product is a stub; needs MedDRA→MONDO/HP mapping + real ingest.
5. **EMA/PMDA contraindications** — [issues/issue_contraindications_ema_pmda.md](issues/issue_contraindications_ema_pmda.md) — contraindications are FDA-only; needs SmPC / package-insert PDF parsing.
6. **Russia Cyrillic grounding improvement** — [issues/issue_russia_cyrillic.md](issues/issue_russia_cyrillic.md) — 17% → higher (fuller transliteration or Russian→INN dictionary). A sub-case of #1.
7. **Research integration** — [issues/issue_research_integration.md](issues/issue_research_integration.md) — ClinicalTrials.gov + systematic PubMed beyond deep research.
8. **Full I-8 transformation traceability** — [issues/issue_transformation_traceability.md](issues/issue_transformation_traceability.md) — capture per-step in→out values; pull China translation (+ RxNorm) fully into the enum-named chain. (Invariant partially met today.)
9. **Fuzzy isotope false-positive tightening** — [issues/issue_fuzzy_isotope.md](issues/issue_fuzzy_isotope.md) — drop digit-substitution edits from `fuzzy_edit1_unique` (`13C`→`14C`).
10. **Build drug normalization index** — [issues/issue_drug_normalization_index.md](issues/issue_drug_normalization_index.md) — `cache/normalization/drugs.db` for CHEBI obsolete/replaced handling.
11. **CI** — [issues/issue_ci_validators.md](issues/issue_ci_validators.md) — run the 3 validators on push.
12. **SSSOM unresolved-tail curation** — [issues/issue_sssom_curation.md](issues/issue_sssom_curation.md) — manual pass over high-value `NoTermFound` rows in `mappings/*.sssom.tsv`.
