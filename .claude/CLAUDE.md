# MeDIC Project Instructions

## On Startup

- Read `docs/architecture.md` to understand the full pipeline architecture before making changes
- Read `docs/source-isolation.md` — this is a hard architectural invariant. No ingester may emit evidence rows for a jurisdiction it does not itself originate.
- Read `SPEC.md` (the single source of truth: requirements, invariants, design decisions, task ledger, and open items) and ask: "Do you want to continue with our implementation plan?" then proceed based on the user's response

## Source Isolation (CRITICAL)

- Each source ingester emits evidence ONLY for its own jurisdiction (see `docs/source-isolation.md`).
- DailyMed → USA only. EMA → EU only. PMDA → JAPAN only. Orange Book / Purple Book → USA only. Russia → RUSSIA only. India → INDIA only. China → CHINA only.
- Cross-jurisdiction *merging* happens in `src/medic/merge/on_label_merge.py` and is allowed there.
- Cross-jurisdiction *emission* at ingest is forbidden, even when the upstream raw file has cross-jurisdictional flag columns. Strip the columns; do not synthesise rows.

## Transformation Traceability (CRITICAL) — invariant I-8

- Every step from the verbatim source string to the final canonical id — **translation, string preprocessing, grounding, normalization** — must be captured as a **named step recording both its incoming and outgoing value**. The chain must be replayable from the record/SSSOM row alone.
- **Every transformation action is a controlled enum value** — `PreprocessingRuleEnum` (`src/medic/schema/grounding.yaml`, mirrored by `RULE_CERTAINTY`/`RULE_PREDICATE` in `preprocess.py`) for string transforms; `GroundingQualityEnum` / `NormalizationQualityEnum` for stage outcomes. No transform is applied anonymously or in-place.
- **Adding any new manipulation means adding an enum value FIRST**, then emitting the in→out pair into provenance (SSSOM `subject_preprocessing` + `match_string`, and the `Grounding`/`Normalization` objects). A code-vs-schema test enforces the enum/code maps agree.

## Git Commits

- Never add Co-Authored-By lines or any AI attribution to commit messages

## CURIE Handling

- Always use `src/medic/curie_utils.py` for all CURIE operations (parsing, prefix extraction, ID extraction)
- The converter uses `curies.get_bioregistry_converter()` chained with MeDIC-specific prefixes (same pattern as sssom-py)
- Never manually split CURIEs with `str.split(":")` — use `parse_curie()`, `get_prefix()`, `get_local_id()`, `find_by_prefix()` from `medic.curie_utils`

## Architecture Documentation

- Keep `docs/architecture.md` up to date when making changes to the pipeline
- This document is the primary reference for external collaborators
- Update it whenever: new sources are added, pipeline stages change, new exports are added, or shared infrastructure changes

## HuggingFace Datasets

- Disease list and EveryCure drug list are sourced from HuggingFace (`everycure/disease-list`, `everycure/drug-list`)
- Use the `datasets` library (not `huggingface_hub`) to load them: `load_dataset("everycure/...", split="train")`
- HuggingFace is the single source for these — **no local-file fallback** (fail loud if HF is unavailable, per the no-legacy-fallback decision, SPEC §9).
