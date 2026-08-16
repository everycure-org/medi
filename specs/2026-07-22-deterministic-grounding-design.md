# Deterministic Grounding + Normalization with SSSOM Decision Stores — Design Spec

**Status:** implemented · **Date:** 2026-07-22 (updated 2026-07-24) · **Branch:** `redesign` · **Author:** Nico Matentzoglu

This spec defines a replacement for MeDIC's current grounding subsystem. It splits entity
resolution into two explicitly-documented, deterministic stages — **grounding** (source string →
initial ID) and **normalization** (initial ID → canonical ID) — each backed by a hand-editable
SSSOM file that is the authoritative cache. It supersedes the grounding cascade described in
`SPEC.md` §6 and amends invariants I-2 and I-4.

---

## 1. Motivation

The current grounder (`src/medic/grounding/`, cascade OAK→Gilda→**NameRes**→OLS, 0.80 threshold,
LLM preprocessor + reranker, per-source JSON cache) has two structural problems:

1. **Non-determinism.** NameRes (a remote SRI service) returns different top hits across runs.
   Regenerated KB files churn on every build; observed real cases include a disease literal
   resolving to `MONDO:1012664` "mucopolysaccharidosis II, Kaka" (a corrupted label) one run and
   `MONDO:0009657` "MPS III C" the next — neither correct, both un-reviewable.
2. **No systematic, reviewable record of decisions.** The `cache/grounding/*.json` files are an
   opaque per-source cache, not a curatable audit of every string→ID and ID→ID decision.

We want: (a) a **systematic, complete, diffable resource of all grounding and normalization
decisions** in SSSOM; (b) a **deterministic, offline, custom** resolver whose decisions are stable
run-to-run and **correctable by hand-editing** the SSSOM files.

## 2. Goals and non-goals

**Goals**
- Deterministic: same input → byte-identical decision stores, no network calls at resolve time.
- Two separately-documented stages (grounding, normalization), each with its own schema class,
  quality enum, and SSSOM store.
- Source string is preserved verbatim and never mutated (Invariant I-7, new — see §9).
- Every decision (including failures) is recorded in an SSSOM file that is the authoritative cache
  and is hand-editable; manual edits survive regeneration.

**Non-goals (v1 — deferred, see §11)**
- LLM preprocessing, translation, or reranking (the `translation_llm` rule is scaffolded but disabled).
- Embedding / similarity matching, and any at-resolve-time network call (NameRes, OLS, NodeNorm).
- Inventing ID↔ID mappings not published by the target namespace.

*(Update: fuzzy edit-1 matching and a curated translation dictionary were brought in scope during
implementation — deterministic, but emitted as curator-reviewable proposals. See §11.)*

## 3. Architecture overview

```
raw source string          preserved verbatim on the record — faithful at all costs (I-7)
   │                        minted a stable MEDICNE:<uuid5> id at extraction (I-9)
   │
   │  STAGE 0 · TRANSLATION        non-English string → English   src/medic/translation/
   │   (non-English sources only: China zh, Russia ru)
   │   documented in  mappings/drug_translation.babelon.tsv   (Babelon, keyed by MEDICNE id)
   │   DeepL via the babelon translator service · hand-editable · deterministic cache
   ▼
English string  (replaces the string the grounder sees; foreign original kept in the Babelon row)
   │
   │  STAGE 1 · GROUNDING          string → initial ID     src/medic/grounding/
   ▼
grounded_id  (any supported vocab)
   │   documented in  mappings/<type>_grounding.sssom.tsv    (SSSOM literal profile)
   │   hand-editable · authoritative cache · rows tagged by mapping_justification
   │
   │  STAGE 2 · NORMALIZATION      ID → ID (target-namespace-supported only)   src/medic/normalization/
   ▼
normalized_id  (canonical target: MONDO for disease, CHEBI for drug)
       documented in  mappings/<type>_normalization.sssom.tsv  (SSSOM term↔term)
       hand-editable · authoritative cache · rows tagged by mapping_justification
```

**Supported grounding vocabularies (cascade priority order, as built):**
- Diseases: `MONDO` > `HPO` > `UMLS`
- Drugs: `CHEBI` > `DRON`

`ICD10CM` (diseases) and `PR` (drugs) were built and measured, then **dropped** as documented,
reversible config in `conf/grounding_sources.yaml`: ICD10CM added only +64 groundings (redundant —
UMLS's own MRCONSO already carries ICD10CM as a source SAB), and PR added only +15 (drug names are
chemicals, not proteins, for a ~1.3 GB download + heavy `robot` convert). Re-enable by uncommenting
the source rows. `UniProt` was never needed (no drug-target grounding in scope).

**Effective ID.** Downstream merge/exports use `normalized_id` when a normalization exists, else
`grounded_id`. Both are always retained on the record.

**Integration (Architecture C).** The Stage-1 grounder is a new `GroundingService` behind the
existing `factory.get_grounding_service` interface, so ingesters are unchanged. The `nameres`
backend is retired. All store I/O goes through dedicated store modules, not the old per-source JSON
cache.

## 4. Schema additions

New schema module `src/medic/schema/grounding.yaml`, imported by `medic.yaml`.

### 4.1 `Grounding` class (record slot: `grounding`)
| slot | range | notes |
|---|---|---|
| `original_string` | string | verbatim source string; required |
| `grounded_id` | uriorcurie | initial ID in any supported vocab; empty if unresolved |
| `grounded_label` | string | canonical label of `grounded_id` |
| `grounding_quality` | `GroundingQualityEnum` | required |
| `confidence` | float | 0–1, derived from the matched rung's rule weight |

All other decision metadata (predicate, matched field, applied preprocessing, match string,
tool/version) lives in the grounding SSSOM row, **not** on the record.

### 4.2 `Normalization` class (record slot: `normalization`)
| slot | range | notes |
|---|---|---|
| `original_id` | uriorcurie | the `grounded_id` fed into normalization |
| `normalized_id` | uriorcurie | canonical target ID; equals `original_id` when quality = `none` |
| `normalized_label` | string | canonical label |
| `normalization_quality` | `NormalizationQualityEnum` | required |
| `tool` | string | tool + version that produced the mapping |

### 4.3 `GroundingQualityEnum` (string-manipulation tiers)
- `curated` — manual assertion in the grounding SSSOM.
- `lexical_exact` — exact match to label or synonym, string unchanged.
- `lexical_exact_normalized` — exact after **non-semantic** normalization (whitespace, comma,
  dashes, case, unicode).
- `lexical_exact_surgery` — exact after **minor semantic surgery** (e.g. `disease`→`disorder`,
  `type 1`→`type I`).
- `unresolved` — no deterministic single match (includes ambiguous multi-candidate hits).

Label-vs-synonym is **not** a quality tier; it is recorded as `object_match_field` in the SSSOM.

### 4.4 `NormalizationQualityEnum` (mapping-directness tiers, analogous)
- `curated` — manual assertion in the normalization SSSOM.
- `asserted_exact` — the target namespace itself asserts a `skos:exactMatch`/db-xref from
  `original_id` to `normalized_id`.
- `deprecated_replacement` — `original_id` is obsolete in the target namespace and carries a
  `replaced_by` to `normalized_id`.
- `none` — no target-namespace-supported normalization exists; `normalized_id` = `original_id`.

### 4.5 `PreprocessingRuleEnum` (the transform catalogue)
Every deterministic string transform the grounder may apply is a permissible value of
`PreprocessingRuleEnum` in `grounding.yaml`. Each value carries a **three-annotation mixin**:
- `certainty` (0–1 float) — how meaning-preserving/trustworthy the transform is (1.0 = no risk).
  This drives the per-match `confidence` written to the grounding SSSOM.
- `rule_family` — a coarse grouping for later selection/aggregation.
- `predicate` — the SSSOM predicate a match via this rule asserts.

These are **mirrored in code** by `RULE_CERTAINTY` / `RULE_PREDICATE` (`preprocess.py`), and a unit
test enforces that the maps and the schema annotations agree. Rule families as implemented:
`base` (always-applied `base_normalization`), `punctuation` (comma/hyphen/cell-token structural),
`lexical_surgery` (disease↔disorder, arabic↔roman, `strip_leading_other`), `spelling_en`
(British↔American), `spelling_inn` (foreign INN transliteration: `inn_suffix_in_to_ine`, `inn_z_to_s`,
`inn_ph_to_f`, `inn_ti_to_thi`, `inn_ae_oe_to_e`), `salt` (`salt_ester_strip`), `qualifier`
(`qualifier_strip`), `structural` (`combination_split`), `formulation` (`formulation_strip` — dosage/
unit/form/pharmacopoeia stripping, `conf/grounding_formulation.yaml`), `transliteration`
(`cyrillic_transliteration`), `translation` (`translation_dictionary` + the deferred `translation_llm`),
`fuzzy` (`fuzzy_edit1_unique`), and `substance_resolver` (`rxnorm_resolve` — RxNav ingredient lookup,
a network-derived curator-reviewable proposal, not run at resolve time). The matched rule ids are
written to the SSSOM `subject_preprocessing` slot, so every "hack to beat a label into shape" is
auditable (invariant I-8, full-traceability).

### 4.6 Migration of existing fields
Today's flat `normalized_id` / `normalized_label` / `grounding_confidence` / `grounding_service`
fields on records are replaced by the structured `grounding` and `normalization` objects. A
compatibility shim may retain the flat fields as read-only mirrors during transition.

## 4b. Stage 0 — Translation (Babelon / DeepL) + MEDICNE mentions

Modules `src/medic/mention.py` + `src/medic/translation/`.

The deterministic lexical grounder only matches English/Latin strings. Non-English sources are
translated to English **before** Stage 1, as a first-class, recorded stage (not a hidden ingester
detail) — its own record slot alongside `grounding`/`normalization`, its own decision store, and its
own `PreprocessingRuleEnum` value (`deepl_translation`).

**MEDICNE mention id (I-9).** `medic.mention.mint_mention_id(surface_form, entity_type)` returns a
deterministic `MEDICNE:<uuid5>` — `uuid5` of `(entity_type, base-normalized surface form)` under a
fixed project namespace. Offline, per-surface-form, byte-identical. The `MEDICNE` prefix is registered
in `curie_utils`. It is assigned **at extraction** by `assign_mention(record, entity_type)`, which
stamps two uniform fields on every source record — `original_literal` (verbatim source string) and
`mention_id` — so the id is the single identifier of the original literal from the very start. The id
then anchors the whole trail: the Babelon `subject_id`, the grounding SSSOM `subject_id` column (a
previously-empty column now filled — a step toward a fully id-keyed term↔term grounding store), the
`Grounding.subject_id` object slot, and the product record. The UI can join
`translation → grounding → normalization` by that one id. For non-English sources the id is minted from
the **foreign** literal before translation overwrites `source_name`, so it stays pinned to the original.

**Translator.** `medic.translation.TranslationService` wraps the `babelon` translator service
(`babelon.translate.translate_profile`, `model="deepl"`). DeepL is a professional MT engine — for drug
names it resolves recognised INNs directly (`Абакавир → Abacavir`, `来那度胺胶囊 → Lenalidomide
Capsules`); residual dose/form/salt words are handled downstream by the grounder's `formulation_strip`
/ `salt_ester_strip` rules. `DEEPL_API_KEY` is loaded from `.env`. Target language is `en-us` (DeepL
rejects bare `en`). Each translation is stamped `translator: wikidata:Q116709136`,
`translator_expertise: ALGORITHM`, `translation_status: CANDIDATE`.

**Store (`mappings/drug_translation.babelon.tsv`).** A minimal Babelon profile (plain header-row TSV),
one row per mention keyed by `subject_id = MEDICNE:<uuid5>`, columns: `predicate_id` (`rdfs:label`),
`source_language`, `source_value` (verbatim foreign — I-7), `translation_language`,
`translation_value`, `translator`, `translator_expertise`, `translation_status`, `translation_date`,
`comment`. The git-tracked table is the **authoritative deterministic cache**: a row with a filled
`translation_value` is never re-translated (mirrors the grounding/normalization SSSOM stores). Manual
edits survive regeneration. `MEDIC_SKIP_EXPENSIVE_CALLS` leaves rows `NOT_TRANSLATED` (offline
plumbing check; they will not ground).

**Record + funnel.** `translate_records(records, source_language)` mints the id (`mention_id`),
attaches the `translation` object (schema class `Translation`), and **replaces** `source_name` with the
English `translation_value` when present (the foreign original stays in `source_value` and the
ingester's own `original_name_*`). Names DeepL cannot translate keep their original value, so Russia's
Cyrillic-transliteration ladder still catches those in Stage 1. `drug_merge` funnels the representative
`translation` object onto `products/drug_list.yaml`, next to `grounding`/`normalization`.

**Wired sources.** China (`zh`) and Russia (`ru`), both drug-list-only. China's previous per-name LLM
translator (`ingest/china/translate.py`, `cache/enrichment/china_translation.json`) is removed.

## 5. Stage 1 — Grounding (deterministic lexical)

Module `src/medic/grounding/lexical/`.

### 5.1 Unified lexical index (`index.py` + `build.py` + loaders)
One compiled offline **SQLite** index per entity type (`diseases`, `drugs`), **queried directly on
disk** (not materialized to RAM — the disease index runs to ~3.69M rows from UMLS `MRCONSO`). Table
`lex` columns:
`(object_id, object_label, string_value, raw_value, norm_value, match_field, synonym_scope,
source_prefix)`, with B-tree indexes on `raw_value` and `norm_value`. `match_field` ∈ {`label`,
`exactSynonym`, `broadSynonym`, `narrowSynonym`, `relatedSynonym`}. Two keyed forms per source
string:
- `raw_value` = whitespace-trimmed, **case-sensitive** (drives the tier-1 "string unchanged" match).
- `norm_value` = `base_normalize()` output (drives tier-2 and, via query-side surgery, tier-3).

Loaders normalize each source into `lex` rows (`conf/grounding_sources.yaml` declares each source's
`prefix`, `loader`, and `path`):
- **OBO** (`loaders/obo_json.py`): MONDO, HPO, CHEBI, DRON parsed from **OBO Graph JSON downloaded
  from the OBO PURLs** into `cache/ontologies/*.json`, using a **stdlib JSON parser** — the repo's
  pinned `oaklib` (0.1.0) is too old for the semantic-sql adapter path originally envisaged, so we do
  not depend on it. We ingest **all** synonyms, including MONDO's `MONDO:LexicalVariation` synonyms
  shipped by mondo PR #10268 (~10k generated lexical variants), so MONDO's surgery variants are
  present index-side for free.
- **UMLS** (`loaders/umls.py`): streamed **directly from the local MRCONSO zip**
  (`background/umls-2021AA-mrconso.zip`, member `MRCONSO.RRF`) — no 2 GB extraction. The zip is
  MRCONSO-only (no `MRSTY.RRF`), so UMLS is filtered to a **disease-oriented SAB allowlist**
  (SNOMEDCT_US, ICD10CM, MSH, NCI, OMIM, ORPHANET, …) as a proxy for true semantic-type filtering.
- **ICD10CM** (`loaders/icd10cm.py`, present but not in the default cascade): a **tolerant
  BioPortal-Turtle line parser** (ICD10CM sourced from BioPortal submission 27 as UMLS2RDF Turtle;
  both OWLAPI/`robot` and rdflib choke on its malformed literals). Kept for reversibility.

Compiled to `cache/grounding/lexical_index/{diseases,drugs}.db`, rebuilt by
`just build-grounding-index` (batched inserts, indexes created after load). All CURIE handling via
`curie_utils` (I-6). Multi-row query results are ordered `ORDER BY object_id` before the
vocab-priority pick, so selection is deterministic regardless of SQLite row order.

### 5.2 Surgery rule engine (`preprocess.py`)
Two documented layers.

- **Base normalization** `base_normalize(s)` (non-semantic; applied to both index `norm_value` and
  query; a match here that was not already tier-1 is `lexical_exact_normalized`): unicode NFKD +
  combining-mark drop (diacritics), casefold, collapse whitespace, normalize commas/dashes/quotes,
  strip bracketed `[...]` content.
- **Surgery rules** `generate_variants(norm) -> [(variant, [rule_id])]` (minor semantic surgery;
  produce `lexical_exact_surgery`). These are **copied and adapted directly into our code** from
  mondo PR #10268's `lexical_variants.py` rule set (R1 arabic↔roman, R3 comma-drop, R6 `type-N`→`type
  N`, R8 British↔American, R10 cell-type hyphen↔space, …), plus the older `disease↔disorder` /
  `^Other ` rules. Because the transforms are **bidirectional**, generating variants from the
  **query** covers an ontology term stored in either form, so we never transform the multi-million-row
  index side (only MONDO's own shipped variants sit index-side). Applied **single-pass** (each rule
  once against the base-normalized string, results unioned — no recursive chaining, bounded and
  deterministic).

  Implementation notes (the guards, not the regexes, are the hard part — see PR #10268's bug table):
  we **keep** the semantic guards that prevent false conversions (roman↔arabic requires an
  indicator prefix like `type|stage|grade`; `(?<!\S)` lookbehind; trailing-`X`/chromosome exclusion;
  closed-list Brit/Am and cell-type swaps; curated proper-noun list to avoid mangling eponyms). We
  **drop** PR #10268's case-preservation machinery (lowercase-skip filter, mis-cased-noun guard) —
  those exist for human-facing synonym *generation* and are moot once both sides are uniformly
  casefolded by `base_normalize`. Closed-list data (spelling pairs, cell tokens, proper nouns) lives
  in small data files under `conf/`; the rule *logic* is Python with unit tests per rule.

Each rule carries a stable `rule_id`; the matched variant's `rule_id`s are written to the SSSOM
`subject_preprocessing` slot (query-side transform). A match against a MONDO-shipped
`LexicalVariation` synonym instead records `object_match_field = oio:hasExactSynonym` with no
subject preprocessing.

### 5.3 Match cascade (`matcher.py`)
Tiered lookup where **each tier is a quality level**, evaluated **rung-major, vocab-minor** (D1).
Per atomic literal, the first tier yielding **exactly one** `object_id` in the winning vocab wins:
0. curated override hit (`store.manual_rows`) → `curated`.
1. `trim(raw)` **== `raw_value`** (case-sensitive) → `lexical_exact`.
2. `base_normalize(raw)` **== `norm_value`** → `lexical_exact_normalized`.
3. **surgery** — each `generate_variants` output **== `norm_value`** → `lexical_exact_surgery`,
   predicate from the rule (`exact`→`skos:exactMatch`, `broad`→`skos:broadMatch`), `rule_id` recorded.
4. entity-scoped tier:
   - **[drugs]** INN spelling variants (`inn_variants`, family `spelling_inn`) → `skos:closeMatch`,
     then salt/ester strip (`salt_ester_strip`) → `skos:closeMatch`.
   - **[diseases]** qualifier strip (`qualifier_variants`, `qualifier_strip`) → `skos:broadMatch`.
5. **translation dictionary** (`translation_variants` over `conf/grounding_translation.yaml`,
   `translation_dictionary`) → `skos:closeMatch`.
6. **fuzzy edit-distance-1 with unique-hit guard** (`edits1` + `lookup_norm_many`,
   `fuzzy_edit1_unique`) → `skos:closeMatch`; accepted only when the edit-1 neighbourhood resolves to
   a single `object_id` in the top-priority vocab. Approximate, so written as curator-reviewable
   proposals.
7. else → `unresolved` (`predicate_id: sssom:NoTermFound`); candidate/normalized string logged.

Within each tier, `label` is tried before `exactSynonym`, and — **drugs only** — `relatedSynonym`
is additionally matched in every tier as `skos:closeMatch` (CHEBI files most US/INN drug-name
variants under `oio:hasRelatedSynonym`; disease exact-synonym coverage in MONDO/HP is already good,
so diseases skip it). Vocabs are walked in priority order. **Ambiguity is never auto-resolved** — a
tier yielding >1 distinct `object_id` in the winning vocab → treated as no-hit for that tier (and
ultimately `unresolved`), never tie-broken. Confidence is `RULE_CERTAINTY[rule]` for rule tiers, and
for base tiers the match-field weight (label 1.0, exactSynonym 0.95, relatedSynonym 0.85) times a
tier factor (raw 1.0, normalized 0.90).

**Combination splitting.** When the whole literal does not resolve, `split_combination` splits it on
`/ ; and +`-style separators; if **every** component grounds, the store receives **one SSSOM row per
component** (each tagged with the extra `combination_split` rule), so a single literal legitimately
yields multiple grounding rows.

## 6. Stage 2 — Normalization (target-namespace ID mapping)

Module `src/medic/normalization/`.

### 6.1 Mapping index
Built from the **target namespace's own** cross-references and obsolescence data: MONDO (diseases)
and CHEBI (drugs) `skos:exactMatch`/db-xref assertions and `replaced_by` for obsolete terms.
Compiled offline; rebuilt alongside the lexical index. We never synthesize a mapping the target
namespace does not publish ("only normalisations supported in the target namespace").

### 6.2 Resolution
Given `grounded_id`: curated override → asserted exact → deprecated replacement → else `none`
(`normalized_id` = `grounded_id`). Emits a `Normalization` and a term↔term SSSOM row.

## 7. SSSOM decision stores

### 7.1 Files (single editable file per stage/type; git-tracked under `mappings/`)
- `mappings/disease_grounding.sssom.tsv`, `mappings/drug_grounding.sssom.tsv` — **literal profile**.
- `mappings/disease_normalization.sssom.tsv`, `mappings/drug_normalization.sssom.tsv` — **term↔term**.

The store already holds **multiple rows per literal** (one per resolved combination component), and
manual rows are preserved over auto rows on regeneration (§7.3). As built, the three grounding files
plus disease normalization are written; drug normalization is a near-identity pass (drugs ground
natively to CHEBI) and is emitted when a drug normalization store is present.

Each file carries a full SSSOM metadata header (`mapping_set_id`, `license`, `mapping_tool`,
`curie_map`, …). Read/write/validate via `sssom-py`.

### 7.2 Row shapes
**Grounding (literal):** `subject_type=rdfs literal` · `subject_label=<raw string>` ·
`subject_id=∅` · `predicate_id=<skos:exactMatch|skos:broadMatch|skos:narrowMatch|sssom:NoTermFound>`
· `object_id/object_label=<match>` · `object_match_field=<rdfs:label|oio:hasExactSynonym|…>` ·
`mapping_justification=<semapv:LexicalMatching|semapv:ManualMappingCuration>` ·
`subject_preprocessing=<applied rule ids>` · `match_string` · `confidence` ·
`mapping_tool=medic-lexical-grounder`.

**Normalization (term↔term):** `subject_id=<grounded_id>` · `predicate_id=<skos:exactMatch|
IAO:0100001 replaced_by>` · `object_id=<normalized_id>` ·
`mapping_justification=<semapv:ManualMappingCuration | semapv:MappingChaining/UnspecifiedMatching>`
· `mapping_tool`.

Unresolved grounding decisions are written too (`predicate_id: sssom:NoTermFound`, no object) so the
store is a **complete** audit of every decision (goal (a); strengthens I-4).

### 7.3 Regeneration / merge semantics
Rows are keyed by `(subject_label, entity_type)` (grounding) or `subject_id` (normalization). On
rerun: rows with `mapping_justification = semapv:ManualMappingCuration` are **preserved untouched**
and take precedence; rows with `semapv:LexicalMatching` (or auto normalization justifications) are
**regenerated in place**. A manual row for a key suppresses auto-writing for that key. Output is
sorted deterministically so diffs are minimal and meaningful.

## 8. Integration and migration

- **Backend.** `src/medic/grounding/lexical_backend.py`::`LexicalCascadeGrounding(GroundingService)`
  wires index + preprocess + matcher + grounding store; registered in `factory.py` as `"lexical"`,
  made the default. `nameres` backend removed.
- **Ingest orchestration.** The orchestration helper `src/medic/grounding/pipeline.py::attach_grounding`
  runs Stage 1 then Stage 2, writes both stores, and attaches `grounding` + `normalization` objects to
  each record (tested). Wiring it into the individual ingesters (`ema.py`, `pmda.py`, `dailymed.py`, …)
  changes real pipeline behaviour and is a separate reviewed change.
- **justfile.** New `just build-grounding-index`; default `--grounding-backend` → `lexical`; new
  `just ground-report` (counts by predicate, quality tier, vocab, unresolved).
- **Backfill.** Seed the grounding/normalization stores via a fresh deterministic pass over all
  current source literals (see §10 decision D2). The old `cache/grounding/*.json` is kept only as a
  diff reference to spot regressions, not imported.
- **`oak`/`ols`/`gilda` backends** remain present but non-default; `oak_backend` is superseded by the
  lexical index and slated for retirement in a follow-up (see §10 decision D3).

## 9. Invariant and SPEC impacts (amend `SPEC.md`)

- **I-2 relaxed.** Grounding no longer forces canonical ChEBI/Mondo. It resolves to the
  highest-priority cascade vocab with a deterministic match; **canonicalization is Stage 2
  (normalization), documented and optional**. Rewrite I-2 accordingly.
- **I-4 strengthened.** Every decision — resolved, ambiguous, or unmatched — is persisted in the
  SSSOM stores.
- **I-6 honored.** All CURIE operations go through `curie_utils`.
- **I-7 (new): Source-string faithfulness.** `original_string` is copied verbatim from the source
  and never overwritten by any resolution stage.
- **§6 rewritten.** Replaces the OAK→Gilda→NameRes→OLS + 0.80 + LLM-rerank contract with the
  two-stage deterministic design; LLM/fuzzy demoted to deferred review-only proposers.

## 10. Decisions

Resolved at spec review:
- **D1 — cascade ordering: RESOLVED → rung-major, vocab-minor** (exact-label in any vocab beats a
  synonym/surgery match in a higher-priority vocab), configurable.
- **D2 — backfill: RESOLVED → fresh deterministic pass**; do not import the non-deterministic
  NameRes cache (kept only as a diff reference).
- **D3 — old backends: RESOLVED → remove `nameres` now**, keep `oak`/`ols`/`gilda` non-default,
  retire `oak_backend` in a follow-up.

Resolved during implementation:
- **D4 — local licensed-file locations/formats: RESOLVED.** UMLS wired to the local MRCONSO **zip**
  (`background/umls-2021AA-mrconso.zip`), streamed in place. ICD10CM was **sourced from BioPortal**
  (submission 27, UMLS2RDF Turtle) via a tolerant line parser, then **dropped from the default
  cascade** (redundant with UMLS; +64 only) but kept as reversible config. UniProt was never needed.
  Optional precision improvement still open: swap the UMLS SAB-allowlist proxy for true semantic-type
  filtering if a full UMLS RRF release with `MRSTY.RRF` becomes available.

## 10a. Results (as built)

Measured on real MeDIC data, deterministic (reruns byte-identical):

- **Diseases:** 75.0% → **80.6%** grounded (cascade MONDO+HP+UMLS; index ~3.69M rows). Stage-2
  normalization maps UMLS→MONDO via MONDO's own asserted `exactMatch`/`replaced_by`.
- **Drugs:** 59.3% → **82.3%** grounded (cascade CHEBI+DRON; index ~1.56M rows). The lift came from
  the drugs-only related-synonym tier (the #1 miss), salt/ester strip, combination split, INN
  spelling, translation dictionary, and fuzzy edit-1.

**Gap analysis.** The residual gap is *not* missing vocabulary coverage. **Brand names are not a real
gap for MeDIC** — its sources are regulatory (INN/generic names), not brands; DRON's ~15k brands would
recover ~1 of ~1,800 unresolved drugs. The remaining drug frontier is **foreign transliteration**
(the fuzzy tier's target — Amlodipin, Levocetirizin, …) plus **vaccine/biologic descriptions** (verbose,
not in CHEBI; would need VO/NCIt). The disease residue is contextual/compositional phrases,
combination lists, and abbreviations.

## 11. Deferred (future work, review-only)

The v1-deferred line has moved during implementation. **Now implemented** (deterministic, but written
as curator-reviewable `skos:closeMatch` proposals, never silently trusted): **fuzzy edit-distance-1
with a unique-hit guard** (`fuzzy_edit1_unique`) and a **curated translation dictionary**
(`translation_dictionary`, `conf/grounding_translation.yaml`). **Still deferred / disabled:**
`translation_llm` (LLM translation of genuinely foreign names — scaffolded in the enum, disabled,
non-deterministic, review-only), plus embeddings/NodeNorm and any at-resolve-time network call. When
enabled these may only **propose** rows for human review (`semapv:ManualMappingCuration` after
acceptance), never auto-accept.

**Fuzzy caveat (to watch):** edit-1 substitutes digits too, so isotope labels can flip
(`13C-urea` → `14C-urea`). All fuzzy rows carry `fuzzy_edit1_unique` for review; tightening option =
drop digit-substitution edits (loses a little recall, removes that error class).

## 12. Testing strategy

- **Golden fixtures**: exact-label, exact-synonym, normalized-exact, surgery-exact, curated-override
  -wins, ambiguous→unresolved, no-match→unresolved — offline and deterministic.
- **Rule-engine unit tests**: each synonymizer rule transforms exactly as documented.
- **Store round-trip**: write → read → `sssom-validate`; manual rows preserved across regeneration;
  manual precedence over auto.
- **Index builder**: tiny fixture ontology per source loader.
- **Determinism regression**: same input run twice → byte-identical stores.
- **Normalization**: asserted-exact and deprecated-replacement paths against fixture xref/obsolete
  data; `none` leaves id unchanged.
- **Rule-map parity**: a test enforces `RULE_CERTAINTY`/`RULE_PREDICATE` match the
  `PreprocessingRuleEnum` schema annotations.

As built: **~46 grounding-related tests pass** (`tests/grounding/` preprocess 13, matcher 15, store 4,
loaders 3, pipeline 1; `tests/normalization/` 3; plus `test_grounding_schema.py` 7).

## 13. Decision log (resolved during brainstorming)

1. Final grounding ID = native matched vocab (no forced MONDO/CHEBI). Canonicalization split into a
   separate normalization stage.
2. SSSOM stores are the read-back authority (hand-editable cache), single file per stage/type, rows
   tagged by `mapping_justification` (manual preserved, auto regenerated).
3. v1 is deterministic-only; fuzzy/LLM deferred to review-only.
4. Supported vocabs in v1 from OBO Graph JSON (PURLs) + local licensed UMLS (MRCONSO zip); ICD10CM and
   PR built then dropped as reversible config (redundant / not worth it), UniProt not needed.
5. Integration via Architecture C (new backend behind existing interface + dedicated store modules).
6. Two-stage split (grounding + normalization), each a schema class + quality enum + SSSOM file.
