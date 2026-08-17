# MeDIC v2 Refactoring Report

What changed from v1.0.0 (Kedro pipeline) to v2 (redesign branch), and why.

---

## Part 1: Output Changes

### Drug list (drug_list_flexible.csv)

| Aspect | v1.0.0 | v2 | Impact |
|--------|--------|----|----|
| **Row count** | 3,883 drugs | 5,089 drugs | +31% — new pipeline includes freshly downloaded Orange Book/EMA data with recent approvals, plus EveryCure (1,810 curated drugs) and China (CDE) as new sources |
| **CURIE overlap** | — | 82.9% of v1.0.0 CURIEs present | 663 drugs resolved to different canonical CURIEs due to fresh NameRes API responses |
| **Column count** | 31 | 31 | Identical structure |
| **Column order** | Matches | Matches | Identical |

#### Field-level changes (for 3,220 overlapping drugs)

| Field | Match % | Change description |
|-------|---------|-------------------|
| `curie_label` | 98.9% | Near-identical; minor label updates from NodeNorm |
| `approved_europe/japan/india/russia` | 99.8% | Near-identical |
| `approved_usa` | 81.6% | Lower because Orange Book was re-grounded from fresh FDA data |
| `is_allergen`, `is_vaccine_or_antigen`, `is_no_therapeutic_value`, `is_radioisotope_or_diagnostic_agent` | 99.7–100% | Identical or near-identical |
| `is_steroid`, `is_metallic_salt`, `is_glucose_regulator` | 97–98% | ATC-derived tags closely match old LLM-derived tags |
| `is_chemotherapy` | 96.1% | ATC-derived vs LLM-derived — slightly different edge cases |
| `is_combination_therapy` | 93.8% | Re-detected via Anthropic Claude (was GPT-4o) |
| `is_cancer_drug` | 93.0% | ATC L01/L02 prefix check vs old LLM classification |
| `is_antimicrobial` | 88.8% | ATC-derived misses drugs with multiple ATC codes where only one is antimicrobial |
| `combination_therapy_ingredients` | 80.1% | Re-extracted via LLM; some differences in ingredient splitting |
| `source_ingredients` | 75.2% | Format change: stored as proper list, exported pipe-separated |
| `atc_codes` | 66.8% | Different lookup cascade; some drugs got additional/different ATC codes |
| `atc_level5` | 69.8% | Same cause as atc_codes |
| `alternate_ids` | 49.8% | Format change (list vs Python string repr) plus new IDs from NodeNorm |
| `marketing_status_usa` | 40.8% | **Intentional**: v2 picks most permissive status (OTC > RX > DISCN); v1 stored all pipe-separated |
| `smiles` | 27.4% | Different coverage from ChEMBL+PubChem vs old pipeline sources |

### Stringent list (drug_list_stringent.csv)

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| Row count | 2,836 | 3,705 |
| CURIE overlap | — | 68.6% of v1.0.0 |

Lower overlap because the stringent filter (USA/Europe/Japan only) is sensitive to CURIE changes. For example, if the Orange Book ingredient "FENTANYL CITRATE" resolved to `CHEBI:119915` in v1.0.0 but resolves to a different CURIE in v2 (because NameRes returns a different canonical form), the v2 record has `approved_usa: True` under the new CURIE but the EMA/PMDA records (which use pre-grounded data from v1.0.0) still link to the old CURIE. The drug now appears as two separate entries — one with USA approval only, one with Europe/Japan only — and may fall out of the stringent filter entirely.

### Per-source files (orangebook.xlsx, ema.xlsx, etc.)

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| Format | XLSX/CSV from Kedro intermediates | XLSX/CSV regenerated from `kb/drugs/<source>/` YAML |
| Columns | Old pipeline column names (`corrected_curie_norm`, etc.) | Same column names for compatibility |
| Row counts | Identical per source | Identical (same underlying data for PMDA/Russia/India; fresh download for OB/EMA) |

### Indication list (on_label_list.yaml)

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| Row count | 11,071 | 11,071 (fallback mode reads same intermediates) |
| Source | Kedro pipeline output | Falls back to same Excel files; raw SPL XML pipeline ready for when DailyMed data is available |

### Contraindication list

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| Row count | 3,981 | 3,981 (same fallback) |

### Disease list

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| Source | `matrix-disease-list.tsv` from Kedro | HuggingFace `everycure/disease-list` (Parquet, 23,148 diseases) |
| Row count | 17,946 | 23,148 |
| Filter flags | 8 flags derived from subset strings | 70+ flags directly from HuggingFace dataset columns |

### Intentional output differences

These differences are by design, not regressions:

1. **`marketing_status_usa`** — v1 stored all statuses pipe-separated (`"RX| DISCONTINUED"`), v2 picks the most permissive (`"RX"`). Rationale: the most permissive status is what matters for drug availability assessment.

2. **`alternate_ids`** — v1 stored as Python list repr string (`"['CHEBI:123', ...]"`), v2 stores as proper list exported pipe-separated. The underlying IDs may also differ because v2 uses fresh NodeNorm canonicalization.

3. **Mondo downfilling removed** — v1 propagated parent-disease indications to all child diseases. v2 does not. Rationale: annotations travel up the ontology graph, not down. A drug approved for "breast cancer" is not necessarily indicated for every subtype. See invariant I-3 and SPEC §9.

4. **Disease list expanded** — v2 uses the latest EveryCure HuggingFace dataset (23,148 diseases vs 17,946) with richer metadata and more filter flags.

---

## Part 2: Pipeline Design Changes

### Architecture

| Aspect | v1.0.0 (Kedro) | v2 (redesign) |
|--------|----------------|---------------|
| **Framework** | Kedro data pipelines | Standalone Python modules with justfile orchestration |
| **Data flow** | Kedro catalog YAML → node functions → artifacts | `just ingest-<source>` → `just build-drug-list` → `just export-legacy` |
| **Configuration** | `conf/base/parameters.yml` (Kedro) | `conf/source_urls.yaml` + environment variables |
| **Entry points** | Kedro runner | `python -m medic.ingest.<source>` with typer CLI |

### Entity grounding

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Service** | NameRes only | Cascade: OAK → Gilda → NameRes → OLS |
| **Confidence** | None (rank-based pseudo-scores) | Jaro-Winkler similarity (real 0–1 scores) |
| **QC approach** | Post-hoc LLM binary check ("Are these the same drug?") then LLM picks best from top-30 | LLM preprocessor extracts active moiety before grounding; LLM reranker for ambiguous cases |
| **Failure handling** | Silently dropped from output | Persisted with `grounding_status: unresolved` and reported in `grounding_report.yaml` |
| **Caching** | In-memory dict, lost between runs | Persistent JSON files on disk, shareable via git |
| **Normalization** | NodeNorm | Same (NodeNorm via SRI) |

### Drug classification tags

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Method** | 10 separate GPT-4o API calls per drug (one per tag) | Deterministic ATC prefix matching for 9/11 tags; LLM fallback for 2 tags (`is_no_therapeutic_value`, `is_metallic_salt`) |
| **Cost** | ~$50+ per full run (10 × 4000 drugs × $0.005) | ~$2 per full run (LLM only for 2 tags on drugs without ATC codes) |
| **Reproducibility** | Non-deterministic (LLM temperature, model updates) | Deterministic for ATC-derived tags; LLM tags cached |

### ATC code lookup

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Sources** | ChEBI API, ChEMBL, PubChem PUG View, DrugCentral, RxNorm, WHO CC (scraped) | ChEMBL (primary), PubChem PUG View, DrugCentral, RxNorm, ChEBI API |
| **WHO CC** | HTML scraping (violates ToS) | Removed — see invariant I-5 and SPEC §9 |
| **UniChem** | Not used | Used to map CURIEs to ChEMBL IDs |
| **Caching** | None | Persistent at `cache/enrichment/atc_smiles.json` |

### LLM provider

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Drug QC/tagging** | OpenAI GPT-4o | Not needed (ATC-derived) |
| **Disease extraction** | Google Gemini 2.0 Flash (batch API) | Anthropic Claude Haiku 4.5 |
| **Combination therapy** | OpenAI GPT-4o-mini | Anthropic Claude Haiku 4.5 |
| **Preprocessing/reranking** | OpenAI GPT-4o + GPT-4o-mini | Anthropic Claude Sonnet 4 |

### Data sources

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Drug sources** | 6 (OB, PB, EMA, PMDA, Russia, India) | 8 (+ China CDE, EveryCure from HuggingFace) |
| **Source acquisition** | Pre-processed intermediates from Kedro | Direct download (OB/PB/EMA) or pre-grounded from main branch (PMDA/Russia/India) |
| **Disease source** | Local TSV from Kedro | HuggingFace `everycure/disease-list` |
| **Dependency on `/medi/`** | Required | Only for DailyMed fallback (reads old indication Excel files) |

### CURIE handling

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Method** | Manual `str.split(":")` throughout | `curies` package with bioregistry converter (same pattern as sssom-py) |
| **Module** | None (inline) | `src/medic/curie_utils.py` |

### Schemas

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Definition** | Implicit in DataFrame column conventions | Explicit LinkML YAML schemas at `src/medic/schema/` |
| **Validation** | None | Three-layer: schema (linkml-validate), terms (linkml-term-validator), references (linkml-reference-validator) |

### Caching and reproducibility

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Grounding cache** | In-memory per-function, lost between runs | Persistent JSON at `cache/grounding/*.json` |
| **Enrichment cache** | None | Persistent JSON at `cache/enrichment/*.json` |
| **Git-trackable** | No | Yes — sorted keys, no timestamps, deterministic output |
| **Re-run time (cached)** | Full re-run every time | Near-instant after first run |

### Cost control

| Aspect | v1.0.0 | v2 |
|--------|--------|------|
| **Skip mode** | None | `MEDIC_SKIP_EXPENSIVE_CALLS=1` bypasses all LLM and rate-limited API calls |
| **Estimated full run cost** | ~$75+ (GPT-4o heavy usage) | ~$27 (Anthropic Claude, mostly free APIs) |
| **Estimated full run time** | ~6 hours | ~4 hours (cached re-runs: seconds) |

### New capabilities (not in v1.0.0)

- **PHAROS cross-references** — harvests additional drug identifiers from PHAROS GraphQL API
- **RxNorm Extension** — stub for cross-jurisdiction drug identity resolution via OHDSI Athena (planned)
- **CURE ID** — stub for off-label drug-disease evidence from NCATS (planned)
- **China (CDE)** — new drug source with Selenium scraper
- **Grounding reports** — per-source QC reports with confidence tier breakdowns
- **KGX export** — Biolink-compliant knowledge graph nodes
- **SSSOM export** — drug identifier mappings in SSSOM format
