# MeDIC Source Analysis: Raw vs Processed Data

## The core question

For every MeDIC source, how much of the pipeline runs end-to-end from raw regulator data, and how much still depends on pre-processed Kedro v1.0 intermediates? This document traces each source from its upstream origin through to the YAML emitted under `kb/`, and flags exactly which legacy steps remain.

Per-source docs in `docs/sources/<source>.md` go deeper on individual ingest modules. This page is the cross-source view.

---

## Drug sources (8 sources)

### FDA Orange Book — FULLY RAW

| | |
|---|---|
| **Raw file** | `data/raw/orangebook/products.txt` — downloaded fresh from `https://www.fda.gov/media/76860/download` (URL in `conf/source_urls.yaml`) |
| **What v2 reads** | The raw `products.txt` directly |
| **Processing** | v2 parses the `~`-delimited file, groups by `Ingredient`, re-grounds via the cascade, pipe-joins all NDA application numbers |
| **Output** | `kb/drugs/orangebook/orangebook.yaml` (~2,700 records) |
| **Status** | Clean. End-to-end from raw FDA data. No v1.0 dependency. |

### FDA Purple Book — PARTLY RAW (legacy fallback gated)

| | |
|---|---|
| **Raw file (preferred)** | Raw CSV downloaded to `cache/downloads/purplebook/purplebook.csv` when a working URL is configured in `conf/source_urls.yaml` |
| **Legacy fallback** | `data/raw/purplebook/pb_norm.xlsx` — pre-grounded v1.0 intermediate. Disabled by default; requires `--allow-legacy-fallback` |
| **Processing (raw path)** | v2 auto-detects the header row, groups by `Proper Name`, captures pipe-joined `BLA Number`s, re-grounds via the cascade |
| **Processing (legacy path)** | Accepts v1.0 CURIEs at face value (`confidence=0.9, service=nameres_legacy`); recovers BLA numbers from any "BLA"/"License" column when present |
| **Output** | `kb/drugs/purplebook/purplebook.yaml` (~640 records) |
| **Status** | Clean when the URL works; gated fallback otherwise. The ingest fails loudly if the raw CSV is missing and the legacy flag is not passed — so missing BLAs surface immediately. |

### EMA — FULLY RAW

| | |
|---|---|
| **Raw file** | Downloaded fresh from `https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx`, cached at `cache/downloads/ema/ema_medicines.xlsx` |
| **What v2 reads** | The fresh EMA XLSX |
| **Processing** | Filters to Human + Authorised, groups by INN, takes the earliest authorisation date, re-grounds via the cascade. Indications are re-extracted from the `Therapeutic indication` column via LLM disease extraction; EPAR landing URL → `reference`, deterministic Product Information PDF URL → `source_document_url`. Contraindications are an opt-in `--extract-contras` run against EPAR PDFs. |
| **Output** | `kb/drugs/ema/ema.yaml` (~990 records); `kb/indications/ema/indications.yaml` (~4,100 records); optionally `contraindications.yaml` |
| **Status** | Clean. End-to-end from raw EMA data for both drugs and indications. |

### PMDA — DRUG NAMES PRE-TRANSLATED; INDICATIONS RE-EXTRACTED

| | |
|---|---|
| **Raw files (tried in order)** | `data/raw/pmda/pmda_norm.xlsx` (v1.0 normalized, Japanese already → English) → `data/raw/pmda/pmda_approvals.csv` |
| **What v2 reads** | `pmda_norm.xlsx` by default |x§
| **Drug processing** | v2 re-grounds via the cascade (old CURIEs are discarded). Drug names rely on v1.0's Japanese→English translation. |
| **Indication processing** | The `indication` column is **re-extracted via LLM** and re-grounded through the v2 cascade. Per-product PMDA review-report URLs are looked up via `medic.ingest.pmda.review_lookup`; when a per-product PDF is found, it lands on `source_document_url` with `confidence: HIGH`. |
| **Contraindications** | Opt-in `--extract-contras` step: downloads per-product review-report PDFs and extracts the contraindications section. |
| **Output** | `kb/drugs/pmda/pmda.yaml` (~1,170 records); `kb/indications/pmda/indications.yaml` (~2,800 records); optionally `contraindications.yaml` |
| **Status** | Mixed. Drug translation is v1.0; everything else (grounding, disease extraction, evidence rows) is v2. To go fully raw on drug names we'd need a fresh Japanese→English MT pass over `pmda_approvals.csv`. |
| **YJ code gap** | PMDA's 12-digit YJ code is not in either upstream file. `original_drug_id` is left blank; the plumbing is ready for when a YJ source is wired in. See the module's top docstring. |

### Russia (GRLS) — PRE-TRANSLATED

| | |
|---|---|
| **Raw files (tried in order)** | `data/raw/russia/russia_norm.csv` → `data/raw/russia/russia_translated.xlsx` — both v1.0 intermediates with names already transliterated/translated from Russian |
| **What v2 reads** | The v1.0 CSV |
| **Processing** | v2 re-grounds via the cascade. Drug names rely on the v1.0 translation; there is no indication text in the upstream file. |
| **Why not raw?** | GRLS (`https://grls.rosminzdrav.ru`) returns empty search results from non-Russian IPs and gates per-product detail pages behind authentication. See the long investigation in `src/medic/ingest/russia/__main__.py` and `issues/russia-grls-source-migration.md`. |
| **Output** | `kb/drugs/russia/russia.yaml` (~1,830 records) |
| **Status** | Not raw. No path to per-product deep links until an upstream dump with `routing_guid` / Cyrillic MNN / registration number arrives. |

### India (CDSCO) — PRE-NORMALIZED; INDICATIONS RE-EXTRACTED

| | |
|---|---|
| **Raw files (tried in order)** | `data/raw/india/india_norm.csv` (v1.0) → `data/raw/india/indian_drugs.csv` |
| **Raw PDFs (not wired)** | `data/raw/india/primary/` — CDSCO year-batch "List of New Drugs Approved" PDFs, not yet parsed |
| **What v2 reads** | `india_norm.csv` |
| **Drug processing** | v2 re-grounds via the cascade. Drug names rely on v1.0 extraction from some prior CDSCO source. |
| **Indication processing** | The `indication` column is **re-extracted via LLM** and re-grounded. Each evidence row uses the generic CDSCO landing page (CDSCO has no per-product URLs). |
| **Contraindications** | None — CDSCO publishes no contraindication field and no SPL-equivalent label feed. |
| **Output** | `kb/drugs/india/india.yaml` (~110 records); `kb/indications/india/indications.yaml` (~165 records) |
| **Status** | Drug names not raw; indications are LLM-re-extracted from the existing column. The next step is parsing the CDSCO PDFs in `data/raw/india/primary/` — see `issues/india-pdf-source-migration.md`. |

### China (CDE) — INGEST READY, NO DATA

| | |
|---|---|
| **Raw file expected** | `data/raw/china/cde_drugs.csv` — not present in the repo |
| **What v2 reads** | Nothing. The ingest logs "China CDE source file not found — skipping ingest" and exits. |
| **Why not raw?** | Acquiring the CDE registry requires a Selenium scrape across ~1,360 paginated pages plus an LLM Chinese→English INN translation step. The scraper is not in this repo. |
| **Status** | Ingest implemented; data acquisition pending. |

### EveryCure — CURATED EXTERNAL

| | |
|---|---|
| **Source** | HuggingFace `everycure/drug-list` (~1,810 drugs) loaded via the `datasets` library |
| **Local fallback** | `data/raw/drugs/everycure-drug-list.{tsv,csv}` and `drug-list.{tsv,csv}` |
| **Processing** | CURIEs accepted at face value (`grounding_confidence=1.0`, `grounding_service=everycure`). Carries ATC codes, drug classes, drug function/target, and boolean property tags. |
| **Output** | `kb/drugs/everycure/everycure.yaml` (~1,810 records) |
| **Status** | Clean. External curated source. |

---

## Indication sources

Three regulator-derived indication streams now feed `products/indication_list.yaml`: DailyMed (USA), EMA (EU), and PMDA (Japan), plus CDSCO (India) for indication-only coverage. The state of "raw vs processed" is **no longer uniform across them**:

### EMA — FULLY RAW (drugs + indications)

EMA indications are now extracted *fresh* from the freshly-downloaded EMA medicines XLSX via the same `extract_diseases_from_text` LLM extraction used by DailyMed, then re-grounded via the cascade. There is no v1.0 dependency on the indication path.

### PMDA and India — DRUG NAMES PRE-PROCESSED, INDICATIONS RE-EXTRACTED

Both read their `indication` column from a v1.0 normalized file (PMDA: `pmda_norm.xlsx`, India: `india_norm.csv`) but re-run LLM disease extraction and v2 cascade grounding on every record. Only the drug-name translation step is inherited from v1.0.

### DailyMed — RAW SPL PIPELINE IMPLEMENTED, FALLBACK TO V1.0 BY DEFAULT

`src/medic/ingest/dailymed/__main__.py` is end-to-end ready: it mines SPL ZIPs in `data/raw/dailymed/`, parses the indications and contraindications sections by LOINC code (`34067-9` / `34070-3`), runs LLM disease extraction with caching, grounds diseases via the cascade and drugs to ChEBI, and writes per-source YAML.

Today the `data/raw/dailymed/` directory is empty, so the ingest falls back to:

- `medi/indications/data/03_primary/matrix_indication_list.xlsx` (~11,000 rows)
- `medi/indications/data/03_primary/matrix_contraindication_list.xlsx` (~3,981 rows)

These are v1.0 outputs that already had LLM extraction + NameRes grounding applied. The fallback path produces 21% non-Mondo disease CURIEs because v1.0 used an older NameRes API.

### Raw data that IS available

| Source | Raw file exists? | Path |
|---|---|---|
| FDA DailyMed labels (~50,628) | ✅ | `medi/indications/data/01_raw/dailymed_labels.xlsx` |
| FDA DailyMed contraindications | ✅ | `medi/indications/data/01_raw/dailymed_contraindications_sections.xlsx` |
| EMA medicines + indication text | ✅ | downloaded fresh each run |
| PMDA approvals (Japanese-translated) | ✅ | `medi/indications/data/01_raw/pmda_approvals.csv`, `data/raw/pmda/pmda_norm.xlsx`, `pmda_approvals.csv` |
| Mondo hierarchy | ✅ | `medi/indications/data/01_raw/mondo_edges.tsv` / `mondo_nodes.tsv` |

### What it would take to make DailyMed fully raw

| Step | Effort | Cost | Blocker |
|---|---|---|---|
| Download DailyMed SPL ZIPs into `data/raw/dailymed/` | 1 day | Free | ~60 GB download |
| Re-extract diseases (LLM) over ~50K labels | 2 days | ~$300 (cache-friendly) | Need API budget |
| Re-ground all diseases via the v2 cascade | 1 day | Free (local OAK + cascade) | None |
| **Total** | **~4 days** | **~$300** | None (all raw data exists, parser is ready) |

The parser is in `src/medic/ingest/dailymed/__main__.py`; it is wired up but inactive until SPL ZIPs land. Running it would also drop the non-Mondo disease percentage substantially (v2 grounding favours Mondo).

---

## Research sources

### Deep Research (Perplexity / Falcon / etc.) — AI-GENERATED

| | |
|---|---|
| **Raw files** | `research/*-deep-research-{provider}.md` — AI-generated literature reviews |
| **What v2 reads** | These markdown files, parsed by `parse_deep_research.py` |
| **Processing** | LLM provider generates the report → parser extracts drug sections → resolves PMC→PMID → fetches titles |
| **Nature** | Secondary. The primary sources are the PMIDs cited inside the reports. |

### CURE-ID — FULLY RAW

| | |
|---|---|
| **Raw file** | `data/raw/cureid/cureid_data.tsv` — downloaded fresh from `https://opendata.ncats.nih.gov/public/cureid/cureid_data.tsv` |
| **What v2 reads** | The TSV directly |
| **Processing** | NCATS pre-maps subject/object CURIEs. v2 filters to drug treatment edges, aggregates by (drug, disease), folds related phenotype reports into the disease association's notes, and builds per-`report_id` and per-PMID evidence items. |
| **Output** | `kb/research/cureid_associations.yaml` |
| **Status** | Clean. Authoritative source with NCATS-mapped CURIEs. |

---

## Adverse event sources

### PVLens — RAW INPUT, INGEST GATED ON EXTERNAL RUN

| | |
|---|---|
| **Raw file** | `data/pvlens/product_ae.csv` — output of running PVLens (`https://github.com/GSK-Global-Safety/pvlens`) externally |
| **Status** | Implemented. The ingest exits cleanly with a warning if the CSV is absent. PVLens itself is not run inside this repo. |

### FAERS — RAW INPUT, INGEST IMPLEMENTED

| | |
|---|---|
| **Raw files** | `data/faers/faers_ascii_<YEAR>Q<Q>.zip` — downloaded via `--download YEAR QUARTER` |
| **Processing** | Parses DRUG and REAC tables, joins on `primaryid`, aggregates per `(drug, MedDRA term)`, computes PRR, filters `report_count >= 3` and `prr >= 2.0`, re-grounds drug names |
| **Status** | Implemented. Skips cleanly when no quarterly ZIPs are present. |

---

## Summary

| Source | Product | Raw available? | What v2 reads | Processing applied before v2 | Fully raw? |
|---|---|---|---|---|---|
| FDA Orange Book | Drug list | ✅ | Raw `products.txt` | None | ✅ Yes |
| FDA Purple Book | Drug list | ⚠️ | Raw CSV (preferred); `pb_norm.xlsx` (gated fallback) | None (raw) / NameRes grounding (fallback) | ✅ Raw path / ❌ fallback |
| EMA | Drug list + indications | ✅ | Fresh XLSX download | None | ✅ Yes (drugs + indications) |
| PMDA | Drug list + indications | ⚠️ | `pmda_norm.xlsx` | Japanese→English translation | ❌ Drug names; ✅ indication re-extraction |
| Russia (GRLS) | Drug list | ❌ | `russia_norm.csv` | Russian→English translation + v1.0 grounding | ❌ No |
| India (CDSCO) | Drug list + indications | ⚠️ | `india_norm.csv` | Unknown v1.0 extraction | ❌ Drug names; ✅ indication re-extraction |
| China (CDE) | Drug list | ❌ | Nothing (no data) | Not yet ingested | ❌ N/A |
| EveryCure | Drug list | ✅ | HuggingFace dataset | External curation | ✅ Yes |
| FDA DailyMed | Indications + contraindications | ✅ (text exists) | v1.0 merged Excel (fallback) | LLM extraction + NameRes grounding | ❌ Default; ✅ if SPL ZIPs are placed in `data/raw/dailymed/` |
| FDA contraindications (DailyMed) | Contraindications | ✅ (text exists) | v1.0 merged Excel | LLM extraction + NameRes grounding | ❌ Default |
| EMA contraindications | Contraindications | ✅ (PDFs) | EPAR Product Information PDFs (opt-in) | None | ✅ When `--extract-contras` is on |
| PMDA contraindications | Contraindications | ✅ (PDFs) | Per-product review-report PDFs (opt-in) | None | ✅ When `--extract-contras` is on |
| PVLens | Adverse events | ⚠️ | External `product_ae.csv` | PVLens external pipeline | n/a (external) |
| FAERS | Adverse events | ✅ | FAERS quarterly ZIPs | None | ✅ Yes |
| Deep research | Research | n/a (AI-generated) | Markdown reports | LLM generation + parsing | ⚠️ Secondary |
| CURE-ID | Research | ✅ | Raw TSV | None (pre-mapped by NCATS) | ✅ Yes |

### Bottom line

**Drug sources**: 4 of 8 are fully raw (Orange Book, EMA, EveryCure, Purple Book on the raw path). PMDA, India, Russia all carry v1.0 translation/extraction baggage on drug names. China has no data.

**Indication sources**: EMA is fully raw; PMDA and India re-extract indications from v1.0-translated text via LLM and re-ground via the v2 cascade. DailyMed has a fully raw SPL pipeline ready but falls back to the v1.0 merged Excel today because the SPL ZIPs are not in the repo.

**Research / adverse events**: CURE-ID and FAERS are fully raw. PVLens depends on an externally-run pipeline. Deep research is secondary by nature.

**Priority recommendation**: Drop the ~$300 / ~4-day cost to download DailyMed SPL ZIPs and re-run the existing v2 ingest. This is the single biggest remaining win — it would make the FDA indication path fully raw, cut the non-Mondo disease-CURIE share, and unlock the same machinery for EMA/PMDA contraindications.
