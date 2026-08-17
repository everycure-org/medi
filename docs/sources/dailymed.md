# FDA DailyMed

## Overview

DailyMed is the NIH/NLM repository of FDA-approved Structured Product Labels (SPLs). MeDIC mines the "Indications and Usage" (LOINC `34067-9`) and "Contraindications" (LOINC `34070-3`) sections from SPL XML, extracts diseases via LLM, and grounds drugs/diseases to ChEBI/Mondo CURIEs.

## Source data

- **Portal**: <https://dailymed.nlm.nih.gov/dailymed/>
- **Format**: SPL XML inside per-product ZIP files
- **Raw SPL ZIPs (preferred)**: `data/raw/dailymed/*.zip` — one ZIP per drug, each containing the SPL XML. Not checked into the repo; downloading the full set is ~60 GB.
- **Legacy fallback** (used when raw SPL ZIPs are missing):
  - `medi/indications/data/03_primary/matrix_indication_list.xlsx` — v1.0 merged indications (11,067 rows)
  - `medi/indications/data/03_primary/matrix_contraindication_list.xlsx` — v1.0 merged contraindications (3,981 rows)
  - `medi/indications/data/01_raw/dailymed_labels.xlsx` — raw label text (50,628 labels), available for re-extraction
- **Status**: The raw SPL pipeline is implemented end-to-end but is not the default until ZIPs are downloaded. Today the ingest reads the v1.0 merged XLSXs; future runs with raw SPL ZIPs will re-extract diseases from the original section text.

## ETL module

`src/medic/ingest/dailymed/__main__.py`

Main flow:

1. `mine_spl_labels(data_dir)` — for each ZIP in `data/raw/dailymed/`, parse the XML, pull active ingredients (`activeMoiety`), the indications section, the contraindications section, and the SPL `setId`. Build a DataFrame of `(drug_names, indications_text, contraindications_text, set_id)`.
2. If the SPL DataFrame is non-empty, run `_process_spl_data` (LLM disease extraction → cascade grounding → drug grounding to ChEBI) and produce both indication and contraindication records.
3. Otherwise, fall back to the v1.0 merged Excel files via `_fallback_indications` / `_fallback_contraindications`.
4. Write `kb/indications/dailymed/indications.yaml` and `kb/indications/dailymed/contraindications.yaml` (plus a top-level `products/contraindication_list.yaml`).
5. Resolve SPL `setId` lookups (used to build `https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=…` deep links) and write a summary report to `kb/indications/dailymed/setid_lookup_report.yaml`.

Caches: per-disease LLM extraction is cached at `cache/enrichment/dailymed_diseases.json`, contraindication extraction at `cache/enrichment/dailymed_contra_diseases.json`, allergen classification at `cache/enrichment/dailymed_allergen.json`.

## Output schema

Each YAML entry is an `IndicationAssociation` (per `src/medic/schema/indication.yaml`). Key fields per record:

| Field | Description |
|---|---|
| `drug_disease` | Composite key `<drug_curie>\|<disease_curie>` |
| `final_normalized_drug_id` / `final_normalized_drug_label` | Drug CURIE (ChEBI preferred) and label |
| `final_normalized_disease_id` / `final_normalized_disease_label` | Mondo (or fallback ontology) CURIE and label |
| `fda` / `ema` / `pmda` | Jurisdiction flags (DailyMed sets `fda = True`) |
| `relationship_type` | `INDICATION` or `CONTRAINDICATION` |
| `indications_text` | Raw section text |
| `evidence` | One or more evidence items with `source_type: REGULATORY`, `jurisdiction: USA`, DailyMed setid-based reference URL |

## Source isolation

USA only. DailyMed ingest emits only USA-jurisdiction evidence even when an upstream file carries cross-jurisdiction flag columns — those columns are stripped, not synthesised.

## Justfile target

```bash
just ingest-dailymed
```

## Licence

FDA Structured Product Labels are published by NIH/NLM as public-domain regulatory disclosure and
are treated as freely redistributable, which is settled practice. Note the nuance: the label text
itself is authored by manufacturers, so this is not a US-government work in the 17 U.S.C. §105
sense — the freedom comes from its status as required public disclosure, not from §105.

Attribution is a courtesy, not an obligation. MeDIC redistributes derived indication and
contraindication records. See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
