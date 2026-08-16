# India Drug Registry (CDSCO)

## Overview

The Central Drugs Standard Control Organisation (CDSCO, <https://cdsco.gov.in>) publishes year-batch tabulations of newly approved drugs in India. The ingest parses the CDSCO "List of New Drugs Approved in YYYY" PDFs directly, extracts indications, then grounds.

## Source data

- **Format**: PDF (year-batch CDSCO tabulations)
- **Raw PDFs**: `data/raw/india/primary/` — the CDSCO "List of New Drugs Approved in YYYY" PDFs, fetched by `medic.ingest.india.fetch_primary`.
- **Status**: Raw. The CDSCO PDFs are the single acquisition path, parsed with `pdfplumber`. If the PDFs cannot be fetched or parsed, ingest fails loudly — there is no legacy CSV fallback.

## ETL module

`src/medic/ingest/india/__main__.py`

1. Fetch and parse the CDSCO annual approval PDFs (`fetch_all_pdfs` / `parse_all_pdfs`); de-duplicate by drug name. A missing or unparseable PDF set is a hard error.
2. Ground each drug name through the cascade.
3. Write `kb/drugs/india/india.yaml` + `grounding_report.yaml`.
4. **Indication extraction** (unless `--skip-indications`): for each grounded drug with indication text, run `extract_diseases_from_text` (LLM disease extraction) and re-ground each disease. Each evidence row carries `jurisdiction: INDIA`, `source_type: REGULATORY`, and the generic CDSCO landing page as `reference` (no per-product URLs exist). Output: `kb/indications/india/indications.yaml`.

## Why no contraindications

CDSCO publishes only year-batch tabulation PDFs with an "Indication" column and no contraindications field. There is no per-drug landing page, no SPL-equivalent, and no machine-readable Indian package-insert feed. Until a per-product authoritative document source appears, CDSCO contributes INDIA-jurisdiction indications only — never contraindications. (See the module's top docstring.)

## Source isolation

India only. Indications carry `jurisdiction: INDIA`. India ingest does not emit contraindications or adverse events.

## Justfile target

```bash
just ingest-india
```

## Licence

**Unverified.** cdsco.gov.in declares no licence for the "List of New Drugs Approved" PDFs. Indian
government open data is normally released under the Government Open Data License – India
(GODL-India), which requires attribution, but that has not been confirmed for these documents.

MeDIC treats attribution as required and redistributes derived records only. Confirm the terms
before any commercial redistribution. See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
