# EMA (European Medicines Agency)

## Overview

The European Medicines Agency centrally authorizes medicines for use in the EU. MeDIC ingests the EMA "medicines output" spreadsheet (the human medicines table behind <https://www.ema.europa.eu/en/medicines>) and links each product back to its European Public Assessment Report (EPAR) landing page and Product Information PDF.

## Source data

- **URL**: <https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx> (configurable via `conf/source_urls.yaml`)
- **Format**: Excel (`.xlsx`); EMA ships several metadata rows before the real header — the parser auto-skips them by scanning for `Category`
- **Raw file**: `cache/downloads/ema/ema_medicines.xlsx` (downloaded fresh each run; re-used unless `--force-download`)
- **Status**: Fully raw for both drugs and indications

## ETL module

`src/medic/ingest/ema/__main__.py`

1. Download the EMA medicines XLSX
2. Filter to `Category == "Human"` and `Authorisation status == "Authorised"`
3. Group by INN, take the earliest `Marketing authorisation date`, and keep per-product entries (`indication_entries`) so each indication remains linked to its EPAR URL
4. Ground each drug name through the cascade and write `kb/drugs/ema/ema.yaml` + `grounding_report.yaml`
5. **Indication extraction** (unless `--skip-indications`): for each grounded drug with `Therapeutic indication` text, call `extract_diseases_from_text` (LLM disease extraction shared with DailyMed), re-ground each disease, and write `kb/indications/ema/indications.yaml`. The EPAR landing URL is emitted as `reference`; the deterministic Product Information PDF URL (`…/<slug>-epar-product-information_en.pdf`) is emitted as `source_document_url`.
6. **Contraindication extraction** (opt-in, `--extract-contras`): download each product's EPAR Product Information PDF, extract the "4.3 Contraindications" section, run LLM extraction, and write `kb/indications/ema/contraindications.yaml`. Multi-hour run across ~2,500 PDFs — off by default.

## Output schemas

- `kb/drugs/ema/ema.yaml` — DrugSource records. Drug-level fields include `atc_code`, `epar_url`, `product_name`, `ema_product_number`, and `indication_entries` (per-row indication text + URL).
- `kb/indications/ema/indications.yaml` — IndicationAssociation records with `jurisdiction: EU`, `source_type: REGULATORY`, `confidence: HIGH`, EPAR URL as the reference, PDF URL as `source_document_url`.
- `kb/indications/ema/contraindications.yaml` — same shape with `relationship_type: CONTRAINDICATION` (only present when `--extract-contras` ran).

## Source isolation

EU only. The EMA XLSX does not contain contraindication text — those come from the per-product EPAR Product Information PDFs. No DailyMed-style cross-jurisdiction flags are read or emitted.

## Justfile targets

```bash
just ingest-ema             # drugs + indications
just ingest-ema-contras     # drugs + indications + EPAR PDF contraindications
```

## Licence

**Attribution is mandatory.** EMA's [legal notice](https://www.ema.europa.eu/en/about-us/about-website/legal-notice)
permits reproduction and distribution for both commercial and non-commercial purposes on one
condition: *"EMA is always acknowledged as the source of the material"*, in each copy.

Two consequences:

- MeDIC cannot place EMA-derived rows under CC0 — CC0 waives exactly the attribution requirement
  EMA imposes. Any redistribution of a merged product must carry the notice in
  [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
- The EU sui generis database right (Directive 96/9/EC) protects substantial investment in
  obtaining and verifying database contents independently of copyright, so the "these are only
  facts" argument that works in the US does not dispose of the question here.
