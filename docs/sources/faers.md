# FAERS (FDA Adverse Event Reporting System)

## Overview

FAERS is the FDA's post-market spontaneous-reporting system. MeDIC parses the quarterly ASCII archives, joins drugs to MedDRA preferred terms, computes proportional reporting ratios (PRR), and emits drug-event signals to `kb/adverse_events/faers/`.

## Source data

- **Portal**: <https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html>
- **Download base**: <https://fis.fda.gov/content/Exports/>
- **Format**: `faers_ascii_<YEAR>Q<Q>.zip` — `$`-delimited TXT files (DRUG, REAC, DEMO, OUTC, INDI, THER)
- **Cache**: `data/faers/faers_ascii_<YEAR>Q<Q>.zip`
- **Status**: Implemented; not run by default. The pipeline is gated on raw ZIPs being present in `data/faers/`; if none are found the ingest logs a warning and exits cleanly.

## ETL module

`src/medic/ingest/faers/__main__.py`

Two CLI modes:

- `--download YEAR QUARTER` — fetch the quarterly ZIP into `data/faers/` (skip if already cached).
- Default — for every ZIP in `data/faers/`:
  1. Parse `DRUG*.TXT` and `REAC*.TXT` with `$` separators.
  2. Filter `DRUG` rows to `role_cod == "PS"` (primary suspect) and pick `prod_ai` if available, else `drugname`.
  3. Inner-join DRUG and REAC on `primaryid`, group by `(drug_name, pt)` → `report_count`, then compute PRR per pair (`(a/(a+b)) / (c/(c+d))`).
  4. Concatenate across quarters, filter to `report_count >= min_reports` and `prr >= min_prr` (defaults 3 and 2.0).
  5. Ground each unique drug name through the configured backend (default `nameres`).
  6. Write `kb/adverse_events/faers/faers_signals.yaml`.

## Output schema

Each record carries `source: FAERS`, the source/normalized drug fields, the MedDRA preferred term as `adverse_event_term`, `report_count`, `prr`, and an evidence item with `source_type: POST_MARKET`, `jurisdiction: USA`, `reference: "FAERS"`, `support: SUPPORT`, `confidence: MEDIUM`. Adverse events are not yet mapped to MedDRA CURIEs.

## Source isolation

USA only.

## Justfile target

```bash
just ingest-faers
# To pull a specific quarter first:
uv run python -m medic.ingest.faers --download 2024 4
```

## Licence

The FAERS quarterly files are a work of the U.S. Food and Drug Administration and are in the public
domain — **but the reaction terms in them are MedDRA preferred terms**, and MedDRA is licensed by
the ICH/MSSO under a subscription that restricts redistribution of dictionary content.

That makes the FAERS-derived product **blocked for release** until the MSSO terms are confirmed.
Nothing MedDRA-derived is currently committed (`kb/adverse_events/faers/` holds only a `.gitkeep`)
and no AE asset is published by `.github/workflows/release.yml`. Keep it that way.
See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
