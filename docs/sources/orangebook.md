# FDA Orange Book

## Overview

The FDA Orange Book ("Approved Drug Products with Therapeutic Equivalence Evaluations") lists small-molecule drugs approved for sale in the United States under New Drug Applications (NDAs) and Abbreviated New Drug Applications (ANDAs).

## Source data

- **URL**: <https://www.fda.gov/media/76860/download> (downloaded fresh each run; URL is configurable in `conf/source_urls.yaml`)
- **Format**: `~`-delimited text (`products.txt`) inside a ZIP
- **Update frequency**: Monthly
- **Raw file**: `data/raw/orangebook/products.txt` (cached after first download)
- **Status**: Fully raw — end-to-end from the FDA download with no v1.0 intermediate dependency

## ETL module

`src/medic/ingest/orangebook/__main__.py`

1. Download and extract the Orange Book ZIP to `data/raw/orangebook/products.txt` (skipped if already present unless `--force-download`)
2. Parse `products.txt`, group rows by `Ingredient`, and compute earliest approval date and most permissive marketing status (OTC > RX > DISCONTINUED → mapped to `DISCN`)
3. Pipe-join all `Appl_No` (NDA / ANDA) values as `application_number` — used downstream to build per-product Drugs@FDA URLs in `on_label_merge`
4. Ground each ingredient name through the configured grounding cascade (default `nameres`)
5. Write `kb/drugs/orangebook/orangebook.yaml` (DrugSource records) and `kb/drugs/orangebook/grounding_report.yaml`

## Output schema

`kb/drugs/orangebook/orangebook.yaml` is a flat list of `DrugSource` records (per `src/medic/schema/drug.yaml`). Per-record fields:

| Field | Description |
|---|---|
| `source` | Always `ORANGEBOOK` |
| `source_name` | Active ingredient as it appears in `products.txt` |
| `normalized_id` | Canonical drug CURIE (CHEBI preferred) from the grounding cascade |
| `normalized_label` | Canonical drug label |
| `alternate_ids` | All equivalent identifiers collected by NodeNorm |
| `grounding_confidence` / `grounding_service` / `grounding_status` | Provenance for the grounding call |
| `approval_date` | Earliest `Approval_Date` for the ingredient (`YYYYMMDD`) |
| `marketing_status_usa` | Most permissive Orange Book Type (`RX`, `OTC`, `DISCN`) |
| `application_number` | Pipe-joined NDA/ANDA application numbers |

## Source isolation

USA only. Orange Book records contribute `marketing_status_usa`, approval date, and `application_number` to the merged drug list. They do not emit indication or contraindication rows.

## Justfile target

```bash
just ingest-orangebook
```

## Licence

A work of the U.S. Food and Drug Administration, in the public domain under 17 U.S.C. §105.
Attribution is a courtesy, not an obligation. MeDIC redistributes derived records freely.
See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
