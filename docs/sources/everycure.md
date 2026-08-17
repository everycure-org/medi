# EveryCure Curated Drug List

## Overview

EveryCure maintains a curated list of ~1,810 drugs on HuggingFace (`everycure/drug-list`), enriched with chemical identifiers, drug classifications, ATC codes, and therapeutic-property tags. MeDIC uses this list to supplement the regulatory sources with expert-curated drug properties.

## Source data

- **Primary**: HuggingFace dataset `everycure/drug-list` (Parquet, CC-BY-4.0) — loaded via `datasets.load_dataset("everycure/drug-list", split="train")`
- **Local fallbacks** (tried in order if HuggingFace is unreachable):
  - `data/raw/drugs/everycure-drug-list.tsv`
  - `data/raw/drugs/everycure-drug-list.csv`
  - `data/raw/drugs/drug-list.tsv`
  - `data/raw/drugs/drug-list.csv`
- **Status**: Fully raw — pre-curated by EveryCure; CURIEs accepted at face value (`grounding_confidence=1.0`, `grounding_service=everycure`).

## ETL module

`src/medic/ingest/everycure_drugs/__main__.py`

1. Load the drug list from HuggingFace (or local fallback).
2. Filter out rows marked `deleted = True`.
3. For each row, build a DrugSource record:
   - `normalized_id` from `translator_id` (e.g. `CHEBI:421707`)
   - `alternate_ids` includes `translator_id` and a prefixed `DRUGBANK:` ID
   - Optional metadata: `drug_class`, `therapeutic_area`, `drug_function`, `drug_target`, `atc_main`, `atc_level_1..5`, `l1_label..l5_label`, `synonyms`, `approved_usa`
   - Boolean tag columns from the HF dataset: `is_antipsychotic`, `is_sedative`, `is_antimicrobial`, `is_antifungal`, `is_antiviral`, `is_antiparasitic`, `is_immunosuppressant`, `is_chemotherapy`, `is_hormone`, `is_biologic`, `is_small_molecule`, `is_repurposed`
4. Write `kb/drugs/everycure/everycure.yaml`.

## Source isolation

External curated source — no jurisdictional flags. EveryCure-only drugs do not get a regulatory artifact in `on_label_merge`; their contribution is the metadata layer (ATC codes, drug-class tags, etc.).

## Justfile target

```bash
just ingest-everycure-drugs
```

## Licence

CC BY 4.0, per the HuggingFace dataset card for `everycure/drug-list`. **Attribution required** —
credit EveryCure in any redistribution of a product built from this list.
See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
