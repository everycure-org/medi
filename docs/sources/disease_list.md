# Disease List

## Overview

The MeDIC disease list is sourced from the EveryCure curated `everycure/disease-list` dataset on HuggingFace. It supplies the canonical disease label, definition, synonyms, crossreferences, and filter flags (rare, hereditary, chromosomal, cancer, etc.) used by every other product to validate disease CURIEs.

## Source data

- **Primary**: HuggingFace dataset `everycure/disease-list` — loaded via `datasets.load_dataset("everycure/disease-list", split="train")`
- **Local fallbacks** (tried in order if HuggingFace is unreachable):
  - `medi/indications/data/04_feature/matrix-disease-list.tsv` (legacy)
  - `data/raw/diseases/matrix-disease-list.tsv`
- **Status**: External curated source. CURIEs are accepted as-is (the HF dataset already grounds to Mondo plus selected non-Mondo identifiers).

## ETL module

`src/medic/ingest/disease_list/__main__.py`

1. Try the HuggingFace dataset, then fall through to the legacy TSVs.
2. From the HuggingFace dataset, read `id` (Mondo/UMLS/HP/... CURIE), `name`, `definition`, semicolon-separated `synonyms`, `subsets`, and `crossreferences`. Carry through HF boolean columns: `is_clingen`, `is_cancer_or_benign_tumor`, `is_rare`, `is_gard_rare`, `is_nord_rare`, `is_ordo_subtype`, `is_hereditary_disease`, `is_chromosomal_disorder`, `is_disorder_of_development`, `is_musculoskeletal`.
3. From the legacy TSV, read `category_class`, `label`, `definition`, `synonyms`, `subsets`, `crossreferences`. Derive boolean flags by parsing `subsets` strings (`f_gard_rare`, `f_nord_rare`, `f_is_rare`, `f_ordo_subtype`, `f_mondo_top_grouping_*`).
4. Write `kb/diseases/disease_list.yaml` as a `DiseaseList` (top-level `diseases:` key).

## Source isolation

Not jurisdictional. The disease list is the authority for all disease entities consumed by the on-label merge, the research list, the adverse-event list, and the export layer.

## Justfile target

```bash
just ingest-disease-list
```

## Licence

CC BY 4.0, per the HuggingFace dataset card for `everycure/disease-list`. **Attribution required** —
credit EveryCure in any redistribution of a product built from this list.
See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
