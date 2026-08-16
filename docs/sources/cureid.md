# CURE-ID

## Overview

CURE-ID is the FDA/NCATS open-data portal of clinician-submitted drug repurposing case reports for difficult-to-treat diseases. MeDIC ingests the open data dump, filters to drug treatment edges, and emits research associations into `kb/research/cureid_associations.yaml`.

## Source data

- **Portal**: <https://opendata.ncats.nih.gov/public/cureid/>
- **Download**: <https://opendata.ncats.nih.gov/public/cureid/cureid_data.tsv>
- **Format**: TSV with pre-mapped CURIEs (`subject_final_curie`, `object_final_curie`, `biolink_predicate`, `report_id`, `pmid`, `link`, `outcome`)
- **Raw file**: `data/raw/cureid/cureid_data.tsv` (downloaded on first run, re-used unless `--force-download`)
- **Status**: Fully raw — primary data from an authoritative source, CURIEs already mapped by NCATS.

## ETL module

`src/medic/ingest/cureid/__main__.py`

1. Download `cureid_data.tsv` if missing (via `download_file`).
2. Filter to rows where `subject_type == "Drug"` and `biolink_predicate == "biolink:applied_to_treat"`.
3. Split by `object_type`: `Disease` rows become research associations directly; `PhenotypicFeature` rows sharing a `report_id` with a disease edge are folded into that association's `notes` ("Also treated symptoms: …"); orphan phenotype rows become their own associations.
4. Group by `(drug_curie, disease_curie)` and build one evidence item per unique `report_id` (`source_type: DATABASE`) and one per unique PMID (`source_type: LITERATURE`).
5. Map the CURE-ID `outcome` text to `confidence` (`improved`/`recovered` → `MEDIUM`, else `LOW`) and `support` (`improved`/`recovered` → `SUPPORT`, else `PARTIAL`).
6. Each association is tagged `curation_status: VALIDATED`, `curator: "cureid"`, `evidence_source: HUMAN_CLINICAL`, `approval_status: OFF_LABEL`, `max_research_phase: CASE_REPORT`.
7. Write `kb/research/cureid_associations.yaml`.

## Source isolation

The DATABASE evidence row carries `jurisdiction: USA` (CURE-ID is FDA/NCATS-hosted). CURE-ID feeds the research-association list, not the on-label indication list, so no jurisdiction flags propagate to `products/indication_list.yaml`.

## Justfile target

```bash
just ingest-cureid
```

## Licence

NIH/NCATS open data, in the public domain. Attribution is a courtesy, not an obligation. MeDIC
redistributes derived research associations freely. See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
