# Disease List

## Overview

The disease list is a curated set of diseases from the Mondo Disease Ontology used as the disease axis of the MeDIC drug-disease matrix. It includes filter flags derived from Mondo subsets, cross-references, and LLM-based categorization.

## Source

- **Primary source**: [EveryCure disease-list](https://huggingface.co/datasets/everycure/disease-list) (HuggingFace)
- **Current source**: `medi/indications/data/04_feature/matrix-disease-list.tsv` (existing product)
- **Ontology**: [Mondo Disease Ontology](https://mondo.monarchinitiative.org/)

## Product

- **File**: `kb/diseases/disease_list.yaml`
- **Current size**: 17,946 diseases
- **Format**: YAML conforming to the `Disease` LinkML schema

## Schema

| Field | Description |
|-------|-------------|
| `category_class` | Mondo CURIE (e.g., `MONDO:0017545`) |
| `label` | Disease name |
| `definition` | Text definition from Mondo |
| `synonyms` | Alternative names (semicolon-separated in TSV, list in YAML) |
| `subsets` | Mondo subset tags (e.g., `mondo:gard_rare`, `mondo:nord_rare`) |
| `crossreferences` | External database xrefs (MEDGEN, UMLS, GARD, Orphanet, ICD) |

### Filter Flags (derived from subsets)

| Flag | Description |
|------|-------------|
| `f_gard_rare` | Listed as rare in GARD |
| `f_nord_rare` | Listed as rare in NORD |
| `f_is_rare` | Considered rare (union of GARD, NORD, Orphanet) |
| `f_ordo_subtype` | Is an ORDO subtype |
| `f_mondo_top_grouping_*` | Various Mondo top-level groupings |

## Build

```bash
just build-disease-list
```
