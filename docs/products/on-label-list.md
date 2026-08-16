# Indication List

## Overview

The Indication List captures drug-disease associations from regulatory drug labels, clinical trials, and curated research sources. It covers approved (on-label), investigational (clinical trial), and off-label associations. It combines evidence from regulatory agencies (FDA, EMA, PMDA), clinical trial registries, and curated databases, each supporting or independently confirming associations.

## Products

### Indication List (`products/indication_list.yaml`)

- **Current size**: 11,071 drug-disease indication pairs
- **Sources**: FDA DailyMed (8,739), EMA (866), PMDA (1,466)

### Contraindication List (`products/contraindication_list.yaml`)

- **Current size**: 3,981 drug-disease contraindication pairs
- **Sources**: FDA DailyMed only (currently)

## Schema

### Indication Record

| Field | Description |
|-------|-------------|
| `final_normalized_drug_id` | Canonical drug CURIE (CHEBI preferred) |
| `final_normalized_drug_label` | Drug name |
| `final_normalized_disease_id` | Mondo disease CURIE |
| `final_normalized_disease_label` | Disease name |
| `drug_disease` | Compound key `DRUG_ID\|DISEASE_ID` |
| `relationship_type` | `INDICATION` |
| `source` | Source name from `IndicationSourceNameEnum` (e.g., FDA, EMA, PMDA, CLINICAL_TRIAL, CUREID) |
| `fda` | Boolean: supported by FDA DailyMed |
| `ema` | Boolean: supported by EMA EPAR |
| `pmda` | Boolean: supported by PMDA |

### Contraindication Record

Adds:

| Field | Description |
|-------|-------------|
| `relationship_type` | `CONTRAINDICATION` |
| `is_allergen` | Contraindication due to allergen |
| `is_diagnostic_agent` | Contraindication due to diagnostic agent |
| `indications_text` | Raw contraindication text from label |

## Pipeline

```
DailyMed ────────┐
EMA ─────────────┤
PMDA ────────────┼──► Per-source extraction ──► Grounding ──► LLM QC ──► Merge ──► Export
ClinicalTrials ──┤
CURE-ID ─────────┘
```

### Extraction Steps

1. Parse regulatory label text (SPL XML for DailyMed, EPAR table for EMA)
2. LLM extracts structured disease names from label text
3. Ground diseases to Mondo via Name Resolution
4. Ground drugs to canonical IDs
5. LLM validates grounding quality
6. Deduplicate across sources
7. Merge with source tracking (FDA/EMA/PMDA flags)

### Hyperrelations (Planned)

Future versions will include symptom-level specificity:
- "reduces tremor in Parkinson's" (not just "treats Parkinson's")
- Modeled as `Hyperrelation` objects with target symptom, relationship type, and supporting text

### Mondo Downfilling (Planned)

Indications can be propagated down the Mondo disease hierarchy to child terms, increasing coverage for rare disease subtypes.

## Build

```bash
just build-indication-list
```
