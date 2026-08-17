# Drug List

## Overview

The drug list is the primary product of MeDIC's drug pipeline. It merges drug records from 6 regulatory sources into a unified list with canonical identifiers, approval status across jurisdictions, drug classification flags, ATC codes, and SMILES structures.

## Variants

### Flexible List (`drug_list_flexible.csv`)

All drugs with a valid canonical identifier from any regulatory source. Includes drugs approved in any of the 6 jurisdictions (USA, Europe, Japan, India, Russia) plus drugs from the EveryCure curated list.

- **Current size**: 4,192 drugs (v2) / 3,883 drugs (v1.0.0)
- **Columns**: 31

### Stringent List (`drug_list_stringent.csv`)

Subset of the flexible list restricted to drugs approved in at least one "stringent" regulatory jurisdiction (USA, Europe, or Japan). Excludes `approved_india` and `approved_russia` columns.

- **Current size**: ~3,055 drugs (v2) / 2,836 drugs (v1.0.0)
- **Columns**: 29

## Schema (31 columns)

| Column | Type | Description |
|--------|------|-------------|
| `curie` | string | Primary canonical identifier (Biolink ChemicalEntity prefix priority) |
| `curie_label` | string | Canonical drug name |
| `source_ingredients` | string | Pipe-separated original ingredient names from all sources |
| `approved_usa` | float | 1.0 if approved by FDA (Orange Book or Purple Book) |
| `marketing_status_usa` | string | FDA marketing status: RX, OTC, DISCONTINUED |
| `approved_europe` | float | 1.0 if approved by EMA |
| `approved_japan` | float | 1.0 if approved by PMDA |
| `approved_india` | float | 1.0 if approved in India |
| `approved_russia` | float | 1.0 if approved in Russia |
| `is_combination_therapy` | bool | Whether this is a combination product |
| `combination_therapy_ingredients` | string | Pipe-separated individual ingredients |
| `combination_therapy_ingredients_curies` | string | CURIEs for individual ingredients |
| `is_steroid` | int | LLM-classified as corticosteroid |
| `is_antimicrobial` | int | LLM-classified as antibiotic/antiviral/antifungal/etc. |
| `is_chemotherapy` | int | LLM-classified as cytotoxic chemotherapy |
| `is_glucose_regulator` | int | LLM-classified as glucose regulator |
| `is_vaccine_or_antigen` | int | LLM-classified as vaccine or antigen |
| `is_no_therapeutic_value` | int | LLM-classified as no therapeutic value (vehicles, excipients) |
| `is_metallic_salt` | int | LLM-classified as simple metallic salt |
| `is_allergen` | int | LLM-classified as allergen for testing |
| `is_radioisotope_or_diagnostic_agent` | int | LLM-classified as radioisotope/diagnostic |
| `is_cancer_drug` | int | LLM-classified as cancer treatment |
| `alternate_ids` | string | All equivalent identifiers from NodeNorm |
| `atc_codes` | string | ATC classification code(s) |
| `atc_main` | string | Primary ATC code |
| `atc_level1` through `atc_level5` | string | ATC hierarchy levels |
| `smiles` | string | SMILES chemical structure |

## Identifier Priority

Drug identifiers follow the [Biolink ChemicalEntity prefix priority](https://biolink.github.io/biolink-model/ChemicalEntity/#valid-id-prefixes):

1. CHEBI (preferred)
2. UNII
3. PUBCHEM.COMPOUND
4. CHEMBL.COMPOUND
5. DRUGBANK
6. MESH

## Pipeline

```
Orange Book ─┐
Purple Book ─┤
EMA ─────────┤
PMDA ────────┼──► Merge by canonical ID ──► Enrich (tags, ATC, SMILES) ──► Export
Russia ──────┤
India ───────┘
```

## Build

```bash
just build-drug-list
```
