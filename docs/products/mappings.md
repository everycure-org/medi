# Drug Mappings (SSSOM)

## Overview

MeDIC produces a comprehensive SSSOM (Simple Standard for Sharing Ontological Mappings) file containing cross-reference mappings between drug identifiers from multiple coding systems.

## Product

- **File**: `exports/medic_drug_mappings.sssom.tsv`
- **Current size**: 4,191 mappings
- **Format**: [SSSOM TSV](https://mapping-commons.github.io/sssom/)

## Schema

| Column | Description |
|--------|-------------|
| `subject_id` | Primary drug identifier (CHEBI preferred) |
| `subject_label` | Drug name |
| `predicate_id` | `skos:exactMatch` |
| `object_id` | Equivalent identifier in another system |
| `object_label` | (empty - resolved at query time) |
| `mapping_justification` | `semapv:LexicalMatching` |

## Covered Coding Systems

Mappings include identifiers from:

- **CHEBI** - Chemical Entities of Biological Interest
- **UNII** - FDA Unique Ingredient Identifiers
- **PUBCHEM.COMPOUND** - PubChem compound IDs
- **CHEMBL.COMPOUND** - ChEMBL compound IDs
- **DRUGBANK** - DrugBank IDs
- **MESH** - Medical Subject Headings
- **RXNORM** / **RXCUI** - RxNorm concept identifiers
- **CAS** - Chemical Abstracts Service numbers
- **DRON** - Drug Ontology (planned)
- **UMLS** - Unified Medical Language System
- **KEGG.COMPOUND** - KEGG compound IDs
- **INCHIKEY** - InChI key chemical identifiers

## Build

```bash
just export-sssom
```
