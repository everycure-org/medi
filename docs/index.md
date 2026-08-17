# MeDIC: Medicines, Diseases, Indications, and Contraindications

MeDIC is a LinkML-driven knowledge base of drug-disease associations drawn from regulatory sources worldwide, with AI-assisted evidence curation capabilities.

## Overview

MeDIC integrates drug approval data from multiple international regulatory agencies (FDA, EMA, PMDA, and others) with literature-derived evidence to produce comprehensive drug-disease association datasets.

## Products

- **Drug List** - Unified list of approved drugs across jurisdictions with rich cross-reference mappings
- **Disease List** - Curated rare disease list based on Mondo ontology
- **Indication List** - Approved, investigational, and off-label drug-disease associations from regulatory labels, clinical trials, and curated sources
- **Research Pipeline** - Drug-disease associations from research and repurposing efforts
- **Adverse Event List** - Adverse event associations from PVLens and FAERS
- **SSSOM Mappings** - Drug identifier cross-reference mappings

## Sources

| Source | Type | Jurisdiction |
|--------|------|-------------|
| FDA Orange Book | Drug approvals | USA |
| FDA Purple Book | Biologics | USA |
| FDA DailyMed | Indications/Contraindications | USA |
| EMA (EPAR) | Drug approvals + indications | EU |
| PMDA | Drug approvals + indications | Japan |
| Russia Registry | Drug approvals | Russia |
| India Registry | Drug approvals | India |
| China CDE | Drug approvals | China |
| EveryCure drug-list | Curated drugs | International |
| CURE-ID | Repurposing evidence (planned) | International |
| ClinicalTrials.gov | Clinical trial indications (planned) | International |
| PVLens | Adverse events | USA |
| FAERS | Post-market adverse events | USA |

## Getting Started

```bash
# Install dependencies
just install

# Build all products
just build-all

# Validate all KB files
just validate-all
```
