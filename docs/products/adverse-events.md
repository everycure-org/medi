# Adverse Event List

## Overview

The adverse event list captures drug-adverse event associations from label mining (PVLens) and post-market surveillance (FAERS). This is a new product in MeDIC v2, not present in v1.0.0.

## Products

### Adverse Event List (`products/adverse_event_list.yaml`)

- **Status**: Integration in progress
- **Planned sources**: PVLens, FAERS

## Schema

| Field | Description |
|-------|-------------|
| `drug_id` | Canonical drug CURIE |
| `drug_label` | Drug name |
| `adverse_event_id` | MedDRA preferred term ID |
| `adverse_event_label` | Adverse event name |
| `adverse_event_hpo_id` | HPO mapping (where available) |
| `label_section` | Label section (adverse reactions, black box, post-market) |
| `frequency` | Reported frequency |
| `severity` | Severity classification |
| `sources` | Which sources support this association |
| `evidence` | Evidence items with provenance |

## Sources

### PVLens (Label Mining)

- Open-source system from GSK Global Safety
- Extracts AEs from all ~650K FDA SPL labels
- Maps to MedDRA, RxNorm, SNOMED CT
- F1=0.899 for adverse event extraction
- Distinguishes: adverse reactions, black box warnings, post-marketing

### FAERS (Post-Market Reports)

- FDA's spontaneous adverse event reporting system
- Quarterly data releases with ~154K+ reports per therapeutic area
- Enables disproportionality analysis (PRR) for signal detection

## Pipeline

```
PVLens ──┐
FAERS ───┼──► Ground drugs/events ──► Map MedDRA→HPO ──► Merge ──► Export
```

## Build

```bash
just build-adverse-event-list
```
