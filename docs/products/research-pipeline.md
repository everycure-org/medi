# Research Pipeline

## Overview

The research pipeline discovers drug-disease associations from multiple sources: PubMed literature search with heuristic drug mention extraction, and external curated databases such as CURE-ID. It is designed for systematic curation of rare disease drug associations, initially targeting ~3,085 priority rare diseases.

## Products

- **KB files**: `kb/research/<MONDO_ID>.yaml` (per-disease research associations from PubMed)
- **KB files**: `kb/research/cureid_associations.yaml` (drug repurposing case reports from CURE-ID)
- **Compiled**: `products/research_list.yaml` (all research associations merged)

## Pipeline

```
Priority disease queue
       │
       ▼
  PubMed search (disease name + drug therapy terms)
       │
       ▼
  Fetch abstracts (with NCBI API key)
       │
       ▼
  Extract drug mentions (heuristic: suffix patterns + context)
       │
       ▼
  Build ResearchAssociation YAML (with evidence snippets)
       │
       ▼
  Cache results per disease
```

## Configuration

### NCBI API Key

Set `NCBI_API_KEY` in `.env` for higher PubMed rate limits (10 req/sec vs 3/sec):

```
NCBI_API_KEY=your_key_here
```

### Priority Disease List

The pipeline processes diseases from `data/priority-diseases-2026-03-11.tsv`, which contains ~3,085 rare diseases with Mondo IDs and phenotype categories. It is tracked in the repo — it used to sit under `background/`, which `.gitignore` excludes because that is where non-redistributable manually-provided source archives land, so `build-research` could not run from a fresh clone.

## Curation Modes

### Interactive (`just research-curate`)

Processes one disease at a time:

```bash
just research-curate disease=MONDO:0007037  # Specific disease
just research-curate                         # Next uncurated disease
```

### Batch (`just research-batch`)

Processes multiple diseases in sequence:

```bash
just research-batch count=10
```

## Evidence Schema

Each research association includes evidence items:

| Field | Description |
|-------|-------------|
| `source_type` | `LITERATURE` |
| `reference` | `PMID:12345678` |
| `reference_title` | Article title |
| `explanation` | How the evidence supports the drug-disease association |
| `snippet` | First sentence mentioning the drug |
| `support` | `PARTIAL` (draft status) |
| `confidence` | `LOW` (requires curator review) |
| `evidence_source` | `HUMAN_CLINICAL` |

## Caching

- **Abstract cache**: `references_cache/PMID_*.json` (shared with reference validator)
- **Disease cache**: `cache/research/<MONDO_ID>.json` (per-disease results)
- **Progress tracker**: `cache/research/progress.yaml`

Caching enables incremental progress across sessions. If a disease has been curated, its cached results are reused.

## CURE-ID Ingest

CURE-ID (https://cure.ncats.io/) is an FDA/NCATS collaboration that collects real-world drug repurposing evidence from clinical case reports. The CURE-ID ingest (`src/medic/ingest/cureid/__main__.py`) adds these associations to the research pipeline.

### How it works

1. Downloads the pre-mapped TSV from `https://opendata.ncats.nih.gov/public/cureid/cureid_data.tsv`
2. Filters to drug→disease treatment edges (`biolink:applied_to_treat`)
3. Aggregates by (drug, disease) pair — multiple case reports become one association with multiple evidence items
4. Drug→PhenotypicFeature edges sharing a report with a disease edge are folded into the disease association's notes
5. Writes `kb/research/cureid_associations.yaml`

### Evidence structure

Each association includes:
- A `DATABASE` evidence item citing the CURE-ID source with report_id and clinical outcome
- `LITERATURE` evidence items for any associated PMIDs
- All marked as `OFF_LABEL`, `CASE_REPORT`, `HUMAN_CLINICAL`
- Confidence: `MEDIUM` for positive outcomes (improved/recovered), `LOW` otherwise

### Running

```bash
just ingest-cureid
```

No API key required — the data is publicly accessible.

## Build

```bash
just build-research
```
