# MeDIC v2 Redesign: Executive Summary

## Vision

Rebuild MeDIC as a dismech-style, LinkML-driven knowledge base of drug-disease associations drawn from regulatory sources worldwide, with AI-assisted evidence curation capabilities. All data flows through validated YAML intermediates with rich provenance. The system produces backward-compatible release products while enabling new use cases (Monarch KG integration, drug repurposing research, pharmacovigilance).

---

## Architecture Overview

```text
Sources (ETL per source)            Schemas                Products
─────────────────────────    ──────────────────    ──────────────────
                              drug_source.yaml
Drug sources ──► YAML ──┐     on_label_source.yaml
                         │    adverse_event.yaml    drug_list.yaml
On-label sources ──► YAML├──► merge ──────────────► on_label_list.yaml
                         │                          contraindication_list.yaml
Adverse event sources ─► YAML                       adverse_event_list.yaml
                         │    drug.yaml             legacy CSV exports
Research pipeline ─► YAML┘    disease.yaml
                              on_label.yaml
Disease list ──────► YAML     research_source.yaml
```

Each box is a justfile target. Each YAML file validates against a LinkML schema compiled to pydantic.

---

## Components

### 1. Drug List

**Current:** 6 regulatory sources (Orange Book, Purple Book, EMA, PMDA, Russia, India) processed via Kedro into CSV drug lists (stringent/flexible). Plus the [EveryCure drug-list](https://huggingface.co/datasets/everycure/drug-list) (~1,817 manually curated drugs for repurposing research with rich classification metadata).

**Redesign:**
- Each source gets a dedicated ETL module producing `DrugSource` YAML (LinkML-validated)
- Captures: drug name, identifiers following the [Biolink ChemicalEntity ID prefix priority order](https://biolink.github.io/biolink-model/ChemicalEntity/#valid-id-prefixes) (CHEBI > UNII > PUBCHEM.COMPOUND > CHEMBL.COMPOUND > DRUGBANK > MESH > ...), approval dates, marketing status, chemical structure (SMILES), regulatory jurisdiction
- Rich cross-reference mappings on each drug node to all available coding schemes
- Merge step combines sources into a unified `DrugList` YAML
- Legacy CSV exports generated from the merged product
- **Mappings product:** A comprehensive SSSOM file with exact matches from the primary identifier to all other relevant coding schemes (RxNorm, ATC, DrugBank, UNII, DRON, etc.), released alongside the drug list

**New requirements from issues:**

- FDA approval dates (#3)
- RxNorm mappings (#4)
- DRON (Drug Ontology): include as an additional mapping target in the SSSOM file; no grounding or other role (#5)

**Drug normalization strategy:** Follow the [Biolink ChemicalEntity ID prefix priority order](https://biolink.github.io/biolink-model/ChemicalEntity/#valid-id-prefixes) to assign each drug its canonical identifier (CHEBI preferred, then UNII, PUBCHEM.COMPOUND, etc.). Leverage RxNorm Extension (Ostropolets/Columbia) which harmonizes drug vocabularies across 12 countries into a unified RxNorm hierarchy, feeding into the cross-reference mappings layer.

### 2. Disease List

**Current:** Complex build pipeline with Mondo SPARQL queries, LLM categorization, manual filtering.

**Redesign:**
- Source from [everycure/disease-list](https://huggingface.co/datasets/everycure/disease-list) on HuggingFace
- Convert to `DiseaseList` YAML validated against a dedicated LinkML schema
- Schema modeled after the existing disease list product: Mondo ID, label, definition, synonyms, subsets, cross-references, filter flags

### 3. On-Label List (Indications + Contraindications)

**Current:** Per-source pipelines (FDA DailyMed, EMA, PMDA) with LLM-assisted entity extraction, name resolution, deduplication, and merging. Produces indication and contraindication lists.

**Redesign:**

- Each source gets a dedicated ETL producing `OnLabelSource` YAML entries
- Each entry: drug (normalized ID), disease (Mondo), relationship type (indication/contraindication), jurisdiction, source document reference, raw text, LLM interpretation, approval status
- **Hyperrelations are core:** symptom-level specificity (e.g., "reduces tremor in Parkinson's" not just "treats Parkinson's") is modeled as a first-class part of the schema, not an optional extension
- Merge step combines across sources into `OnLabelList` YAML
- Addresses issues: source text columns (#2), guidelines approvals (#8)

**New sources to add:**

- Japanese Reimbursements (#6)
- Chinese drug approvals (CDE) (#7)
- Clinical guidelines (NCCN etc.) (#8)

### 3b. Adverse Events List (NEW)

**Motivation:** Melissa explicitly requested adverse event sources (Slack). Multiple conference presentations (AMIA S58 session) demonstrate mature, open-source methods for extracting adverse events from FDA labels and post-market reports.

**Sources:**

- **PVLens** (Painter/GSK) -- Open-source system extracting adverse events, indications, and black box warnings from FDA SPLs. Maps to MedDRA, RxNorm, SNOMED CT. Processes all ~650K SPLs in <1 hour. F1=0.899 for adverse events. Plans to expand to EMA/PMDA. Code: https://github.com/GSK-Global-Safety/pvlens
- **FAERS** (FDA Adverse Event Reporting System) -- Post-market spontaneous adverse event reports. Standard pharmacovigilance source with ~154K+ reports per therapeutic area.
- **SIDER** (legacy, for comparison) -- Static adverse event database, not updated since 2015. Useful as a baseline for coverage comparison but not as a primary source.
- **OnSIDES** -- BERT/LLM-based adverse event extraction from drug labels. Less comprehensive than PVLens (4,423 vs 8,640+ MedDRA terms) but worth evaluating.

**Design:**

- Each source gets a dedicated ETL producing `AdverseEventSource` YAML entries
- Each entry: drug (normalized ID), adverse event (MedDRA term, mapped to HPO where possible), source label section (adverse reactions / black box warning), frequency if available, severity
- Merge step produces `AdverseEventList` YAML
- This is a distinct product from indications/contraindications but uses the same drug normalization layer

### 4. Research Pipeline (NEW)

**Modeled after dismech's AI-assisted curation approach.** A general-purpose pipeline for discovering and curating drug-disease pairs from the literature, applicable to any disease area.

- **Initial focus:** ~3,085 priority rare diseases (driven by current stakeholder needs for RAPID/Monarch goals)
- dismech-style curation: LLM + PubMed search with snippet validation (more performant than full deep research)
- Each curated pair produces a `ResearchSource` YAML with evidence items:
  - Reference (PMID), support classification, evidence source type, snippet, explanation
  - Modeled on dismech's `EvidenceItem` pattern
- **Caching is essential:** intermediate results cached so work can resume when interrupted. The pipeline should support incremental progress across sessions
- **Interactive workflow:** a Claude Code skill guides the curator through diseases one at a time, prompting the user to move to the next disease after each is investigated
- Optional deep research component (e.g., via asta provider) for high-priority pairs
- Output feeds into the merge step as an additional source alongside regulatory data

### 5. Evidence System

**Inspired by dismech, adapted for drug-disease context.**

Every drug-disease pair from any source carries structured evidence:

```yaml
evidence:
  - source_type: REGULATORY        # REGULATORY | LITERATURE | GUIDELINE | DATABASE
    jurisdiction: USA               # for regulatory sources
    reference: "DailyMed:12345"     # or PMID:xxx, NCT:xxx
    document_text: "..."            # raw source text
    interpreted_text: "..."         # LLM interpretation
    support: SUPPORT                # SUPPORT | REFUTE | PARTIAL
    confidence: HIGH                # HIGH | MEDIUM | LOW
    approval_status: APPROVED       # clinical approval status
    max_research_phase: "Phase IV"  # where applicable
```

**Future extensions:** Social media pharmacovigilance signals (e.g., Reddit-derived drug-side effect KGs per Duan et al.) and disproportionality-based signal strength metrics (e.g., prescription-adjusted PRR per Kim et al.) could provide additional evidence layers. These are not in initial scope but the evidence model should be extensible enough to accommodate them.

### 6. Merge & Export

- Merges all source YAMLs (regulatory + research) per product
- Deduplication with provenance tracking (which sources support each pair)
- Produces final products: `DrugList`, `OnLabelList`, `ContraindicationList`, `AdverseEventList`
- **SSSOM mappings file** for drugs (ChEBI to RxNorm, ATC, DrugBank, UNII, DRON, etc.)
- Legacy export step generates CSV/XLSX matching [v1.0.0 release format](https://github.com/marcello-deluca/medic/releases/tag/v1.0.0)
- KGX export for Monarch KG integration (biolink-compliant edges). Downstream Koza ingest is handled separately
- **Update cadence:** manually triggered for now; no automated schedule

---

## Source Inventory

| Source | Type | Status | Jurisdiction |
| --- | --- | --- | --- |
| FDA Orange Book | Drug approvals | Existing | USA |
| FDA Purple Book | Biologics | Existing | USA |
| FDA DailyMed | Indications/Contraindications | Existing | USA |
| EMA (EPAR) | Drug approvals + indications | Existing | EU |
| PMDA | Drug approvals + indications | Existing | Japan |
| Russia registry | Drug approvals | Existing | Russia |
| India registry | Drug approvals | Existing | India |
| EveryCure drug-list | Curated drugs for repurposing | New | International |
| PVLens | Adverse events + indications + black box warnings | New | USA |
| FAERS | Post-market adverse event reports | New | USA |
| OnSIDES | Adverse events (ML-extracted from labels) | Evaluate | USA |
| Japanese Reimbursements | Indications | New (#6) | Japan |
| Chinese CDE | Drug approvals | New (#7) | China |
| Clinical Guidelines (NCCN) | Indications | New (#8) | International |
| RxNorm Extension | Drug normalization (12 countries) | New (reference) | International |
| DrugCentral | Cross-reference | Existing (compare) | USA |
| ROBOKOP | Cross-reference | Existing (compare) | - |
| Literature (PubMed) | Research pipeline | New | - |
| DRON (Drug Ontology) | Ontology alignment | Evaluate (#5) | - |

---

## Infrastructure

| Aspect | Choice | Reference |
|--------|--------|-----------|
| Project template | monarch-project-copier | task.md |
| AI integrations | github-ai-integrations | task.md |
| Build system | justfile (one target per ingest + product) | dismech |
| Package management | uv | dismech |
| Data modeling | LinkML schemas → pydantic | dismech |
| Validation | linkml-validate + term validation | dismech |
| Documentation | MkDocs (docs/) | dismech |
| CI/CD | GitHub Actions | dismech |
| Skills | Claude Code skills for curation workflows | dismech |

---

## Subproject Layout

```
medic/
├── src/medic/
│   ├── schema/              # All LinkML schemas
│   │   ├── drug.yaml
│   │   ├── disease.yaml
│   │   ├── on_label.yaml
│   │   ├── adverse_event.yaml
│   │   ├── drug_source.yaml
│   │   ├── on_label_source.yaml
│   │   ├── adverse_event_source.yaml
│   │   └── research_source.yaml
│   ├── ingest/              # Per-source ETL modules
│   │   ├── orangebook/
│   │   ├── purplebook/
│   │   ├── ema/
│   │   ├── pmda/
│   │   ├── russia/
│   │   ├── india/
│   │   ├── everycure_drugs/
│   │   ├── dailymed/
│   │   ├── pvlens/
│   │   ├── faers/
│   │   ├── chinese_cde/
│   │   ├── japanese_reimbursements/
│   │   └── guidelines/
│   ├── research/            # AI-assisted curation pipeline
│   ├── merge/               # Source → product merge logic
│   ├── export/              # Legacy CSV + KGX export
│   └── validate/            # Validation utilities
├── kb/
│   ├── drugs/               # Drug source YAMLs
│   ├── diseases/            # Disease list YAML
│   ├── on_label/            # On-label source YAMLs (per source)
│   └── research/            # Research-derived pairs
├── products/                # Merged final products (generated)
├── exports/                 # Legacy format exports (generated)
├── docs/                    # MkDocs documentation
├── tests/
├── justfile
├── project.justfile
└── pyproject.toml
```

---

## Priority Workstreams

1. **Schema design** -- Define LinkML schemas for all data types. This is foundational; everything else depends on it.
2. **Drug list rebuild** -- Migrate existing 6 source ETLs to produce schema-validated YAML. Leverage RxNorm Extension for cross-jurisdiction normalization.
3. **Disease list** -- Ingest from HuggingFace, validate against schema.
4. **On-label list rebuild** -- Migrate FDA/EMA/PMDA indication + contraindication ETLs.
5. **Adverse events** -- Integrate PVLens and FAERS as new adverse event sources.
6. **Research pipeline** -- Build literature-based drug-disease pair curation. Initial focus: ~3K priority rare diseases.
7. **New sources** -- Add Chinese, Japanese reimbursements, guidelines.
8. **Merge + export** -- Combine sources into products, generate legacy exports + KGX.
9. **Infrastructure** -- copier migration, AI integrations, docs, CI/CD.

---

## Key Decisions

1. **Drug identifier priority:** Follow the [Biolink ChemicalEntity prefix order](https://biolink.github.io/biolink-model/ChemicalEntity/#valid-id-prefixes) (CHEBI > UNII > PUBCHEM.COMPOUND > CHEMBL.COMPOUND > DRUGBANK > ...). Rich mappings to all available coding schemes on each drug node.
2. **Drug mappings product:** A comprehensive SSSOM file with exact matches from the primary identifier to all other coding schemes, released as part of the drug list.
3. **DRON:** Included only as an additional mapping target in the SSSOM file. No grounding or other role.
4. **Research pipeline approach:** dismech-style LLM + PubMed search with snippet validation. Caching for incremental progress. Interactive Claude Code skill for per-disease curation workflow.
5. **KGX export:** Produced as part of the release. Downstream Monarch KG integration (Koza ingest) is handled separately.
6. **Hyperrelations:** Symptom-level specificity is core, not optional. Modeled as a first-class part of the on-label schema.
7. **Update cadence:** Manually triggered for now.
