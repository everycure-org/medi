# MeDIC v2 Implementation Plan

Reference: [Executive Summary](executive_summary.md)

---

## Phase 0: Project Scaffolding

### 0.1 Initialize project from copier template

- Run `copier copy gh:monarch-initiative/monarch-project-copier medic` to scaffold the new project
- Adapt the generated structure to the MeDIC layout from the executive summary
- Set up `pyproject.toml` with uv, hatchling, Python >=3.12
- Install `github-ai-integrations` per <https://github.com/ai4curation/github-ai-integrations>

**Deliverable:** Empty project skeleton that builds and passes CI.

### 0.2 Set up justfile

Model after dismech. Two files:

- `justfile` -- imports `project.justfile`, provides standard LinkML recipes (install, setup, test, docs)
- `project.justfile` -- all MeDIC-specific targets

Initial targets (stubs that will be filled in later phases):

```just
# Ingests
ingest-orangebook:
ingest-purplebook:
ingest-ema:
ingest-pmda:
ingest-russia:
ingest-india:
ingest-dailymed:
ingest-pvlens:
ingest-faers:

# Products
build-drug-list:
build-disease-list:
build-on-label-list:
build-adverse-event-list:
build-research:

# Validation
validate-schema file:
validate-terms file:
validate-references file:
validate file:          # all three
validate-all:

# Export
export-legacy:
export-kgx:
export-sssom:
```

### 0.3 Set up documentation

- `mkdocs.yml` with Material theme (copy structure from dismech)
- `docs/index.md` -- overview of MeDIC v2
- `docs/schema/` -- auto-generated schema docs
- `docs/sources/` -- one page per source describing provenance and ETL

### 0.4 Set up CI/CD

GitHub Actions:

- `ci.yml`: lint, test, schema validation on every PR
- `release.yml`: build products, generate exports, create GitHub release

### 0.5 Claude Code skills (initial)

- Copy relevant skill patterns from dismech (term validation, reference validation)
- Create `medic-research-curation` skill stub (Phase 6)

---

## Phase 1: Schema Design

This is foundational. All subsequent phases depend on these schemas.

### 1.1 Core entity schemas

**`src/medic/schema/drug.yaml`** -- Drug entity schema

Modeled after the existing `drug_list_flexible.csv` and the [everycure/drug-list](https://huggingface.co/datasets/everycure/drug-list) dataset:

```yaml
# Key classes:
Drug:
  attributes:
    curie:                 # Primary ID per Biolink ChemicalEntity prefix priority
    curie_label:           # Canonical label
    source_ingredients:    # Original ingredient name(s) from source
    synonyms:              # Alternative names
    # Jurisdiction approvals
    approved_usa:          # boolean
    marketing_status_usa:  # RX, OTC, DISCN, etc.
    approved_europe:       # boolean
    approved_japan:        # boolean
    approved_india:        # boolean
    approved_russia:       # boolean
    # Drug properties
    is_combination_therapy:
    combination_therapy_ingredients:
    combination_therapy_ingredients_curies:
    is_steroid:
    is_antimicrobial:
    is_chemotherapy:
    is_glucose_regulator:
    is_vaccine_or_antigen:
    is_no_therapeutic_value:
    is_metallic_salt:
    is_allergen:
    is_radioisotope_or_diagnostic_agent:
    is_cancer_drug:
    # Additional flags from everycure drug-list
    is_antipsychotic:
    is_sedative:
    is_analgesic:
    is_cardiovascular:
    is_cell_therapy:
    drug_class:            # e.g., "Nucleoside reverse transcriptase inhibitor"
    therapeutic_area:      # e.g., "Antimicrobial"
    drug_function:         # e.g., "Antiviral"
    drug_target:           # e.g., "HIV"
    # Identifiers and chemical data
    alternate_ids:         # All other known IDs
    drugbank_id:
    atc_codes:
    atc_main:
    atc_level1: through atc_level5:
    smiles:
```

**`src/medic/schema/disease.yaml`** -- Disease entity schema

Modeled after the existing `matrix-disease-list.tsv`:

```yaml
Disease:
  attributes:
    category_class:        # MONDO CURIE (e.g., MONDO:0017545)
    label:                 # Disease name
    definition:            # Text definition
    synonyms:              # Pipe-separated synonyms
    subsets:               # Space-separated subset tags (e.g., mondo:harrisons_view_hereditary_disease)
    crossreferences:       # Semicolon-separated xrefs (MEDGEN, UMLS, GARD, etc.)
    # Filter flags (from unfiltered list)
    f_matrix_manually_excluded:
    f_matrix_manually_included:
    f_clingen:
    f_grouping_subset:
    f_orphanet_subtype:
    f_omimps:
    f_leaf:
    # ... (26 filter flags total)
```

**`src/medic/schema/on_label.yaml`** -- On-label association schema

Modeled after the existing `matrix_indication_list.xlsx` and `matrix_contraindication_list.xlsx`:

```yaml
OnLabelAssociation:
  attributes:
    # Core fields (from existing indication list)
    final_normalized_drug_id:     # CHEBI CURIE
    final_normalized_drug_label:
    final_normalized_disease_id:  # MONDO CURIE
    final_normalized_disease_label:
    drug_disease:                 # Compound key "CHEBI:xxx|MONDO:xxx"
    # Source flags (which regulatory agencies)
    fda:                          # boolean
    ema:                          # boolean
    pmda:                         # boolean
    # Relationship type
    relationship_type:            # INDICATION | CONTRAINDICATION
    # Contraindication-specific fields
    is_allergen:                  # boolean (from contraindication list)
    is_diagnostic_agent:          # boolean (from contraindication list)
    # Hyperrelations (core, not optional)
    hyperrelations:
      - target_symptom:           # HPO term or free text
        relationship:             # REDUCES | PREVENTS | TREATS | CONTRAINDICATES_IN
        specificity_text:         # raw label text supporting this
    # Provenance
    indications_text:             # raw source text from label
    evidence:                     # list of EvidenceItem
    # Downfill tracking
    downfilled_from_mondo:        # MONDO ID if propagated from parent
```

**`src/medic/schema/adverse_event.yaml`** -- Adverse event association schema

```yaml
AdverseEventAssociation:
  attributes:
    drug:                  # Drug reference
    adverse_event:         # MedDRA term, with optional HPO mapping
    label_section:         # ADVERSE_REACTIONS | BLACK_BOX_WARNING | POST_MARKET
    frequency:             # if available from source
    severity:              # if available
    evidence:
    sources:
```

**`src/medic/schema/evidence.yaml`** -- Shared evidence model (dismech-inspired)

```yaml
EvidenceItem:
  attributes:
    source_type:           # REGULATORY | LITERATURE | GUIDELINE | DATABASE | POST_MARKET
    jurisdiction:          # USA | EU | Japan | ... (for regulatory)
    reference:             # DailyMed:xxx | PMID:xxx | NCT:xxx | FAERS:xxx
    document_text:         # raw source text
    interpreted_text:      # LLM interpretation
    snippet:               # exact quote (for literature, validated against abstract)
    support:               # SUPPORT | REFUTE | PARTIAL
    confidence:            # HIGH | MEDIUM | LOW
    approval_status:       # APPROVED | INVESTIGATIONAL | WITHDRAWN | ...
    max_research_phase:    # Phase I-IV, Pre-clinical, etc.
    evidence_source:       # HUMAN_CLINICAL | MODEL_ORGANISM | IN_VITRO | COMPUTATIONAL
```

### 1.2 Source-level schemas

Each ingest produces instances of these before merging:

- **`drug_source.yaml`** -- per-source drug records (Orange Book row, EMA row, etc.)
- **`on_label_source.yaml`** -- per-source indication/contraindication records
- **`adverse_event_source.yaml`** -- per-source adverse event records
- **`research_source.yaml`** -- literature-derived drug-disease pairs with evidence

### 1.3 Compile schemas to pydantic

- Add `gen-pydantic` target to justfile: `linkml-generate pydantic` for each schema
- Generated pydantic models go to `src/medic/models/`
- All ETL code uses these models for construction and validation

### 1.4 SSSOM mappings schema

- Define the drug mappings output format as SSSOM-compliant TSV
- Include: subject_id, subject_label, predicate_id (skos:exactMatch), object_id, object_label, mapping_justification

---

## Phase 2: Entity Grounding Layer (Modular)

### 2.1 Design: pluggable grounding backends

Three competing efforts need to be supported. The grounding layer is an abstraction with swappable backends selected via CLI parameter.

```text
┌─────────────────────────────────────────────┐
│           GroundingService (ABC)             │
│  ground_disease(name) -> (id, label, score) │
│  ground_drug(name) -> (id, label, score)    │
│  normalize(curie) -> (canonical, alt_ids)   │
└──────────┬──────────┬──────────┬────────────┘
           │          │          │
     NameResBackend  OAKBackend  OLSBackend
     (NCATS SRI)    (Monarch)    (EBI)
```

**`src/medic/grounding/__init__.py`**

```python
def get_grounding_service(backend: str = "nameres") -> GroundingService:
    """Factory. backend = 'nameres' | 'oak' | 'ols'"""
```

**CLI integration:** Every ingest justfile target and the merge step accept a `--grounding-backend` flag (default: `nameres` to preserve current behavior).

### 2.2 NameRes backend (current behavior)

- Wraps the existing `nameres.py` code
- API: `https://name-resolution-sri.renci.org/lookup`
- Normalization via NodeNorm: `https://nodenormalization-sri.renci.org/1.5/get_normalized_nodes`
- LLM QC pass (GPT-4o) for result validation -- keep this as an optional post-grounding step

### 2.3 OAK backend (dismech-style)

- Uses `oaklib` with sqlite adapters (offline, fast)
- Config file: `conf/oak_config.yaml` mapping prefixes to `sqlite:obo:<name>` adapters
- For drugs: CHEBI adapter
- For diseases: MONDO adapter
- Fuzzy matching via OAK search functionality

### 2.4 OLS backend

- Uses the OLS4 MCP server or direct REST API at `https://www.ebi.ac.uk/ols4/api`
- Search endpoint for grounding, term details for validation
- Useful for terms not well covered by pre-built OAK sqlite databases

### 2.5 Post-grounding LLM QC (shared across backends)

The existing LLM-assisted QC step (`check_nameres_llm`, `llm_improve_ids`) is backend-agnostic:

1. Ground entity via selected backend
2. LLM evaluates: "Is `{resolved_label}` a good match for `{original_name}`?"
3. If not, LLM selects best alternative from top-N candidates
4. Normalize final ID via NodeNorm (or OAK equivalent)

This step is optional and controlled by a `--llm-qc` flag.

---

## Phase 3: Validation Stack (dismech-style)

### 3.1 Schema validation

- `just validate-schema <file>`: `linkml-validate` against the appropriate schema
- Runs on every YAML file in `kb/` and `products/`

### 3.2 Term validation

- `just validate-terms <file>`: `linkml-term-validator validate-data <file> -s <schema> -t <class> -c conf/oak_config.yaml`
- Validates all ontology term IDs exist and labels match canonical labels
- OAK adapters configured in `conf/oak_config.yaml`:

```yaml
ontology_adapters:
  MONDO: sqlite:obo:mondo
  HP: sqlite:obo:hp
  CHEBI: sqlite:obo:chebi
  MAXO: sqlite:obo:maxo
  # MedDRA: needs custom adapter or skip
```

### 3.3 Reference validation

- `just validate-references <file>`: `linkml-reference-validator validate data <file> --schema <schema> --target-class <class> --config conf/reference_validator_config.yaml`
- Validates evidence snippets against PubMed abstracts (anti-hallucination)
- Cache in `references_cache/` for offline re-validation
- Config: `conf/reference_validator_config.yaml` with appropriate `skip_prefixes` for non-PubMed references (DailyMed, FAERS, etc.)

### 3.4 Combined validation

- `just validate <file>`: schema + terms + references
- `just validate-all`: run across all `kb/` files
- `just qc`: validate-all + compliance metrics

### 3.5 Pre-commit hook

- Validation script (like dismech's `validate_disorder_hook.py`) that runs on Edit/Write of any YAML in `kb/`
- Schema validation only (fast) -- full validation via explicit `just validate`

---

## Phase 4: Drug List Rebuild

### 4.1 Migrate existing source ETLs

For each of the 6 existing sources, create `src/medic/ingest/<source>/`:

| Source | Input | Key fields to extract |
| --- | --- | --- |
| `orangebook/` | `orangebook.xlsx` | Drug name, active ingredient, approval date, marketing status, application number |
| `purplebook/` | `purplebook.xlsx` | Biologic name, BLA number, approval date, marketing status |
| `ema/` | `ema_norm.xlsx` | Drug name, active substance, ATC, authorization date, therapeutic area |
| `pmda/` | `pmda_norm.xlsx` | Drug name (translated), approval date, indications text |
| `russia/` | `russia_norm.csv` | Drug name (translated), registration number |
| `india/` | `india_norm.csv` | Drug name, approval status |
| `everycure_drugs/` | HuggingFace `everycure/drug-list` | 1,817 curated drugs: name, CHEBI ID, DrugBank ID, drug_class, therapeutic_area, drug_function, drug_target, ATC codes, boolean property flags, synonyms |

Each ETL module:

1. Reads raw source file
2. Extracts and normalizes fields
3. Grounds drug names via `GroundingService` (backend selectable)
4. Constructs `DrugSource` pydantic model instances
5. Writes validated YAML to `kb/drugs/<source>/`

### 4.2 Drug merge

`src/medic/merge/drug_merge.py`:

1. Reads all `kb/drugs/<source>/*.yaml` files
2. Groups by canonical drug ID (using NodeNorm or OAK normalization)
3. Merges metadata across sources (union of approval dates, marketing statuses, identifiers)
4. Resolves conflicts (latest approval date wins, most specific marketing status)
5. Writes `products/drug_list.yaml`

### 4.3 Drug enrichment

Post-merge enrichment (can fail gracefully per drug):

- ATC codes: multi-source fallback (ChEBI API -> ChEMBL -> PubChem -> DrugCentral -> WHO CC)
- SMILES: PubChem REST API
- RxNorm mappings: RxNorm Extension or RxNav API

### 4.4 SSSOM export

`src/medic/export/sssom.py`:

- Reads `products/drug_list.yaml`
- For each drug, emits one SSSOM row per cross-reference mapping
- Includes DRON mappings where available
- Output: `exports/medic_drug_mappings.sssom.tsv`

### 4.5 justfile targets

```just
ingest-orangebook grounding="nameres":
  uv run python -m medic.ingest.orangebook --grounding-backend {{grounding}}

# ... same pattern for all sources

ingest-everycure-drugs:
  uv run python -m medic.ingest.everycure_drugs

build-drug-list grounding="nameres":
  just ingest-orangebook {{grounding}}
  just ingest-purplebook {{grounding}}
  just ingest-ema {{grounding}}
  just ingest-pmda {{grounding}}
  just ingest-russia {{grounding}}
  just ingest-india {{grounding}}
  just ingest-everycure-drugs
  uv run python -m medic.merge.drug_merge
  uv run python -m medic.export.sssom
```

---

## Phase 5: Disease List

### 5.1 Ingest from HuggingFace

`src/medic/ingest/disease_list/`:

1. Download `everycure/disease-list` dataset from HuggingFace
2. Parse the source format (infer schema from raw data)
3. Construct `Disease` pydantic model instances
4. Validate Mondo IDs via term validator
5. Write `kb/diseases/disease_list.yaml`

### 5.2 Schema alignment

The schema is modeled after the existing `matrix-disease-list.tsv` product:
- `category_class` (Mondo CURIE), `label`, `definition`, `synonyms`, `subsets`, `crossreferences`
- 26 filter flags (`f_matrix_manually_excluded`, `f_clingen`, `f_orphanet_subtype`, `f_leaf`, etc.)
- Reconcile with whatever the HuggingFace dataset provides; the existing product structure is authoritative

### 5.3 justfile target

```just
build-disease-list:
  uv run python -m medic.ingest.disease_list
  just validate-schema kb/diseases/disease_list.yaml
  just validate-terms kb/diseases/disease_list.yaml
```

---

## Phase 6: On-Label List Rebuild

### 6.1 DailyMed ETL

`src/medic/ingest/dailymed/`:

1. Mine indications and contraindications from FDA SPL XML labels (existing `mine_fda_indications.py` logic)
2. LLM extraction of structured disease lists from label text (existing `extract_named_diseases` logic)
3. LLM extraction of hyperrelations (symptom-level specificity) from label text
4. Ground diseases via `GroundingService`
5. Ground drugs via `GroundingService`
6. Post-grounding LLM QC (optional, `--llm-qc` flag)
7. Construct `OnLabelSource` YAML entries with full evidence (raw text, interpreted text, hyperrelations)
8. Write to `kb/on_label/dailymed/`

### 6.2 EMA ETL

`src/medic/ingest/ema/`:

- Same pipeline pattern as DailyMed but adapted for EPAR table format
- Standardize EMA rows -> extract diseases -> ground -> QC -> write YAML

### 6.3 PMDA ETL

`src/medic/ingest/pmda/`:

- Same pattern, adapted for PMDA format
- Translation step (Japanese -> English) preserved from existing code

### 6.4 On-label merge

`src/medic/merge/on_label_merge.py`:

1. Read all `kb/on_label/<source>/*.yaml`
2. Deduplicate by (drug_id, disease_id, relationship_type) tuple
3. Merge evidence across sources (union)
4. Track which sources support each association
5. Mondo downfilling: propagate indications down Mondo hierarchy (existing `downfill_list_mondo` logic)
6. Write `products/on_label_list.yaml` and `products/contraindication_list.yaml`

### 6.5 justfile targets

```just
ingest-dailymed grounding="nameres":
  uv run python -m medic.ingest.dailymed --grounding-backend {{grounding}}

build-on-label-list grounding="nameres":
  just ingest-dailymed {{grounding}}
  just ingest-ema {{grounding}}
  just ingest-pmda {{grounding}}
  uv run python -m medic.merge.on_label_merge
```

---

## Phase 7: Adverse Events

### 7.1 PVLens ETL

`src/medic/ingest/pvlens/`:

1. Obtain PVLens output (clone repo, run their pipeline, or download pre-built data)
2. Parse PVLens adverse event extractions (MedDRA-mapped)
3. Map MedDRA terms to HPO where possible (using UMLS mappings or OAK)
4. Ground drugs via `GroundingService`
5. Construct `AdverseEventSource` YAML entries
6. Write to `kb/adverse_events/pvlens/`

### 7.2 FAERS ETL

`src/medic/ingest/faers/`:

1. Download FAERS quarterly data files
2. Parse and aggregate adverse event reports per drug-event pair
3. Compute basic disproportionality metrics (PRR) for signal strength
4. Ground drugs and adverse events
5. Write to `kb/adverse_events/faers/`

### 7.3 Adverse event merge

`src/medic/merge/adverse_event_merge.py`:

- Merge across sources, deduplicate, track provenance
- Write `products/adverse_event_list.yaml`

---

## Phase 8: Research Pipeline

### 8.1 Core pipeline

`src/medic/research/`:

```text
Disease queue (from priority list)
       │
       ▼
  PubMed search (drug + disease terms)
       │
       ▼
  LLM extraction (drug-disease pairs with evidence)
       │
       ▼
  Snippet validation (against PubMed abstracts)
       │
       ▼
  ResearchSource YAML (cached per disease)
```

### 8.2 Caching and incremental progress

- Cache directory: `cache/research/`
- Per-disease cache files: `cache/research/<mondo_id>.json`
- Tracks: PubMed search results, LLM extractions, validation results
- Resume from any point: if cache exists for a disease, skip completed steps
- PubMed abstract cache: `references_cache/` (shared with reference validator)

### 8.3 Interactive curation skill

Claude Code skill: `.claude/skills/medic-research-curation/SKILL.md`

Workflow:

1. Load next uncurated disease from the priority queue
2. Show disease context (name, categories, phenotypes)
3. Run PubMed search, present candidate drug-disease pairs
4. For each pair: LLM drafts evidence, user reviews/edits
5. Write validated `ResearchSource` YAML
6. Prompt user: "Move to next disease? (Y/n)"
7. Update progress tracker

### 8.4 Optional deep research

- For high-priority pairs, invoke deep research client (asta provider)
- Produces richer evidence with more references
- Controlled by `--deep-research` flag

### 8.5 justfile targets

```just
research-curate disease="next":
  uv run python -m medic.research.curate --disease {{disease}}

research-batch count="10":
  uv run python -m medic.research.batch --count {{count}}

build-research:
  uv run python -m medic.research.compile
```

---

## Phase 9: New Sources (future)

Stubs created in Phase 0, implemented when ready:

### 9.1 Japanese Reimbursements (#6)

- Find and ingest the reimbursement list (beyond PMDA new approvals)
- Same ETL pattern: parse -> ground -> QC -> YAML

### 9.2 Chinese CDE (#7)

- Ingest from `https://www.cde.org.cn/hymlj/listpage/...`
- Translation step (Chinese -> English)
- Same ETL pattern

### 9.3 Clinical Guidelines (#8)

- Start with NCCN guidelines
- Extract drug-disease recommendations
- Model as `source_type: GUIDELINE` in evidence

---

## Phase 10: Merge, Export & Release

### 10.1 Final merge

`src/medic/merge/`:

- `drug_merge.py` -- all drug sources -> `products/drug_list.yaml`
- `on_label_merge.py` -- all on-label sources -> `products/on_label_list.yaml` + `products/contraindication_list.yaml`
- `adverse_event_merge.py` -- all AE sources -> `products/adverse_event_list.yaml`
- Research pipeline output incorporated into on-label merge as additional evidence

### 10.2 Legacy CSV export

`src/medic/export/legacy.py`:

Generate files matching [v1.0.0 release format](https://github.com/marcello-deluca/medic/releases/tag/v1.0.0):

- `drug_list_flexible.csv`
- `drug_list_stringent.csv`
- `orangebook.xlsx`
- `purplebook.xlsx`
- `ema.xlsx`
- `pmda.xlsx`
- `russia.csv`
- `india.csv`

### 10.3 KGX export

`src/medic/export/kgx.py`:

- Read merged products
- Emit biolink-compliant nodes and edges
- Edge properties: `biolink:treats`, `knowledge_level`, `agent_type`, `max_research_phase`, `clinical_approval_status`, `aggregator_knowledge_source`, `primary_knowledge_source`
- Per Sierra Moxon's guidance: `agent_type=manual_agent`, add `max_research_phase`, `clinical_approval_status`

### 10.4 SSSOM export

- `exports/medic_drug_mappings.sssom.tsv` -- drug identifier mappings

### 10.5 justfile targets

```just
build-all grounding="nameres":
  just build-drug-list {{grounding}}
  just build-disease-list
  just build-on-label-list {{grounding}}
  just build-adverse-event-list {{grounding}}
  just build-research
  just export-legacy
  just export-kgx
  just export-sssom
  just validate-all

release:
  just build-all
  # package exports/ for GitHub release
```

### 10.6 Release validation (eval criteria)

Before a release is accepted, the legacy export artifacts must be compared against the [v1.0.0 release](https://github.com/marcello-deluca/medic/releases/tag/v1.0.0):

- **Structurally:** all release artifacts (drug_list_flexible.csv, drug_list_stringent.csv, orangebook.xlsx, purplebook.xlsx, ema.xlsx, pmda.xlsx, russia.csv, india.csv) must be derivable from the new pipeline and be ~99% structurally identical (same columns, same format, same file types)
- **Content:** at least ~50% of rows should be identical to the v1.0.0 release. The remaining differences must be justifiable (improved grounding, corrected IDs, additional drugs from new sources, removed duplicates, etc.)
- A diff report should be generated as part of the release process documenting structural and content differences with explanations

---

## Phase 11: Documentation

### 11.1 MkDocs site

```text
docs/
├── index.md              # What is MeDIC, how to use it
├── architecture.md       # System design, data flow
├── schema/               # Auto-generated from LinkML
├── sources/
│   ├── orangebook.md
│   ├── ema.md
│   ├── dailymed.md
│   ├── pvlens.md
│   ├── faers.md
│   └── ...
├── products/
│   ├── drug-list.md
│   ├── on-label-list.md
│   ├── adverse-events.md
│   └── mappings.md
├── grounding.md          # Entity grounding backends
├── validation.md         # Validation stack
├── research-pipeline.md  # How the research curation works
└── contributing.md       # How to add new sources
```

---

## Implementation Order and Dependencies

```text
Phase 0 (scaffolding)
  │
  ├──► Phase 1 (schemas) ──► Phase 3 (validation)
  │         │
  │         ├──► Phase 2 (grounding) ──┐
  │         │                          │
  │         ├──► Phase 4 (drugs) ◄─────┤
  │         │                          │
  │         ├──► Phase 5 (diseases)    │
  │         │                          │
  │         ├──► Phase 6 (on-label) ◄──┤
  │         │                          │
  │         ├──► Phase 7 (adverse) ◄───┘
  │         │
  │         └──► Phase 8 (research)
  │
  ├──► Phase 9 (new sources) -- can start anytime after Phase 2
  │
  ├──► Phase 10 (merge/export) -- after Phases 4-8
  │
  └──► Phase 11 (docs) -- continuous, alongside all phases
```

**Critical path:** Phase 0 -> Phase 1 -> Phase 2 -> Phase 4 (drug list is the first concrete deliverable)

**Parallelizable:** Phases 4, 5, 6, 7, 8 can proceed in parallel once schemas (Phase 1) and grounding (Phase 2) are in place.
