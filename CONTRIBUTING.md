# Contributing to MeDIC

MeDIC (Medicines, Diseases, Indications, and Contraindications) is an open-source knowledge base for drug repurposing research.

## Core Team

- **Marcello DeLuca** ([ORCID](https://orcid.org/0000-0002-4299-3501))
- **Nicolas Matentzoglu** ([ORCID](https://orcid.org/0000-0002-7356-1779))

## Getting Started

```bash
git clone https://github.com/marcello-deluca/medic.git
cd medic
just setup
```

See `docs/architecture.md` for a full technical overview of the pipeline.

## Development Workflow

1. Create a feature branch from `main`
2. Make changes, write tests
3. Run `uv run pytest` to verify all tests pass
4. Run `uv run ruff check src/ tests/` for linting
5. Submit a pull request

## Running the Pipeline

```bash
# Full build (requires API keys, takes several hours on first run)
just build-all

# Fast iteration (skips LLM and external API calls)
MEDIC_SKIP_EXPENSIVE_CALLS=1 just build-drug-list

# Individual source ingests
just ingest-orangebook
just ingest-ema
```

## Adding a New Drug Source

1. Create `src/medic/ingest/<source>/__init__.py` and `__main__.py`
2. Follow the Orange Book implementation as a reference (`src/medic/ingest/orangebook/__main__.py`)
3. Add the source enum to `src/medic/schema/drug_source.yaml`
4. Add a justfile target in `project.justfile`
5. Add the source to `build-drug-list` in `project.justfile`

## Adding a New Export Format

1. Create `src/medic/export/<format>.py`
2. Read from `products/drug_list.yaml` (or other product files)
3. Add a justfile target

## Data Sources

Raw data acquisition is documented in `scripts/obtain_raw_sources.sh`. Sources with download URLs (Orange Book, Purple Book, EMA) are fetched automatically. Sources requiring manual steps (PMDA, Russia, India) use pre-grounded data from the previous pipeline version.

## Schemas

All data schemas are defined in LinkML YAML at `src/medic/schema/`. Changes to schemas should be validated with `just validate-all`.

## Caching

Enrichment and grounding caches (`cache/grounding/*.json`, `cache/enrichment/*.json`) are deterministic JSON files suitable for git tracking. They make re-runs near-instant after the first execution.

## Reporting Issues

Please open issues at https://github.com/marcello-deluca/medic/issues
