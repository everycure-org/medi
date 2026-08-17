# PMDA (Japan)

## Overview

The Pharmaceuticals and Medical Devices Agency (PMDA) regulates drugs in Japan. MeDIC reads the consolidated English new-drug approvals PDF for drug names and indication text, then enriches per-product evidence with deep links to PMDA review reports.

## Source data

- **PMDA approvals page**: <https://www.pmda.go.jp/english/review-services/reviews/approved-information/drugs/0002.html>
- **Format**: PDF (consolidated English approvals)
- **Update frequency**: Periodic
- **Raw file**: `data/raw/pmda/primary/pmda_approvals.pdf` — the consolidated English approvals PDF, fetched by `medic.ingest.pmda.fetch_primary`.
- **Status**: Raw. The approvals PDF is the single acquisition path (parsed by `parse_pmda_pdf`), and CURIEs are grounded through the v2 cascade. If the PDF cannot be fetched or parsed, ingest fails loudly — there is no legacy `pmda_norm.xlsx` fallback.

## ETL module

`src/medic/ingest/pmda/__main__.py`

1. Fetch and parse the consolidated approvals PDF (`fetch_primary_pdf` / `parse_pmda_pdf`); de-duplicate by ingredient. A missing or unparseable PDF is a hard error.
2. Ground each drug name and write `kb/drugs/pmda/pmda.yaml` + `grounding_report.yaml`.
3. **Indication extraction** (unless `--skip-indications`): for each grounded drug with indication text, call `extract_diseases_from_text`, re-ground each disease, and look up a per-product PMDA review-report URL via `medic.ingest.pmda.review_lookup`. When the lookup succeeds, the per-product PDF is emitted as both `reference` and `source_document_url`; otherwise a brand-search-URL fallback is used. Confidence is `HIGH` when a per-product `product_id` is found, otherwise `MEDIUM`. Output: `kb/indications/pmda/indications.yaml`.
4. **Contraindication extraction** (opt-in, `--extract-contras`): only for drugs whose review URL is a per-product PDF. Downloads the PDF via `fetch_review_report`, extracts the contraindications section with `parse_pdf.extract_contraindications_from_pdf`, runs LLM extraction, re-grounds, and writes `kb/indications/pmda/contraindications.yaml`.

## A note on `yj_code`

PMDA's 12-digit YJ code is the natural per-product identifier (analogous to a DailyMed `set_id`). The English approvals PDF does not carry it, so today `original_drug_id` is left blank on PMDA evidence rows. The slot is plumbed end-to-end; once an upstream source supplies the YJ code, evidence rows will pick it up automatically. See the docstring at the top of the ingest module for the full investigation.

## Source isolation

Japan only. PMDA records set `pmda = true` on merged indications and emit `jurisdiction: JAPAN`. Contraindication output is only present when the opt-in `--extract-contras` step has been run against the per-product review-report PDFs.

## Justfile targets

```bash
just ingest-pmda             # drugs + indications
just ingest-pmda-contras     # drugs + per-product review-report contraindications (skips indications)
```

## Licence

**Attribution is mandatory, and so is disclosing that the data was edited.** PMDA content is
governed by the Japan [Public Data License 1.0](https://www.pmda.go.jp/english/0013.html), which
requires that the user cite the source, and that modified content carry a statement saying it has
been edited and not be presented as originating from the public body.

MeDIC parses, translates, normalises, and maps PMDA records, so the edit disclosure applies to
every derived product. Use the notice in [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
