# China Drug Registry (CDE / NMPA)

## Overview

The Chinese Center for Drug Evaluation (CDE, <https://www.cde.org.cn>) of the
National Medical Products Administration (NMPA) publishes an approved-drugs
table. MeDIC ingests a **manually-provided scrape** of that table — the live
site is not fetched by this repo.

## Source data

- **Format**: a 2-column CSV — `drug_name` (Chinese drug names, with formulation
  suffixes) and `approval_date` (predominantly `YYYY/M/D`).
- **Raw file (canonical, date-free)**: `background/cder_drugs_final_all.csv` —
  the user overwrites this on each rebuild. `background/` is gitignored
  (manual-acquisition sources are not committed).
- **Status**: Primary source (not derived). The CDE table is scraped out-of-band
  (paginated Selenium scrape); there is no live fetch here, so the file is placed
  manually. If it is missing, the ingester raises a clear, actionable error (per
  SPEC, manual-acquisition sources must fail loudly).
- **No indication text**: the scrape has only a drug name and an approval date,
  so China contributes a **drug list only** — no indications or
  contraindications (same as Russia). This resolves the SPEC §9 "China CDE
  indications" open question.
- See `src/medic/ingest/china/README.md` for the format, translation cache, and
  cost notes.

## ETL module

`src/medic/ingest/china/__main__.py` (+ `locate_source.py`, `parse_cde.py`,
`translate.py`)

1. Locate `background/cder_drugs_final_all.csv` (error out if missing).
2. Parse the CSV, de-duplicate by the Chinese `drug_name` (keep the earliest
   approval date), and normalize the date to `YYYYMMDD`.
3. Translate each unique Chinese name to its English INN with an LLM
   (`grounding_preprocess` task), stripping the Chinese formulation suffix as a
   best-effort pre-pass. Results are cached per-name to
   `cache/enrichment/china_translation.json` (resumable; reruns are free).
   Skipped when `MEDIC_SKIP_EXPENSIVE_CALLS=1` (names then do not ground).
4. Ground the English INN through the shared pipeline so China drugs resolve to
   canonical ChEBI CURIEs. The verbatim Chinese name is preserved as
   `original_name_zh`.
5. Write `kb/drugs/china/china.yaml` + `grounding_report.yaml`.

## Output schema

`kb/drugs/china/china.yaml` — DrugSource records with `source: CHINA`,
`source_name` (English INN, grounded), `original_name_zh` (verbatim Chinese),
`approval_date` (`YYYYMMDD`), and full grounding-cascade fields. No indications
or contraindications are emitted.

## Source isolation

China only. China ingest sets `approved_china` via the merged drug list;
`on_label_merge` emits a `CDE_CHINA` / `NMPA_CHINA` regulatory artifact for
China-only drugs. No cross-jurisdiction flags are read or synthesised
(invariant I-1). No indications, contraindications, or adverse events.

## Run

```bash
python -m medic.ingest.china            # full run
python -m medic.ingest.china --limit 30 # validation sample
```

## Licence

**No open licence.** The CDE/NMPA approvals table is published without licensing terms permitting
redistribution, and MeDIC's copy is an out-of-band scrape.

MeDIC therefore **does not redistribute the source archive**. It is not in this repository; it is
hosted out-of-band and downloaded on demand via `MEDIC_MANUAL_SOURCES_URL` — see
[`sources/README.md`](https://github.com/monarch-initiative/medic/blob/main/sources/README.md). Only derived, normalised records enter MeDIC's
products. See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
