# PVLens

## Overview

PVLens (<https://github.com/GSK-Global-Safety/pvlens>) is an open-source pharmacovigilance system from GSK Global Safety that extracts adverse events, indications, and black box warnings from FDA SPLs and maps drugs to RxNorm and adverse events to MedDRA preferred terms. MeDIC ingests PVLens's pre-computed CSV output.

## Source data

- **Repository**: <https://github.com/GSK-Global-Safety/pvlens>
- **Format**: CSV (`product_ae.csv` — PVLens output)
- **Input path**: `data/pvlens/product_ae.csv` (run PVLens externally and place the CSV here, or supply `--ae-file`)
- **Status**: Implemented; gated on the CSV being present. Without `data/pvlens/product_ae.csv` the ingest logs a warning and exits cleanly.

## ETL module

`src/medic/ingest/pvlens/__main__.py`

1. Read the CSV (columns: `product_name`, `meddra_term`, `meddra_code`, `blackbox`, `warning`, `spl_setid`).
2. For each row, ground the drug name through the configured backend (default `nameres`), caching grounding calls per drug.
3. Tag the label section by precedence: `BLACK_BOX_WARNING` > `WARNINGS_AND_PRECAUTIONS` > `ADVERSE_REACTIONS`.
4. Build a `MedDRA:<code>` CURIE when `meddra_code` is present.
5. Write `kb/adverse_events/pvlens/pvlens_ae.yaml` with `source: PVLENS`, the normalized drug fields, the MedDRA term, the label section, and an evidence item carrying `source_type: REGULATORY`, `jurisdiction: USA`, `reference: DailyMed:<spl_setid>`, `confidence: HIGH`.

## Source isolation

USA only — PVLens operates on FDA SPLs.

## Justfile target

```bash
just ingest-pvlens
```

## Licence

PVLens itself is GPL-3.0. MeDIC consumes its **CSV output**, not its code — it neither links nor
distributes PVLens — so no copyleft obligation attaches to MeDIC.

The output is a different problem: it carries MedDRA preferred terms, and MedDRA is licensed by the
ICH/MSSO under a subscription that restricts redistribution of dictionary content. The
PVLens-derived product is therefore **blocked for release** until the MSSO terms are confirmed.
Nothing MedDRA-derived is currently committed. See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
