# FDA Purple Book

## Overview

The FDA Purple Book lists licensed biological products (biosimilars, interchangeables, monoclonal antibodies, vaccines, blood products, gene therapies) approved through Biologics License Applications (BLAs).

## Source data

- **Search portal**: <https://purplebooksearch.fda.gov/>
- **Format**: CSV (raw FDA download)
- **Update frequency**: Monthly
- **Raw file**: downloaded to `cache/downloads/purplebook/purplebook.csv` from the URL configured in `conf/source_urls.yaml`
- **Status**: Raw. The raw CSV is the single acquisition path and re-grounds via the cascade. If the URL is unset or the download fails, ingest fails loudly — there is no legacy fallback.

## ETL module

`src/medic/ingest/purplebook/__main__.py`

- **`parse_purplebook_raw`** — for the raw FDA CSV. Auto-detects the header row (the file ships with preamble rows before "Proper Name"), groups by `Proper Name`, captures `Approval Date`, `Marketing Status`, and pipe-joined `BLA Number`s, then runs the grounding cascade.

Output is written to `kb/drugs/purplebook/purplebook.yaml` plus `grounding_report.yaml`.

## Output schema

Same shape as Orange Book, with `bla_number` (pipe-joined when multiple BLAs are listed) replacing `application_number`. Downstream `on_label_merge` uses `bla_number` to build deep links to <https://purplebooksearch.fda.gov/?query={bla}>.

## Source isolation

USA only. Contributes `marketing_status_usa`, approval date, and `bla_number` to the merged drug list; no indications or contraindications.

## Justfile target

```bash
just ingest-purplebook
```

## Licence

A work of the U.S. Food and Drug Administration, in the public domain under 17 U.S.C. §105.
Attribution is a courtesy, not an obligation. MeDIC redistributes derived records freely.
See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
