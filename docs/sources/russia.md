# Russia Drug Registry (GRLS)

## Overview

The Russian State Register of Medicines (GRLS, <https://grls.rosminzdrav.ru>) is the national drug registry maintained by the Russian Ministry of Health. The live registry is geographically restricted (no anonymous search results outside Russia), so MeDIC ingests a **manually-provided GRLS bulk export**.

## Source data

- **Format**: a zip of 8 register `.xlsx` files with Cyrillic member names (one per registration state).
- **Raw file (canonical, date-free)**: `background/grls.zip` — the user overwrites this on each rebuild. `background/` is gitignored (manual-acquisition sources are not committed).
- **Status**: Primary source (not derived). GRLS is IP-blocked for anonymous non-Russian sessions, so there is no live fetch — the file is placed manually. If it is missing, the ingester raises a clear, actionable error (per SPEC, manual-acquisition sources must fail loudly). This replaces the legacy `data/raw/russia/russia_norm.csv` v1.0.0 intermediate.
- See `src/medic/ingest/russia/README.md` for the full register table and column map.

## ETL module

`src/medic/ingest/russia/__main__.py` (+ `locate_source.py`, `parse_grls.py`)

1. Locate `background/grls.zip` (error out if missing).
2. Parse the currently-valid registers (all except Excluded/Expired); read the INN (МНН, col 9), trade name (col 8), registration date (col 3), and registration number (col 2). Member names are decoded cp437→cp866 and iterated by index to survive terminal garbling.
3. De-duplicate by drug name (INN, or trade-name fallback for herbals/complex products with no INN), keeping the earliest registration date and collecting registration certificate numbers.
4. Ground each drug name through the shared pipeline. The LLM preprocessor translates the Cyrillic INN to the English INN before lexical grounding.
5. Write `kb/drugs/russia/russia.yaml` + `grounding_report.yaml`.

The module also documents (see its docstring) a multi-step investigation into per-product GRLS deep links of the form `Grls_View_v2.aspx?routingGuid=<guid>`. Conclusion: the search-result HTML is only populated for authenticated Russian-IP sessions, so until an alternative dump (rlsnet.ru, GRLS bulk export, etc.) is ingested with `routing_guid` / Cyrillic MNN / registration number preserved, `on_label_merge` continues to emit the generic `https://grls.rosminzdrav.ru/Default.aspx` link.

## Output schema

`kb/drugs/russia/russia.yaml` — DrugSource records with `source: RUSSIA`, `source_name` (Cyrillic INN, translated to English INN at grounding time), `original_name_ru` (Cyrillic original), `approval_date`, `application_number`/`application_numbers` (GRLS registration certificate numbers), optional `trade_name`, and full grounding-cascade fields. No indications or contraindications are emitted (the GRLS export does not carry indication text).

## Source isolation

Russia only. Russia ingest sets `approved_russia` via the merged drug list; it does not contribute to indications/contraindications/adverse events.

## Justfile target

```bash
just ingest-russia
```

## Licence

**No open licence.** GRLS is a state register published without licensing terms permitting
redistribution, and the bulk export is obtained out-of-band from a Russian-IP session.

MeDIC therefore **does not redistribute the source archive**. It is not in this repository; it is
hosted out-of-band and downloaded on demand via `MEDIC_MANUAL_SOURCES_URL` — see
[`sources/README.md`](https://github.com/monarch-initiative/medic/blob/main/sources/README.md). Only derived, normalised records enter MeDIC's
products. See [`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).
