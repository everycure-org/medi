# China (CDE / NMPA) ingester

Ingests the approved-drug list from **CDE** — the Center for Drug Evaluation
(<https://www.cde.org.cn>) of China's National Medical Products Administration
(NMPA).

## Manual acquisition (no live fetch)

The CDE approvals table is scraped **out-of-band** (a paginated Selenium scrape
of the public listing); this repo does not run the scraper. The resulting CSV
is placed manually at the stable, date-free path:

```
background/cder_drugs_final_all.csv
```

`background/` is gitignored (manually-provided sources are not committed).
Overwrite this file on each rebuild — **never** use a dated filename. The
ingester reads only this canonical path and raises a clear, actionable error if
it is missing (see `locate_source.py`).

## Source format

Exactly **2 columns**:

| Column          | Meaning                                                             |
|-----------------|---------------------------------------------------------------------|
| `drug_name`     | Chinese drug product name, with a formulation suffix (e.g. `来那度胺胶囊` = lenalidomide capsule, `盐酸二甲双胍片` = metformin hydrochloride tablet). |
| `approval_date` | Approval date, predominantly `YYYY/M/D` (`2019/10/21`), with a small tail of other shapes (`YYYYMMDD`, `YYYY-MM-DD`, `YYYY年M月D日`, `YYYY.MM.DD`). |

There is **no indication column**, so China contributes a **drug list only** —
no indications or contraindications (same as Russia). This resolves the SPEC §9
"China CDE indications" open question: the scrape has no indication text.

## Chinese → INN translation (LLM, cached)

The deterministic lexical grounder only matches Latin/English, and — unlike
Russian Cyrillic, which transliterates deterministically — Chinese has **no
deterministic transliteration** to the INN. So each **unique** Chinese
`drug_name` is translated to English by the shared **Stage-0 translation stage**
(`medic.translation`) *before* grounding:

1. Each unique name is minted a stable `MEDICNE:<uuid5>` id
   (`medic.mention.mint_mention_id`, invariant I-9).
2. `medic.translation.translate_records(records, "zh")` translates the Chinese
   name to English with **DeepL** through the `babelon` translator service.
   DeepL resolves recognised drug names directly (`来那度胺胶囊 → "Lenalidomide
   Capsules"`); residual dose/form/salt words are stripped downstream by the
   grounder's `formulation_strip` / `salt_ester_strip` rules.
3. Every translation is a row in the Babelon store
   `mappings/drug_translation.babelon.tsv` (keyed by the `MEDICNE` id). That
   git-tracked table is the deterministic cache — a filled row is never
   re-translated, so reruns cost nothing and manual fixes survive regeneration.

The English `translation_value` replaces the record's `source_name` (what the
shared grounder grounds); the verbatim Chinese is preserved both as
`original_name_zh` and in the Babelon row's `source_value` (invariant I-7). The
`translation` object (schema class `Translation`) is attached to each record and
funneled to `products/drug_list.yaml`.

> With `MEDIC_SKIP_EXPENSIVE_CALLS=1` translation is bypassed: the raw Chinese
> name is left in `source_name` and will **not** ground. Use that mode only to
> validate parsing, not to produce the real drug list.

## Grounding

The (translated) English INN is grounded through the shared pipeline
(`ground_records`, default lexical backend), so China drugs resolve to
canonical ChEBI CURIEs exactly like every other source. A drug that translates
cleanly but has no ChEBI entry (e.g. `蒙脱石散` → `diosmectite`) is a legitimate
grounding miss, not a translation failure.

## Output

`kb/drugs/china/china.yaml` — `DrugSource` records with `source: CHINA`,
`source_name` (English INN, grounded), `original_name_zh` (verbatim Chinese),
`approval_date` (`YYYYMMDD`), plus the grounding-cascade fields. Plus
`kb/drugs/china/grounding_report.yaml`.

## Source isolation (invariant I-1)

China emits evidence **only** for the CHINA jurisdiction. The scrape carries no
cross-jurisdiction flag columns; none are synthesised.

## Run

```bash
# Full run (translates every unique Chinese name — see cost note below).
python -m medic.ingest.china

# Validation sample (first N unique names only, keeps LLM volume small).
python -m medic.ingest.china --limit 30
```

### Full-run cost / volume

The scrape has ~9,679 rows → on the order of ~8k unique Chinese names, so a
cold full run makes roughly that many `grounding_preprocess` LLM calls (short
prompt, `max_tokens=120`). The per-name cache makes the run resumable and every
rerun free, so the cost is paid **once**. Run the full translation as a deliberate
step; do not fold it into fast offline rebuilds.
