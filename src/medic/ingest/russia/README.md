# Russia (GRLS) ingester

Ingests the drug list from **GRLS** — the Russian State Register of Medicines
(Государственный реестр лекарственных средств,
<https://grls.rosminzdrav.ru>), maintained by the Russian Ministry of Health.

## Manual acquisition (no live fetch)

GRLS is **IP-blocked** for anonymous, non-Russian sessions: its search
endpoints return an empty form shell rather than result rows unless you
authenticate from a Russian IP. There is therefore **no automated fetch** for
Russia.

To rebuild Russia you must manually obtain the GRLS bulk export and place it at
the stable, date-free path:

```
background/grls.zip
```

`background/` is gitignored (manually-provided sources are not committed).
Overwrite this file on each rebuild — **never** use a dated filename. The
ingester reads only this canonical path and raises a clear, actionable error if
it is missing (see `locate_source.py`).

## Expected zip contents

The export is a zip of **8 register `.xlsx` files** with Cyrillic member names
(one per registration state):

| # | Register (Cyrillic)                                          | Meaning                              | Used |
|---|-------------------------------------------------------------|--------------------------------------|------|
| 0 | Действующий                                                 | Active / valid                       | yes  |
| 1 | Изменённый                                                  | Modified                             | yes  |
| 2 | Исключённый                                                 | Excluded / struck-off                | no   |
| 3 | Истёкший                                                    | Expired                              | no   |
| 4 | Выдано по правилам ЕАЭС                                     | Issued under EAEU rules              | yes  |
| 5 | Действует на подтверждении государственной регистрации      | Active, pending confirmation         | yes  |
| 6 | Приостановлено применение                                  | Suspended                            | yes  |
| 7 | Действует в иностранных упаковках                          | Active in foreign packaging          | yes  |

We include every **currently-valid** register and exclude the two former-
registration registers (Excluded, Expired). Member filenames are decoded
defensively (cp437 → cp866) and iterated by index, so a terminal that garbles
the Cyrillic names does not matter.

## Table and columns used

All registers share one 17-column layout. A title banner occupies the first
rows; the header is the row containing `Дата регистрации` and data begins two
rows below it. The columns used (0-based index → meaning):

| Index | Cyrillic header                                        | English meaning                    |
|-------|--------------------------------------------------------|------------------------------------|
| 2     | Номер регистрационного удостоверения                   | Registration certificate number    |
| 3     | Дата регистрации                                       | Registration date (`DD.MM.YYYY`)   |
| 6     | Юридическое лицо, на имя которого выдано ...            | Marketing-authorisation holder     |
| 8     | Торговое наименование лекарственного препарата         | Trade name                         |
| 9     | Международное непатентованное или химическое ...        | INN / chemical name (МНН)          |

The drug name to ground is the **INN** (col 9); when the INN is a GRLS
placeholder (`~`, for herbals / complex products) we fall back to the **trade
name** (col 8). Records are de-duplicated by name, keeping the earliest
registration date and collecting the registration certificate number(s).

**No indication column** exists in the GRLS export, so Russia contributes a
**drug list only** — no indications or contraindications.

## Grounding

Each record's `source_name` holds the Cyrillic INN. The shared grounding
pipeline's LLM preprocessor (`medic.grounding.preprocessor.preprocess_drug_name`)
translates the Cyrillic INN to the English INN before lexical grounding, so
names resolve to canonical ChEBI CURIEs. The Cyrillic original is preserved on
each record as `original_name_ru`.

> Note: translation requires an LLM API key. With `MEDIC_SKIP_EXPENSIVE_CALLS=1`
> the preprocessor is bypassed, so Cyrillic names will not ground — use that
> mode only to validate parsing, not to produce the real drug list.

## Output

`kb/drugs/russia/russia.yaml` — `DrugSource` records with `source: RUSSIA`,
`source_name`, `original_name_ru`, `approval_date`, `application_number(s)`,
optional `trade_name`, plus the grounding-cascade fields. Plus
`kb/drugs/russia/grounding_report.yaml`.

## Source isolation (invariant I-1)

Russia emits evidence **only** for the RUSSIA jurisdiction. The GRLS export
carries no cross-jurisdiction flag columns; none are synthesised.

## Run

```bash
python -m medic.ingest.russia
```
