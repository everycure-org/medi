# How a record is built

The [data model](data-model.md) explains what a finished record looks like. This page follows one
through the pipeline, layer by layer, using a real drug: **`乳酸环丙沙星氯化钠注射液`**, as
China's Center for Drug Evaluation published it, which ends up as `CHEBI:100241` — ciprofloxacin.

There are four layers, and the separation between them is the design:

```
sources/          what a regulator published        (out-of-band, fingerprinted)
   ↓ ingest
kb/               one record per source, per source (git-tracked)
   ↓ ground        ⇄  mappings/   every decision, including failures (git-tracked, editable)
products/         merged, canonical                 (regenerable build output)
```

## Layer 0 — the source

China's CDE publishes a paginated approvals table with two useful columns: a Chinese drug name and
an approval date. There is no bulk export and no API, so the scrape is produced out-of-band and
its fingerprint recorded in `data/source_manifest.json`:

```json
"china": {"file": "cder_drugs_final_all.csv", "sha256": "980262a8a7adc7ec",
          "row_count": 1521, "modified": "2026-07-25"}
```

That fingerprint is how a build says *which* snapshot produced it. If the file changes and the
manifest does not, `just check-manual-sources` says so.

## Layer 1 — ingest writes `kb/`

`medic.ingest.china` reads the row and writes one record per drug to
`kb/drugs/china/china.yaml`. Ingest does three things and no more: it reads the value verbatim, it
mints an id, and it asks the translation and grounding stages for a decision.

```yaml
source: CHINA
original_literal: 乳酸环丙沙星氯化钠注射液          # verbatim (I-7)
mention_id: MEDICNE:31cb393a-52af-50a1-927a-10330ed004c1
approval_date: '19970116'
source_name: Ciprofloxacin Lactate and Sodium Chloride Injection
normalized_id: CHEBI:100241
normalized_label: ciprofloxacin
grounding_status: accepted
translation:
  source_value: 乳酸环丙沙星氯化钠注射液
  translation_value: Ciprofloxacin Lactate and Sodium Chloride Injection
  translator: wikidata:Q116709136
  translation_status: CANDIDATE
```

The `mention_id` is a `uuid5` of the normalized surface form. It is deterministic and offline, so
the same string always mints the same id — which is what lets the next layer find this drug's
decisions without a database.

**Each source stays in its own file.** `kb/drugs/china/` never contains a row about the FDA. That
is invariant **I-1**, source isolation: an ingester may only emit evidence for the jurisdiction it
itself originates. Cross-jurisdiction work happens exactly once, at merge, where it is visible.

## Layer 2 — `mappings/` records every decision

The interesting part is what ingest *did not* do: it did not decide anything privately. Both
decisions were written to git-tracked stores, keyed by that mention id.

**The translation**, in Babelon format:

```
subject_id            MEDICNE:31cb393a-…
source_value          乳酸环丙沙星氯化钠注射液
translation_value     Ciprofloxacin Lactate and Sodium Chloride Injection
translator            wikidata:Q116709136        (DeepL)
translator_expertise  ALGORITHM
translation_status    CANDIDATE                  (machine, unreviewed)
```

**The grounding**, in SSSOM:

```
subject_id            MEDICNE:31cb393a-…
subject_label         Ciprofloxacin Lactate and Sodium Chloride Injection
predicate_id          skos:closeMatch
object_id             CHEBI:100241   (ciprofloxacin)
subject_preprocessing salt_ester_strip|combination_split
match_string          ciprofloxacin
confidence            0.9000
mapping_justification semapv:LexicalMatching
```

Three things worth noticing.

**The grounder saw English, but the row is keyed to the Chinese original.** `subject_label` is the
translation; `subject_id` belongs to `乳酸环丙沙星氯化钠注射液`. That is what makes the whole trail
join up, and it is why a store row's id often differs from the mint of its own label.

**`subject_preprocessing` says how the match was reached.** `salt_ester_strip` removed the lactate,
`combination_split` separated the sodium chloride. Both are `PreprocessingRuleEnum` values, not
free text, so "which drugs matched only after surgery?" is a query. The predicate is
`skos:closeMatch` rather than `exactMatch` because the source string names a formulation and the
target names an ingredient.

**Failures are recorded too**, as `sssom:NoTermFound` rows. A drug MeDIC could not ground is a
recorded decision, not a silent absence — which is what makes the unresolved tail curatable.

These files are the authoritative layer. **A rerun never re-decides what is already recorded**: it
reads the store, which is why builds are offline and byte-identical, and why correcting a grounding
by hand is a one-line edit that survives the next build.

## Layer 3 — merge builds `products/`

`medic.merge.drug_merge` assembles the identity mention by replaying those recorded decisions into
an ordered chain, and folds in every authority that approved the drug:

```yaml
identity:
  original_literal: 乳酸环丙沙星氯化钠注射液
  resolved_id: CHEBI:100241
  resolution:
    confidence: 0.855                       # 1.0 × 0.95 × 0.9 × 1.0
    pipeline: [EXTRACTION, TRANSLATION, GROUNDING, NORMALIZATION]
approvals:
  - {source: CDE_CHINA,  authority: NMPA_CHINA}
  - {source: CDSCO,      authority: CDSCO}
  - {source: ORANGEBOOK, authority: FDA, regulatory_document_url: "https://accessdata.fda.gov/…"}
```

The merge *assembles* provenance; it does not *decide* anything. Every step in that chain
corresponds to a row in `mappings/`. That is the separation the whole design rests on: decisions
are made once, recorded, and replayed — never re-derived at merge time from whatever happens to be
in memory.

Indications merge the same way, but the unit is different. Each source record becomes one
`SourceAssertion` on a canonical pair, so `lecanemab → mild cognitive impairment` ends up holding
three attestations — DailyMed, EMA and PMDA — rather than one row with a jurisdiction flag.

## Why the layers are separate

Each boundary is there to make a specific failure impossible.

| Boundary | What it prevents |
|---|---|
| source → `kb/` | A source silently changing under a build. The manifest pins which snapshot was used. |
| `kb/` per source | One source's data leaking into another's jurisdiction claim (I-1). |
| decisions → `mappings/` | Grounding logic being unreviewable. A human can correct a row; the next build honours it. |
| `kb/` → `products/` | The merge inventing provenance. It may only replay what was recorded. |

Products are gitignored build outputs. Everything needed to regenerate them — the source
fingerprints, the per-source records, and every decision — is committed. That is the practical
meaning of reproducibility here: delete `products/`, run the build, get the same bytes.

## Checking it yourself

```bash
just build-on-label-list   # ingest, ground, merge
just qc                    # reconcile every source row against the output
just determinism           # build twice, compare hashes
```

`just qc` writes `reports/build_qc.yaml`, which reconciles per-source row counts, sweeps every
enum-valued field, checks the invariants across every pair, and fails on unexplained drift from
`conf/qc_baseline.yaml`.

## Where to go next

- [The shape of a MeDIC record](data-model.md) — the objects in detail
- [`docs/source-isolation.md`](../source-isolation.md) — the jurisdiction invariant
- [`docs/provenance-walkthrough.md`](../provenance-walkthrough.md) — two more end-to-end traces
