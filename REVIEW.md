# MeDIC — Human Curation SOP

**Purpose.** MeDIC is built by a deterministic, offline pipeline, but a machine cannot decide
everything correctly. This document is the standing operating procedure for **every point in the
pipeline that needs human eyes** — what to review, where the review artifact lives, how to record a
decision so it *survives regeneration*, and how to prioritise. If you only have an hour, work the
**Priority triage** table at the bottom.

Companion docs: `FAILURE_MODES.md` (what can go wrong and why), `SPEC.md` (invariants + contract),
`docs/source-isolation.md` (I-1).

## Guiding principles

1. **The source string is sacred (I-7).** Never "fix" a record by rewriting the verbatim source
   (`original_string`, `original_literal`, `source_value`, `original_*_label`). Correct the *decision*
   (mapping / translation), never the evidence of what the source said.
2. **Every decision is a diffable row (I-4).** Grounding, normalization and translation decisions —
   including failures — live as rows in `mappings/`. Curation = editing those rows in a PR, reviewable
   like code.
3. **Manual decisions win and lock.** In the SSSOM stores, a row tagged
   `mapping_justification: semapv:ManualMappingCuration` is preserved on regeneration and overrides the
   auto (`semapv:LexicalMatching`) rows for that subject. In the Babelon table, an edited
   `translation_value` is never re-translated. **Your edits stick** — that is the whole point.
4. **A low machine confidence means "look", not "wrong".** Most flags are correct-but-uncertain; the
   job is to confirm or correct, not to assume error.
5. **When in doubt, leave the trail.** Add a `comment` on the row explaining the call, so the next
   curator (or you, in six months) understands it.

---

## 0. The three curation moves: correct, confirm, reject

A reviewer never edits ETL code and never edits the generated products (`products/*`, `kb/*`) — both are
overwritten on rebuild, and code changes don't scale to per-record fixes. Instead a record is a
*composition of per-stage decisions*, and you act on the decision, in that stage's hand-editable store:

- **Correct** — the record is *fixably wrong*. Edit the decision **at the stage that introduced the
  error**, in that stage's store; the record recomposes correctly on the next rebuild (and its
  reliability tier rises through the gates automatically):

  | What's wrong | Correct it in | How |
  |---|---|---|
  | Drug/disease grounded to the wrong id, or unresolved | `mappings/{drug,disease}_grounding.sssom.tsv` | add/edit the row → correct `object_id`, `mapping_justification: semapv:ManualMappingCuration` |
  | Canonical-id normalization wrong | `mappings/{drug,disease}_normalization.sssom.tsv` | add a manual term↔term row |
  | Machine translation wrong (zh/ru → wrong English) | `mappings/drug_translation.babelon.tsv` | edit `translation_value`, set `translation_status: OFFICIAL` |
  | **Wrong / missing disease extracted from free text** | *(no store yet — the gap)* | see `issues/issue_extraction_correction_store.md` |

- **Confirm** — the composed statement is right. Add a `CONFIRMED` row to
  `mappings/statement_review.tsv` (keyed by `statement_key`) → forces reliability `HIGH`, overriding the
  automated gates. Use this when a record is correct but the automated score under-rates it (e.g. a
  legitimate synonym the entailment check can't see).

- **Reject** — the composed statement is wrong and not worth correcting now. Add a `REJECTED` row to
  `mappings/statement_review.tsv` → forces `EXCLUDED`. (Prefer *correct* when the fix is a one-line store
  edit; reject is for genuinely bad or out-of-scope statements.)

**ETL code changes are reserved for *systematic* fixes** — a new preprocessing rule, a parser bug that
mis-reads many rows, a new source — never a single record. If you find yourself wanting to edit code to
fix one drug, that belongs in a store instead.

**One gap to know about:** the extraction stage (LLM pulling diseases out of indication/contraindication
free text) has **no correction store yet** — you can *reject* a wrong extraction, but you cannot yet
*add* a disease the LLM missed or *re-point* a wrong one as hand-edited data. Until that store lands
(`issues/issue_extraction_correction_store.md`), extraction fixes are the one thing that still requires a
code/prompt change or a `REJECT` + manual note.

---

## 1. Grounding decisions — `mappings/{drug,disease}_grounding.sssom.tsv`

Every source string → ontology id decision, including failures. This is the single biggest curation
surface. Review in this priority order:

- **1a. Unresolved, high-value** (`predicate_id: sssom:NoTermFound`). A real drug/disease the grounder
  could not map. Sort by how often the string appears / how important the drug is. **To fix:** add (or
  edit) the row, set `object_id` to the correct CURIE, set `predicate_id: skos:exactMatch`,
  `mapping_justification: semapv:ManualMappingCuration`. It will now ground on the next run and never be
  overwritten.
- **1b. Fuzzy matches** (`subject_preprocessing` contains `fuzzy_edit1_unique`, `predicate_id:
  skos:closeMatch`, `confidence 0.60`). Edit-distance-1 guesses — deterministic but approximate.
  **Known trap:** isotope digit-swaps (`13C`→`14C`) — see `issues/issue_fuzzy_isotope.md`. Confirm →
  promote to `semapv:ManualMappingCuration`; reject → set `sssom:NoTermFound` or the right id.
- **1c. Other `closeMatch`/`broadMatch` proposals** — INN spelling (`inn_*`), salt strip
  (`salt_ester_strip`), formulation strip (`formulation_strip`), Cyrillic transliteration
  (`cyrillic_transliteration`), translation (`translation_*`, `deepl_translation`), qualifier strip
  (`qualifier_strip`, a *broadening* — the grounded term is more general than the source). Spot-check;
  broadenings especially, because they weaken the claim.
- **1d. RxNorm proposals** (`mapping_justification: RXNORM`). Network-derived product→ingredient
  resolutions, US-centric, already *locked*. Confirm the ingredient is right, then either leave locked or
  promote to `ManualMappingCuration`.
- **1e. Ambiguity.** The grounder refuses multi-candidate hits (records them unresolved). If a string
  legitimately has one intended target, add the manual row.

**Cadence:** after every full rebuild, at least skim the *new* `NoTermFound` rows (diff against the last
committed store). Deep passes as time allows.

## 2. Normalization decisions — `mappings/{drug,disease}_normalization.sssom.tsv`

Initial id → canonical MONDO/CHEBI. Auto rows use only mappings the target namespace itself asserts.
**Review:** IDs that MONDO/CHEBI do *not* cross-reference (the residue) — a grounded UMLS/HP/DRON id with
no path to MONDO/CHEBI. Add a `ManualMappingCuration` term↔term row if a correct canonical mapping
exists. This residue is also the limiter for cross-source overlap (FDA↔EMA↔PMDA), so it is high-value.

## 3. Translations — `mappings/drug_translation.babelon.tsv`

Every non-English (China `zh`, Russia `ru`) source name → English, via DeepL. **All 7,150 current rows
are `translation_status: CANDIDATE`** (machine, unreviewed). Review priority:

- **3a. Failed-to-ground after translation.** Cross-reference with the grounding store: a translated
  name that is still `NoTermFound` is the highest-value fix (bad translation *and* no ground).
- **3b. Trade-name-sourced.** Russia falls back to the trade name when the INN is a placeholder; DeepL
  may render a brand to the wrong molecule (FAILURE_MODES §2.3, §7.4). Verify against the known INN.
- **3c. Spot-audit the rest.** Compare `translation_value` to the expected INN.

**To record:** edit `translation_value` if wrong (it sticks — deterministic cache); set
`translation_status: OFFICIAL` once a human has confirmed a row. Add a `comment` for non-obvious calls.
Do **not** touch `source_value` (the verbatim foreign string).

## 4. Extraction fidelity — `just validate-extraction`

The anti-hallucination / anti-inversion review. Run `just validate-extraction` (or
`uv run python -m medic.validation.extraction_fidelity`); it writes `extraction_flagged.tsv` — every
extracted disease whose label is **not lexically stated in its source snippet**, worst first.

- **Score 0** — no content-word overlap. Likeliest hallucination, mis-sectioned extraction
  (contraindication pulled into indications), or a synonym the label spells differently. **Open the
  `reference` URL / read the snippet** and decide: correct the disease, drop the association, or accept
  (genuine synonym).
- **0 < score < 0.5** — usually LLM canonicalization; quick glance.

Current baseline: ~123 flagged of ~12,274 (≈1%). **Cadence:** every rebuild; treat any *new* score-0
row as blocking until triaged. (Prevention half — flag/drop at ingest — is
`issues/issue_snippet_entailment_regulatory.md`.)

## 5. Assertion type & negation

Two layers now guard indication↔contraindication inversion (FAILURE_MODES §4.1–4.2):

- **Ingest drop (prevention).** The shared extractor drops any indication whose disease is stated only
  negatively in the source — but *strictly*: a **full-phrase** match, every occurrence negated, and
  **every drop is logged** (`grep "Dropping negated"` in the ingest log). It never invents a new claim.
- **Review flag (detection).** `just validate-extraction` reports a **Polarity** section — the same
  check but *sensitive* (it also anchors on the head word), so it catches more, including things the
  strict drop deliberately keeps.

**Review every polarity flag, but do NOT re-file it as a contraindication.** A negated indication is
*not reliably* a contraindication — on the current corpus only ~1/3 (7 of 21) carry a `contraindicated`
cue; the rest are *limitations / excluded subtypes* ("except in active TB") or plain *absence of
indication* ("metformin should not be used in type 1 diabetes" — not an approval, but **also not a
contraindication**; type 1 simply needs insulin). Re-routing those to contraindications would fabricate
a safety claim the regulator never made — the same inversion, reversed. The correct action is: **drop
the false indication, or correct the disease** — and if a genuine contraindication is missing, recover
it from the *contraindications section we already parse*, not by inferring it from an indication-section
negation. Head-word-only flags (flagged but not dropped, e.g. `raloxifene → vertebral fractures`
inheriting `hip fractures`' negation) especially need a human read before any action.

## 6. Manual source provisioning & versioning

Two sources are human-supplied because they are IP-blocked / scrape-only:

- **Russia** — `background/grls.zip` (see `src/medic/ingest/russia/README`). 
- **China** — `background/cder_drugs_final_all.csv`.

**SOP:** two automatic controls now guard this (`medic.ingest.sanity`). On ingest, each source is
stamped into `data/source_manifest.json` with its file fingerprint (sha256 + size + mtime date) and row
count — **review that diff** when refreshing a source, and note the upstream "as-of" date the manifest
can't infer. A **row-count floor** (`ROW_FLOORS`) refuses a China/Russia ingest that yields fewer records
than ~2/3 of the known-good scale (truncated/partial export, FAILURE_MODES §1.3); if a drop is genuine,
lower the floor deliberately in the same PR (don't just `MEDIC_SKIP_ROW_FLOORS=1` past it). Never rebuild
a release off a source whose manifest entry you cannot date. Floors are wired for China/Russia today;
extend `ROW_FLOORS` + call `check_row_floor`/`record_source` in the other ingesters as they're touched.

## 7. Validation failures

When any validator fails, a human triages before release:

- `just validate-schema` — structural; fix the offending record/schema.
- `just validate-terms` — a CURIE doesn't exist / label mismatch; usually a grounding-store fix (§1).
- `just validate-references` — a literature snippet not found in its abstract; fix or drop the evidence.
- `just validate-extraction` — see §4.

**Note the blind spots (FAILURE_MODES §14):** the validators do **not** check regulatory URLs resolve,
that approval status is accurate, or that no downfilling occurred. Those need the manual passes here.

## 8. Research evidence & study output

- **Snippets** (`scripts/curate_snippets.py`, `kb/research/*`): LLM-extracted excerpts are verified as
  verbatim substrings of the cited abstract, but a human should confirm the excerpt actually *supports*
  the drug→disease claim, not merely mentions both.
- **`/study` output:** the deep-research pipeline leaves a large `[FAIL]` tail of hallucinated URLs.
  **Keep those out of any load-bearing claim and out of registry `evidence`** — human filter required.

## 9. Release gate (run before `just gh-release`)

Before packaging a release, a human confirms:

- [ ] All three LinkML validators pass (`just validate-all`).
- [ ] `just validate-extraction` reviewed; no untriaged score-0 rows.
- [ ] New `NoTermFound` grounding rows since last release skimmed (diff `mappings/`).
- [ ] Manually-provided sources (Russia/China) are dated and row-count-sane.
- [ ] `mappings/` diffs reviewed like code (no unexpected mass churn of manual rows).
- [ ] Spot-check of a handful of indications for assertion-type/negation correctness (§5).

---

## Priority triage (if you have limited time)

| Priority | Review surface | Why it matters | Section |
|---|---|---|---|
| **P0** | New score-0 rows in `extraction_flagged.tsv` | Clinically wrong drug→disease claims (hallucination / inversion) | §4, §5 |
| **P0** | Manual source dating before a release build | A stale/partial source silently corrupts a whole jurisdiction | §6 |
| **P1** | New `NoTermFound` for high-frequency drugs/diseases | Real approvals silently dropped from products | §1a |
| **P1** | Translations that still fail to ground | Wrong molecule identity for China/Russia | §3a, §3b |
| **P2** | Fuzzy / broadening grounding proposals | Approximate or weakened claims trusted silently | §1b, §1c |
| **P2** | Normalization residue (no MONDO/CHEBI xref) | Limits cross-source merge & canonicalization | §2 |
| **P3** | Confirm CANDIDATE translations → OFFICIAL | Builds a trusted, auditable translation base | §3c |

**How every decision is recorded:** edit the relevant `mappings/*.tsv` row in a branch, set the
justification/status to the human-curated value (`semapv:ManualMappingCuration` for SSSOM;
`translation_status: OFFICIAL` for Babelon), add a `comment`, and open a PR. The next rebuild honours it
automatically.
