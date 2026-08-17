# MeDIC — Extraction Fidelity Failure Modes

**Scope.** This document catalogues the ways MeDIC can record something *different from what a
regulatory source actually asserts* about a drug or disease (an approval, an indication, a
contraindication, a date, a jurisdiction). It is deliberately about **fidelity of extraction** —
faithfully carrying the source's claim into our records — and treats **entity grounding**
(string → ontology id) only where it interacts with fidelity. Grounding failure modes proper are
covered by the deterministic-grounding spec and the SSSOM decision stores.

The guiding question for every row below: *if the source says X, could we end up asserting Y ≠ X, and
would anything catch it?*

**Mitigation legend:** ✅ in place · 🟡 partial / by-construction only · ❌ gap (recommended control).

> **Now queryable per record.** Many of the failure modes below are first-class, controlled-enum
> `flags` in the transformation-provenance model (`src/medic/schema/provenance.yaml`), attached to a
> `Drug`/`IndicationAssociation`. They sort into two distinct layers — **recognising an entity** vs
> **asserting a claim about it** — because an entity can be recognised perfectly while the relation is wrong:
>
> | Layer | Where | Flags |
> |---|---|---|
> | Entity recognition + linking | `Mention.resolution.pipeline` steps | `ExtractionFlag` (§5.1 `hallucination`, §5.6 `truncated_snippet`, §5.5 `coreference_ambiguity`) · `TranslationFlag` (§7.3 `unreviewed_machine`, §7.4 `trade_name_source`) · `GroundingFlag` (§2.5 `script_transliteration`, §11.2 `broadened`, …) · `NormalizationFlag` (§10.2 `no_target_xref`) |
> | The claim / relation | `IndicationAssociation.assertion` | `AssertionFlag` (§4.1–4.2 `negated_inversion`, §5.2 `over_extraction`, §3.5 `wrong_section`, §5.4 `wrong_pairing`) |
>
> So §5.2 over-extraction (VITAMIN A → hyperthyroidism, a *depleting condition*) is a flag on the
> **assertion** — the disease mention itself was recognised correctly. Detector maturity varies: the
> assertion `confidence` and `negated_inversion` are auto-populated; richer flags (`over_extraction`,
> `hallucination`, `wrong_section`) are curator- or future-detector-set — the slot exists regardless. The
> record-level `reliability` tier is computed from these. See `docs/architecture.md` §9.9 and
> `docs/provenance-walkthrough.md`.

**Sources at a glance (extraction surface):**

| Source | Kind | Free-text LLM extraction? | Carries indications? |
|---|---|---|---|
| DailyMed | SPL XML + free-text sections | **Yes** (indications 34067‑9, contraindications 34070‑3) | Yes (+contra) |
| EMA | XLSX + optional EPAR PDF §4.3 | **Yes** (indication cell + contra PDF) | Yes (+contra opt-in) |
| PMDA | PDF tables (pdfplumber) | **Yes** (Notes column) | Yes (+contra opt-in) |
| India (CDSCO) | PDF tables (bordered) | **Yes** (Indication column) | Yes (no contra) |
| Orange/Purple Book | Delimited text / CSV | No | No (drug list) |
| Russia (GRLS) | XLSX (Cyrillic) + DeepL | No (translation only) | No (drug list) |
| China (CDE) | CSV (Chinese) + DeepL | No (translation only) | No (drug list) |
| EveryCure | HF parquet (pre-grounded) | No | No |
| CURE-ID | TSV (pre-mapped Biolink) | No | Treatment edges (research) |

---

## 1. Source acquisition, versioning, and provenance

Failures here mean we faithfully extract from the *wrong bytes*.

- **1.1 Stale snapshot.** A manually provided or downloaded source (GRLS `background/grls.zip`, CDE
  `background/cder_drugs_final_all.csv`, EMA XLSX, PMDA PDF) is older than the live registry; approvals
  added/withdrawn since are missing or wrong. *No source carries an ingest-time version stamp.*
  → **Mitigation:** ❌ Record a `source_version` / retrieval date + upstream "as-of" date per source in
  a manifest; surface it on every evidence item. Re-download cadence policy per source.
- **1.2 Wrong file substituted.** The stable filename (`grls.zip`, `cder_drugs_final_all.csv`) is a
  human-provided artifact; a wrong or truncated export silently ingests. → **Mitigation:** 🟡 fail-loud
  on missing file exists; ❌ add row-count sanity floors + checksum/expected-shape assertions.
- **1.3 Partial download / truncated PDF.** India/PMDA multi-PDF fetch or a truncated PDF yields fewer
  pages/rows than reality; looks like a smaller-but-valid dataset. → **Mitigation:** 🟡 India has
  partial-download gates in tests; ❌ generalise: assert expected page/row counts, alert on drops vs.
  previous run.
- **1.4 Wrong upstream URL / endpoint drift.** `conf/source_urls.yaml` points at a moved page; DailyMed
  v2 API path changes. → **Mitigation:** ✅ URLs centralised in config; ❌ periodic liveness check.

## 2. Source-schema misinterpretation (structured sources)

We read the columns, but assign the wrong meaning to them.

- **2.1 Column-index drift.** Russia uses **fixed 0-based column indices** (INN=9, trade=8, reg#=2,
  date=3). If a register's layout shifts a column, we read the holder as the drug, etc. → **Mitigation:**
  🟡 header-marker row detection anchors the *start*; ❌ validate the header labels at each fixed index,
  not just presence of `Дата регистрации`.
- **2.2 Header auto-detection picks the wrong row.** EMA/Purple Book skip preamble rows and look for a
  keyword ("Category", "Proper Name"). A localized/renamed header or a stray matching cell selects the
  wrong header row → every column offset by one. → **Mitigation:** 🟡 keyword match; ❌ assert the full
  expected header set matches before parsing.
- **2.3 Semantic column confusion.** Trade name vs INN vs chemical name (Russia falls back to trade
  name when INN is a placeholder `~`/`-`) — a trade name can translate/ground to the wrong molecule.
  → **Mitigation:** 🟡 `name_source` recorded; ✅ original preserved; ❌ flag `name_source=trade_name`
  rows as lower-confidence.
- **2.4 Unit / format assumptions.** Dates assumed `DD.MM.YYYY` (Russia/India) vs `MM/DD/YYYY`
  (US) vs `YYYY/M/D` (China). A `03/04/2024` is ambiguous; misread month/day silently produces a valid
  but wrong date. → **Mitigation:** 🟡 per-source parsers assume the source's own convention; ❌ no
  cross-check; add range/plausibility + "day>12 disambiguates" assertions and log ambiguous dates.
- **2.5 Encoding corruption.** GRLS zip member names are cp437-garbled Cyrillic (decoded cp437→cp866);
  China CJK; stray BOM/quote chars in CDE dates. A decode miss drops/ō-mangles a whole register or name.
  → **Mitigation:** ✅ defensive decode + CJK/quote handling with tests; 🟡 silent fallback returns the
  garbled name — ❌ count/alert on undecodable members.

## 3. Structural parsing errors (unstructured / PDF sources)

pdfplumber table extraction is inherently lossy.

- **3.1 Table cell misalignment.** PMDA/India rely on `pdfplumber` line- or lattice-based tables. A
  merged cell, wrapped multi-line ingredient, or missing border shifts values between columns (approval
  date lands in "No.", ingredient in "Notes"). → **Mitigation:** 🟡 header heuristics + salt/footnote
  stripping; ❌ per-row column-count assertions; sample-based manual audit; legacy-parity gate exists for
  PMDA — extend coverage.
- **3.2 Row splitting / merging.** A single indication spanning two PDF rows, or two drugs collapsed
  into one row, yields a drug with a truncated or merged indication. → **Mitigation:** ❌ detect
  multi-line continuations; validate 1 row → 1 approval.
- **3.3 Footnotes / annotations bleed into content.** `[Orphan drug]`, `(1)` footnote markers,
  company parentheticals are stripped heuristically; an unexpected marker leaks into the ingredient or
  indication text. → **Mitigation:** 🟡 known-marker stripping; ❌ residual-marker detection.
- **3.4 Delimiter ambiguity.** Orange Book is tab- or `~`-delimited with a fallback; combination
  ingredients split on `;`/`/`/`AND`. A drug name legitimately containing `/` or `and` gets split into
  phantom components. → **Mitigation:** 🟡 combination handled downstream; ❌ guard split against known
  single-entity names.
- **3.5 Section boundary errors (SPL).** DailyMed selects the Indications (34067‑9) and
  Contraindications (34070‑3) LOINC sections. A label that mislabels or nests sections, or puts
  "Limitations of Use" inside Indications, feeds contra/limitation text into the indication extractor.
  → **Mitigation:** 🟡 LOINC keying is precise; ❌ no handling of "Limitations of Use" sub-blocks →
  over-broad indications (see 5.4).

## 4. Assertion-type misclassification

The most consequential fidelity class: recording the *right entities* in the *wrong relationship*.

- **4.1 Contraindication recorded as indication (or vice versa).** DailyMed/PMDA/EMA rely on **LLM
  prompt separation** ("Do not include contraindicated conditions") + running different extractor
  functions per section. If the LLM ignores the instruction, or section boundaries are wrong (3.5), a
  contraindicated disease becomes an approved indication — a clinically inverted claim.
  → **Mitigation:** ✅ deterministic negation/polarity second pass now runs — at *ingest* it drops an
  indication the source states only negatively (strict full-phrase, all occurrences negated, every drop
  logged; `medic.validation.extraction_fidelity.screen_indications`), and `just validate-extraction`
  flags the sensitive superset for review. 🟡 still no check that a *positive* extraction came from the
  right section (entailment flags help but don't prove section provenance).
  **Do NOT re-route a dropped negated indication to a contraindication.** Empirically only ~1/3 of
  negated indications carry a `contraindicated` cue; the rest are *limitations* ("except in active TB")
  or plain *absence of indication* (metformin "should not be used in type 1 diabetes" is **not** a
  contraindication — type 1 needs insulin). Re-routing would fabricate a safety claim = the same
  inversion reversed. Recover genuine contraindications from the contraindications section, not from
  indication-section negations.
- **4.2 Negation missed.** "Not indicated for X", "should not be used in X", "except X" → X extracted
  as a positive indication. LLM free-text extraction is the weak point. → **Mitigation:** ✅ addressed by
  the same deterministic negation screen (4.1). Residual: synonym phrasing the cue-matcher can't locate
  (kept, then caught by the entailment flag).
- **4.3 Warning / precaution / dosing condition read as indication.** Renal-impairment dosing, boxed
  warnings, drug-interaction conditions mention diseases that are not indications. DailyMed extracts
  only from the Indications section (good), but EMA/PMDA free-text notes are less clean.
  → **Mitigation:** 🟡 section scoping for DailyMed; ❌ tighten EMA/PMDA note scoping.
- **4.4 "Limitation of Use" over-broadening.** Label says "indicated for X, *but not* the Y subtype";
  we extract X (broad) and drop the limitation → we over-state the approval. → **Mitigation:** ❌ capture
  limitations as negative qualifiers (relates to I-3 / §6).
- **4.5 Approved vs investigational/off-label mislabel.** `approval_status: APPROVED` is **hardcoded**
  for all regulatory ingesters. True for DailyMed/Orange/Purple Book by construction, but EMA/PMDA
  free-text could carry a conditionally-approved or under-review indication recorded as fully APPROVED.
  → **Mitigation:** 🟡 safe by source design for FDA; ❌ EMA "conditional/exceptional" and PMDA orphan/
  conditional flags exist in source text — parse and reflect them in `approval_status`/qualifiers.

## 5. Relationship / entity-pair extraction from unstructured text (LLM)

Where a single free-text block yields (drug, disease) pairs.

- **5.1 Hallucinated disease.** LLM invents a disease not in the source. Current safety net is
  *grounding failure* → logged as `sssom:NoTermFound`. But a plausible hallucination ("type 2 insulin
  diabetes") can still ground to a real MONDO id and be accepted. → **Mitigation:** 🟡 grounding-failure
  gate; ❌ verify each extracted disease name is a substring/paraphrase actually present in the source
  snippet before accepting (the strongest single control — see 8.1).
- **5.2 Over-extraction / splitting.** "diabetic neuropathy" → {diabetes, neuropathy}; a comorbidity
  mentioned as context becomes a second indication. → **Mitigation:** 🟡 "be specific" prompt; ❌ snippet
  co-occurrence check per extracted entity.
- **5.3 Under-extraction / silent drop.** A real indication is missed (long/complex sentence, list).
  Invisible — nothing flags a *missing* record. → **Mitigation:** ❌ sample-based recall audit against
  human-read labels; count extracted-per-label distribution and alert on outliers (0 diseases from a
  non-empty Indications section).
- **5.4 Wrong drug↔disease pairing.** In multi-drug or combination labels, an indication is attributed
  to the wrong active moiety. → **Mitigation:** 🟡 DailyMed extracts per-SPL (usually one product); ❌
  combination products can misattribute — verify pairing against the snippet.
- **5.5 Coreference / scope errors.** "It is also indicated for…", pronouns, or a shared indication
  across a drug class mis-scoped. → **Mitigation:** ❌ prompt to resolve the subject explicitly; keep the
  snippet tight.
- **5.6 Truncation-induced loss.** Evidence `snippet` is truncated to **500 chars**; the extractor sees
  the section but the *stored* snippet may not contain the sentence that supports a given extracted
  disease → later human/automated verification against the snippet fails or misleads.
  → **Mitigation:** 🟡 raw section is what the LLM saw; ❌ store the specific supporting sub-span per
  extracted disease, not a blanket 500-char prefix.

## 6. Qualifier and clinical-context loss

The entities and relation are right, but the *conditions* on the claim are dropped.

- **6.1 Population / age.** "in adults", "pediatric", "in patients ≥ 6 years" dropped → a
  pediatric-only or adult-only indication looks universal. → **Mitigation:** ❌ capture population
  qualifiers.
- **6.2 Line of therapy / combination context.** "second-line", "in combination with methotrexate",
  "after failure of X" → recorded as a standalone first-line monotherapy indication.
  → **Mitigation:** ❌ capture therapy-line and "in combination with" as qualifiers.
- **6.3 Severity / stage / biomarker.** "moderate-to-severe", "stage III", "EGFR-mutation-positive" →
  qualifier-stripped to the bare disease, broadening the claim. → **Mitigation:** 🟡 grounding may keep a
  more specific id when present; ❌ otherwise the restriction is lost.

## 7. Translation-induced meaning drift (China zh, Russia ru)

New surface as of the DeepL Stage‑0 stage. China/Russia are **drug-list only**, so the risk is drug
*identity*, not indication meaning — but still fidelity.

- **7.1 Mistranslation of the active moiety.** DeepL renders a Chinese/Russian trade name or unusual
  INN to the wrong English drug (or a literal gloss), which then grounds to the wrong CHEBI.
  → **Mitigation:** 🟡 original literal preserved (`original_name_zh/ru` + Babelon `source_value`);
  translation is a recorded, curator-editable Babelon row; grounding failure still gates; ❌ spot-audit
  Babelon `translation_value` vs known INN; flag low-agreement.
- **7.2 Formulation/salt words survive translation.** "Lenalidomide Capsules" — handled downstream by
  `formulation_strip`/`salt_ester_strip`, but a novel form word could block grounding (fail-loud, not
  silent). → **Mitigation:** ✅ downstream strip rules; 🟡 unresolved logged.
- **7.3 Non-determinism / model drift.** DeepL output can change over time. → **Mitigation:** ✅ the
  git-tracked Babelon table is the cache — a filled row is never re-translated, so committed runs are
  byte-identical; ❌ but no signal if DeepL *would now* translate differently (only matters on cache
  rebuild).
- **7.4 Trade-name → wrong molecule.** Compounds 2.3 (trade-name fallback) with translation.
  → **Mitigation:** ❌ mark trade-name-sourced translations for review.

## 8. Evidence, snippet, and reference fidelity

Whether the stored proof actually supports the stored claim.

- **8.1 Snippet does not entail the claim (regulatory).** For **regulatory** indications the `snippet`
  is the raw section text (≤500 chars), and there is **no automated check that it supports the specific
  extracted disease**. Only **research** (PMID) snippets are verified verbatim by `curate_snippets.py`.
  → **Mitigation:** 🟡 research-only verification; ❌ extend snippet-entailment verification to
  regulatory extractions (highest-value control; see 5.1).
- **8.2 URL does not resolve or does not contain the claim.** `linkml-reference-validator` **explicitly
  skips** DailyMed/PMDA/EMA/OrangeBook/PurpleBook URLs. A constructed `source_document_url` (e.g. a
  DailyMed setid PDF, EMA EPAR slug) is assumed valid; a wrong slug/setid points at a *different
  product's* document. → **Mitigation:** ❌ periodic HTTP resolution + (for HTML/PDF) a presence check
  that the drug/indication string appears in the fetched document.
- **8.3 Wrong or multi-valued identifier in the link.** Orange Book uses **only the first** of a
  pipe-joined NDA list to build the Drugs@FDA URL; if ordering is off, the link points to a different
  approval. → **Mitigation:** ❌ carry all NDAs; pick deterministically or emit all.
- **8.4 Confidence over-stated.** `confidence: HIGH` is largely hardcoded; it reflects "we had a
  per-product URL", not extraction certainty. → **Mitigation:** ❌ derive confidence from actual signals
  (section cleanliness, snippet-entailment pass, grounding quality).
- **8.5 Original strings not always captured.** Audit relies on `original_drug_label`,
  `original_disease_label`, `original_drug_id`. PMDA has **no YJ code** and India **no per-drug id**, so
  those provenance anchors are empty — harder to trace a record back to a source row.
  → **Mitigation:** 🟡 documented; ❌ capture PDF page/row coordinates as a fallback provenance key.

## 9. Jurisdiction and source-isolation errors

- **9.1 Cross-jurisdiction leakage.** A source with cross-jurisdiction flag columns could synthesise a
  foreign-jurisdiction row. → **Mitigation:** ✅ invariant I-1 (jurisdiction hardcoded per ingester;
  flag columns stripped; enforced by `docs/source-isolation.md` + tests). Low residual risk.
- **9.2 Wrong authority attribution.** DailyMed is a *republisher* (INTERMEDIARY), Orange/Purple Book
  are PRIMARY; mislabeling role/authority overstates authoritativeness. → **Mitigation:** ✅ `source_role`
  distinguishes; 🟡 ensure downstream consumers honour PRIMARY > INTERMEDIARY.

## 10. Aggregation / merge distortions

Fidelity can be lost when many source assertions become one product record.

- **10.1 Distinct assertions collapsed.** `on_label_merge` keys on
  `(drug_id, disease_id, relationship_type)`; two *different* qualified claims (adult vs pediatric,
  first- vs second-line) that ground to the same pair collapse into one, losing the distinction (compounds
  §6). → **Mitigation:** ❌ include salient qualifiers in identity or retain per-source evidence with
  qualifiers intact.
- **10.2 Over-merge via ID normalization.** Non-MONDO → MONDO xref mapping ("few" IDs) or grounding
  differences can fold two clinically distinct diseases onto one id (or fail to merge two that should).
  → **Mitigation:** 🟡 Stage-2 uses only asserted xrefs; ❌ residue documented (SPEC §11).
- **10.3 Flag ORing hides disagreement.** `fda/ema/pmda` booleans are ORed across sources; a
  disagreement (one source approves, another doesn't) is flattened to "approved". → **Mitigation:** ❌
  retain per-jurisdiction evidence (already unioned) and expose conflicts.
- **10.4 Earliest-date dedup drops later approvals.** Drug-list dedup keeps the **earliest** approval
  date per name; supplemental/later approvals (and their indications) may be under-represented.
  → **Mitigation:** 🟡 earliest is intended for "first approval"; ❌ retain the full date set.
- **10.5 Representative-object bias.** `drug_merge` keeps one "best" grounding/translation/normalization
  object; the discarded ones' provenance is dropped from the product. → **Mitigation:** 🟡 SSSOM stores
  retain all decisions; ❌ product shows only the representative.

## 11. Inference beyond the source

- **11.1 Downfilling (I-3).** Propagating a broad approval down the MONDO hierarchy to subtypes the
  regulator never named. → **Mitigation:** 🟡 **by design, not enforced** — the field was removed and the
  LLM doesn't reason over the hierarchy, but no code *rejects* a downfilled record. ❌ add an explicit
  merge-time assertion that no disease id is a strict descendant introduced by inference.
- **11.2 Implicit generalization by grounding.** Qualifier-stripping rules (e.g. "severe X" → "X")
  broaden a claim during grounding. → **Mitigation:** 🟡 recorded as `broadMatch` with certainty < 1;
  ❌ ensure broadenings are visible/reviewable, not silently canonical.

## 12. Completeness and silent drops

- **12.1 Unresolved records dropped from products.** Drug merge filters `grounding_status ==
  "unresolved"`; a real, correctly-extracted approval with an ungroundable name silently vanishes from the
  product (still in kb/ + SSSOM). → **Mitigation:** 🟡 decision retained in stores; ❌ report dropped-count
  per source; expose an "ungrounded but asserted" list.
- **12.2 Records with `Error` ids skipped.** `on_label_merge` skips ids containing `Error`.
  → **Mitigation:** 🟡 intentional; ❌ count + log.
- **12.3 500-char / >200-char guards drop content.** Snippet truncation (500) and the disease-name
  length guard (>200 chars rejected as LLM prose) can discard legitimate long content.
  → **Mitigation:** 🟡 guards catch LLM refusals; ❌ log rejections for audit.
- **12.4 `MEDIC_SKIP_EXPENSIVE_CALLS` degradation.** With the flag set, non-English names aren't
  translated and won't ground; if a build is run this way, China/Russia silently under-populate.
  → **Mitigation:** 🟡 warns in logs; ❌ mark such a build as non-releasable.

## 13. LLM operational risks

- **13.1 Non-determinism / model change.** Extraction model (default Haiku 4.5) upgrades change outputs;
  a re-run produces different diseases. → **Mitigation:** 🟡 results cached by text-hash; ❌ pin model id
  in provenance; re-validate on model change.
- **13.2 Cache staleness.** Enrichment caches are keyed by *input text*, with **no source timestamp**; if
  a DailyMed label is revised but the section text hash is unchanged-enough, a stale extraction persists.
  → **Mitigation:** ❌ include source version/hash in the cache key; invalidation policy.
- **13.3 Parse-fallback swallows errors.** JSON/pipe-list parsing has try/except fallbacks; a malformed
  LLM response can degrade to empty/partial silently. → **Mitigation:** 🟡 defensive parsing; ❌ count and
  surface parse failures rather than treating as "no diseases".
- **13.4 Refusal/prose leakage.** LLM returns explanation prose instead of data; length/refusal filters
  catch most. → **Mitigation:** 🟡 filters + tests; ❌ log filtered items.

## 14. Validation blind spots (what the three validators do *not* catch)

- ✅ **Schema** (`linkml-validate`): structure, required fields, enum/range.
- ✅ **Terms** (`linkml-term-validator`): MONDO/CHEBI/HP/etc. **exist** and labels match — but **not**
  that the evidence id equals the association id, and **skips** non-OBO prefixes (UNII, DRUGBANK, RXNORM,
  DRON, MedDRA).
- 🟡 **References** (`linkml-reference-validator`): PMID snippet checks only; **skips all regulatory
  URLs**; no URL resolution; no snippet-entailment for regulatory claims.
- ❌ **Not run in CI** — regressions can land uncaught (see `issues/issue_ci_validators.md`).
- ❌ **No relation-level validation** — nothing checks indication-vs-contraindication correctness,
  negation, approval-status accuracy, or downfilling.

---

## Highest-leverage mitigations (priority order)

1. **Snippet-entailment verification for regulatory extractions** (§5.1, §8.1). Re-read the source
   snippet and confirm each extracted disease + its relation (indication/contra) is actually stated.
   Single biggest defence against LLM hallucination and indication↔contraindication inversion.
2. **Negation & assertion-type second pass** (§4.1–4.2). Explicitly check for "not indicated / except /
   contraindicated" against the snippet before accepting a positive indication.
3. **Source version stamping + row-count sanity floors** (§1). Kill stale/truncated-source and silent
   under-population classes at ingest.
4. **Regulatory URL resolution + document-presence check** (§8.2–8.3). Verify constructed links resolve
   and contain the drug/indication string; fix Orange Book multi-NDA handling.
5. **Qualifier capture** (§6, §10.1). Population / line-of-therapy / severity / "limitation of use" —
   preserve them or the merge collapses distinct clinical claims.
6. **Run validators in CI + add coverage counters** (§12, §14) — dropped-record counts, parse-failure
   counts, extracted-per-label distribution — so silent losses become visible.

**Reusable structural safeguards MeDIC already has** (build on these): the SSSOM/Babelon decision stores
(every grounding/translation decision, incl. failures, is persisted and diffable), original-string
preservation (`original_*`, `source_value`), source isolation (I-1), no-downfilling by design (I-3),
fail-loud on missing sources, deterministic offline grounding, and the MEDICNE mention id (I-9) anchoring
the whole trail — which makes per-step auditing (I-8) tractable.
