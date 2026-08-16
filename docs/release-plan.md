# MeDIC — first public release runbook

A step-by-step plan to cut the first public GitHub release, ensuring the data is **fresh,
committed, and carries the reliability provenance** so consumers can select the trustworthy
subset. Work top to bottom; each step says what to run and what "good" looks like.

**The next release is `v2.0.0`** (decided 2026-08-16). It has to be a major bump, not a `v0.x`:
`v1.0.0` and `v1.0.1` are already published, so anything lower sorts *below* the release it
replaces — `gh release --latest`, Zenodo ordering and every semver consumer would treat the new
build as the older one. A major bump is also honest about the change: the redesign alters every
product's schema. Use `v2.0.0-rc1` for a release candidate.

`just gh-release v2.0.0` refuses to run unless the artefacts are stamped with that same version
(`medic_version` comes from the installed distribution, not the tag), so the order is:
`git tag v2.0.0 && uv sync && just build-all` before releasing.

---

## 0. Prerequisites (verify once)

These are needed to *build*; they are not shipped in the release.

- [ ] **Manual sources present** (they can't be auto-fetched):
  - `background/grls.zip` (Russia), `background/cder_drugs_final_all.csv` (China),
    `background/umls-2021AA-mrconso.zip` (grounding index input).
- [ ] **Grounding indexes built** — `ls -lh cache/grounding/lexical_index/` shows
  `diseases.db` (~1 GB) and `drugs.db` (~700 MB). If missing: `just build-grounding-index`.
- [ ] **DailyMed SPLs acquired** — `ls data/raw/dailymed | wc -l` is ~2000. If empty:
  `just ingest-dailymed-acquire`.
- [ ] **API keys in `.env`** — `ANTHROPIC_API_KEY` (extraction/enrichment) and
  `DEEPL_API_KEY` (China/Russia translation).
- [ ] **`gh` default set to the upstream repo**: `gh repo set-default monarch-initiative/medic`.
  (This used to say `matentzn/medic` because MeDIC was a fork. It is not one any more —
  `gh repo view monarch-initiative/medic` reports `isFork: false` — so pointing `gh` at the old
  fork is what now causes the bogus "workflow scope" error, not what fixes it.)

## 1. Build everything fresh

- [ ] Clean rebuild of all products, exports, and validation:
  ```bash
  just build-all 2>&1 | tee /tmp/medic_build.log
  ```
  This ingests every source (incl. China + the re-grounded EveryCure), merges, mines
  indications/contraindications, builds the disease list, writes all exports **including the
  reliability exports**, and validates.
  **Good:** the run reaches `just validate-all` and exits 0. (A full run is minutes–tens of
  minutes; the DailyMed/EMA/PMDA LLM extraction is the slow part and is cached.)
- [ ] If a step fails, fix and re-run — do **not** release a partial build. Common ones:
  a missing manual source (fail-loud, re-provide it), or a transient network 500 (re-run).

## 2. Verify data quality

- [ ] **Validators pass:** `just validate-all` → no `[SCHEMA FAILED]` / term / reference
  errors. (Adverse events are a known empty stub — that's fine.)
- [ ] **Reliability distribution looks sane:** `just reliability-report` — confirm the
  reliable-core fraction (~90%) matches your expectation; no statement type is unexpectedly
  all-LOW/EXCLUDED.
- [ ] **Extraction fidelity triaged:** `just validate-extraction` — review any *new* score-0
  or negated-polarity rows (`extraction_flagged.tsv`); these are likely hallucinations /
  inversions and should be curated or accepted before release (see REVIEW.md §5).
- [ ] **Coverage sanity:** `just coverage-gaps` — the UNII/CHEBI duplicate count should be
  small (~tens); a large number means EveryCure re-grounding regressed.
- [ ] **Reliability exports written:** `exports/medic_statements.tsv` and
  `exports/medic_reliable.tsv` exist and the reliable count is non-trivial.
- [ ] **Source provenance stamped:** `data/source_manifest.json` lists each source's file
  fingerprint + row count + date. Confirm the manually-provided sources are dated as you
  expect (you can't infer their upstream "as-of" — note it in the release notes).

## 3. Review gate (REVIEW.md §9)

A human confirms before packaging:

- [ ] `mappings/*` diffs reviewed like code — no unexpected mass churn of manual/curated rows.
- [ ] New `sssom:NoTermFound` grounding rows for high-value drugs/diseases skimmed.
- [ ] Spot-check a handful of indications for assertion-type/negation correctness.
- [ ] Decide the release scope: the reliable subset (`exports/medic_reliable.tsv`) is what
  most consumers should use — make sure the README/release notes say so.

## 4. Commit the release data

The build regenerates `kb/`, `mappings/`, `products/`, `exports/`. For a reproducible
release, commit them (products/exports may be gitignored — confirm with `git status` and
`git check-ignore`; if products/exports are ignored, that's fine — they ship as release
*assets*, not repo files, but `mappings/` and `kb/` **should** be committed).

- [ ] Review: `git status` and skim the diff.
- [ ] Commit the regenerated data + any tooling changes on a release branch:
  ```bash
  git add kb/ mappings/ src/ docs/ specs/ *.md  # (exclude cache/; add products/exports only if tracked)
  git commit -m "Build vX.Y.Z: refreshed products with reliability provenance"
  ```
- [ ] Push and open a PR into `main` (or merge per your workflow). The release is cut from a
  committed, reviewed state — never from a dirty tree.

## 5. Tag and publish the GitHub release

- [ ] Ensure `products/` and `exports/` on disk are the freshly built ones (step 1).
- [ ] Create a **draft** release (default), which uploads every non-empty `products/*.yaml`
  plus all `exports/*` — including `medic_statements.tsv` and `medic_reliable.tsv`:
  ```bash
  just gh-release vX.Y.Z            # draft
  ```
- [ ] Edit the draft release notes to include:
  - what MeDIC is + the source list and jurisdictions;
  - **how to use the reliable subset** — point at `medic_reliable.tsv` and link
    `docs/reliability.md`;
  - the build's source `data/source_manifest.json` snapshot (which registry versions);
  - known limitations (adverse events stub; coverage caveats; MEDIUM = machine-translated /
    inexact, review before high-stakes use).
- [ ] Verify the uploaded assets list is complete (drug/indication/contraindication/research
  lists + KGX + SSSOM + the two reliability TSVs).
- [ ] Publish the draft (or `just gh-release vX.Y.Z false` to publish immediately).

## 6. Post-release

- [ ] Download `medic_reliable.tsv` from the published release and confirm it opens and
  filters as documented (`docs/reliability.md`).
- [ ] Announce, linking `docs/reliability.md` as the "how to pick what to trust" guide.
- [ ] Open follow-up issues for anything deferred (e.g. the 232 true coverage gaps, the
  disease-side ingest-loss measurement, per-jurisdiction approval reliability).

---

### What guarantees your three goals

- **Up to date** → step 1 (`just build-all` from fresh sources).
- **Committed** → step 4 (release cut from a reviewed, committed tree).
- **Carries provenance for the reliable subset** → the reliability exports
  (`export-reliability`, run inside `build-all`): every statement is annotated with
  `statement_type` + `reliability`, and `medic_reliable.tsv` is the ready-to-import subset.
  See `docs/reliability.md`.
