---
name: medic-research-batch
description: >
  Orchestrate batch curation of drug-disease research associations across N
  diseases. Picks the next N uncurated diseases from a curatable priority
  queue (background/research_queue.tsv), runs deep research for any disease
  without existing markdown, then runs the per-disease medic-research-curation
  flow to write kb/research/MONDO_*.yaml. Use when the user wants to "process
  the next batch", "do the next 20 diseases", or "pick up where we left off"
  on research curation.
---

# MeDIC Research Batch Orchestrator

This skill picks the next N uncurated diseases and runs them through the full
research curation pipeline. State lives entirely on disk:
- Skip signal: `kb/research/MONDO_<id>.yaml` exists
- Reuse-deep-research signal: `research/<safe_label>-deep-research-*.md` exists

There is no separate progress file. To re-curate a disease, delete its YAML.

## Step 1: Parse arguments

The skill is invoked as `/medic-research-batch [count] [provider]`:

- `count` — integer, default `20`
- `provider` — one of `perplexity`, `falcon`, `cyberian`, `openai`, `asta`;
  default `perplexity`

Both are optional and positional. Examples:
- `/medic-research-batch` → 20 diseases, perplexity
- `/medic-research-batch 30` → 30 diseases, perplexity
- `/medic-research-batch 20 falcon` → 20 diseases, falcon

## Step 2: Pick candidates

Run the candidate picker:

```bash
uv run python scripts/research_batch_candidates.py --count <COUNT>
```

This will:
- Auto-seed `background/research_queue.tsv` from the priority TSV if missing
- Skip diseases that already have `kb/research/MONDO_<id>.yaml`
- Print up to `<COUNT>` TSV rows: `MONDO:xxxx<TAB>label`

Capture stdout into a list of `(mondo_id, label)` pairs. If the list is
empty, print "Queue exhausted: no uncurated diseases remain in
background/research_queue.tsv" and stop.

## Step 3: For each disease, in sequence

For each `(mondo_id, label)` pair:

### 3a: Compute the safe label

Convention (matches `just research-disease`): `safe_label = label.replace(' ', '_')`.

### 3b: Check for existing deep research markdown

Use `Glob` for pattern: `research/<safe_label>-deep-research-*.md` and then
**filter out any match whose filename contains `.citations.`** — the glob
matches both the primary research file and its `.citations.md` companion,
and only the primary file counts as "existing research."

- If at least one non-citations match exists → **reuse**, skip to 3d.
- If no match → run deep research (3c).

### 3c: Run deep research (only when no markdown exists)

```bash
just research-disease <PROVIDER> "<LABEL>" <MONDO_ID>
```

This produces `research/<safe_label>-deep-research-<provider>.md` and a
companion `.citations.md`. Wait for completion. If the command has not
returned after **10 minutes**, treat it as a timeout failure and move on.

**On failure** (non-zero exit, network error, provider down, timeout):
record `(mondo_id, "deep_research_failed: <error>")` in the failure log and
continue to the next disease. Do not retry. Do not write a partial YAML.

### 3d: Curate this disease

Run the existing per-disease flow from
`.claude/skills/medic-research-curation/SKILL.md` for the steps that extract
drugs from the markdown, resolve CHEBI IDs via cascade grounding, build
evidence items with curator provenance, and write `kb/research/MONDO_<id>.yaml`.
**Do NOT run the per-disease skill's validation step** — Step 3e of this
skill handles validation (and the failure-recovery semantics differ).
Read the per-disease skill if you have not already.

**Curator override.** When following the per-disease flow, override only
the `curator.curator_id` and `curator.name` fields on every evidence item.
All other instructions in the per-disease skill apply unchanged.

Get the current commit hash:

```bash
git rev-parse HEAD
```

Use the orchestrator's URL (NOT the per-disease skill's URL):
`https://github.com/monarch-initiative/medic/blob/<hash>/.claude/skills/medic-research-batch/SKILL.md`

The `name` field on the curator should be:
`MEDIC research batch skill extracting evidence from <PROVIDER> deep research`

### 3e: Validate

```bash
just validate-schema kb/research/<MONDO_ID>.yaml ResearchAssociationList
```

Replace the colon in `MONDO_ID` with an underscore for the path
(e.g., `MONDO:0010602` → `kb/research/MONDO_0010602.yaml`).

**On validation failure**: record `(mondo_id, "validation_failed: <error>")`,
**delete the invalid YAML** so the next batch invocation will retry this
disease (the YAML's existence is the skip signal — leaving it would silently
strand the disease as "done"), and continue to the next disease. If you
want to preserve the bad file for inspection, copy it to
`/tmp/medic-batch-failure-<MONDO_ID>-<TIMESTAMP>.yaml` before deleting.

## Step 4: Rebuild the rapid report

After all diseases in the batch have been processed:

```bash
just build-mondo-drugs-rapid
```

If this fails, log the error in the summary but do not abort.

## Step 5: Print summary

Print a structured summary:

```
Batch complete:
  Processed:               <N>
  Successfully curated:    <Y>
  Reused existing markdown: <R>
  Ran fresh deep research: <F>
  Failed (deep research):  <count>
    - MONDO:xxxx — <error>
  Failed (curation/validation): <count>
    - MONDO:xxxx — <error>
  Remaining uncurated in queue: <K>

Provider used: <PROVIDER>
Queue file: background/research_queue.tsv
```

For the "Remaining uncurated in queue" count, re-run the picker with a
large `--count` (e.g., 10000) after the batch finishes and count the rows
on stdout — the initial picker invocation's count is stale by this point
because new YAMLs have been written.

## Step 6: Do NOT commit

Per project convention (CLAUDE.md), do not commit any artifacts produced by
this batch — including new `kb/research/*.yaml` files, new
`research/*-deep-research-*.md` (and `.citations.md`) files, or changes to
`background/research_queue.tsv`. Show the user the diff and let them decide.

## Key project conventions to follow

- **CURIE handling**: always use `src/medic/curie_utils.py`. Never split with
  `str.split(":")`.
- **Drug ID resolution**: always use
  `get_grounding_service("cascade").ground_drug_best()`.
- **YAML output**: `yaml.dump()`, no flow style, allow unicode, width=120,
  strip non-printable characters. Overwrite existing files.
- **Sequential processing**: do not parallelize. Providers rate-limit and
  errors are easier to triage in sequence.
