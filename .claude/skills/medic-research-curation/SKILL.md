---
name: medic-research-curation
description: >
  Curate drug-disease research associations from deep research markdown.
  Reads deep research output, extracts individual drugs with proper names,
  resolves CHEBI IDs, and writes structured kb/research/ YAML files.
  Can also trigger fresh deep research or fall back to PubMed-only mode.
---

# MeDIC Research Curation Skill

## Overview

This skill replaces the regex-based deep research parser. You (the AI agent)
read deep research markdown files and extract individual drug-disease
associations with proper drug names and CHEBI IDs.

## Step 1: Ask the user for curation mode

Present these three options:

1. **Reuse existing deep research** — process `research/*-deep-research-*.md` files already on disk
2. **Run fresh deep research** — execute deep research first, then process the output
3. **PubMed-only** — skip deep research, search PubMed directly for drug-disease pairs

If the user chooses **"Run fresh"**, ask which provider:
- perplexity, falcon, cyberian, openai, or asta

## Step 2: Identify diseases to process

Load the priority disease list:

```bash
head -1 background/priority-disease-2026-03-11categories_by_pheno_with_highlevel.tsv
```

The file has columns: `mondo id`, `mondo label`, and others.

For **reuse mode**: find all diseases that have deep research markdown files in `research/`.
Match files by pattern: `research/<disease_name>-deep-research-<provider>.md`

For **fresh mode**: process all diseases from the priority list (or a user-specified subset).

For **PubMed-only mode**: process all diseases from the priority list.

## Step 3: Process each disease

### 3a: If running fresh deep research

For each disease, run:

```bash
just research-disease <provider> "<disease_label>" <mondo_id>
```

Wait for completion before proceeding to extraction.

### 3b: Extract drugs from deep research

For each disease, read ALL available deep research markdown files at once:
- `research/<disease_name>-deep-research-*.md` (may be multiple providers)
- `research/<disease_name>-deep-research-*.md.citations.md` (companion citation files)

**Critical extraction rules:**

1. **One association per individual drug** — never per therapeutic category
2. If a section heading is a category (e.g., "Approved Drug Therapies",
   "Symptomatic Pharmacological Therapies", "Neuropathic Pain Management"),
   extract the individual drugs mentioned in the prose beneath it
3. Drug names should be generic/INN names (e.g., "pregabalin" not "Lyrica")
4. For drug classes mentioned without specific drugs (e.g., "corticosteroids"
   as a class), use the class name only if no specific drugs are named
5. Skip sections that are purely about disease background, pathophysiology,
   or non-pharmacological interventions (surgery, physical therapy, etc.)

### 3c: Resolve drug IDs

For each extracted drug name, resolve the CHEBI ID using the project's
canonical grounding service:

```bash
uv run python -c "
from medic.grounding.factory import get_grounding_service
svc = get_grounding_service('cascade')
result = svc.ground_drug_best('<DRUG_NAME>')
if result:
    print(f'{result.id}\t{result.label}\t{result.score}')
else:
    print('UNRESOLVED')
"
```

Use the returned `result.id` as `drug_id` and `result.label` as the
canonical `drug_label`. If unresolved, set `drug_id` to empty string
and use the drug name as-is for `drug_label`.

### 3d: Resolve citation references

For each citation number in the deep research markdown, look up the URL
in the companion `.citations.md` file. Convert URLs to reference IDs:

- PubMed URLs → `PMID:12345678`
- PMC URLs → `PMC:PMC12345678` (try to resolve to PMID first)
- DOI URLs → `DOI:10.xxxx/xxxxx`
- ClinicalTrials.gov → `NCT12345678`
- Other URLs → keep as-is

Use `scripts/parse_deep_research.py` utility functions for this:
- `load_citations(path)` — load numbered citation list
- `url_to_reference(url)` — convert URL to (ref_id, source_type)
- `resolve_title(ref_id, url)` — fetch document title with caching

### 3d.1: Extract page/section hints

Many `.citations.md` files (especially Falcon outputs) include locator
hints inside the citation entry, e.g.:

```
1. callejon2024investigationofstrategies pages 1-2
12. macido2024anumbrellaliterature pages 6-10
```

Other free-form mentions can appear in the deep-research prose
surrounding a citation, e.g. "(Smith 2023, Section 3.2)",
"see Box 2", "p. 47", "Figure 1B", "Table 4".

For each citation, scan **both** the matching `.citations.md` line
and the prose snippet captured for the evidence item, and extract the
first matching locator hint using these regex patterns (case-insensitive):

| Pattern | Regex | Output example |
|---|---|---|
| Page range  | `\bpages?\s+(\d+)\s*[-–]\s*(\d+)\b` | `pages 6-10` |
| Single page | `\bpages?\s+(\d+)\b` | `page 47` |
| `p.` form   | `\bpp?\.\s*(\d+)(?:\s*[-–]\s*(\d+))?\b` | `pp. 47-49` |
| Section     | `\bsection\s+(\d+(?:\.\d+)*)\b` | `Section 3.2` |
| Box         | `\bbox\s+(\d+[A-Za-z]?)\b` | `Box 2` |
| Figure      | `\bfigure\s+(\d+[A-Za-z]?)\b` | `Figure 1B` |
| Table       | `\btable\s+(\d+[A-Za-z]?)\b` | `Table 4` |

**Precedence** (use the most specific hit, in this order):
1. page range / single page / `p.` form  → e.g. `pages 6-10`
2. section  → e.g. `Section 3.2`
3. table  → e.g. `Table 4`
4. figure  → e.g. `Figure 1B`
5. box  → e.g. `Box 2`

Normalize the output as a single string preserving the matched form,
title-cased for the unit (`pages`, `Section`, `Box`, `Figure`, `Table`,
`p.`, `pp.`). Keep the numeric span exactly as written.

If no hint is found, leave `page_or_section` **unset / blank**. Never
invent or guess page numbers.

**Verification example:** Given the snippet
`"Smith J et al. (2023) Treatment outcomes. NEJM 388:415-425. See pages 6-10."`
the page-range regex matches `pages 6-10` → emit `page_or_section: "pages 6-10"`.

### 3e: Build evidence items

For each drug, create evidence items from the citations that support it.
Each evidence item must have:

```yaml
- source_type: LITERATURE  # or DATABASE for non-publication URLs
  reference: PMID:12345678
  reference_title: "Paper title here"
  page_or_section: "pages 6-10"  # optional locator hint; see Step 3d.1
  snippet: >
    Verbatim quote from the deep research markdown around this citation.
    This is what shows up on the user-facing evidence card; treat the
    `explanation` as analyst commentary and the `snippet` as the source
    excerpt. Required.
  explanation: >
    One to three sentences explaining how this reference supports the
    drug-disease association. Be specific about findings, not generic.
  confidence: MEDIUM  # MEDIUM for peer-reviewed lit, LOW for websites/databases
  evidence_source: HUMAN_CLINICAL  # or MODEL_ORGANISM, IN_VITRO, COMPUTATIONAL
  original_drug_label: "<exactly the drug-name token used in the source markdown>"
  original_disease_label: "<exactly the disease-name token used in the source markdown>"
  curator:
    curator_id: https://github.com/monarch-initiative/medic/blob/<COMMIT_HASH>/.claude/skills/medic-research-curation/SKILL.md
    curator_type: AI_AGENT
    name: "MEDIC research skill extracting evidence from <PROVIDER> deep research"
```

**Snippet capture rule:** copy the sentence(s) immediately surrounding the
citation in the deep-research markdown — verbatim, without paraphrasing. If
the markdown text is too long, take 1–3 contiguous sentences that contain
both the drug name and the citation reference number. Truncate to ~500
characters if longer.

**Original label rule:** preserve the EXACT casing/spelling the source used
(e.g. "rituximab", "Rituximab", "rituxan" — whatever appears). Reviewers use
this to audit grounding decisions.

For the `curator_id`, get the current commit hash:

```bash
git rev-parse HEAD
```

Then construct: `https://github.com/monarch-initiative/medic/blob/<hash>/.claude/skills/medic-research-curation/SKILL.md`

For the `name` field, use the actual provider name from the deep research
file's YAML frontmatter (e.g., "perplexity", "falcon").

### 3f: Write the YAML file

Write the output to `kb/research/<MONDO_ID>.yaml` where `MONDO_ID` has
the colon replaced with underscore (e.g., `MONDO_0008087.yaml`).

The file must conform to `ResearchAssociationList` schema:

```yaml
associations:
- drug_id: CHEBI:8356
  drug_label: pregabalin
  disease_id: MONDO:0008087
  disease_label: hereditary neuropathy with liability to pressure palsies
  curation_status: DRAFT
  curation_date: "2026-04-10T12:00:00"
  curator: medic-research-skill
  deep_research_used: true
  notes: Brief summary of this drug's relevance to the disease.
  evidence:
  - source_type: LITERATURE
    reference: PMID:39839199
    reference_title: "Paper title"
    page_or_section: "pages 6-10"  # optional; omit when no hint is found
    explanation: >
      How this evidence supports the drug-disease association.
    confidence: MEDIUM
    evidence_source: HUMAN_CLINICAL
    curator:
      curator_id: https://github.com/monarch-initiative/medic/blob/abc123/.claude/skills/medic-research-curation/SKILL.md
      curator_type: AI_AGENT
      name: MEDIC research skill extracting evidence from Perplexity deep research
```

**Important YAML rules:**
- Use `yaml.dump()` style — no flow style, allow unicode, width=120
- Strip non-printable characters
- Overwrite existing files — the deep research markdown is the source of truth

### 3g: Validate the written file

After writing each file, validate it:

```bash
just validate-schema kb/research/<MONDO_ID>.yaml ResearchAssociationList
```

If validation fails, fix the issue and re-write.

## Step 4: Rebuild the rapid report

After all diseases are processed:

```bash
just build-mondo-drugs-rapid
```

## Step 5: Summary

Print a summary:
- Number of diseases processed
- Total drug associations extracted
- Number of drugs with resolved CHEBI IDs vs unresolved
- Any validation failures

## PubMed-Only Mode

If the user chose PubMed-only mode, use the existing pipeline:

```bash
uv run python -m medic.research.curate --disease <MONDO_ID>
```

This produces `kb/research/` files via the PubMed fallback path. The
output uses the same `ResearchAssociation` schema. After processing
all diseases, rebuild the rapid report as in Step 4.

## Key Project Conventions

- **CURIE handling**: Always use `src/medic/curie_utils.py` for CURIE operations.
  Never split CURIEs with `str.split(":")`.
- **Drug ID resolution**: Always use `get_grounding_service("cascade").ground_drug_best()`.
  Never manually construct CHEBI IDs.
- **Git**: Do NOT commit unless the user explicitly asks.
- **GitHub**: Do NOT comment on PRs or issues directly.
