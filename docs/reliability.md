# MeDIC reliability tiers — selecting the trustworthy subset

MeDIC is built from many regulatory sources, each with its own quirks. To let you import
**just the part that is already fairly trustworthy** without learning those quirks, every
MeDIC statement carries two labels:

- **`statement_type`** — *what kind* of claim it is.
- **`reliability`** — *how trustworthy* it is, scored the **same way for every source**.

The default "soft-launch" subset is a one-line filter: **core statement types at HIGH or
MEDIUM reliability**. It's pre-computed for you in `exports/medic_reliable.tsv`.

## The two labels

### `statement_type`

| Type | Meaning | Core? |
|---|---|---|
| `DRUG_APPROVAL` | a drug is approved / marketed in a jurisdiction | ✅ |
| `INDICATION` | a drug is approved to treat a disease | ✅ |
| `CONTRAINDICATION` | a drug is contraindicated in a disease | ✅ |
| `ADVERSE_EVENT` | a drug is associated with an adverse event | — |
| `RESEARCH_ASSOCIATION` | a drug↔disease link from literature / trials | — |

The three **core** types are the regulatory backbone. Adverse events and research
associations are non-core (opt in explicitly if you want them).

### `reliability`

`HIGH` > `MEDIUM` > `LOW` > `EXCLUDED`. A statement's tier is the **worst of four
independent checks** ("gates"), each guarding one way an extracted claim can be wrong:

| Gate | Asks | HIGH | MEDIUM | LOW | EXCLUDED |
|---|---|---|---|---|---|
| **Grounding** | Was the drug/disease resolved to the right ontology id? | exact / curated | inexact but confident (salt, formulation) | fuzzy | unresolved |
| **Extraction** | Is an LLM-extracted disease actually *stated* in the source (and not negated)? | stated | partial / synonym | not found | it's negated (an inversion) |
| **Translation** | Did a non-English name survive machine translation unreviewed? | English / human-reviewed | machine (DeepL) | — | — |
| **Provenance** | Is there any verifiable provenance (a supporting quote, a resolvable reference)? | yes | — | none | — |

Two guarantees hold by design:

1. **Every source can reach HIGH on its own merits.** No source is capped below HIGH for a
   reason a good record can't overcome — e.g. we do *not* require a direct PDF deep link
   (Orange Book publishes none, yet its approvals are HIGH).
2. **Human review always wins.** A curator can force any statement to `HIGH` (confirmed) or
   `EXCLUDED` (rejected). Reliability rises automatically as MeDIC is curated.

## How to select the reliable subset

**Easiest — use the pre-filtered export.** `exports/medic_reliable.tsv` is already the
default subset (core types, HIGH+MEDIUM):

```bash
# every reliable statement, one per row
column -t -s $'\t' exports/medic_reliable.tsv | less
```

**Full control — filter the annotated table.** `exports/medic_statements.tsv` has *every*
statement with the two labels, so you choose your own bar:

```python
import csv
rows = list(csv.DictReader(open("exports/medic_statements.tsv"), delimiter="\t"))

# the default reliable subset
reliable = [r for r in rows if r["is_reliable"] == "True"]

# stricter: HIGH-only indications
gold = [r for r in rows
        if r["statement_type"] == "INDICATION" and r["reliability"] == "HIGH"]

# include research associations you've decided to trust
mine = [r for r in rows
        if r["reliability"] in ("HIGH", "MEDIUM")
        and r["statement_type"] in ("DRUG_APPROVAL", "INDICATION", "RESEARCH_ASSOCIATION")]
```

Columns: `statement_type, reliability, is_reliable, drug_id, drug_label, disease_id,
disease_label, relationship, jurisdictions, approval_status, reference`.

## What the tiers mean in practice

- **HIGH** — exact grounding, verbatim-supported, English or human-reviewed. Safe to trust.
- **MEDIUM** — reliable but one dimension is softer: an inexact (salt/formulation) grounding,
  an LLM synonym the source spells differently, or a **machine translation** not yet
  human-reviewed (most China/Russia drugs sit here until a curator marks the translation
  official). Import for breadth; review before high-stakes use.
- **LOW** — a fuzzy match or a disease not clearly stated in the source. Excluded from the
  default subset; useful only as a curation worklist.
- **EXCLUDED** — do not import: unresolved grounding, a hallucinated disease, an
  indication↔contraindication inversion, or a "drug" that isn't actually approved.

## Caveats worth knowing

- **Approvals come only from regulators.** A drug a curated list calls FDA-approved but that
  no MeDIC regulatory source confirms is *not* a HIGH approval — it simply isn't in the
  approval set (see `just coverage-gaps`).
- **Coverage is conditional on grounding.** A source record whose name never grounds is
  dropped before it becomes a statement, so it can't appear at any tier.
- **The score reflects the current build.** Curation (fixing a grounding, confirming a
  translation, reviewing a statement) raises tiers on the next rebuild — the reliable subset
  grows over time without any change on your side.

See also: `specs/2026-07-26-soft-launch-reliability-design.md` (design), `REVIEW.md` (how
curators raise reliability), `FAILURE_MODES.md` (the failure modes each gate guards).
