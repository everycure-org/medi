# The shape of a MeDIC record

MeDIC records are shaped by one commitment: **nothing is asserted without a recorded reason.**
Every identifier can be walked back to the string a regulator published, and every step in between
names what did it. This page explains the objects that make that possible.

If you only want to use the data, you need two fields — `resolved_id` and `resolved_label` on a
mention. Everything else is there so you can check the answer rather than trust it.

## Mention — a source string, and what it turned out to be

A `Mention` is one string as some source actually wrote it, plus its resolution:

```yaml
id: MEDICNE:31cb393a-52af-50a1-927a-10330ed004c1
original_literal: 乳酸环丙沙星氯化钠注射液      # verbatim, never rewritten
entity_type: drug
mention_source: CHINA
source_language: zh
resolved_id: CHEBI:100241
resolved_label: ciprofloxacin
resolution: {...}                              # how we got from one to the other
```

`original_literal` is sacrosanct (**invariant I-7**): it is what the source said, not a cleaned-up
version. If MeDIC ever writes the canonical label back over it, the record is lying about its
source.

`id` is a `MEDICNE:` identifier — a `uuid5` of the normalized surface form, so the same string
always mints the same id, offline and without a counter. That id is what joins a mention to its
translation and grounding decisions in `mappings/`.

## Resolution — the ordered chain

`resolution` holds the aggregate input and output, a confidence, and the ordered steps:

```yaml
resolution:
  input_value: 乳酸环丙沙星氯化钠注射液
  output_value: CHEBI:100241
  confidence: 0.855
  pipeline:
    - category: EXTRACTION       # read verbatim from the CDE approvals table
      method: STRUCTURED_FIELD
      input_value: 乳酸环丙沙星氯化钠注射液
      output_value: 乳酸环丙沙星氯化钠注射液
      confidence: 1.0
      confidence_basis: DETERMINISTIC
      tool: medic-ingest-china
    - category: TRANSLATION
      method: API
      agent: {agent_name: DeepL, agent_id: "wikidata:Q116709136"}
      input_value: 乳酸环丙沙星氯化钠注射液
      output_value: Ciprofloxacin Lactate and Sodium Chloride Injection
      flags: [unreviewed_machine]
      confidence: 0.95
      confidence_basis: PRIOR
      tool: babelon
      tool_version: 0.3.6
    - category: GROUNDING
      method: LEXICAL_MATCH
      applied_rules: [salt_ester_strip, combination_split]
      input_value: Ciprofloxacin Lactate and Sodium Chloride Injection
      output_value: CHEBI:100241
      confidence: 0.9
      confidence_basis: MEASURED
      tool: medic-lexical-grounder
    - category: NORMALIZATION
      input_value: CHEBI:100241
      output_value: CHEBI:100241
      quality: identity
      confidence: 1.0
      confidence_basis: DETERMINISTIC
```

Three things make this more than a log.

**The chain is contiguous.** Each step's `output_value` is the next step's `input_value`, exactly
(**I-8**). You can replay it by eye. A break means something was transformed without saying so.

**Every step names its actor.** Deterministic components carry a hand-bumped version
(`medic-lexical-grounder/1`); third-party tools carry their real release (`babelon 0.3.6`); LLM
agents carry the **dated model id**, because a model upgrade silently changes output and without
the pin a re-run is not comparable.

**Every transform is a named enum value, never anonymous.** `salt_ester_strip` and
`combination_split` are `PreprocessingRuleEnum` members. Adding a new manipulation means adding
the enum value first — so "what was done to this string?" is always answerable, and queryable.

A terminal `NORMALIZATION` step is always present even when it changes nothing (`quality:
identity`), so you can tell *no normalization was needed* from *none was recorded*.

## Confidence means "how sure are we we linked the right thing"

Not "how likely is this claim to be true". Every confidence in MeDIC is a **data-quality** number.

Each step declares one *and* where it came from:

| `confidence_basis` | meaning |
|---|---|
| `MEASURED` | a scoring component actually evaluated this input |
| `DETERMINISTIC` | the step cannot be wrong — an identity hop, a verbatim field read |
| `PRIOR` | no score was available; a calibrated per-producer constant was used |

Confidence **decays** along a chain — `1.0 × 0.95 × 0.9 × 1.0 = 0.855` above — because each step
is another chance to have linked the wrong entity. `PRIOR` values are declared in
`conf/confidence_priors.yaml` and are currently uncalibrated, which is why they say so in the
record.

## Assertion — the claim, kept separate from the entities

A `Mention` answers *"what entity is this string?"*. An `Assertion` answers *"what is the source
claiming about it?"*. They are deliberately different objects, because an entity can be recognised
perfectly while the asserted relation is wrong.

```yaml
assertion:
  relationship: INDICATION
  trigger_cue: indication_phrase
  trigger_span: "is indicated for the treatment of"    # located verbatim in the text
  span_index: 1
  negation_scope: [1]
  flags: []
  confidence: {subject: 0.9, object: 0.9, relationship: 1.0, overall: 0.81, basis: MEASURED}
```

The rationale is **extractive**: `trigger_span` is a substring found in the source, not a generated
explanation. A confident-sounding narration of a wrong extraction is worse than none.

Failure modes are split the same way. `ExtractionFlag` covers recognition (`hallucination`,
`scope_narrowed`); `AssertionFlag` covers the claim (`negated_inversion`, `over_extraction`,
`wrong_section`).

## An indication is a pair holding one assertion per document

The top level is the canonical drug–disease–relationship pair. All provenance lives one level
down, in `assertions` — **one per attesting source document**:

```yaml
drug_id: CHEBI:755918
drug_label: lecanemab
disease_id: UMLS:C1270972
disease_label: mild cognitive impairment
relationship_type: INDICATION
reliability: HIGH
confidence: {method: NOISY_OR, n_assertions: 3, overall: 0.993}
assertions:
  - source: DAILYMED
    jurisdiction: USA
    document: "DailyMed:9d1ff786-e577-410a-a273-c4d7d0e4e975"
    spans: [{role: SECTION_TEXT, text: "LEQEMBI is indicated for the treatment of Alzheimer's…"}]
    drug: {original_literal: LECANEMAB, ...}
    disease: {original_literal: mild cognitive impairment, ...}
    assertion: {...}
    evidence: {jurisdiction: USA, approval_status: APPROVED, ...}
    regulatory_status: {authority: FDA, status: APPROVED}
  - source: EMA
    jurisdiction: EU
    document: "EMA:leqembi"
    drug: {original_literal: lecanemab, ...}          # EMA's own spelling
  - source: PMDA
    jurisdiction: JAPAN
    document: "PMDA:LECANEMAB#22-20230925"
    drug: {original_literal: LECANEMAB, ...}
```

Each assertion is **internally single-source** (**I-10**): its drug mention, disease mention and
every span come from the same document. Three regulators agreeing shows up as three independent
attestations rather than collapsing into one row — which is the whole point, since corroboration
is the signal a repurposing user cares about.

Pair confidence aggregates by **noisy-OR** — `1 − Π(1 − cᵢ)` — so more sources *raise* it. That is
the opposite direction from a resolution chain, deliberately: more steps means more chance of
error, more sources means more corroboration. Above, three attestations at 0.81 give 0.993.

`evidence` and `regulatory_status` are singular on an assertion, because one document attests one
thing. The pair-level views are derived (`product_view.assoc_evidence`).

## Why the separation matters

Before this shape, one record mixed a drug trail elected by the drug merge, a disease trail from
whichever source was read first, and an evidence list from all sources at once. It produced
records whose drug provenance came from a different jurisdiction than the indication text — each
part internally consistent, the whole incoherent.

The invariants exist so that cannot recur, and the build checks them on every run:

| | |
|---|---|
| **I-7** | `original_literal` is the source string, never a canonical label written back |
| **I-8** | every step records its in and out value; the chain is contiguous |
| **I-9** | every mention has a stable `MEDICNE:` id derived from its surface form |
| **I-10** | an assertion's mentions and spans all come from its own document |
| **I-11** | every step declares a confidence and its basis |
| **I-12** | a chain ending in a CURIE ends with a `NORMALIZATION` step |
| **I-13** | pair confidence is the noisy-OR of its assertions |

## Where to go next

- [How a record is built](record-lifecycle.md) — the same data, followed through the pipeline
- [`docs/provenance-walkthrough.md`](../provenance-walkthrough.md) — the decision stores in detail
- [`SPEC.md`](../../SPEC.md) — the full invariant list
