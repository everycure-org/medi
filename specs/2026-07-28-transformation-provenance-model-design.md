# Transformation-provenance model + cleaner statement model — Design Spec

**Status:** **implemented** (2026-08-08), with as-built revisions recorded in §2.1 · **Date:** 2026-07-28
· **Supersedes** the flat `IndicationAssociation` model. **Companions:** `FAILURE_MODES.md` (the failure
catalogue the `flags` encode), `specs/2026-07-26-soft-launch-reliability-design.md` (reliability tiers,
which read this trail), `review_model.md` (the as-built shape, with real worked examples) and
`docs/provenance-walkthrough.md` (how recording/caching/assembly interact, end to end).

> **Read §2.1 first.** The design below was implemented with four substantive revisions agreed during
> review. Where §3–§5 describe the original shape, §2.1 is authoritative.

## 1. Motivation

The current `IndicationAssociation` is flat and conflated (real examples from
`products/indication_list.yaml`):

- **Identity duplicated 3–4×:** `final_normalized_drug_id/label`, `final_normalized_disease_id/label`,
  `drug_disease` (a dedup key leaking into the model), plus `disease_grounding.grounded_id`, plus
  `evidence.original_*`.
- **Jurisdiction represented 3×:** deprecated `fda/ema/pmda` booleans + `regulatory_status` +
  `evidence.jurisdiction`.
- **Three clashing "confidence" numbers** — `evidence.confidence: HIGH` (hardcoded per source,
  meaningless), `disease_grounding.confidence: 1.0` (rule certainty), and the real reliability tier is
  **absent from the YAML** (only in `medic_statements.tsv`).
- **Drug provenance missing:** only the *disease* side carries grounding/normalization; the drug's
  grounding + translation live on `drug_list.yaml` and are neither shown nor referenced.
- **Two different claims jammed into one `evidence` list:** "the drug is *approved*" (Orange Book
  application numbers) and "the drug is *indicated for this disease*" (the SPL snippet).

And it does nothing to make the failure modes we catalogued **queryable per record** — the
`VITAMIN A → hyperthyroidism` mis-extraction (hyperthyroidism appears in the snippet as a *depleting
condition*, not an indication) is indistinguishable from a real indication in the current shape.

**Goal:** a clean, detailed model where every step of *raw source value → canonical entity* is its own
well-slotted class with the failure modes as first-class detail — and where that transformation layer is
a **standalone, reusable LinkML schema** (its own namespace, no MeDIC coupling) that other projects can
adopt.

## 2. Scope

Two artifacts:

1. **`transformation-provenance`** — a new standalone LinkML schema (own `w3id` namespace, generic
   naming) holding the transformation classes. MeDIC imports it.
2. **MeDIC adoption** — `Drug` and `IndicationAssociation` re-modelled to reference `Mention`s from that
   schema, dropping the flat cruft and carrying the real `reliability`.

Out of scope here (tracked separately): the release-asset curation (blind `exports/*` glob → explicit
list), and the extraction *quality* improvements themselves (this spec models them, it doesn't fix the
`VITAMIN A` extractor).

## 2.1 As-built revisions (authoritative over §3–§5)

Four substantive changes were agreed during implementation review. Everything else landed as designed.

**(a) The steps are wrapped in a `Resolution` container.** `Mention.steps` (a bare list) became
`Mention.resolution`, a `Resolution {input_value, output_value, confidence, pipeline}`. The container
carries the aggregate raw→final values and one `confidence` = **the product of the step confidences**;
`pipeline` is the ordered step list. A **chaining invariant** is now enforced at assembly:
`pipeline[i].output_value == pipeline[i+1].input_value`, `resolution.input_value ==
pipeline[0].input_value`, and `resolution.output_value == pipeline[-1].output_value == resolved_id`.
`Mention.resolution_quality` was dropped (subsumed by the per-step `quality`). *Combination splits* do not
fan out: the chain stays linear per resolved entity and component ids ride as `components[]` on the
grounding step.

**(b) Entity recognition and relation extraction are separated.** The original `Extraction` step carried
`asserted_relationship` + `entailment_score` + `negation_cue` — i.e. it conflated *"what entity is this
string?"* with *"what is the source claiming about it?"*. As built:

- `ExtractionStep` is **entity recognition only** (NER for free text, a verbatim field read for structured
  sources). `asserted_relationship` is **deleted** (the relation was already named by
  `IndicationAssociation.relationship_type` one level up); `entailment_score` collapsed into the
  inherited `confidence` slot. A disease `Mention` therefore mentions no relationship at all and is
  reusable standalone.
- A new **`Assertion`** class holds the claim's provenance — `{input_value (supporting quote), method,
  tool, tool_version, agent, confidence, status, negation_cue, flags, comment}` — inlined on
  `IndicationAssociation.assertion`. The relation is named once by the owning record, never repeated.
- The flags re-sorted along the same seam: `ExtractionFlag` keeps the **recognition** failures
  (`hallucination`, `truncated_snippet`, `coreference_ambiguity`); a new `AssertionFlag` takes the
  **relation** failures (`negated_inversion`, `over_extraction`, `wrong_section`, plus a new
  `wrong_pairing` for FAILURE_MODES §5.4). Rationale: in the `VITAMIN A → hyperthyroidism` case the entity
  is recognised *perfectly* — it is the relation that is wrong, so filing it as an extraction failure was
  dishonest.

**(c) Naming.** Three renames were forced or agreed:

| Spec name | As built | Why |
|---|---|---|
| `Extraction`/`Translation`/`Grounding`/`Normalization` | `ExtractionStep`/`TranslationStep`/`GroundingStep`/`NormalizationStep` | `grounding.yaml` already defines classes with the bare names; LinkML would silently merge and corrupt them |
| `Agent {id, type, name}` | `ProvenanceAgent {agent_id, agent_type, agent_name}` | clashes with `evidence.Agent`; bare `id`/`type`/`name` collide with global slots |
| `Mention.mention_id`, `Drug.mention`, `Mention.source` | `Mention.id`, `Drug.identity`, `Mention.mention_source` | reviewer preference; `source` collides with `evidence` |

Step slots also gained a uniform **`output_label`** (replacing per-subclass `grounded_label` /
`normalized_label`) and **`tool_version`** split out of `tool`. The step-type discriminator remains the
`category` enum (a top-level `type` slot re-triggers the collision class above).

**(d) Drug metadata tidied** (beyond the original scope, same migration pattern): the eight flat `atc_*`
fields nest under one `atc` object (`ATCClassification`), and the sixteen `is_*` booleans collapse into a
single `features` list over a new `DrugFeatureEnum`. These are the **only** remaining additive leftovers —
the flat fields still ride alongside pending `issues/issue_drop_flat_atc_is_fields.md`.

**Worked examples of the as-built shape** live in `review_model.md` (an approval + an indication) and
`docs/provenance-walkthrough.md` (full drug + disease traces with their backing store rows). The examples
in §3.5 below predate revisions (a)–(c) and are kept only as design rationale.

## 3. The transformation-provenance schema

Proposed id: `https://w3id.org/monarch-initiative/transformation-provenance` (namespace is an open item,
§8). Generic names, no MeDIC terms — so it lifts cleanly into other projects.

### 3.1 Shared enums

```yaml
TransformationCategory:    # WHICH stage (discriminator, so a heterogeneous steps[] list is readable)
  [EXTRACTION, TRANSLATION, GROUNDING, NORMALIZATION]

TransformationMethod:      # HOW a step was performed
  [LLM, DETERMINISTIC_RULE, LEXICAL_MATCH, TRANSLITERATION, API, STRUCTURED_FIELD, HUMAN]

StepStatus:                # curation lifecycle — the human-review hook, uniform across stages
  [MACHINE, CANDIDATE, UNDER_REVIEW, CONFIRMED, REJECTED]
```

### 3.2 `TransformationStep` (abstract)

Every stage shares this; it enforces invariant I-8 (record both the incoming and outgoing value of
each named step) at the schema level.

```yaml
TransformationStep:
  abstract: true
  slots:
    category:       {range: TransformationCategory, required: true}   # discriminator for steps[]
    input_value:    {required: true}          # I-8 incoming value (verbatim; never mutated, I-7)
    output_value:                              # I-8 outgoing value ('' if the step failed)
    method:         {range: TransformationMethod, required: true}
    tool:                                      # tool + version, e.g. "deepl", "medic-lexical-grounder/1"
    agent:          {range: Agent}             # who/what ran it (HUMAN vs AI_AGENT)
    confidence:     {range: float, minimum_value: 0, maximum_value: 1}
    status:         {range: StepStatus}
    applied_rules:  {multivalued: true}        # named transforms applied (subclasses narrow the range)
    quality:                                   # stage outcome (subclasses narrow to their enum)
    flags:          {multivalued: true}        # detected failure-mode signals (subclasses narrow)
    comment:
```

`Agent` is `{id, type: HUMAN|AI_AGENT, name}`. **Reality note:** MeDIC *does* have an `Agent` class
(`src/medic/schema/evidence.yaml`), but its slots are `{curator_id, curator_type: CuratorTypeEnum, name}`
— a different shape and enum. So "reuse" would mean renaming slots and reconciling the enum; **vendoring a
fresh `{id, type, name}` into the standalone schema is the cleaner path** (and the §8 recommendation).

### 3.3 Concrete stages (each narrows `quality`/`flags`/`applied_rules` via `slot_usage`)

| Class | input → output | Extra slots | `quality` enum | `flags` enum (→ FAILURE_MODES.md) |
|---|---|---|---|---|
| **`Extraction`** | supporting quote → mention string | `asserted_relationship, entailment_score, negation_cue` | `ExtractionQuality` = verbatim / canonicalized / synonym / not_stated | `hallucination`(§5.1), `negated_inversion`(§4.1), `over_extraction`(§5.2), `wrong_section`(§3.5), `truncated_snippet`(§5.6), `coreference_ambiguity`(§5.5) |
| **`Translation`** | foreign string → English | `source_language, target_language, predicate_id, translator_expertise` | `TranslationPrecision` = exact / broader / narrower / close | `unreviewed_machine`(§7.3), `trade_name_source`(§7.4) |
| **`Grounding`** | string → ontology id | `grounded_label, match_field, predicate_id, source_vocabulary, components[]` | `GroundingQualityEnum` (existing) | `fuzzy`(§grounding), `ambiguous_resolved`, `broadened`(§11.2), `isotope_risk`(§9), `formulation_stripped`, `rxnorm_proposed`, `script_transliteration`(§2.5) |
| **`Normalization`** | initial id → canonical id | `normalized_label, target_namespace, mapping_justification` | `NormalizationQualityEnum` (existing) | `no_target_xref`(§10.2), `deprecated_replacement` |

`Grounding.applied_rules` narrows to the existing `PreprocessingRuleEnum` — the string-transform
catalogue moves here too, so the whole "how did this string become an id" story is in one place.

**Reality note — the step classes are net-new, the enums are the reuse.** `GroundingQualityEnum`,
`NormalizationQualityEnum`, and `PreprocessingRuleEnum` already exist verbatim in
`src/medic/schema/grounding.yaml` (values check out — `lexical_exact`, `lexical_exact_surgery`, `none`
are all real) and get **moved** into the standalone schema. But the *step classes* `Grounding`/
`Normalization` here are a **redesign**, not the current `grounding.yaml` classes: those use
`original_string`/`grounded_id`/`grounding_quality`, whereas the abstract step uses
`input_value`/`output_value` + the new `match_field`/`source_vocabulary`/`predicate_id`-on-grounding.
Treat them as new classes that supersede the old ones — do not assume field-level reuse. One genuine
reuse: `grounding.yaml` already has a `Translation` class with `TranslationStatusEnum` /
`TranslatorExpertiseEnum`, which seed the `Translation` step's `status` lifecycle and `translator_expertise`.

**`Extraction.input_value` is the minimal supporting quote, not the whole section.** A free-text
disease extraction reads a long label section but only one sentence supports the extracted disease.
Storing the whole section on every step would bloat records (and duplicate across every disease pulled
from the same section). So the full source text lives **once** on the `Mention`'s `source_spans`
(a `TextSpan` list, §3.4) and each `Extraction` step's `input_value` holds the exact supporting quote — a
substring of one span's `text`. That substring *is* the "refer back": I-8's in→out is preserved
(quote → extracted string), the verbose context sits one level up, and there is no separate `snippet`
slot (input_value serves it). The document/section provenance (`source_reference`, `section_code`) lives
on the `TextSpan`, not on the step. Structured extractions (Orange Book / CDE columns) are already tiny —
their single span's `text` is the raw cell.

### 3.4 `Mention` — the trail holder

```yaml
Mention:
  slots:
    mention_id:        {range: uriorcurie, required: true}   # the stable id (MeDIC: MEDICNE:<uuid5>)
    original_literal:  {required: true}                       # verbatim source string (I-7)
    entity_type:                                              # e.g. drug / disease
    source_language:
    source:                                                   # originating source name
    source_spans:      {range: TextSpan, multivalued: true, inlined_as_list: true}
                       # the source text this mention was extracted from (Extraction.input_value quotes it)
    steps:             {range: TransformationStep, multivalued: true, inlined_as_list: true}
                       # ordered: Extraction? -> Translation? -> Grounding -> Normalization?
    resolved_id:       {range: uriorcurie}                    # final canonical id
    resolved_label:
    resolution_quality:                                       # worst step quality (a summary)

TextSpan:
  slots:
    text:              {required: true}    # the source text chunk (a paragraph, sentence, or structured cell)
    source_reference:                      # document id the span came from (e.g. DailyMed:<setid>, a CSV path)
    section_code:                          # section within that document (e.g. LOINC:34067-9, a column name)
  # Deferred (add when a consumer needs them): char offsets, page/line numbers, and a span `role`
  # (assertion vs disease vs limitation). Multivalued because one indication can be assembled from two
  # DISCONNECTED spans — e.g. prose "indicated for the conditions in Table 2" + the disease rows in Table 2,
  # or an indication whose scope is narrowed by a separate "Limitations of Use" span.
```

`steps` is the replayable I-8 trail: each element is one of the four subclasses, in order. A
consumer reads `original_literal` + `resolved_id` for the quick answer, or walks `steps` for the full
"what happened and what could have gone wrong" audit (via `flags`).

**`mention_id` is a primary key, not a cross-record join pointer.** It stays on the `Mention` because
it is the SSSOM subject id (grounding/normalization mapping rows need a subject even when grounding
*fails*) and the cross-source dedup key (same string → same `uuid5`). It is deliberately **not** used to
link one product record to another: every product record is **self-contained** — an
`IndicationAssociation` carries the drug's resolved `id`+`label` and the disease's full inlined trail, so
it is readable without joining to `drug_list.yaml`. See §4.

### 3.5 Worked examples (complete, slotted instances)

> **Superseded — design rationale only.** These predate as-built revisions (a)–(c) in §2.1: they show a
> bare `steps:` list (no `resolution` container), `mention_id`/`Drug.mention` (now `id`/`Drug.identity`),
> `category: Extraction` (now the `EXTRACTION` enum on an `ExtractionStep`), and `asserted_relationship` /
> `entailment_score` on the extraction step (now on the association's `Assertion`). For the **current**
> shape see `review_model.md` and `docs/provenance-walkthrough.md`. Kept here because the annotations
> explain *why* each slot exists.

Real MeDIC data. `# ↳` comments call out what each slot buys you.

#### Example 1 — all four steps: a China drug (`Drug` record)

`坎地沙坦酯片` → structured-extract → DeepL-translate → ground (formulation strip) → normalize.

```yaml
Drug:
  reliability: MEDIUM          # ↳ capped by the Translation step's CANDIDATE status (unreviewed MT)
  approvals:
    - authority: NMPA
      status: APPROVED
      source: CDE
      source_role: PRIMARY
  mention:
    mention_id: MEDICNE:d945e415-0e43-51ae-b479-b3ef9ed9c7dd
    original_literal: "坎地沙坦酯片"      # ↳ verbatim source string, never mutated (I-7)
    entity_type: drug
    source_language: zh
    source: CDE
    source_spans:                         # ↳ a structured cell is a degenerate length-1 span
      - text: "坎地沙坦酯片"
        source_reference: background/cder_drugs_final_all.csv
        section_code: drug_name
    resolved_id: CHEBI:3348
    resolved_label: Candesartan cilexetil
    resolution_quality: lexical_exact_surgery
    steps:
      - category: Extraction
        input_value: "坎地沙坦酯片"       # ↳ the cell (a substring of source_spans[0].text — here the whole cell)
        output_value: "坎地沙坦酯片"
        method: STRUCTURED_FIELD          # ↳ not LLM -> no hallucination possible
        tool: medic-cde-parser/1
        agent: {type: AI_AGENT, name: MeDIC CDE parser}
        confidence: 1.0
        status: MACHINE
        quality: verbatim
        flags: []                         # ↳ structured extraction carries no extraction failure modes
      - category: Translation
        input_value: "坎地沙坦酯片"
        output_value: "Candesartan Cilexetil Tablets"
        method: API
        tool: DeepL
        agent: {id: "wikidata:Q116709136", type: AI_AGENT, name: DeepL}
        confidence: 0.85
        status: CANDIDATE                 # ↳ machine, not human-reviewed -> drives reliability to MEDIUM
        source_language: zh
        target_language: en-us
        predicate_id: rdfs:label
        translator_expertise: ALGORITHM
        quality: close
        flags: [unreviewed_machine]       # ↳ FAILURE_MODES §7.3
      - category: Grounding
        input_value: "Candesartan Cilexetil Tablets"
        output_value: CHEBI:3348
        method: LEXICAL_MATCH
        tool: medic-lexical-grounder/1
        confidence: 0.8
        status: MACHINE
        applied_rules: [base_normalization, formulation_strip]  # ↳ "Tablets" stripped to reach the ingredient
        grounded_label: Candesartan cilexetil
        match_field: label
        predicate_id: skos:closeMatch     # ↳ closeMatch (not exact) because a word was removed
        source_vocabulary: CHEBI
        quality: lexical_exact_surgery
        flags: [formulation_stripped]     # ↳ FAILURE_MODES §5.4 / formulation grounding
      - category: Normalization
        input_value: CHEBI:3348
        output_value: CHEBI:3348          # ↳ identity: already canonical CHEBI
        method: DETERMINISTIC_RULE
        tool: medic-normalizer/1
        status: MACHINE
        normalized_label: Candesartan cilexetil
        target_namespace: CHEBI
        quality: none
        flags: []
```

**Reads:** `original_literal` (Chinese) → `resolved_id: CHEBI:3348` for the quick answer; walk `steps` for
the full audit. Curator marks the Translation `status: CONFIRMED` → reliability rises to HIGH on rebuild.

#### Example 2 — the teaching case: a free-text disease extraction that is *wrong*

`VITAMIN A → hyperthyroidism` (real). The disease is *inlined* on the association (extracted per label).

```yaml
IndicationAssociation:
  relationship_type: INDICATION
  reliability: LOW                        # ↳ the Extraction flags drag it down (see below)
  drug:                                   # ↳ DrugRef {id, label} — self-contained outcome; full trail lives once in drug_list (join by CHEBI id if wanted)
    id: CHEBI:12777
    label: vitamin A
  disease:                                # ↳ inlined Mention: extracted from THIS label's text
    mention_id: MEDICNE:2202015a-9836-5aba-a849-7eb3741a4923
    original_literal: hyperthyroidism
    entity_type: disease
    source: DailyMed
    source_spans:                         # ↳ the whole Indications section, stored ONCE (Extraction quotes a substring)
      - text: >-
          BACMIN is indicated for prophylactic or therapeutic nutritional supplementation in
          physiologically stressful conditions. These include: Conditions causing depletion,
          reduced absorption ... chronic alcoholism, ... hyperthyroidism ...
        source_reference: DailyMed:34beae32-51e4-4bdc-8ca5-1d8d79f193b2
        section_code: LOINC:34067-9       # ↳ Indications section
    resolved_id: MONDO:0004425
    resolved_label: hyperthyroidism
    steps:
      - category: Extraction
        input_value: "... Conditions causing depletion ... hyperthyroidism ..."  # ↳ minimal supporting quote (substring of source_spans[0].text)
        output_value: hyperthyroidism
        method: LLM
        tool: claude-haiku-4-5
        agent: {type: AI_AGENT, name: MeDIC extraction}
        confidence: 0.5
        status: MACHINE
        asserted_relationship: INDICATION
        entailment_score: 1.0             # ↳ the WORD is in the text — entailment ALONE says "fine"
        quality: verbatim
        flags: [over_extraction]          # ↳ ...but it's a *depleting condition*, NOT an indication (§5.2)
      - category: Grounding
        input_value: hyperthyroidism
        output_value: MONDO:0004425
        method: LEXICAL_MATCH
        confidence: 1.0
        applied_rules: [base_normalization]
        predicate_id: skos:exactMatch
        source_vocabulary: MONDO
        quality: lexical_exact
        flags: []
      - category: Normalization
        input_value: MONDO:0004425
        output_value: MONDO:0004425
        quality: none
        flags: []
  evidence:
    - source: DailyMed
      jurisdiction: USA
      source_role: INTERMEDIARY
      document_url: https://dailymed.nlm.nih.gov/dailymed/downloadpdffile.cfm?setid=34beae32-...
      approval_status: APPROVED           # ↳ NOTE: no `confidence: HIGH` — the record-level `reliability` is the metric
```

**Why this example matters:** `entailment_score: 1.0` (the word is present) would pass a naive check —
the flat model *cannot* express "present but not an indication". The `over_extraction` flag on the
`Extraction` step is exactly the slot that captures it, and it's what lets a future gate (or a curator)
knock the statement to LOW. This is the single biggest reason to model extraction as its own class.
(Detector maturity: `entailment_score` and `negated_inversion` are auto-populated today; richer flags
like `over_extraction` are curator- or future-detector-set — the model provides the slot regardless.)

#### Example 3 — the minimal trail: an English drug (2 steps)

Orange Book `ASPIRIN`: structured-extract → ground. No translation, normalization is identity → the
`steps` list is short. Shows that steps are ordered-and-optional, not mandatory.

```yaml
Drug:
  reliability: HIGH
  mention:
    mention_id: MEDICNE:<aspirin>
    original_literal: ASPIRIN
    entity_type: drug
    source_language: en
    source: ORANGEBOOK
    source_spans:
      - {text: ASPIRIN, source_reference: orangebook, section_code: Ingredient}
    resolved_id: CHEBI:15365
    resolved_label: acetylsalicylic acid
    steps:
      - category: Extraction
        input_value: ASPIRIN
        output_value: ASPIRIN
        method: STRUCTURED_FIELD
        quality: verbatim
        flags: []
      - category: Grounding
        input_value: ASPIRIN
        output_value: CHEBI:15365
        method: LEXICAL_MATCH
        confidence: 1.0
        applied_rules: [base_normalization]
        predicate_id: skos:exactMatch
        grounded_label: acetylsalicylic acid   # ↳ matched via a synonym
        match_field: synonym
        source_vocabulary: CHEBI
        quality: lexical_exact
        flags: []
```

#### Example 4 — Russia Cyrillic (transliteration + fuzzy grounding)

`Абакавир` where DeepL is unavailable/skipped, so it falls to the deterministic transliteration +
fuzzy path — showing two more `flags`.

```yaml
mention:
  original_literal: "Абакавир"
  entity_type: drug
  source_language: ru
  source_spans:
    - {text: "Абакавир", source_reference: grls, section_code: trade_name}
  resolved_id: CHEBI:421707
  steps:
    - {category: Extraction, input_value: "Абакавир", method: STRUCTURED_FIELD, output_value: "Абакавир", quality: verbatim, flags: []}
    - category: Grounding
      input_value: "Абакавир"
      output_value: CHEBI:421707
      method: TRANSLITERATION
      applied_rules: [cyrillic_transliteration, fuzzy_edit1_unique]  # ↳ "Абакавир"->"abakavir"->"abacavir"
      confidence: 0.6
      predicate_id: skos:closeMatch
      quality: lexical_exact_surgery
      flags: [script_transliteration, fuzzy]   # ↳ §2.5, §grounding — both curator-reviewable
    - {category: Normalization, input_value: CHEBI:421707, output_value: CHEBI:421707, quality: none}
```

(Note: `category` is a small explicit type-tag slot on `TransformationStep` so a consumer can read a
heterogeneous `steps` list without relying on LinkML's inheritance-based typing.)

## 4. MeDIC adoption (the cleaner statement model)

MeDIC imports the schema and re-models:

```yaml
# products/drug_list.yaml — a Drug owns its identity Mention + approval evidence
Drug:
  mention: Mention            # identity trail (grounding, + translation for zh/ru)
  approvals: [RegulatoryStatus]   # per-jurisdiction approval (the "drug is approved" claim)
  reliability: ReliabilityTier
  # (metadata: ATC, tags, synonyms, alternate_ids as today)

# products/indication_list.yaml — an association is a relationship between two mentions
IndicationAssociation:
  relationship_type: INDICATION | CONTRAINDICATION
  reliability: ReliabilityTier          # THE single quality metric
  drug: DrugRef                         # {id, label} -> self-contained outcome; full trail in drug_list (join by CHEBI id)
  disease: Mention                      # inlined: extracted per-indication, so its trail (incl. source_spans) lives here
  evidence: [EvidenceItem]              # ONLY indication-supporting evidence (the SPL/SmPC snippet)
  hyperrelations: [Hyperrelation]       # keep
```

`DrugRef` is `{id, label}` — the resolved drug identity, no `mention_id` pointer, so the association is
readable standalone (§3.4). The source paragraph lives on the inlined `disease.source_spans`
(a `TextSpan` list, §3.4) — so it sits *inside* the association ("at indication level") and each
`disease.steps[Extraction].input_value` is a substring of one span's `text`. `source_spans` is
multivalued because one indication can be assembled from two disconnected spans (a table reference, or a
separate "Limitations of Use" scope).

Key moves:

- **Drug is referenced, disease is inlined.** The drug is a shared entity (its full trail lives once in
  `drug_list.yaml`; the association carries a light `DrugRef` = `{id, label}`, no `mention_id` — every
  record stays self-contained, §3.4). The disease is *extracted from this label's text*, so its `Mention`
  (incl. the `Extraction` step with entailment + negation) is association-specific and inlined — exactly
  where the `VITAMIN A → hyperthyroidism` flag belongs.
- **Drop** `final_normalized_drug_id/label`, `final_normalized_disease_id/label` (→ `drug.id` /
  `disease.resolved_id`), `drug_disease` (dedup key — internal, not modelled), `fda/ema/pmda` booleans
  (→ `Drug.approvals` / evidence jurisdiction), `disease_grounding`/`disease_normalization` (→
  `disease.steps`). **`indications_text` is not dropped — it becomes the disease Mention's `source_spans`**
  (a `TextSpan` list, stored once), which the disease `Extraction.input_value` quotes a substring of.
- **Remove `evidence.confidence`.** Replaced by the record-level computed `reliability`.

**Reality note — today's shape (verified).** The current `Drug` (`src/medic/schema/drug.yaml`) is the flat
model: `curie`/`curie_label` + boolean jurisdiction flags (`approved_usa/europe/japan/india/russia/china`,
`marketing_status_usa`) + single-valued inlined `translation`/`grounding`/`normalization`. It has **no**
`mention`, `approvals`, or `reliability` slot — the target shape above is a full replacement, not an
extension. `RegulatoryStatus` (`src/medic/schema/authority.yaml`) already exists but is currently wired
only to `IndicationAssociation`; the re-model points `Drug.approvals` at it too. `on_label_merge._make_key`
and the `drug_disease` sort key rely on the dedup key being dropped, so keying logic must be reworked.

## 5. How reliability derives from the trail

The reliability gates (soft-launch spec) become a pure read over the mentions' `resolution.pipeline`:

- grounding gate ← the `GroundingStep`'s `quality`/`flags` on each mention.
- **recognition** gate ← the `ExtractionStep`'s flags (`hallucination` → EXCLUDED; `truncated_snippet` /
  `coreference_ambiguity` → MEDIUM). *As built (§2.1b): this is entity recognition only.*
- **assertion** gate ← the association's `Assertion` (`negated_inversion` → EXCLUDED;
  `over_extraction` / `wrong_section` / `wrong_pairing` → LOW; else by `confidence`). *As built: this
  replaces the original single "extraction gate", which conflated recognition with the claim.*
- translation gate ← the `TranslationStep` `status` (CONFIRMED → HIGH).
- provenance gate ← `evidence` presence.
- human override ← any step `status: CONFIRMED`/`REJECTED`, or a statement-level review row.

So `reliability` is *computed from* the model, and every gate points at a concrete slot — no hidden
logic, and a curator fixing a step's `status` raises the tier on rebuild.

**Reality note — this is a rewrite, not a re-wire.** Today `ReliabilityTier`/`StatementType` are
**Python-only enums** (`src/medic/reliability.py`), the tier is computed at export time and written **only
to `exports/medic_statements.tsv` / `medic_reliable.tsv`** — it appears on **no** product YAML (exactly
the "absent from the YAML" gap in §1). And the four gates currently read the **flat fields**
(`disease_grounding`, `evidence`, `final_normalized_*`), not `steps`. So this section is real work, not a
rename: (a) promote `ReliabilityTier` to a LinkML enum in the schema and add a `reliability` slot to
`Drug`/`IndicationAssociation`; (b) stamp it onto records at build time; (c) rewrite the gates in
`reliability.py` to read the mentions' `steps` instead of the flat fields. (§6 Task 5.)

## 6. Implementation plan (after this spec is approved)

1. Author `src/medic/schema/provenance.yaml` (standalone: enums, `TransformationStep`, 4 subclasses,
   `Mention`, `TextSpan`, vendored `Agent`). **Move** `PreprocessingRuleEnum` + `GroundingQualityEnum` +
   `NormalizationQualityEnum` out of `grounding.yaml` — and update `tests/test_grounding_schema.py` (pins
   the enum to `grounding.yaml`'s path against `RULE_CERTAINTY`/`RULE_PREDICATE` in
   `grounding/lexical/preprocess.py`) to the new path. Add `provenance.yaml` to `medic.yaml` imports.
2. Add the `*Flag` enums (§3.3) and wire the **existing** detectors to populate them: grounder → grounding
   flags; `validation/extraction_fidelity.py` (`entailment_score`, `assertion_negated`) → the extraction
   `entailment_score` + `negated_inversion` flag; translation `status`. *Note:* `over_extraction`,
   `hallucination`, `wrong_section` have **no detector today** — the slot is provided; population is
   curator- or future-detector-set.
3. Build `Mention` objects in the ingest/grounding path — reuse `mention.py` (`mint_mention_id` /
   `assign_mention` already exist) — one per resolved string, carrying its ordered `steps` + `source_spans`.
4. Re-model `Drug` (`schema/drug.yaml`) + `IndicationAssociation` (`schema/indication.yaml`); rewrite
   `merge/drug_merge.py` (`_merge_group`) + `merge/on_label_merge.py` (`_init_association`,
   `_carry_disease_grounding`, `_make_key`) to emit the new shape; regenerate the datamodel.
5. Promote `ReliabilityTier`/`StatementType` to LinkML enums, add the `reliability` slot, stamp it onto
   records at build, and **rewrite the four gates in `reliability.py`** to read `steps` instead of the flat
   fields (folds the soft-launch stamping work in here).
6. Update exports + validators — the concrete breakage points: `export/kgx.py` (reads
   `final_normalized_*`, `fda/ema/pmda`, `drug.curie`), `export/legacy.py` (`DRUG_LIST_COLUMNS` hardcodes
   `curie`/`approved_*` booleans), `export/sssom.py`, `reliability_export.py`, and `coverage.py` (reads
   `drug_list.yaml`). Then full rebuild + `just validate-all` (linkml-validate against the `*List` classes).
7. Docs: update `docs/reliability.md`, `SPEC.md` (I-8 now schema-enforced), `FAILURE_MODES.md`
   (flags cross-reference).

Migration note: this is a **breaking** product-schema change → it belongs in the `v2.0.0` release (the
one now on hold). No backwards-compat shim for the flat fields — they were the problem.

**Scope:** this spans six loosely-coupled subsystems (standalone schema · Mention construction · Drug
re-model + drug exports · IndicationAssociation re-model + KGX · reliability schema/stamp/gate-rewrite ·
validators + docs). Per the writing-plans scope check it should be built as a **sequence of plans**, not
one monster plan — the standalone `provenance.yaml` (step 1) is the self-contained foundation everything
else imports, so it is plan #1.

### 6.1 Status (as of 2026-08-08)

Steps 1–7 are **done**, with the §2.1 revisions. The flat identity/approval fields were removed and every
product regenerated; reads go through `src/medic/product_view.py`; `just validate-all` and the full test
suite are green.

Remaining, each with an issue:

| Open item | Issue |
|---|---|
| Disease-side `applied_rules` / predicate / `broadened` funnel (drug side only today) | `issues/issue_disease_grounding_applied_rules.md` |
| Drop the last additive leftovers — flat `atc_*` / `is_*` | `issues/issue_drop_flat_atc_is_fields.md` |
| Source-literal vs translation-store mismatches that force chain coercion | `issues/issue_provenance_chain_breaks.md` |
| Register the `w3id` redirect (the namespace is still a placeholder) | `issues/issue_register_transformation_provenance_w3id.md` |
| Detectors for the claim-level flags (`over_extraction`, `wrong_section`, `hallucination`) | `issues/issue_snippet_entailment_regulatory.md`, `issues/issue_assertion_type_negation.md` |

## 7. Why this is reusable

The `transformation-provenance` schema names nothing MeDIC-specific: any pipeline that resolves messy
source strings to canonical ids (ontology curation, ETL, NER/NEL systems) has the same
extract→translate→ground→normalize shape and the same failure surface. It ships as its own schema +
`w3id` id; MeDIC is just its first consumer. The `flags` enums are the one place a reuser would extend
for their domain's failure modes.

## 8. Decisions & open items

1. **Namespace** for the standalone schema — `w3id.org/monarch-initiative/transformation-provenance`?
   (needs a `w3id` redirect entry). **STILL OPEN** — the id is authored as a placeholder and does not
   resolve; the serving target must be decided (the schema currently lives inside the MeDIC repo) before
   the redirect is registered. Tracked in `issues/issue_register_transformation_provenance_w3id.md`.
2. **`Agent`** — vendor a fresh `{id, type, name}` into the standalone schema. Verified: MeDIC's existing
   `Agent` (`evidence.yaml`) is `{curator_id, curator_type: CuratorTypeEnum, name}` — a different shape, so
   "import MeDIC's" would force a rename + enum reconciliation. Vendoring wins on both reuse-cleanliness and
   avoiding that churn. (Resolved per reality-check, 2026-08-04.)
3. **Drug: reference vs inline** — resolved as *reference*, and `DrugRef` is `{id, label}` only (no
   `mention_id` join pointer): records are **self-contained**, the drug's full trail lives once in
   `drug_list.yaml` and is recoverable by CHEBI id (§3.4). Revisit only if a consumer needs the drug
   trail inlined into every association (accepting the duplication).
4. **`Extraction.input_value`** — the **minimal supporting quote** (a substring of one of the Mention's
   `source_spans[].text`, which holds the full source text once); there is no separate `snippet` slot. For
   structured sources (Orange Book / CDE columns) the span is a degenerate length-1 cell and `input_value`
   = that cell with `method: STRUCTURED_FIELD` — structured and free-text extraction share the class,
   structured extraction just never has hallucination flags. (Resolved per review, 2026-08-03.)
5. **`TextSpan` / `source_spans`** — lives on `Mention` (works for both a free-text paragraph and a
   structured cell), is **multivalued** (one indication can be assembled from two disconnected spans:
   a table reference, or a separate "Limitations of Use" scope), and carries `{text, source_reference,
   section_code}` — the latter two **migrated off `Extraction`**, killing that duplication. *Deferred until
   a consumer needs them:* character offsets, page/line numbers, and a per-span `role` (assertion vs
   disease vs limitation). (Resolved per review, 2026-08-04.)
6. **Hyperrelations** — kept as-is for now. Now that §2.1(b) separates entity recognition from the claim,
   the natural home for a hyperrelation is its **own `Assertion`** (a symptom-level claim about the same
   drug/disease pair), not an extraction step. Revisit when hyperrelations are next touched.
7. **Aggregate `confidence` semantics** — resolved as the **product** of the step confidences (missing
   treated as 1.0), so a long or shaky chain compounds. It is a secondary numeric summary only: the
   categorical `reliability` tier remains THE quality metric, and it reads `quality`/`flags`, not this
   float. (Resolved per review, 2026-08-08.)
8. **Combination splits vs the chaining invariant** — resolved: a combination is *one product string that
   decomposes into several entities*, each with its own strictly-linear chain, so the invariant never
   fans out. Component ids ride as `components[]` metadata on the grounding step; combinations are not
   modelled as first-class child mentions. Revisit only if a consumer needs per-component trails.
   (Resolved per review, 2026-08-08.)
