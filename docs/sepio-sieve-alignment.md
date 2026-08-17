# MeDIC and SEPIO/sieve — how they relate, and what adoption would cost

Companion to [`docs/provenance-walkthrough.md`](provenance-walkthrough.md) and
`specs/2026-08-09-source-scoped-association-provenance-design.md` (§9 summarises this document).

Reference points: `~/ws/projects/sieve` — `schema/minimal.yaml` (the promotable "minimal
microschema", `id: https://w3id.org/sepio/minimal`, status **PROVISIONAL**), `schema/sieve.yaml`
(the full curation model), `schema/sepio_classes.yaml` (the trimmed SEPIO base), and
`specs/2026-07-31-minimal-microschema-and-sieve-alignment-design.md`.

---

## 1. Why this document exists

MeDIC and sieve model the same subject matter — a claim about a drug and a disease, with something
backing it — and arrive at almost disjoint schemas. That is not an accident or a failure of
coordination. They answer different questions, and the difference propagates all the way down to how
confidence arithmetic works. This document explains the divergence, maps the two models onto each
other class by class, draws the line between evidence and provenance, and sets out concretely what
MeDIC would have to build to be a SEPIO-style evidence system.

---

## 2. Two ways of thinking

### MeDIC: a pipeline audit trail

MeDIC's core abstraction is the **transformation chain**. A verbatim source string enters, a
canonical ontology id leaves, and every step in between is named, typed, and records both its
incoming and its outgoing value (invariant I-8). The defining property is *contiguity*:

```
pipeline[n].output_value == pipeline[n+1].input_value
```

Order matters and adjacency is constrained. The chain is **monotone** (each step moves strictly
forward), **mechanical** (no step is a judgement about the world), and **replayable** — the SSSOM and
Babelon decision stores make a rebuild byte-identical and offline. MeDIC's question is:

> *How did this string become this ID, and would I get the same answer again?*

Nothing in the chain argues that the claim is true. The chain would look identical if the regulator
had made a mistake.

### sieve/SEPIO: an epistemic assessment

sieve's core abstraction is the **evidence line**: a set of items bearing on a proposition, carrying
a `direction_of_evidence_provided` (`SUPPORTS` / `REFUTES` / `NEUTRAL`) and a
`strength_of_evidence_provided` (`STRONG` / `MODERATE` / `WEAK`). Lines accumulate; a curator reads
them and accepts, rejects, or flags the packet. sieve's question is:

> *Should I believe this claim?*

`EvidenceLine.has_evidence_items` is `multivalued: true, inlined_as_list: true` — a **bag**. There is
no ordering semantics and no constraint between adjacent items. Evidence is **dialectical**: two
lines can point in opposite directions, and that conflict is meaningful (`CONTROVERSIAL` is a
first-class curation status).

### The mismatch, stated sharply

**MeDIC's chain is a sequence with an equality constraint between adjacent elements. sieve's evidence
line is a set.** That is the single deepest incompatibility, and it is structural rather than
cosmetic: any mapping of a MeDIC chain into a sieve line loses the ordering and the contiguity
guarantee unless the chain survives intact *inside one item*.

### Side by side

| | MeDIC provenance | sieve / SEPIO |
|---|---|---|
| Question | *How did this string become this ID?* | *Should I believe this claim?* |
| Core abstraction | transformation chain — ordered, contiguous, replayable | evidence line — an unordered bag of items |
| Polarity | none; every step is forward | `SUPPORTS` / `REFUTES` / `NEUTRAL` |
| Conflict | impossible by construction | expected; `CONTROVERSIAL` is a status |
| Confidence maths | **decays** multiplicatively along a chain | **accumulates** across lines (Net Evidence Ratio) |
| What confidence means | linking fidelity | degree of belief |
| Truth of the claim | assumed — a regulator asserted it | the thing under review |
| Curation locus | the decision stores (`mappings/*.sssom.tsv`), **upstream**, then rebuild | the packet in DuckDB, **downstream**, in a UI |
| System type | deterministic **build system** | stateful **workflow system** |
| Failure mode captured | mis-*linking* (wrong ID for a string) | mis-*believing* (wrong claim) |
| Identity | `MEDICNE:` uuid5 per surface form | items are usually anonymous |

---

## 3. Where evidence ends and provenance begins

This is the question that decides most of the modelling choices below, so it is worth being precise.
There are **three** distinct concepts, not two, and MeDIC currently mixes all three in places.

### The three questions

**Provenance — *where did this record come from and how was it produced?***
The derivation history of the data artifact. W3C PROV territory: entities, activities, agents.
Answerable with no opinion whatever about whether the claim is true. MeDIC's transformation chain is
pure provenance: `Этифоксин → Etifoxin → CHEBI:135272`, by DeepL then by the lexical grounder.
Monotone, mechanical, replayable.

**Evidence — *what are the reasons to believe or disbelieve the claim?***
Epistemic support for a proposition. Requires a proposition to be *about*, and has polarity and
weight. SEPIO territory. Crucially, evidence is **external to the data-production process**: "three
regulators independently approved this" is evidence; "we translated the drug name with DeepL" is not
evidence for the claim, no matter how relevant it is to trusting the record.

**Data quality — *how faithfully does this record represent its source?***
The fidelity of the artifact to what the source actually said. Not the truth of the claim, and not
the history of production — it is the **derivative of provenance**: quality signals are read *off*
the chain. `unreviewed_machine`, `broadened`, `hallucination`, `scope_narrowed`, a blank
`subject_id` — each says the pipeline may have introduced error.

### A test for classifying any field

1. **Does it survive if the claim turns out to be false?** If yes → provenance. (The translation
   still happened.)
2. **Would it change if a different pipeline produced the identical record?** If yes → provenance or
   data quality, never evidence.
3. **Would a domain expert who trusts your pipeline completely still want to see it?** If yes →
   evidence.

### Applied to MeDIC

| Field | Kind | Note |
|---|---|---|
| `resolution.pipeline[]`, `input_value`/`output_value`, `tool`, `agent`, `*_version` | **provenance** | the derivation history |
| step `quality`, `flags`, `applied_rules`, `confidence`, `confidence_basis` | **data quality** | derived from provenance; about fidelity, not truth |
| `evidence.jurisdiction`, `approval_status`, `approval_date`, `authority`, `regulatory_status` | **evidence** | "FDA approved this" is a reason to believe |
| `evidence.confidence` (HIGH/MEDIUM/LOW), `source_role` (PRIMARY/INTERMEDIARY) | **evidence** | how good this reason is |
| `spans[].text` (the quote) | **both** | see below |
| `reliability` (cross-jurisdiction agreement) | **evidence synthesis** | an aggregation over independent sources |
| `assertion.trigger_cue`, `trigger_span`, `section_warrant` | **data quality** | how sure we are we *read* the claim right |
| `assertion.flags` (`negated_inversion`, `over_extraction`, `wrong_section`) | **data quality** | extraction failure modes, not counter-evidence |

**The quote is genuinely both**, and that is not a defect of either model. `spans[].text` is
provenance (this is the string we read) *and* evidence (this is what the regulator asserted). SEPIO
resolves it by making the quote an evidence **item** (`TextSpan.value` hanging off an
`EvidenceLine`); MeDIC treats it as provenance (`Mention.source_spans`). Both are coherent; the
difference is what the quote is attached to and therefore what it is taken to be arguing.

### The consequence that matters most

**Every confidence number in MeDIC today is a data-quality number, not an evidence number.**

`0.765` on the etifoxine record does **not** mean "there is a 76.5% chance etifoxine treats anxiety".
It means "we are 76.5% confident we linked the right CHEBI id". These are different quantities on
different scales measuring different things, and a downstream consumer reading
`score_of_evidence_provided: 0.765` will read it as belief.

> **This revises §9 note 1 of the design spec.** That note proposed mapping MeDIC's per-assertion
> `confidence.overall` onto sieve's `score_of_evidence_provided`. That is wrong, for the reason
> above. The correct mapping is:
>
> - **Evidence strength** ← MeDIC's `evidence.confidence` (HIGH/MEDIUM/LOW) combined with
>   `source_role` (PRIMARY beats INTERMEDIARY). These are already evidence-kind judgements about how
>   good a reason the source is. They map to `strength_of_evidence_provided` (STRONG/MODERATE/WEAK).
> - **Chain confidence** → **not** an evidence score. It is a **gate**: a low linking confidence
>   should suppress or flag the line, not weaken the belief in a claim that was correctly linked.
>   Emit it as a data-quality annotation on the item, never as the line's score.
>
> The spec has been updated to match.

Getting this wrong in an integration would silently convert "we're unsure this is the right CHEBI
id" into "the evidence for this treatment is weak" — a category error that would propagate into any
consumer's ranking.

---

## 4. What MeDIC already has (more than expected)

Three findings from the current schema that change the cost estimate:

**MeDIC already contains a near-copy of sieve's kernel enum.** `EvidenceSourceTypeEnum`
(`src/medic/schema/evidence.yaml:145`) is `HUMAN_CLINICAL`, `MODEL_ORGANISM`, `IN_VITRO`,
`COMPUTATIONAL`, `OTHER` — sieve's minimal-kernel `EvidenceSource` minus `EXPERT_CONSENSUS`. It was
arrived at independently and it is the same axis.

**But MeDIC has two parallel evidence-kind vocabularies applied to different products.**
`SourceTypeEnum` (`evidence.yaml:25`) is `REGULATORY` / `LITERATURE` / `GUIDELINE` / `DATABASE` /
`POST_MARKET` — used on regulatory evidence. `EvidenceSourceTypeEnum` is used on
`products/research_list.yaml`. sieve splits these two axes cleanly (`DocumentType` = what kind of
document; `EvidenceSource` = what kind of study), and MeDIC's split does not align with either. Note
also that `EvidenceSourceTypeEnum`'s own description reads *"The provenance/source of the evidence"*
— the §3 conflation, written into the schema.

**MeDIC already has curation state, on one product.** `products/research_list.yaml` carries
`curation_status: DRAFT`, `curator`, `curator_type: AI_AGENT`, and `curation_date`. That is the seed
of a workflow model, present in exactly one of five products.

The practical upshot: MeDIC's regulatory half and research half already use different evidence
models, and unifying them is work that has to happen regardless of sieve. Doing that unification
*against the kernel's axes* costs little more than doing it ad hoc.

---

## 5. Structural correspondence

| MeDIC (post-redesign) | sieve / minimal kernel | Fit |
|---|---|---|
| pair record | `EvidencePacket` / `EvidencedClaim` | **good** |
| `(drug_id, relationship_type, disease_id)` | `SieveStatement` (`subject` / `predicate: Coding` / `object`) | **good** — `relationship_type` becomes a `Coding` |
| `assertions[i]` (one per source document) | `EvidenceLine` | **good** — the kernel's guidance is one line per source, which is exactly D2 |
| `evidence.confidence` + `source_role` | `strength_of_evidence_provided` | **good** (per §3) |
| `spans[i]` | `TextSpan` (shape **A**): `value` + `reported_in: Document` | **good** — `text`→`value`, `document`→`reported_in.id` |
| `spans[i].role`, `.section_code` | belongs on `TextMiningResult` (shape **C**) | **gap — C is unimplemented** |
| `ExtractionStep` | `TextMiningResult` (`extraction_score`, `document_section`, `text_location`, `extraction_method`) | close, but C does not exist and has no structured offsets |
| `GroundingStep` / `NormalizationStep` | `ConcordanceItem` (`mapping_justification`, `mapping_set`, `source_*`) | **surprisingly good** — a MeDIC grounding decision *is* an SSSOM row with a `semapv:` justification, which is what `ConcordanceItem` is for |
| `TranslationStep` | nothing | **gap** |
| `Resolution` (the ordered chain) | nothing | **the fundamental gap** |
| `MEDICNE:` uuid5 ids | nothing (items usually anonymous) | MeDIC strength; keep |
| `reliability` (HIGH/MEDIUM/LOW) | `EvidenceSynthesis` + `Score` | right idea, bespoke rules |
| chain `confidence` | nothing (deliberately — see §3) | keep MeDIC-side as data quality |
| `EvidenceSourceTypeEnum` | `EvidenceSource` | near-identical, missing `EXPERT_CONSENSUS` |
| regulatory approval as an evidence kind | `EvidenceSource` has no `REGULATORY` | **gap — contributable** |
| a drug label as a document | `DocumentType` has no `REGULATORY_LABEL` | **gap — contributable** |
| — | `Direction` (`SUPPORTS`/`REFUTES`/`NEUTRAL`) | **MeDIC has no counterpart** |
| — | `CurationStatus`, `CurationDecision` | MeDIC has these on `research_list` only |

Note what rows 7–9 imply: **a single MeDIC chain decomposes across two different sieve item types** —
extraction/translation → `TextMiningResult`, grounding/normalization → `ConcordanceItem` — and the
sequencing between them has no home. That is the concrete cost of set-vs-sequence.

---

## 6. What MeDIC would need to build

Five stages, each independently valuable and each shippable alone. Stages 0–2 are worth doing on
their own merits even if sieve never enters the picture; stages 3–4 are only worth it if MeDIC
actually feeds a curation system.

### Stage 0 — Annotate (cost: hours)

Pin `class_uri` / `slot_uri` on MeDIC's classes to their SEPIO equivalents. No structural change, no
import, no behaviour change. This is precisely how sieve itself is SEPIO-aligned without importing
SEPIO, and the 07-31 spec endorses the pattern explicitly.

- `TextSpan` → `class_uri: sepio:DataItem`; `text` → `slot_uri: sepio:value`; `document` →
  `slot_uri: sepio:reported_in`
- `SourceAssertion` → `class_uri: sepio:EvidenceLine`
- pair record → `class_uri: sepio:Statement`
- `GroundingStep` / `NormalizationStep` gain a `mapping_set` slot alongside the existing
  `mapping_justification`, making them `ConcordanceItem`-shaped

**Buys:** a later `is_a` re-base becomes mechanical rather than a rewrite. **Do this now**, inside
the §5 schema work the design spec already requires.

### Stage 1 — Unify MeDIC's own evidence vocabulary (cost: days)

Independent of sieve, forced by §4's finding.

- Collapse `SourceTypeEnum` and `EvidenceSourceTypeEnum` onto the kernel's two axes: **what kind of
  document** (`DocumentType`) versus **what kind of study/observation** (`EvidenceSource`).
- Add `REGULATORY` to the study axis and `REGULATORY_LABEL` to the document axis (§8).
- Apply the unified vocabulary to `research_list.yaml` *and* the regulatory products, so MeDIC has
  one evidence model rather than two.
- Fix the `EvidenceSourceTypeEnum` description, which currently says "provenance/source of the
  evidence".

**Buys:** regulatory and literature evidence become comparable; the ECO hook becomes possible.

### Stage 2 — Add the missing evidence concepts (cost: 1–2 weeks)

The real semantic gain, and the point at which MeDIC starts *modelling evidence* rather than
recording provenance.

- **`direction` on each assertion.** Everything in MeDIC is implicitly `SUPPORTS`. There is no way to
  say "this source states the drug is *not* indicated for this". MeDIC has that data already —
  `LIMITATION_STATEMENT` spans (see the UBRELVY case in the design spec §4.3) and the whole
  contraindication product — with nowhere to put it. **This is the single largest semantic gain
  available.**
- **`strength` as an axis distinct from chain confidence**, populated from `evidence.confidence` +
  `source_role` per §3.
- **A `Document` class.** Today the document is a scattering of strings — `setid`, `reference`,
  `source_document_url`, `regulatory_document_url`. Promote it to an object with `id`, `title`,
  `document_type`. The design spec's D2 already makes the document the unit of an assertion, so this
  is a small step from there.
- **`EvidenceSynthesis`.** `reliability` already *is* a synthesis; give it the shape
  (`summary`, `score`, `direction`, `cited_evidence`) so the reasoning is inspectable rather than
  implicit in `reliability.py`.
- **ECO codes** per evidence item, refining the coarse `EvidenceSource`.

**Buys:** MeDIC can express disagreement between sources, which today it structurally cannot.

### Stage 3 — Import the kernel (cost: days, but blocked)

Replace MeDIC's hand-rolled `TextSpan` / evidence classes with `is_a` the kernel's, plus mixins for
MeDIC-specific slots — the pattern sieve uses (`is_a` the minimal class, `mixins:
[InformationEntityProvenance, CuratedEvidence]`).

**Blocked on three sieve-side items**, all open as of 2026-07-31: B1 (the kernel's schema id is
unconfirmed and is baked into every downstream import), B2 (the `meaning:` IRIs are placeholders
pending OLS lookup), and phase 3 (`TextMiningResult`, the shape MeDIC would actually use, exists only
as a comment in `minimal.yaml`).

**Recommendation: do not attempt until those close.** Stage 0 makes waiting cheap.

### Stage 4 — Curation integration (cost: weeks, and architecturally invasive)

This is where the two systems genuinely fight, because **MeDIC is a deterministic build system and
sieve is a stateful workflow system**. MeDIC's guarantee is that a rebuild is byte-identical; a
curation status living *in the product* is mutable state that a rebuild would destroy.

The resolution is a hard constraint, and it should be written down before any code:

> **Curation decisions are inputs to the MeDIC build, never outputs of it.**

Concretely, a curator's verdict must land in a git-tracked decision store — the same architectural
slot as `mappings/*.sssom.tsv` — which the build *reads*. A verdict that lives only in sieve's DuckDB
packet store is **silently reverted by the next MeDIC rebuild**. See §7.

---

## 7. Hazards

**H1 — Two curation surfaces for one fact.** The top integration risk, restated from §6 stage 4
because it is the one that will actually bite. MeDIC curates upstream and rebuilds; sieve curates
downstream in a UI. Without a write-back path into the stores, every sieve decision has a silent
expiry date set by the next `just merge`. Settle the direction of flow before any data moves.

**H2 — The confidence category error.** Covered in §3. Mapping chain confidence onto
`score_of_evidence_provided` converts "unsure of the CHEBI id" into "weak evidence for the
treatment".

**H3 — Do not collapse contraindications into `REFUTES`.** Tempting once `direction` exists, but in
SEPIO terms "X is indicated for Y" and "X is contraindicated in Y" are *different statements with
different predicates*, not opposing evidence about one statement. Keep the products separate. The
genuinely `REFUTES`-shaped signal is a source stating a non-indication — the `LIMITATION_STATEMENT`
span. Note that the kernel's `Direction` docstring explicitly excludes operational/QC signals from
the enum, which correctly rules out MeDIC's `negated_inversion` flag as a direction value.

**H4 — Net Evidence Ratio is degenerate on MeDIC data.** `(S⁺−S⁻)/(S⁺+S⁻+S⁰)` with no `REFUTES`
lines is always exactly 1.0. Until stage 2 lands `direction`, computing NER over MeDIC packets
produces a constant and tells a curator nothing.

**H5 — Losing the chain.** If MeDIC data is ever exported as sieve packets, the chain must survive as
an ordered structure inside a single item. Flattening it into `has_evidence_items` discards the I-8
contiguity guarantee, which is the property the entire provenance model exists to provide.

---

## 8. What MeDIC should contribute upward

sieve's kernel blocker B2 (real `meaning:` IRIs, to be looked up rather than invented) is still open,
so the vocabulary is not frozen. Good timing for three contributions:

1. **`EvidenceSource: REGULATORY`** — regulatory approval is a distinct evidence kind. Forcing a
   drug label into `EXPERT_CONSENSUS` or `DATABASE_RECORD` is wrong, and MeDIC is 9,332 rows of the
   use case.
2. **`DocumentType: REGULATORY_LABEL`** — same argument on the document axis.
3. **Structured character offsets on `TextMiningResult`.** The 07-31 spec defers these: *"Structured
   character offsets on `TextMiningResult` stay a single `text_location` string for now; a structured
   span type is a future extension."* The design spec's `char_start` / `char_end` / `co_mentions` is
   that extension, with a concrete requirement behind it. Feeding MeDIC's needs into sieve phase 3
   is cheaper than inventing a parallel vocabulary and reconciling later.

MeDIC's `MEDICNE:` surface-form identity layer is also worth proposing: the kernel has no notion of a
stable id for a mention, and it is what makes MeDIC's SSSOM joins work.

---

## 9. Recommendation

Do **stage 0 now**, folded into the schema work the design spec already requires — it is annotations
on classes being rewritten anyway, and it makes everything later cheaper.

Do **stage 1 and stage 2 on their own merits**, on MeDIC's schedule, because MeDIC needs one internal
evidence vocabulary and needs to be able to express a non-indication regardless of what sieve does.

Do **not** import the kernel (stage 3) until B1, B2 and phase 3 close. Revisit then.

Treat **stage 4 as a separate project** with H1 settled in writing first.

The summary judgement: MeDIC should become *SEPIO-shaped* without becoming *SEPIO-dependent*. Its
distinctive contribution — the replayable transformation chain — has no counterpart in SEPIO and
should not be bent to fit one. What it lacks, and should adopt, is the vocabulary of evidence:
direction, strength, document, and a synthesis that shows its reasoning.
