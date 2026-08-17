# Source-scoped association provenance — Design Spec

*Date: 2026-08-09*

**Status:** implemented, 2026-08-10 (uncommitted). See §12 for what shipped, what deviated, and
the verification numbers. Tracking issue: `issues/issue_source_scoped_provenance.md`.

**Relationship to prior specs.** `specs/2026-07-28-transformation-provenance-model-design.md`
defines the *entity-level* model — `Mention`, `Resolution`, the ordered `TransformationStep`
pipeline. That model is sound and is kept unchanged. This spec replaces its §4 (MeDIC adoption,
`products/indication_list.yaml`) with an *association-level* model, because the entity model was
grafted onto a merge that has no concept of a source. Everything in §3 of the 07-28 spec stands.

---

## 1. Motivation

A review of two complete records from `products/indication_list.yaml` (built 2026-08-09) found an
association whose disease trail came from India and whose drug trail came from Russia, on a record
whose asserted text was Indian. The drug's pipeline traced the Cyrillic string `Этифоксин` — a string
that appears nowhere in the Indian source. Each `Mention` was internally consistent; the
*association* was not.

This is not an isolated defect. Three mechanisms give a single association three different
provenance scopes:

| Component | Scope today | Mechanism |
|---|---|---|
| `drug` mention | **merge-global** | `_build_drug_identities()` (`on_label_merge.py:214`) maps canonical CURIE → the one identity elected by the drug merge; `_init_association` (line 1106) stamps it onto every association naming that drug |
| `disease` mention | **first source wins** | `_build_disease_provenance()` builds it from the first record's `evidence[0]`; `_merge_into()` never rebuilds it, so later sources' disease literals are discarded |
| `evidence` / `regulatory_status` | **all sources** | `_make_key()` (line 414) is `drug_id\|disease_id\|relationship_type` — no source — so every jurisdiction collapses into one record |

Measured consequences on the current build:

- **462 of 6,338 associations (7.3%) carry a drug mention with no `resolution` at all** — the same
  462 that carry no `mention_source`. These are drugs absent from `drug_list.yaml`; the fallback
  branch writes `original_literal: <canonical label>`, so e.g. an association whose source said
  `THIOSULFATE ION` records `original_literal: thiosulfate(2-)`, the CHEBI label. **That is a
  verbatim-fidelity violation (I-7) and a traceability violation (I-8): the record asserts a source
  literal no source ever emitted.**
- **~3,000 document-level attestations are silently dropped.** Sources supply 9,332
  indication rows (DailyMed 4,024 / EMA 3,195 / PMDA 1,976 / India 137) which collapse to 6,338
  associations. DailyMed alone contributes 4,024 rows over 3,357 distinct pairs — 667 duplicate
  pairs from *different* SPLs — and `_merge_into`'s `_ev_key` of `(jurisdiction, source)` keeps one.
  Two independent SPLs agreeing is corroborating signal that is currently thrown away.
- **85 disease resolutions omit the `NORMALIZATION` step; 0 drug resolutions do.** Identity
  normalization is emitted inconsistently, so a reader cannot tell "no-op" from "not recorded".
- **`quality: none`** is the enum value for identity normalization, which reads as missing data.
- **Aggregate confidence is a product over whichever steps happen to declare one.** Steps with no
  measured confidence (`STRUCTURED_FIELD` extraction, DeepL translation, identity normalization)
  contribute nothing, so `resolution.confidence` usually just echoes the grounding confidence. An
  unreviewed machine translation flagged `unreviewed_machine` costs the record exactly zero.
- **Spans are inconsistently populated and structurally flat.** Only DailyMed carries a
  `section_code`; India carries `text` only. Worse, `indications_text` concatenates a section header,
  the indication sentence, a subsection header, and a scope-limiting sentence into one string — see
  §4.3, where this causes a live negation-detection bug.

Separately, and needed by the fix: **8,883 of 21,179 rows (42%) in `mappings/drug_grounding.sssom.tsv`
and 2,138 of 7,273 (29%) in `disease_grounding.sssom.tsv` have a blank `subject_id`** (no MEDICNE
id). `GroundingStoreView.decision_for` masks this with a normalized-literal fallback
(`grounding_store_view.py:133`), but it is an I-4 hole.

---

## 2. Scope

**In scope.** The shape of `products/indication_list.yaml` and `products/contraindication_list.yaml`;
`src/medic/merge/on_label_merge.py`; `src/medic/schema/provenance.yaml` and `indication.yaml`;
`src/medic/product_view.py` accessors; the consumers listed in §8.

**Out of scope.** The entity-level `Mention`/`Resolution`/`TransformationStep` model (07-28 spec §3)
— reused verbatim. `products/drug_list.yaml` keeps its `identity` Mention. The grounding and
translation decision stores keep their current format. No change to ingesters' source isolation.

**Non-goals.** Importing the sieve/SEPIO schema (see §9, decision A6). Modelling contraindications
as negative evidence on indication claims (§9, note 2). A curation UI.

---

## 3. Decisions

Locked in brainstorming, 2026-08-09.

- **D1 — Two-level record.** The top level stays one canonical
  `(drug_id, disease_id, relationship_type)` pair. All provenance moves into an `assertions` list.
  Rejected: a flat one-record-per-source product, because `reliability.py`, `kgx.py` and
  `reliability_export.py` all consume a per-pair reliability and would each need their own grouping
  logic.
- **D2 — One assertion per source *document*.** Not per source system, not per jurisdiction. A
  DailyMed setid, an EMA EPAR, a PMDA review PDF, a CDSCO entry. Rationale: an assertion's span text,
  its chain's `input_value` and its evidence must come from the same document for the chain to be
  coherent; document grain is the only grain where that is true by construction. It also recovers the
  ~3,000 dropped attestations.
- **D3 — An assertion is internally single-source, enforced.** `drug.mention_source ==
  disease.mention_source == assertion.source`, and every span's `document` equals the assertion's
  `document`. Validated, not merely intended (§7).
- **D4 — The drug mention is built per-assertion, exactly like the disease mention.**
  `_build_drug_identities()` and the merge-elected identity are deleted. The per-source drug literal
  comes from `evidence.original_drug_label`; its trail is recovered from the grounding store by the
  same `GroundingStoreView.decision_for` path the disease already uses. Verified feasible: the store
  holds the per-source literals as distinct rows (`ALBUTEROL`, `ALBUTEROL SULFATE`,
  `ALBUTEROL SULFATE; BUDESONIDE`).
- **D5 — Extraction stays scalar; co-mentions are informational.** `output_value` remains a single
  string so `pipeline[n].output_value == pipeline[n+1].input_value` holds exactly. A `co_mentions`
  list records the other entities found in the same span, each with its minted MEDICNE id and
  character offsets. Rejected: a list-valued `output_value` with `selected_index`, which would force
  every validator and replay tool to special-case the extraction step.
- **D6 — Spans are multivalued and typed.** `spans` is a list; each span carries a required `role`
  from a new `TextSpanRoleEnum`, plus optional `section_code` and a `document` reference. DailyMed's
  header is split from its body. Sources with no text emit no spans rather than `snippet: ''`.
- **D7 — Confidence is a required four-part object with a declared basis.** Every step declares a
  confidence *and* a `confidence_basis` of `MEASURED` / `DETERMINISTIC` / `PRIOR`. Deterministic
  steps are legitimately 1.0 (an identity transform cannot be wrong); unmeasured non-deterministic
  steps take a calibrated per-method prior from config and are marked `PRIOR`, so an unreviewed
  machine translation finally costs something.
- **D8 — Pair-level confidence aggregates by noisy-OR** over its assertions: `1 − Π(1 − cᵢ)`. Two
  independent regulators at 0.7 give 0.91 — corroboration raises confidence, which is the point of
  collecting jurisdictions. Categorical `reliability` keeps its current cross-jurisdiction-agreement
  semantics and is unaffected.

---

## 4. The record model

### 4.1 Shape

```yaml
- drug_id: CHEBI:135272                 # canonical pair identity
  drug_label: etifoxine
  disease_id: MONDO:0011918
  disease_label: anxiety
  relationship_type: INDICATION
  reliability: HIGH                     # cross-source agreement (semantics unchanged)
  confidence:
    overall: 0.91                       # noisy-OR over assertions (D8)
    method: NOISY_OR
    n_assertions: 2

  assertions:
  - source: CDSCO                       # the ingester
    jurisdiction: INDIA
    document: CDSCO:approved-new-drugs-2024-03-21
    spans: [...]                        # §4.3
    drug:    {…Mention, trail from THIS document's literal…}
    disease: {…Mention, trail from THIS document's literal…}
    assertion:
      relationship: INDICATION
      trigger_cue: indication_phrase
      trigger_span: Indicated for
      flags: []
      confidence:
        subject: 0.72                   # == drug.resolution.confidence
        object: 1.0                     # == disease.resolution.confidence
        relationship: 1.0
        overall: 0.72                   # product of the three
        basis: MEASURED                 # weakest basis in the chain
    evidence:          {jurisdiction: INDIA, approval_date: '20240321', …}   # singular
    regulatory_status: {authority: CDSCO, status: APPROVED, …}               # singular
  - source: GRLS
    jurisdiction: RUSSIA
    …
```

Four structural changes:

1. **`evidence` and `regulatory_status` become singular inside an assertion.** One document attests
   one thing. The pair-level lists survive only as derived views — which is what
   `_dedup_evidence_prefer_primary` and `_build_regulatory_status_from_evidence` become.
2. **`_make_key` gains the document**: `drug_id|disease_id|relationship|source|document`.
   `_merge_into` stops merging evidence into a shared record and appends an assertion instead.
3. **`_build_drug_identities` is deleted** (D4).
4. **Source-consistency becomes stateable and testable** (D3).

### 4.2 Worked example — the cross-source record, rebuilt

The record from §1, correctly split. Note that each assertion now traces *its own* document's
literals, and Russia's disease literal (`тревога`) — currently discarded — is preserved with its own
translation step.

```yaml
- drug_id: CHEBI:135272
  drug_label: etifoxine
  disease_id: MONDO:0011918
  disease_label: anxiety
  relationship_type: INDICATION
  reliability: HIGH
  confidence: {overall: 0.913, method: NOISY_OR, n_assertions: 2}

  assertions:
  - source: CDSCO
    jurisdiction: INDIA
    document: CDSCO:approved-new-drugs-2024-03-21
    spans:
    - role: TABLE_CELL
      document: CDSCO:approved-new-drugs-2024-03-21
      text: Indicated for psychosomatic manifestations of anxiety
    drug:
      entity_type: drug
      id: MEDICNE:…                                  # minted from India's literal
      mention_source: CDSCO
      original_literal: Etifoxine Hydrochloride capsules 50 mg
      resolved_id: CHEBI:135272
      resolved_label: etifoxine
      resolution:
        input_value: Etifoxine Hydrochloride capsules 50 mg
        output_value: CHEBI:135272
        confidence: 0.72
        confidence_basis: MEASURED
        pipeline:
        - category: EXTRACTION
          method: STRUCTURED_FIELD
          input_value: Etifoxine Hydrochloride capsules 50 mg
          output_value: Etifoxine Hydrochloride capsules 50 mg
          quality: verbatim
          confidence: 1.0
          confidence_basis: DETERMINISTIC
          tool: medic-ingest-india
          tool_version: 1.0.0
        - category: GROUNDING
          method: LEXICAL_MATCH
          input_value: Etifoxine Hydrochloride capsules 50 mg
          output_value: CHEBI:135272
          output_label: etifoxine
          predicate_id: skos:closeMatch
          applied_rules: [salt_strip, formulation_strip]
          quality: lexical_exact_surgery
          flags: [broadened]
          confidence: 0.72
          confidence_basis: MEASURED
          source_vocabulary: CHEBI
          tool: medic-lexical-grounder
          tool_version: '1'
        - category: NORMALIZATION
          method: DETERMINISTIC_RULE
          input_value: CHEBI:135272
          output_value: CHEBI:135272
          output_label: etifoxine
          quality: identity                          # was `none`
          confidence: 1.0
          confidence_basis: DETERMINISTIC
          target_namespace: CHEBI
          tool: medic-normalizer
          tool_version: '1'
    disease:
      entity_type: disease
      id: MEDICNE:45674d8c-f70c-5ee4-a60d-2c4eab902a0d
      mention_source: CDSCO
      original_literal: anxiety
      resolved_id: MONDO:0011918
      resolved_label: anxiety
      resolution:
        input_value: Indicated for psychosomatic manifestations of anxiety
        output_value: MONDO:0011918
        confidence: 1.0
        confidence_basis: MEASURED
        pipeline:
        - category: EXTRACTION
          method: LLM
          agent: {agent_name: anthropic/claude-haiku-4-5-20251001,
                  agent_type: AI_AGENT, agent_version: claude-haiku-4-5-20251001}
          span_role: TABLE_CELL
          input_value: Indicated for psychosomatic manifestations of anxiety
          output_value: anxiety
          char_start: 46
          char_end: 53
          quality: verbatim
          confidence: 1.0
          confidence_basis: MEASURED
          co_mentions: []
          tool: medic-extractor
          tool_version: '1'
        - category: GROUNDING
          method: LEXICAL_MATCH
          input_value: anxiety
          output_value: MONDO:0011918
          output_label: anxiety
          predicate_id: skos:exactMatch
          quality: lexical_exact
          confidence: 1.0
          confidence_basis: MEASURED
          source_vocabulary: MONDO
          tool: medic-lexical-grounder
          tool_version: '1'
        - category: NORMALIZATION                    # now always emitted
          method: DETERMINISTIC_RULE
          input_value: MONDO:0011918
          output_value: MONDO:0011918
          output_label: anxiety
          quality: identity
          confidence: 1.0
          confidence_basis: DETERMINISTIC
          target_namespace: MONDO
          tool: medic-normalizer
          tool_version: '1'
    assertion:
      relationship: INDICATION
      method: LLM
      agent: {agent_name: anthropic/claude-haiku-4-5-20251001,
              agent_type: AI_AGENT, agent_version: claude-haiku-4-5-20251001}
      trigger_cue: indication_phrase
      trigger_span: Indicated for
      flags: []
      confidence: {subject: 0.72, object: 1.0, relationship: 1.0,
                   overall: 0.72, basis: MEASURED}
    evidence:
      jurisdiction: INDIA
      approval_date: '20240321'
      approval_status: APPROVED
      confidence: HIGH
      explanation: CDSCO-approved indication from India primary source PDF
      original_drug_label: Etifoxine Hydrochloride capsules 50 mg
      original_disease_label: anxiety
      reference: https://cdsco.gov.in/opencms/opencms/en/Approval_new/Approved-New-Drugs/
      source_role: PRIMARY
      source_type: REGULATORY
    regulatory_status:
      authority: CDSCO
      status: APPROVED
      approval_date: '20240321'
      source: CDSCO
      source_role: PRIMARY

  - source: GRLS
    jurisdiction: RUSSIA
    document: GRLS:…
    spans: []                                        # Russia carries no indication text
    drug:
      entity_type: drug
      mention_source: GRLS
      original_literal: Этифоксин
      source_language: ru
      resolved_id: CHEBI:135272
      resolution:
        input_value: Этифоксин
        output_value: CHEBI:135272
        confidence: 0.727                            # 1.0 × 0.95 × 0.765 × 1.0
        confidence_basis: PRIOR                      # weakest basis in the chain
        pipeline:
        - category: EXTRACTION
          method: STRUCTURED_FIELD
          input_value: Этифоксин
          output_value: Этифоксин
          confidence: 1.0
          confidence_basis: DETERMINISTIC
          tool: medic-ingest-russia
          tool_version: 1.0.0
        - category: TRANSLATION
          method: API
          agent: {agent_id: wikidata:Q116709136, agent_name: DeepL, agent_type: AI_AGENT}
          input_value: Этифоксин
          output_value: Etifoxin
          source_language: ru
          target_language: en-us
          status: CANDIDATE
          translator_expertise: ALGORITHM
          quality: close
          flags: [unreviewed_machine]
          confidence: 0.95                           # DEEPL family default
          confidence_basis: PRIOR                    # <- now costs the record something
          tool: babelon
          tool_version: 0.3.6
        - category: GROUNDING
          method: LEXICAL_MATCH
          input_value: Etifoxin
          output_value: CHEBI:135272
          output_label: etifoxine
          quality: lexical_exact_normalized
          confidence: 0.765
          confidence_basis: MEASURED
          source_vocabulary: CHEBI
          tool: medic-lexical-grounder
          tool_version: '1'
        - category: NORMALIZATION
          input_value: CHEBI:135272
          output_value: CHEBI:135272
          quality: identity
          confidence: 1.0
          confidence_basis: DETERMINISTIC
          target_namespace: CHEBI
          tool: medic-normalizer
          tool_version: '1'
    disease:
      entity_type: disease
      mention_source: GRLS
      original_literal: тревога                      # Russia's own literal — currently discarded
      source_language: ru
      resolved_id: MONDO:0011918
      resolution:
        confidence: 0.95
        confidence_basis: PRIOR
        pipeline: [EXTRACTION, TRANSLATION(DeepL), GROUNDING, NORMALIZATION]
    assertion:
      relationship: INDICATION
      flags: []
      confidence: {subject: 0.727, object: 0.95, relationship: 1.0,
                   overall: 0.691, basis: PRIOR}
    evidence:
      jurisdiction: RUSSIA
      approval_date: '20080701'
      approval_status: APPROVED
      explanation: Russian (MOH) marketing approval per GRLS record
      reference: https://grls.rosminzdrav.ru/Default.aspx
      source_role: PRIMARY
      source_type: REGULATORY
    regulatory_status:
      authority: MOH_RUSSIA
      status: APPROVED
      approval_date: '20080701'
      source: GRLS
      source_role: PRIMARY
```

Pair confidence: `1 − (1 − 0.72)(1 − 0.691) = 0.913`.

### 4.3 Worked example — multivalued typed spans

This is a real record: `setid fd9f9458-fd96-4688-be3f-f77b3d1af6ab`, UBRELVY (ubrogepant),
`CHEBI:234515` → `MONDO:0005475`. Today the entire SPL section arrives as one flat string:

```yaml
indications_text: UBRELVY is indicated for the acute treatment of migraine with or
  without aura in adults. Limitations of Use UBRELVY is not indicated for the
  preventive treatment of migraine.
```

Four semantically distinct spans are concatenated: the section header, the indication sentence, a
subsection header, and a scope-limiting sentence. **This causes a live bug.**
`_build_disease_provenance` computes `check_text = " ".join([snippet, section])`
(`on_label_merge.py:977`) and runs `assertion_negated(raw, check_text)` over it, so the negation
detector evaluating the *positive* claim has the sentence "UBRELVY is **not** indicated for the
preventive treatment of migraine" in scope — and "migraine" is the head term of both. The
`entailment_score` is computed over the same conflated blob. Typed spans fix this by construction:
negation and entailment are scoped to the span the extraction actually read.

Under the new model:

```yaml
- source: DAILYMED
  jurisdiction: USA
  document: DailyMed:fd9f9458-fd96-4688-be3f-f77b3d1af6ab

  spans:
  - role: SECTION_HEADER
    document: DailyMed:fd9f9458-fd96-4688-be3f-f77b3d1af6ab
    section_code: LOINC:34067-9
    text: INDICATIONS AND USAGE
  - role: SECTION_TEXT
    document: DailyMed:fd9f9458-fd96-4688-be3f-f77b3d1af6ab
    section_code: LOINC:34067-9
    text: UBRELVY is indicated for the acute treatment of migraine with or without
      aura in adults.
  - role: SUBSECTION_HEADER
    document: DailyMed:fd9f9458-fd96-4688-be3f-f77b3d1af6ab
    section_code: LOINC:34067-9
    text: Limitations of Use
  - role: LIMITATION_STATEMENT
    document: DailyMed:fd9f9458-fd96-4688-be3f-f77b3d1af6ab
    section_code: LOINC:34067-9
    text: UBRELVY is not indicated for the preventive treatment of migraine.

  disease:
    entity_type: disease
    id: MEDICNE:6b146c8c-c364-5687-a2bd-c356ae42c11e
    mention_source: DAILYMED
    original_literal: migraine with aura
    resolved_id: MONDO:0005475
    resolved_label: migraine with aura
    resolution:
      input_value: UBRELVY is indicated for the acute treatment of migraine with or
        without aura in adults.
      output_value: MONDO:0005475
      confidence: 0.85
      confidence_basis: MEASURED
      pipeline:
      - category: EXTRACTION
        method: LLM
        agent: {agent_name: anthropic/claude-haiku-4-5-20251001, agent_type: AI_AGENT,
                agent_version: claude-haiku-4-5-20251001}
        span_role: SECTION_TEXT              # <- names WHICH span was read
        span_index: 1                        #    (spans[1], not spans[3])
        input_value: UBRELVY is indicated for the acute treatment of migraine with
          or without aura in adults.
        output_value: migraine with aura
        char_start: 52
        char_end: 82
        quality: not_stated                  # source reads "with or without aura"
        flags: [scope_narrowed]              # new ExtractionFlag, see §5
        confidence: 0.85
        confidence_basis: MEASURED
        co_mentions: []                      # no other entity in THIS span
        tool: medic-extractor
        tool_version: '1'
      - category: GROUNDING
        method: LEXICAL_MATCH
        input_value: migraine with aura
        output_value: MONDO:0005475
        output_label: migraine with aura
        predicate_id: skos:exactMatch
        quality: lexical_exact
        confidence: 1.0
        confidence_basis: MEASURED
        source_vocabulary: MONDO
        tool: medic-lexical-grounder
        tool_version: '1'
      - category: NORMALIZATION
        input_value: MONDO:0005475
        output_value: MONDO:0005475
        quality: identity
        confidence: 1.0
        confidence_basis: DETERMINISTIC
        target_namespace: MONDO
        tool: medic-normalizer
        tool_version: '1'

  assertion:
    relationship: INDICATION
    trigger_cue: indication_phrase
    trigger_span: is indicated for the acute treatment of
    span_index: 1                            # the cue is in spans[1]
    negation_scope: [1]                       # negation checked ONLY against spans[1]
    flags: []
    confidence: {subject: 0.90, object: 0.85, relationship: 1.0,
                 overall: 0.765, basis: MEASURED}
```

Three things the typed spans buy, all visible here:

1. **Negation is correctly scoped.** `negation_scope: [1]` excludes `spans[3]`, so
   "not indicated for the preventive treatment" no longer bears on the positive claim.
2. **The limitation is retained rather than discarded or conflated.** `LIMITATION_STATEMENT` is a
   first-class span. A future pass can mine it into a scope qualifier or a separate negative
   assertion without re-parsing the label.
3. **The extraction names its input.** `span_role` + `span_index` make
   `pipeline[0].input_value == spans[1].text` a checkable invariant rather than an assumption.

A second, denser illustration of `co_mentions` — the EMA voriconazole (Vfend) record, whose single
`SECTION_TEXT` span names five diseases. The record for *invasive aspergillosis* is:

```yaml
- category: EXTRACTION
  method: LLM
  span_role: SECTION_TEXT
  span_index: 1
  input_value: 'Voriconazole is a broad spectrum, triazole antifungal agent and is
    indicated in adults and children aged 2 years and above as follows: treatment of
    invasive aspergillosis; treatment of candidaemia in non-neutropenic patients;
    treatment of fluconazole-resistant serious invasive Candida infections (including
    C. krusei); treatment of serious fungal infections caused by Scedosporium spp.
    and Fusarium spp.'
  output_value: invasive aspergillosis          # <- the one this Mention is about
  char_start: 133
  char_end: 155
  quality: verbatim
  confidence: 1.0
  confidence_basis: MEASURED
  mention_index: 1        # index/total count same-entity_type mentions in this span:
  mention_total: 5        # 5 diseases. The drug co-mention below is not counted.
  co_mentions:
  - {value: candidaemia, entity_type: disease,
     mention_id: MEDICNE:…, char_start: 179, char_end: 190}
  - {value: fluconazole-resistant serious invasive Candida infections,
     entity_type: disease, mention_id: MEDICNE:…, char_start: 232, char_end: 288}
  - {value: Scedosporium spp., entity_type: disease,
     mention_id: MEDICNE:…, char_start: 344, char_end: 361}
  - {value: Fusarium spp., entity_type: disease,
     mention_id: MEDICNE:…, char_start: 366, char_end: 379}
  - {value: Voriconazole, entity_type: drug,
     mention_id: MEDICNE:…, char_start: 0, char_end: 12}
  tool: medic-extractor
  tool_version: '1'
```

`co_mentions` is informational: the chain still runs `input_value → invasive aspergillosis →
MONDO:0000240`, one in, one out. But the record now answers "was this string the only candidate, or
one of five?" — which is the difference between a confident extraction and a lucky one — and the
`mention_id`s make the sibling associations joinable without re-running the extractor.

---

## 5. Schema changes

In `src/medic/schema/provenance.yaml`:

**New enum `TextSpanRoleEnum`** — `SECTION_HEADER`, `SECTION_TEXT`, `SUBSECTION_HEADER`,
`SUBSECTION_TEXT`, `LIMITATION_STATEMENT`, `TABLE_HEADER`, `TABLE_CELL`, `LIST_ITEM`,
`DOCUMENT_TITLE`, `STRUCTURED_FIELD`, `FULL_DOCUMENT`, `UNKNOWN`.

**New enum `ConfidenceBasis`** — `MEASURED`, `DETERMINISTIC`, `PRIOR`.

**New `ExtractionFlag` value `scope_narrowed`** — the extracted mention is narrower than the entity
the source names (UBRELVY's *"migraine with or without aura"* extracted as *"migraine with aura"*,
§4.3). This is an entity-recognition failure, not a relation failure, so it belongs on
`ExtractionFlag` and not `AssertionFlag` per that enum's stated boundary. Note that
`ExtractionQuality` needs no new value: `not_stated` already covers "output not literally present in
the supporting quote", which is exactly the case here — the flag carries the *direction* of the
discrepancy that `quality` alone cannot express.

**`TextSpan`** gains required `role` (`TextSpanRoleEnum`) and `document` (`uriorcurie`);
`source_reference` is renamed to `document` and `text` keeps its name (see §9 A1 for the sieve-facing
alias).

**`TransformationStep`** gains required `confidence` and required `confidence_basis`.

**`ExtractionStep`** gains `span_role`, `span_index`, `char_start`, `char_end`, `mention_index`,
`mention_total`, and `co_mentions` (multivalued, range `CoMention`).

**New class `CoMention`** — `value` (required), `entity_type`, `mention_id`, `char_start`,
`char_end`.

**New class `ConfidenceBreakdown`** — `subject`, `object`, `relationship`, `overall` (all required,
0–1) and `basis` (`ConfidenceBasis`, required). Replaces the four flat `*_confidence` slots on
`Assertion`.

**New class `PairConfidence`** — `overall` (required), `method` (`NOISY_OR`), `n_assertions`.

**`Assertion`** gains `span_index` and `negation_scope` (multivalued int); drops
`subject_confidence` / `object_confidence` / `relationship_confidence` / `confidence` in favour of
the nested `confidence: ConfidenceBreakdown`.

**`NormalizationQualityEnum`** — `none` → `identity`. Per the transformation-traceability rule in
`.claude/CLAUDE.md`, the enum value is added first and the code map updated in the same change; the
code-vs-schema test enforces agreement.

In `src/medic/schema/indication.yaml`: a new `SourceAssertion` class (`source`, `jurisdiction`,
`document`, `spans`, `drug`, `disease`, `assertion`, `evidence`, `regulatory_status`) and
`IndicationAssociation` restructured to `{drug_id, drug_label, disease_id, disease_label,
relationship_type, reliability, confidence: PairConfidence, assertions: [SourceAssertion]}`.

New config `conf/confidence_priors.yaml`, governed by two new schema classes **`ConfidencePrior`**
and **`ConfidencePriorSet`** (tree_root). It is a **flat list of records**, not a map keyed by
identifiers — identifiers as mapping keys cannot be schema-validated, cannot carry their own
metadata, and force composite keys to be concatenated into strings:

```yaml
priors:
- category: EXTRACTION          # TransformationCategory enum
  method: LLM                   # TransformationMethod enum
  agent_name: anthropic/claude-haiku-4-5-20251001
  agent_version: claude-haiku-4-5-20251001
  value: 0.80
  calibrated: false
  rationale: >-
    Placeholder for an LLM extraction that returned no self-reported score…
- category: TRANSLATION
  method: API
  tool: babelon
  tool_version: '0.3.6'
  value: 0.95
  calibrated: false
  rationale: …
```

**A prior is only valid for the exact thing that produced it.** Each record names *exactly one* of
two scopes, enforced at load:

- **`agent_name` + `agent_version`** — a versioned model. A model upgrade silently changes output
  (FAILURE_MODES 13.1), so a prior calibrated on one model says nothing about the next.
- **`tool` + `tool_version`** — everything else, **including DeepL**, which publishes no engine
  version, so the babelon release that called it is what is actually knowable.

`calibrated` and `rationale` are required, so an uncalibrated judgement call cannot masquerade as a
measurement. Lookup builds an in-memory tuple index (`index_priors`); a duplicate raises rather than
last-one-wins, and a miss is a **hard failure, not a fallback** — bumping a model or a component
version breaks the build until the new prior is added. That is deliberate: an uncalibrated prior
silently carried across a model upgrade is the failure mode the structure exists to prevent.

Also added: **`TransformationMethod.SOURCE_ASSERTED`**. A grounding where the source supplied the id
and nothing was matched previously recorded `LEXICAL_MATCH`, which claims a match that never ran.

---

## 6. Build pipeline changes

`src/medic/merge/on_label_merge.py`:

1. `_make_key` → `drug_id|disease_id|relationship|source|document`; add `_pair_key` →
   `drug_id|disease_id|relationship`.
2. `_init_association` becomes `_build_source_assertion(record, …)` returning one `SourceAssertion`.
3. Delete `_build_drug_identities` and the `drug_identities` parameter; add `_build_drug_mention`
   mirroring `_build_disease_provenance`, sourced from `evidence.original_drug_label` +
   `GroundingStoreView("mappings/drug_grounding.sssom.tsv", "drugs")`.
4. `_merge_into` → `_append_assertion`: append to `assertions`, then recompute the pair's
   `confidence` (noisy-OR) and `reliability`. Evidence dedup logic moves to the derived pair view.
5. Split spans at ingest: DailyMed's section header, body, and `Limitations of Use` subsection become
   separate typed spans. Per-source span-role mapping: DailyMed → `SECTION_HEADER`/`SECTION_TEXT`/
   `SUBSECTION_HEADER`/`LIMITATION_STATEMENT`; EMA → `STRUCTURED_FIELD`; India → `TABLE_CELL`;
   PMDA → `SECTION_TEXT`; Russia/China → no spans.
6. Scope `entailment_score` and `assertion_negated` to the extraction's span, not to
   `" ".join([snippet, section])` (§4.3).

Backfill task, independent and mechanical: populate the 11,021 blank `subject_id` cells across the
two grounding stores using `mint_mention_id(subject_label, entity_type)`, and add a store-load
assertion that every row has one.

**PMDA data-quality follow-up (not blocking).** PMDA snippets are several review-report sentences
pipe-joined into one string (`"A drug with a new indication… | A new combination drug… | A drug with
new indications…"`). Under D2 these are plausibly separate documents; the ingester should split them
rather than the merge. Tracked separately.

---

## 7. Invariants and validation

Extends `validate_mention_chain`; all are hard test assertions.

- **I-8a (chain contiguity, unchanged)** — `pipeline[n].output_value == pipeline[n+1].input_value`;
  `resolution.input_value == pipeline[0].input_value`;
  `resolution.output_value == pipeline[-1].output_value == mention.resolved_id`.
- **I-8b (span anchoring, new)** — `pipeline[0].input_value == spans[extraction.span_index].text`,
  and `spans[i].text[char_start:char_end] == output_value` when offsets are present.
- **I-10 (source consistency, new)** — for every assertion,
  `drug.mention_source == disease.mention_source == source`, and every span's `document` equals the
  assertion's `document`.
- **I-11 (confidence completeness, new)** — every step has `confidence` and `confidence_basis`; every
  assertion has all four confidence components; `overall == subject × object × relationship`
  (within float tolerance); `basis` is the weakest of the chain's bases
  (`MEASURED > DETERMINISTIC > PRIOR` is *not* an ordering — the rule is: `PRIOR` if any step is
  `PRIOR`, else `MEASURED` if any is `MEASURED`, else `DETERMINISTIC`).
- **I-12 (normalization always emitted, new)** — every resolution ending in a CURIE has a terminal
  `NORMALIZATION` step, identity or not.
- **I-13 (pair aggregation, new)** — `pair.confidence.overall == 1 − Π(1 − aᵢ.confidence.overall)`,
  and `pair.confidence.n_assertions == len(assertions)`.

---

## 8. Migration and blast radius

| File | Change |
|---|---|
| `src/medic/merge/on_label_merge.py` | the rework in §6 (~1,200 lines, the bulk of the work) |
| `src/medic/schema/provenance.yaml` | §5 enums and classes |
| `src/medic/schema/indication.yaml` | `SourceAssertion`, restructured association |
| `src/medic/product_view.py` | `assoc_drug_id` / `assoc_drug_label` / `assoc_disease_id` / `assoc_disease_label` read the pair level (simpler); `assoc_authorities` reads `assertions[].regulatory_status.authority` |
| `src/medic/reliability.py` (`:492`) | reads `assertions` instead of `evidence` for jurisdiction agreement |
| `src/medic/reliability_export.py` (`:37`) | same |
| `src/medic/export/kgx.py` (`:53`) | one KGX edge per pair (unchanged) with per-assertion provenance in edge properties |
| `conf/confidence_priors.yaml` | new |
| `tests/test_on_label_provenance.py` | extend for I-10 … I-13 |

`products/contraindication_list.yaml` is produced by the same merge and changes identically.

Because `product_view.py` is the single read-side accessor, consumers other than the four above need
no change. Record count goes from 6,338 associations to ~6,338 pairs / ~9,332 assertions; file size
grows roughly 1.5× (both mentions now carry a trail on every assertion).

---

## 9. Relationship to sieve / SEPIO

`~/ws/projects/sieve` models the same subject matter — a claim with evidence — from the opposite
direction. Understanding *why* they differ matters before deciding how much to align.

**The full treatment is [`docs/sepio-sieve-alignment.md`](../docs/sepio-sieve-alignment.md)**: the
evidence / provenance / data-quality distinction (§3), what MeDIC already has (§4), a class-by-class
correspondence (§5), and a five-stage costed adoption path (§6). What follows is the summary.

### 9.1 The two ways of thinking

| | MeDIC provenance | sieve / SEPIO |
|---|---|---|
| Question | *How did this string become this ID?* | *Should I believe this claim?* |
| Core abstraction | **Transformation chain** — ordered, contiguous, replayable | **Evidence line** — an unordered bag of items bearing on a claim |
| Polarity | none; every step is forward | `Direction`: `SUPPORTS` / `REFUTES` / `NEUTRAL` |
| Confidence maths | **decays** multiplicatively along a chain (`0.95 × 0.765 = 0.727`) | **accumulates** across lines; Net Evidence Ratio `(S⁺−S⁻)/(S⁺+S⁻+S⁰)` |
| What confidence means | linking fidelity — did we point at the right entity? | belief — does the evidence support the claim? |
| Truth of the claim | assumed; the regulator said it | the thing under review |
| Human curation locus | the **decision stores** (`mappings/*.sssom.tsv`), upstream, then rebuild | the **packet** (accept / reject / flag), downstream, in a UI |
| Failure mode captured | mis-*linking* (wrong ID for a string) | mis-*believing* (wrong claim) |
| Time model | one-shot deterministic build; byte-identical reruns | workflow with status transitions and decision history |

The sharpest statement of the difference: **MeDIC's chain is a sequence with an equality constraint
between adjacent elements; sieve's evidence line is a set.** MeDIC's whole value proposition is that
`pipeline[n].output_value == pipeline[n+1].input_value` — order and contiguity *are* the model.
`EvidenceLine.has_evidence_items` is `multivalued: true, inlined_as_list: true` with no ordering
semantics and no inter-item constraint. Any mapping of a MeDIC chain into a sieve line is lossy
unless the chain survives as an ordered structure inside a single item.

They are, however, **orthogonal rather than competing**, and they meet at exactly one join point:
a MeDIC per-source assertion *is* a sieve `EvidenceLine` with `direction: SUPPORTS`.

### 9.2 Structural correspondence

| MeDIC (this spec) | sieve / minimal kernel | Fit |
|---|---|---|
| pair record | `EvidencePacket` / `EvidencedClaim` | good |
| `(drug_id, relationship_type, disease_id)` | `SieveStatement` (`subject` / `predicate: Coding` / `object`) | good — MeDIC's `relationship_type` enum becomes a `Coding` |
| `assertions[i]` | `EvidenceLine`, one per source document | good; D2's document grain matches the kernel's "one line, one source" guidance |
| `assertion.confidence.overall` | `score_of_evidence_provided` | usable, with the caveat in note 1 |
| `spans[i]` | `TextSpan` (shape **A**): `value` + `reported_in: Document` | good; MeDIC `text`→`value`, `document`→`reported_in.id` |
| `spans[i].role`, `.section_code` | **nothing in A**; belongs on `TextMiningResult` (shape **C**) | **gap — C is unimplemented** |
| `ExtractionStep` | `TextMiningResult` (`extraction_score`, `document_section`, `text_location`, `extraction_method`) | close, but C does not exist yet and has no structured offsets |
| `GroundingStep` / `NormalizationStep` | `ConcordanceItem` (`mapping_justification`, `mapping_set`, `source_*`) | **surprisingly good** — a MeDIC grounding decision is literally an SSSOM row with a `semapv:` justification, which is what `ConcordanceItem` is for |
| `TranslationStep` | nothing | gap |
| `Resolution` (the ordered chain) | **nothing** | the fundamental gap |
| `MEDICNE:` uuid5 mention ids | nothing (items are anonymous) | MeDIC-specific strength; keep |
| `reliability` (HIGH/MEDIUM/LOW) | `Strength` (STRONG/MODERATE/WEAK) on a line | different level — MeDIC's is cross-source, sieve's is per-line |
| regulatory approval as evidence kind | `EvidenceSource` has no `REGULATORY` value | **gap — contributable** |
| a drug label as a document | `DocumentType` has no `REGULATORY_LABEL` value | **gap — contributable** |

Note the decomposition implied by rows 7–9: a single MeDIC chain maps onto **two different sieve item
types** — extraction/translation → `TextMiningResult`, grounding/normalization → `ConcordanceItem` —
and the sequencing between them has no home. That is the concrete cost of the set-vs-sequence
mismatch.

### 9.3 Alignment notes

**Note 1 — the confidence collision is the real hazard.** Three different numbers are in play:
MeDIC's multiplicative chain confidence, MeDIC's noisy-OR pair confidence (D8), and sieve's Net
Evidence Ratio. They are not interconvertible, and **all of MeDIC's confidence numbers are
data-quality numbers, not evidence numbers**: `0.765` means "we are 76.5% sure we linked the right
CHEBI id", not "there is a 76.5% chance etifoxine treats anxiety".

Therefore: **do not** map `confidence.overall` onto `score_of_evidence_provided`. Doing so silently
converts "unsure of the identifier" into "weak evidence for the treatment". The correct mapping is
`evidence.confidence` (HIGH/MEDIUM/LOW) plus `source_role` (PRIMARY beats INTERMEDIARY) →
`strength_of_evidence_provided`; the chain confidence stays a data-quality annotation and acts as a
*gate* (suppress or flag a low-confidence line) rather than a score. The pair-level noisy-OR maps to
nothing. Note that NER with no `REFUTES` lines is always exactly 1.0, so it is degenerate on MeDIC
data until `direction` exists.

See [`docs/sepio-sieve-alignment.md`](../docs/sepio-sieve-alignment.md) §3 for the underlying
evidence / provenance / data-quality distinction this rests on.

**Note 2 — do not collapse contraindications into `REFUTES`.** It is tempting, since sieve has the
polarity slot. But in sieve terms "drug X is indicated for Y" and "drug X is contraindicated in Y"
are *different statements with different predicates*, not opposing evidence about one statement. Keep
the two products separate. The genuinely `REFUTES`-shaped signal in MeDIC is the `negated_inversion`
assertion flag — an extraction that read a negated sentence as positive — and that is a QC signal,
which the sieve kernel's `Direction` docstring explicitly excludes from the enum.

**Note 3 — two curation surfaces for one fact is the top integration risk.** MeDIC's curation surface
is `mappings/*.sssom.tsv`: a human edits a row, the pipeline rebuilds, the product changes. sieve's
is the DuckDB packet store: a curator accepts or rejects a packet. If MeDIC ever feeds sieve packets,
a rejection recorded only in sieve is **silently reverted by the next MeDIC rebuild**. Any
integration must route a curator's verdict back to the store row (or make MeDIC read sieve decisions
as an input to the build). This should be settled before any data flows, not after.

**Note 4 — MeDIC is the natural driver for the kernel's unimplemented shape C.** The 07-31 alignment
spec defines `TextMiningResult` as phase 3, still unbuilt (`minimal.yaml` mentions it only in a
comment), and explicitly defers structured offsets: *"Structured character offsets on
`TextMiningResult` stay a single `text_location` string for now; a structured span type is a future
extension."* This spec's `char_start`/`char_end`/`co_mentions` (D5) is exactly that future extension,
with a concrete use case behind it. Feeding MeDIC's requirements into sieve phase 3 is cheaper than
inventing a parallel vocabulary and reconciling later.

**Note 5 — two enum values are worth contributing upward now.** `EvidenceSource: REGULATORY` and
`DocumentType: REGULATORY_LABEL`. Neither exists; both are needed the moment MeDIC data enters a
packet, and forcing a drug label into `DATABASE_RECORD` or `EXPERT_CONSENSUS` would be wrong.
Blocker B2 in the sieve spec (real `meaning:` IRIs, to be looked up in OLS rather than invented) is
still open, so the vocabulary is not yet frozen — good timing.

**Note 6 — align by annotation, not by import (recommended).** Do **not** import `minimal.yaml` into
MeDIC in this work. Reasons: the kernel is marked `PROVISIONAL` with its schema id unconfirmed
(B1) and its `meaning:` IRIs unlooked-up (B2); shape C, the part MeDIC would actually use, does not
exist yet; and MeDIC's chain — its distinctive contribution — has no kernel counterpart, so the
import would buy alignment on the shallow half of the model while the deep half stays MeDIC-specific.

Instead do what sieve itself does to be SEPIO-aligned without importing SEPIO: **pin
`class_uri` / `slot_uri` annotations**. Concretely, while §5's schema rewrite is happening anyway:

- `TextSpan` → `class_uri: sepio:DataItem`, slot `text` gets `slot_uri: sepio:value` (and, if cheap,
  an alias `value`), `document` gets `slot_uri: sepio:reported_in`.
- `SourceAssertion` → `class_uri: sepio:EvidenceLine`, with `slot_uri: sepio:has_evidence_items` on
  whichever slot ends up holding the spans.
- the pair record → `class_uri: sepio:Statement`.
- `GroundingStep` / `NormalizationStep` keep their SSSOM `mapping_justification` and gain a
  `mapping_set` slot, so they are `ConcordanceItem`-shaped.

Cost is near zero — annotations on classes being written regardless — and it makes a later
`is_a`-based re-base mechanical instead of a rewrite. Revisit importing the kernel once B1/B2 close
and phase 3 lands.

---

## 10. Testing strategy

TDD, per `superpowers:test-driven-development`. Each invariant in §7 gets a failing test first.

- **Fixtures from real records**, not invented ones: the etifoxine cross-source case (§4.2), the
  UBRELVY four-span case (§4.3), the Vfend five-co-mention case, the NITHIODOTE broadened-grounding
  case, and one of the 462 drugs currently missing a trail.
- **Regression on the known defects**: a test asserting no association mixes sources (I-10); a test
  asserting `original_literal` never equals the canonical label when the evidence carries a different
  `original_drug_label`; a test asserting the 667 duplicate DailyMed pairs produce 2+ assertions.
- **Negation scoping**: the UBRELVY record must not be flagged negated, and a genuinely negated
  record must still be.
- **Confidence**: `overall` equals the product; noisy-OR matches a hand-computed value; a `PRIOR`
  step propagates `basis: PRIOR` to the assertion.
- **Round-trip**: `just merge` twice produces byte-identical output (determinism, unchanged
  requirement).
- **Schema/code agreement**: the existing code-vs-schema enum test extended to `TextSpanRoleEnum` and
  `ConfidenceBasis`.

---

## 11. Open items and risks

- **R1 — Per-source drug trail coverage.** D4 depends on `evidence.original_drug_label` being present
  and groundable for every source. It is absent from some PMDA rows. Fallback must record an honest
  `source_asserted` grounding rather than a fabricated literal — never repeat the current
  canonical-label write-back.
- **R2 — File size.** Both mentions now carry a full trail on every assertion, with ~9,332 assertions
  instead of 6,338 single-trail records. Estimated 1.5–2× growth on a 50 MB file. Acceptable; revisit
  if it crosses ~150 MB.
- **R3 — Span splitting is per-source ingest work** and is the least certain estimate in §6, since
  the SPL section structure is only partly recoverable from the current `indications_text` string. If
  a source cannot be split reliably, it emits one `SECTION_TEXT` span — degraded, not wrong.
- **R4 — `confidence_priors.yaml` values are judgement calls.** The 0.95 for unreviewed DeepL is a
  placeholder. It should be calibrated against a sample of manually-reviewed translations before it
  is trusted in a released product; until then it is declared as `PRIOR`, which is the honest label.
- **R5 — PMDA pipe-joined snippets** (§6) mean PMDA's document grain is currently wrong. Assertions
  from PMDA will be coarser than D2 intends until that ingester is fixed.


---

## 12. Delivery

Implemented in three passes. `uv run python -m pytest` → **539 passed, 0 failed**.
Products rebuilt end to end (`drug_merge` → `on_label_merge` → `kgx`).

### Verification

| Check | Before | After |
|---|---|---|
| drug mentions with no `resolution` | 462 | **0** |
| drug mentions with no `mention_source` | 462 | **0** |
| canonical-label write-backs into `original_literal` (I-7) | 462 | **0** |
| cross-source assertions (I-10) | present | **0** |
| document-level attestations | 8,737 | **11,915** (+3,178 recovered) |
| pairs with more than one attesting document | 0 | 1,653 |
| `quality: none` (invalid enum) across products | 11,268 | **0** |
| grounding-store rows with a blank `subject_id` (I-4) | 11,021 | **0** |
| invariant violations reported by the merge | n/a | **0** |
| KGX edges | 8,737 | 8,737 (unchanged, one per pair) |

Final shape: **8,737 pairs carrying 11,915 `SourceAssertion`s** (6,338 indication pairs / 8,937
assertions; 2,399 contraindication pairs / 2,978 assertions).

### Deviations from the plans

- **Confidence priors are auto-minted from family defaults, not hard failures.** A version bump
  mints a prior from its family (DEEPL 0.95, HAIKU 0.85, SONNET 0.90, OPUS 0.95, FABLE 0.97),
  writes it to `conf/confidence_priors.yaml` with `auto_generated: true`, and proceeds. Human
  edits are never overwritten. A producer matching no family still raises.
- **Both conf files became schema-governed record lists.** `conf/confidence_priors.yaml` and
  `conf/section_warrants.yaml` used identifiers as mapping keys, which cannot be validated and
  forced composite keys into strings. Now `ConfidencePriorSet` / `SectionWarrantSet` classes with
  `linkml-validate` tests; composite lookup keys are built in memory only.
- **`TransformationMethod.SOURCE_ASSERTED` added.** A grounding where the source supplied the id
  and nothing was matched used to record `LEXICAL_MATCH` — claiming a match that never ran.
- **I-12 forced identity normalization steps on both sides.** Neither `_build_drug_mention` nor
  India's disease records emitted a terminal `NORMALIZATION`; the invariant caught 118 cases on
  the first real rebuild.
- **`reliability` reads through `product_view`.** New `assoc_mentions` / `assoc_claims` /
  `assoc_evidence` / `assoc_jurisdictions` accessors keep it statement-type agnostic, so the
  research and adverse-event products (never two-level, flat `evidence` list) still work.
- **The `subject_id` backfill nearly corrupted the drug store.** 7,123 populated ids do not equal
  `mint(subject_label)` — correctly, because for a translated drug the row's label is English
  while its id is pinned to the *original foreign literal*. Minting from the label would have
  broken every join. The script now consults the Babelon store first, and the test encodes the
  rule instead of the naive equality.

### Found after the fact, then corrected: artifact evidence rows

Spot-checking a real pair after delivery showed `_build_source_assertions` kept only
`evidence[0]`, discarding 14,585 injected artifact rows (Orange Book, Purple Book, GRLS, CDE —
keyed by drug, not by indication). My first fix, on 2026-08-10, restored the same-jurisdiction
ones as their own assertions. **That was wrong**, and reviewing it on 2026-08-12 showed why:

- Orange Book attests a **drug approval**, not a drug–disease claim. The restored assertions were
  relabelled copies of the real one — `disease.mention_source: ORANGEBOOK` for a disease Orange
  Book never saw, on a DailyMed document, with an empty snippet. I-10 passed only because the
  code rewrote `mention_source` to match the fabricated source. 3,799 such assertions shipped.
- Every artifact **already lives on `Drug.approvals`** (Orange Book 2,186, Purple Book 406, GRLS
  1,705, CDE 598) with its deep link, so the pair-level copy was duplication regardless.
- **No source record carries more than one native evidence row** — every extra was injected — so
  the cross-jurisdiction split added alongside the bad fix guarded a case that cannot arise.

Final: injection and both fixes removed, `_inject_fda_evidence_from_artifacts` deleted, assertion
count settled at **11,915**. Per-pair authority sets unchanged. Separately, span documents had
been hardcoded to the DailyMed setid, leaving EMA/PMDA/India spans document-less; all 11,915 now
carry their assertion's document and I-10 rejects an empty one.

This was the fourth defect in this work that a green suite did not catch, and the third found
only by reading real output.

### Lessons the tests did not catch

Two defects reached a green suite and were found only by rebuilding the real products: the
11,268 invalid `quality: none` values (Plan 1) and the 118 missing normalization steps
(Plan 3). Both are now covered, but the standing rule holds — **a unit-test pass is not proof
for this pipeline; rebuild and inspect the products.**
