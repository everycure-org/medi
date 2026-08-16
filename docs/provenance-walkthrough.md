# Provenance: recording, caching, and assembly (walkthrough)

How a source string becomes the `resolution` container you see on a product, and how the
hand-editable decision stores under `mappings/` sit in the middle. Two real end-to-end traces — a
**drug** and a **disease** — with the actual store rows that back them.

Companion to [`docs/architecture.md`](architecture.md) §9 (grounding) and §9.9 (the model), and
`review_model.md` (the shape).

> **Shape note (2026-08-10).** The two walkthroughs below trace *entity* resolution, which is
> unchanged. What changed is where those trails hang: an association is now a canonical pair
> holding one `SourceAssertion` per source **document**, and both the drug and disease Mentions
> live on the assertion, built from that document's own literals. See
> `specs/2026-08-09-source-scoped-association-provenance-design.md`.

## The three moments

```
DECIDE (once, at ingest)          RECORD                        ASSEMBLE (every merge)
DeepL / lexical grounder     →  mappings/*.sssom + *.babelon  →  build_mention → resolution
  (+ JSON speed caches)           (git-tracked, editable)          (pure offline read)
```

Provenance is **recorded** once, upstream, into the stores. It is **assembled** into the `resolution`
container later by a pure read. Those are different moments — that separation is the whole design.
Curate the stores and rebuild; never hand-edit the assembled provenance.

## What ran it: tool and agent versions

Every step records **what produced it**, so a record can be compared against a re-run:

- **`tool` + `tool_version`** — the code. MeDIC's deterministic components carry a *hand-bumped
  component version* (`medic-lexical-grounder/1`, `medic-normalizer/1`) that you bump when the
  component's behaviour changes; per-source ingest parsers fall back to the released MeDIC version
  (`medic-ingest-china` @ `1.0.0`); third-party tools carry their real distribution version
  (`babelon` @ `0.3.6`).
- **`agent` + `agent_version`** — the non-deterministic actor, when there is one. For an LLM this is the
  **dated model id** (`claude-haiku-4-5-20251001`) — the single most important pin, because a model
  upgrade silently changes extraction output (FAILURE_MODES §13.1) and without it a re-run is not
  comparable. DeepL gets an agent with **no** version: it publishes no engine version, and inventing one
  would be worse than the honest gap (the babelon `tool_version` is what is actually knowable).

All of this resolves in one place, `src/medic/versions.py`. Note it deliberately does **not** stamp the
full package version: `importlib.metadata.version("medic")` is
`1.0.0.post70.dev0+<commit>` under uv-dynamic-versioning, so stamping it would rewrite all ~10k product
records on every commit and bury the diffs that matter. `medic_release()` keeps only `MAJOR.MINOR.PATCH`.

## The stores (`mappings/`) — the authoritative, editable layer

Five git-tracked TSVs. These are the source of truth, not the products (invariant I-4 — *every*
decision, including failures, is persisted):

| File | Profile | Records |
|---|---|---|
| `drug_grounding.sssom.tsv`, `disease_grounding.sssom.tsv` | SSSOM literal | every string→ID decision (failures as `sssom:NoTermFound`) |
| `drug_normalization.sssom.tsv`, `disease_normalization.sssom.tsv` | SSSOM term↔term | every initial-ID → canonical-ID decision |
| `drug_translation.babelon.tsv` | Babelon | every non-English→English translation, keyed by MEDICNE id |

Grounding columns: `subject_id (MEDICNE)  subject_label  predicate_id  object_id  object_label
mapping_justification  subject_preprocessing  match_string  confidence  mapping_tool`. The
`subject_preprocessing` column is the list of `PreprocessingRuleEnum` rules that fired (salt strip,
formulation strip, qualifier strip, transliteration, fuzzy…) — the per-step "how".

## Two kinds of cache — do not conflate them

| | Authoritative decision cache | Speed cache |
|---|---|---|
| Files | `mappings/*.sssom.tsv`, `*.babelon.tsv` | `cache/grounding/*.json`, `cache/enrichment/*.json` |
| Holds | every string→ID / ID→ID / translation decision | LLM extractions, ATC/SMILES/PHAROS lookups |
| Git-tracked | **yes** (diffable, reviewable, editable) | no (regenerable) |
| A filled entry is | **never recomputed** — no DeepL, no re-ground | reused if the input is unchanged |
| Source of truth | **yes** | no |

The SSSOM/Babelon stores *are* the cache that makes reruns byte-identical and offline. The JSON caches
only spare repeat network/LLM cost; delete one and it refills. (This is why
`MEDIC_SKIP_EXPENSIVE_CALLS=1` blanks ATC — it suppresses the enrichment speed-cache's network refills;
the grounding/translation stores are untouched.)

---

## The two layers of extraction (why a disease record says nothing about indications)

One more separation, orthogonal to the three moments above. Two different things get "extracted" from a
label, and the model keeps them apart:

| | Question it answers | Where it lives | Failure flags |
|---|---|---|---|
| **Entity recognition + linking** | *"What entity is this string?"* | `Mention.resolution.pipeline` | `ExtractionFlag`: `hallucination`, `truncated_snippet`, `coreference_ambiguity` |
| **The claim (relation extraction)** | *"What is the source asserting about it?"* | `IndicationAssociation.assertion` | `AssertionFlag`: `negated_inversion`, `over_extraction`, `wrong_section`, `wrong_pairing` |

A disease `Mention` is therefore **relation-agnostic and reusable** — it carries no `relationship_type`, no
"indication" anywhere. The relation is named once, on the association. This matters because an entity can
be recognised *perfectly* while the asserted relation is *wrong* (the VITAMIN A → hyperthyroidism case,
where "hyperthyroidism" is correctly recognised but the sentence lists it as a depleting condition, not an
indication). That is an `over_extraction` on the **assertion**, not an extraction failure on the mention.

---

## Walkthrough A — a DRUG: `来那度胺胶囊` → `CHEBI:63791` (lenalidomide)

### 1. The backing store rows — what was decided and recorded, once, at ingest

```
# mappings/drug_translation.babelon.tsv
subject_id: MEDICNE:194fb00d-…   source_value: 来那度胺胶囊   translation_value: Lenalidomide Capsules
translator: wikidata:Q116709136  translator_expertise: ALGORITHM   translation_status: CANDIDATE

# mappings/drug_grounding.sssom.tsv
subject_label: Lenalidomide Capsules      # ↳ the grounder saw the ENGLISH string…
subject_id: MEDICNE:194fb00d-…            # ↳ …but the row is pinned to the original literal's id
predicate_id: skos:closeMatch   object_id: CHEBI:63791   object_label: lenalidomide
subject_preprocessing: formulation_strip   confidence: 0.8000
```

### 2. Ingest — decide + record (China source)

1. **Mint** `MEDICNE:194fb00d-…` = `uuid5(entity_type, base_normalize("来那度胺胶囊"))`. Deterministic and
   offline; the same string always mints the same id (the cross-source dedup key, I-9).
2. **Stage-0 translate.** Look up the Babelon store by that id. **Row present → use it, no DeepL call.**
   Absent → call DeepL once and write the row (`CANDIDATE`, `ALGORITHM`). The English `translation_value`
   is what the grounder sees next.
3. **Stage-1 ground.** Look up the grounding store by subject. **Decision present → short-circuit
   (deterministic).** Absent → run the offline lexical ladder; `formulation_strip` fires ("Capsules"
   removed), landing `CHEBI:63791` at `skos:closeMatch`, conf 0.8 — write that row with
   `subject_preprocessing: formulation_strip`.
4. **Stage-2 normalize.** Look up the normalization store; identity here (already canonical CHEBI).
5. The three decisions are also stamped onto the kb source record (`kb/drugs/china/…`) with the MEDICNE id.

### 3. Merge — assemble (`build_mention`; offline, decides nothing)

Reads the stage objects, lays them out as ordered steps, **reaches back into the grounding store** for the
per-step rules (`_load_applied_rules` → `applied_rules: [formulation_strip]`), wraps them in `resolution`,
enforces the chain, and multiplies the step confidences. `reliability.py` then reads those steps.

### 4. The assembled product record — `products/drug_list.yaml` (complete, verbatim)

Note the elected representative here is the **EMA English** trail (the merge picks the highest-confidence
on-target grounding across all five sources), so this record shows a clean 3-step chain; the China `zh`
trail above is what a *translated* mention's pipeline looks like — shown after.

```yaml
- identity:                                    # ↳ the drug's resolved identity (a Mention)
    id: MEDICNE:b2db83a9-fb84-5710-b6a4-6380946f8894
    original_literal: lenalidomide             # ↳ verbatim surface form (I-7); the MEDICNE hash key (I-9)
    entity_type: drug
    mention_source: EMA
    resolved_id: CHEBI:63791
    resolved_label: lenalidomide
    resolution:                                # ↳ the container
      input_value: lenalidomide                # ↳ == pipeline[0].input_value
      output_value: CHEBI:63791                # ↳ == pipeline[-1].output_value == resolved_id
      confidence: 1.0                          # ↳ product of the step confidences
      pipeline:
        - category: EXTRACTION                 # ↳ a structured field read — nothing to "recognise"
          input_value: lenalidomide
          output_value: lenalidomide
          method: STRUCTURED_FIELD
          tool: medic-ingest-ema              # ↳ the per-source parser that read the cell
          tool_version: "1.0.0"               # ↳ released MeDIC version (no agent: deterministic)
          quality: verbatim
          flags: []
        - category: GROUNDING
          input_value: lenalidomide            # ↳ == prev output_value ✓ chain holds
          output_value: CHEBI:63791
          output_label: lenalidomide
          method: LEXICAL_MATCH
          tool: medic-lexical-grounder
          tool_version: "1"                   # ↳ hand-bumped component version
          confidence: 1.0
          quality: lexical_exact
          source_vocabulary: CHEBI
          flags: []
        - category: NORMALIZATION
          input_value: CHEBI:63791             # ↳ == prev output ✓
          output_value: CHEBI:63791            # ↳ identity: already canonical CHEBI
          output_label: lenalidomide
          method: DETERMINISTIC_RULE
          tool: medic-normalizer
          tool_version: "1"
          quality: none
          target_namespace: CHEBI
          flags: []
  approvals:                                   # ↳ the "drug is approved" claim, one row per authority
    - {authority: NMPA_CHINA, source: CDE_CHINA, status: APPROVED, source_role: PRIMARY,
       approval_date: "20130122",
       regulatory_document_url: "https://www.cde.org.cn/main/xxgk/listpage/2f78f372c1867de05a2cd5c26a793612"}
    - {authority: EMA, source: EMA_EPAR, status: APPROVED, source_role: PRIMARY,
       approval_date: "20070614",
       regulatory_document_url: "https://www.ema.europa.eu/en/medicines/human/EPAR/lenalidomide-krka"}
    - {authority: FDA, source: ORANGEBOOK, status: APPROVED, source_role: PRIMARY,
       approval_date: "20051227", marketing_status: RX,
       application_number: "021880|201452|209348|210154|210435|210480|211022|211846|212414|213165|213405|213885|213912|214398|214618|215759|216213|217265|217281|217554|218872",
       regulatory_document_url: "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=021880"}
    - {authority: PMDA, source: PMDA, status: APPROVED, source_role: PRIMARY,
       approval_date: "20100625", regulatory_document_url: "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"}
    - {authority: MOH_RUSSIA, source: GRLS, status: APPROVED, source_role: PRIMARY,
       approval_date: "20090522", application_number: "ЛП-003551",
       regulatory_document_url: "https://grls.rosminzdrav.ru/Default.aspx"}
  reliability: HIGH                            # ↳ exact grounding + verifiable provenance + approved
  atc:                                         # ↳ nested (supersedes the flat atc_* below)
    codes: [L04AX04]
    main: L
    level1: L04
    level2: L04A
    level3: L04AX
    level4: L04AX04
    level5: L04AX04
  features: []                                 # ↳ no tags fire (L04 is not L01/L02); supersedes the is_* below
  smiles: Nc1cccc2c1CN(C1CCC(=O)NC1=O)C2=O
  source_ingredients: [Lenalidomide Capsules, lenalidomide, Lenalidomide, LENALIDOMIDE, LENALIDOMIDE HYDRATE]
  alternate_ids: [CHEBI:63791, ChEMBL:CHEMBL848, DRUGBANK:DB00480, DrugCentral:3317,
                  LyCHI:67DVZ6C9NYMH, PHAROS:67DVZ6C9NYMH, PubChem:216326, pt:LENALIDOMIDE, unii:F0P408N6V4]
  evidence:                                    # ↳ one regulatory row per contributing source
    - {source_type: REGULATORY, jurisdiction: CHINA, approval_status: APPROVED, confidence: HIGH,
       explanation: "China CDE/NMPA approved drug (approved 20130122)", reference: "https://www.cde.org.cn/…"}
    - {source_type: REGULATORY, jurisdiction: EU, approval_status: APPROVED, confidence: HIGH,
       explanation: "European Medicines Agency authorized medicinal product (approved 20070614)", reference: "…"}
    - {source_type: REGULATORY, jurisdiction: USA, approval_status: APPROVED, confidence: HIGH,
       explanation: "FDA Orange Book approved drug product (approved 20051227)", reference: "…"}
    - {source_type: REGULATORY, jurisdiction: JAPAN, …}
    - {source_type: REGULATORY, jurisdiction: RUSSIA, …}
  # --- additive leftovers, deleted at the next cutover (issue_drop_flat_atc_is_fields.md) ---
  atc_codes: [L04AX04]
  atc_main: L
  atc_level1: L04     # … level2..level5
  is_allergen: false
  is_antimicrobial: false
  is_cancer_drug: false     # … twelve more `is_*: false`
  combination_therapy_ingredients: []
  combination_therapy_ingredients_curies: []
```

### 5. What a *translated* trail looks like (the same drug's China contribution)

The representative above is English, so it has no `TRANSLATION` step. The China source record for this same
drug resolves through the four-step chain the store rows in §1 describe:

```yaml
pipeline:
  - {category: EXTRACTION, input_value: 来那度胺胶囊, output_value: 来那度胺胶囊,
     method: STRUCTURED_FIELD, tool: medic-ingest-china, tool_version: "1.0.0", quality: verbatim, flags: []}
  - category: TRANSLATION
    input_value: 来那度胺胶囊                    # ↳ == prev output ✓
    output_value: Lenalidomide Capsules
    method: API
    tool: babelon                              # ↳ the MeDIC translator service…
    tool_version: "0.3.6"                      # ↳ …its real distribution version
    agent: {agent_id: "wikidata:Q116709136", agent_type: AI_AGENT, agent_name: DeepL}
    # ↳ DeepL is the engine; no agent_version — it publishes none, and inventing one would be worse
    confidence: 0.85
    status: CANDIDATE                          # ↳ machine, unreviewed → caps reliability at MEDIUM
    source_language: zh
    target_language: en-us
    translator_expertise: ALGORITHM
    quality: close
    flags: [unreviewed_machine]                # ↳ FAILURE_MODES §7.3 — queryable per record
  - category: GROUNDING
    input_value: Lenalidomide Capsules         # ↳ == prev output ✓ (the grounder sees the English)
    output_value: CHEBI:63791
    output_label: lenalidomide
    method: LEXICAL_MATCH
    tool: medic-lexical-grounder
    tool_version: "1"
    confidence: 0.8
    applied_rules: [formulation_strip]         # ↳ funneled back off the SSSOM row
    quality: lexical_exact_surgery
    source_vocabulary: CHEBI
    flags: [formulation_stripped]              # ↳ FAILURE_MODES §5.4
  - {category: NORMALIZATION, input_value: CHEBI:63791, output_value: CHEBI:63791,
     method: DETERMINISTIC_RULE, tool: medic-normalizer, tool_version: "1", quality: none, flags: []}
```

---

## Walkthrough B — a DISEASE: `primary systemic carnitine deficiency` → `MONDO:0008919`

**Levocarnitine** (`CHEBI:16347`), a DailyMed indication. This exercises the whole machine: a hard
extraction, a genuine two-stage grounding (UMLS → MONDO), and the recognition/claim split.

### 1. The section the extractor read (stored once as the Mention's `TextSpan`)

> *Levocarnitine tablets are indicated in the treatment of **primary systemic carnitine deficiency**. In the
> reported cases, the clinical presentation consisted of recurrent episodes of Reye-like encephalopathy,
> hypoketotic hypoglycemia, and/or cardiomyopathy. Associated symptoms included hypotonia, muscle weakness
> and failure to thrive. A diagnosis of primary carnitine deficiency requires that serum, red cell and/or
> tissue carnitine levels be low … Levocarnitine tablets are also indicated for acute and chronic treatment
> of patients with an inborn error of metabolism which results in a secondary carnitine deficiency.*

**Why it is a complex extraction:** the section names the indication buried among six *associated symptoms*
(encephalopathy, hypoglycemia, cardiomyopathy, hypotonia, muscle weakness, failure to thrive) plus a
*second* indication in the last sentence. A naive "pull every disease" extractor would emit eight
associations — the over-extraction failure mode (FAILURE_MODES §5.2).

### 2. The backing store rows

```
# mappings/disease_grounding.sssom.tsv
subject_label: primary systemic carnitine deficiency   subject_id: MEDICNE:7cf4d54a-…
predicate_id: skos:broadMatch            # ↳ broadMatch — the match broadened the claim
object_id: UMLS:C0342788   object_label: Renal Carnitine Transport Defect
subject_preprocessing: qualifier_strip   confidence: 0.7500

# mappings/disease_normalization.sssom.tsv
UMLS:C0342788   skos:exactMatch   MONDO:0008919   asserted_exact   medic-normalizer/1
```

Two things to read here:

- **Grounding lands in UMLS, not MONDO** (invariant I-2). Stage-1 resolves to the highest-priority cascade
  vocab that yields a deterministic single match (diseases MONDO > HP > UMLS). No MONDO label matched after
  `qualifier_strip`, but a UMLS concept did — so it stops there. It is *not* forced to MONDO.
- **Normalization does the real work** (Stage-2): `UMLS:C0342788 → MONDO:0008919` via an `skos:exactMatch`
  that *MONDO itself publishes* (`asserted_exact`) — never synthesised. On the drug side this step was
  usually identity; here it earns its place.

### 3. The assembled product record — `products/indication_list.yaml` (complete, verbatim)

```yaml
- relationship_type: INDICATION                # ↳ the relation is named ONCE, here
  reliability: MEDIUM                          # ↳ capped by the grounding surgery (conf 0.75)
  drug:                                        # ↳ inlined Mention — the drug's FULL trail travels here too
    id: MEDICNE:32c8ca09-aaaa-5563-8658-6b39d6fbfd4c
    original_literal: Levocarnitine
    entity_type: drug
    mention_source: EVERYCURE
    resolved_id: CHEBI:16347
    resolved_label: (R)-carnitine
    resolution:
      input_value: Levocarnitine
      output_value: CHEBI:16347
      confidence: 0.85
      pipeline: [EXTRACTION(medic-ingest-everycure), GROUNDING(CHEBI:16347, 0.85), NORMALIZATION(identity)]
  disease:                                     # ↳ inlined Mention — recognised from THIS label's text
    id: MEDICNE:7cf4d54a-56e6-5ae6-82cc-f6c49426572d
    original_literal: primary systemic carnitine deficiency   # ↳ what the label said (preserved, I-7)
    entity_type: disease
    resolved_id: MONDO:0008919
    resolved_label: Renal Carnitine Transport Defect          # ↳ canonical label ≠ the surface form
    source_spans:                              # ↳ the whole Indications section, stored ONCE
      - text: "Levocarnitine tablets are indicated in the treatment of primary systemic carnitine
               deficiency. … Levocarnitine tablets are also indicated for acute and chronic treatment of
               patients with an inborn error of metabolism which results in a secondary carnitine deficiency."
        source_reference: DailyMed:0f749812-8f02-4eeb-91d9-5dd6fc9fa159
        section_code: LOINC:34067-9            # ↳ FDA SPL Indications & Usage
    resolution:
      input_value: "Levocarnitine tablets are indicated in the treatment of … [full section]"
      output_value: MONDO:0008919
      confidence: 0.75                         # ↳ 1.0 (recognition) · 0.75 (grounding) · 1.0 (normalization)
      pipeline:
        - category: EXTRACTION                 # ↳ NER: which entity — says NOTHING about indications
          input_value: "Levocarnitine tablets are indicated in the treatment of … [full section]"
          output_value: primary systemic carnitine deficiency
          method: LLM
          tool: medic-extractor
          tool_version: "1"
          agent: {agent_type: AI_AGENT, agent_name: anthropic/claude-haiku-4-5-20251001,
                  agent_version: claude-haiku-4-5-20251001}   # ↳ the DATED model id (FM 13.1)
          confidence: 1.0                      # ↳ the phrase is verbatim in the section
          quality: verbatim
          flags: []                            # ↳ recognition failures only
        - category: GROUNDING
          input_value: primary systemic carnitine deficiency   # ↳ == prev output ✓
          output_value: UMLS:C0342788
          output_label: Renal Carnitine Transport Defect
          method: LEXICAL_MATCH
          tool: medic-lexical-grounder
          tool_version: "1"
          confidence: 0.75
          quality: lexical_exact_surgery
          source_vocabulary: UMLS              # ↳ landed in UMLS, not MONDO (I-2)
          flags: []
        - category: NORMALIZATION
          input_value: UMLS:C0342788           # ↳ == prev output ✓
          output_value: MONDO:0008919
          method: DETERMINISTIC_RULE
          tool: medic-normalizer
          tool_version: "1"
          quality: asserted_exact              # ↳ MONDO's own published xref
          target_namespace: MONDO
          flags: []
  assertion:                                   # ↳ the CLAIM's provenance — sibling of `disease`, not inside it
    input_value: "Levocarnitine tablets are indicated in the treatment of primary systemic carnitine
                  deficiency. …"                # ↳ the supporting quote
    method: LLM
    tool: medic-extractor
    tool_version: "1"
    agent: {agent_type: AI_AGENT, agent_name: anthropic/claude-haiku-4-5-20251001,
            agent_version: claude-haiku-4-5-20251001}
    confidence: 1.0                            # ↳ how well the quote supports THIS relation
    flags: []                                  # ↳ negated_inversion / over_extraction / wrong_section land HERE
  evidence:                                    # ↳ indication-supporting evidence + the marketing artifact
    - source_type: REGULATORY
      jurisdiction: USA
      source_role: INTERMEDIARY                # ↳ DailyMed republishes the FDA SPL
      approval_status: APPROVED
      original_disease_label: primary systemic carnitine deficiency
      original_drug_label: LEVOCARNITINE
      setid: 0f749812-8f02-4eeb-91d9-5dd6fc9fa159
      reference: https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=0f749812-…
      source_document_url: https://dailymed.nlm.nih.gov/dailymed/downloadpdffile.cfm?setid=0f749812-…
      snippet: "Levocarnitine tablets are indicated in the treatment of … [≤500-char section quote]"
    - source_type: REGULATORY
      jurisdiction: USA
      source_role: PRIMARY                     # ↳ Orange Book outranks DailyMed (SPEC §3.2)
      approval_status: APPROVED
      approval_date: "19851227"
      application_number: "018948|019257|020182|075567|075861|075881|076851|076858|077399|211676|212533|216384|217430"
      explanation: FDA marketing approval per Orange Book record
      reference: https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=018948
  regulatory_status:                           # ↳ per-authority backing for THIS indication
    - {authority: FDA, source: ORANGEBOOK, status: APPROVED, source_role: PRIMARY, approval_date: "19851227"}
```

### 4. What to notice

1. **The disease record is relation-agnostic.** Nowhere in `disease` does the word "indication" appear. The
   relation lives on `relationship_type`; how the *claim* was read lives on `assertion`. Lift the `disease`
   block out and it is a self-contained, reusable entity record.
2. **Recognition succeeded, and that is stated plainly** — `confidence: 1.0` on the EXTRACTION step, with an
   empty recognition-flag list. If a future over-extraction detector decided this was a *depleting
   condition* rather than an indication, it would write `over_extraction` on the **assertion**, and the
   assertion gate would drop the record to LOW. The mention would stay untouched, because it was right.
3. **`original_literal` ≠ `resolved_label`** — the label said "primary systemic carnitine deficiency"; the
   canonical term is "Renal Carnitine Transport Defect". Both are kept, distinctly.
4. **The chain threads cleanly across vocabularies**: `…deficiency` → `UMLS:C0342788` → `MONDO:0008919`,
   every `output_value` equal to the next `input_value`.

### 5. What this trace used to expose

For a while the assembled disease `GroundingStep` showed no `applied_rules`, no `predicate_id` and no
`broadened` flag, even when the store's row carried `subject_preprocessing: qualifier_strip` and
`predicate_id: skos:broadMatch` — the funnel was wired in `drug_merge` only. That gap is **closed**:
`on_label_merge._build_disease_provenance` now reads the decision store directly
(`store.decision_for(mention_id=..., literal=..., object_id=...)`) and funnels `applied_rules`,
`predicate_id` and the flags onto the step.

The mental model it taught is worth keeping: **the store is the truth; the assembled step is a view
that can lag it.** When the two disagree, the store wins and the funnel is the bug.

---

## The payoff: a human edit → the provenance and reliability change

Because assembly is a pure read of the stores, editing a store *is* editing the provenance:

- **Fix/confirm a translation:** set `translation_status: OFFICIAL` (or correct `translation_value`) in
  `drug_translation.babelon.tsv`. Next build: Stage-0 reads it (never re-translates), `TranslationStep.status`
  → `CONFIRMED`, `unreviewed_machine` drops, the translation gate → HIGH.
- **Fix a wrong grounding:** edit `object_id` (or set a curated `mapping_justification`) in the grounding
  store. The row is treated as **locked** — the offline grounder short-circuits to it and never overwrites
  it — so the `GroundingStep` reflects the curated decision and its flags clear.
- **Override the whole statement:** `mappings/statement_review.tsv`, keyed by `statement_key`,
  `CONFIRMED`→HIGH / `REJECTED`→EXCLUDED, read by `reliability.py` above every automated gate.

Every edit is a git-tracked TSV row: survives regeneration, read deterministically, and shows up as a
clean diff in a PR.

## Mental model (one sentence)

The `resolution` block on a product is a **materialized view** of the `mappings/` decision stores; the
stores are the editable truth and the deterministic cache at once — so you curate the TSVs and rebuild,
and never hand-edit the assembled provenance.
