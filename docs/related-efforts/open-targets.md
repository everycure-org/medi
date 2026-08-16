# Open Targets vs. MeDIC — Gap Analysis

**Status:** analysis · **Author:** N. Matentzoglu · **Date:** 2026-08-06 · **Branch:** `redesign`

This document analyses how the [Open Targets Platform](https://platform-docs.opentargets.org)
(OT) handles the problems MeDIC is built to solve, where the two projects genuinely overlap,
where they diverge, and what MeDIC could usefully borrow (or deliberately not borrow). It is based
on a review of the Open Targets docs, the `opentargets/*` GitHub org, the ChEMBL documentation, and
the Open Targets *Nucleic Acids Research* platform papers (2017, 2021, 2023, 2025). Sources are cited
inline; claims the research could not fully pin down are marked **⚠**.

---

## 0. TL;DR

**Open Targets and MeDIC are not the same kind of system, and most of OT is irrelevant to MeDIC.**
OT is a *target–disease association* platform whose unit of interest is the **gene/protein target**
(Ensembl gene id) and whose job is to help drug-discovery teams pick and prioritise targets. Drugs and
their indications are only *one* of ~50 evidence streams feeding that goal. MeDIC is a *drug–disease
indication* knowledge base whose entire reason to exist is the layer OT treats as a side input:
"which regulator approved which drug for which disease, with what document behind it."

The overlap is therefore narrow but real: **OT's drug/indication subsystem** vs. **the whole of MeDIC**.
Within that overlap:

- **Sources.** OT gets essentially all US and multilateral drug-indication data *indirectly through
  ChEMBL* (which itself aggregates FDA/DailyMed, EMA, WHO ATC, INN/USAN, ClinicalTrials.gov). As of the
  26.06 release OT *also* ingests **EMA Human Medicines** and **PMDA (Japan)** approvals directly, plus
  ClinicalTrials.gov (AACT) and TTD. It does **not** ingest FDA Orange/Purple Book, **India/CDSCO,
  Russia/GRLS, or China/NMPA at all** — three whole jurisdictions MeDIC covers that OT ignores.
- **Grounding.** OT grounds disease text to **EFO** (the EFO "OTAR slim") via **OnToma** — a
  deterministic, ordered cascade of curated tables + OLS/ZOOMA/OxO lookups. MeDIC grounds to **MONDO**
  (which EFO imports anyway) via its own deterministic **offline** two-stage lexical grounder. These are
  close cousins solving the same problem.
- **Provenance.** This is MeDIC's clearest lead. OT records the *winning* mapping step + a quality tag
  and keeps source-label-plus-mapped-id endpoints; it does **not** persist a replayable, per-step,
  in→out transformation chain, does **not** emit its own SSSOM decision store, and (since 26.06) uses
  **LLM entity extraction** for trial text with no controlled-transform audit trail. MeDIC's I-8
  transformation traceability, SSSOM decision stores, and offline determinism are all things OT does
  not have.
- **Where OT is clearly ahead:** scale and engineering maturity (quarterly immutable dated releases,
  BigQuery + GraphQL + Parquet distribution, Croissant ML-metadata), a single curated drug backbone
  (ChEMBL) instead of per-jurisdiction reinvention, a principled evidence-scoring framework
  (harmonic-sum aggregation with per-source weights), a published FAERS pharmacovigilance methodology
  (which MeDIC's AE stub could adopt wholesale), and a "Clinical Report" indication abstraction with a
  harmonised 13-tier phase ladder.

**Bottom line:** MeDIC is not reinventing Open Targets. It occupies the regulatory-provenance niche OT
deliberately outsources to ChEMBL, and it does provenance/determinism to a standard OT does not attempt.
The highest-value borrowings are (a) OT's FAERS/LRT method for the AE stub, (b) ChEMBL as a
*supplementary* drug-identity and indication backbone (used with eyes open about its provenance limits),
(c) OT's release-immutability + metadata discipline, and (d) mining OT's curated disease-mapping tables
as seed data for MeDIC's SSSOM stores.

---

## 1. What each project actually is

| | **MeDIC** | **Open Targets Platform** |
|---|---|---|
| Core question | Which drug is approved / studied for which disease, in which jurisdiction, with what regulatory document behind it? | Which gene/protein *target* is associated with which disease, and how strongly? |
| Central entity | Drug (ChEBI) ↔ Disease (MONDO) | Target (Ensembl gene) ↔ Disease (EFO) |
| Drugs are… | the whole point | one of ~50 evidence streams (the "known drug" datatype) |
| Primary use case | Drug **repurposing** research | Drug-discovery **target identification & prioritisation** |
| Scale | ~4–5k drugs, ~23k diseases, ~thousands of indications | 22,407 drugs · 47,080 diseases · 78,691 targets · **42.4M evidence** · 17.2M associations (26.06) |
| Funding/backing | small open project (RENCI/Monarch orbit) | EMBL-EBI + GSK + Sanger + industry consortium |

Sources: [OT platform overview](https://platform-docs.opentargets.org/) · [OT release notes](https://platform-docs.opentargets.org/release-notes) · MeDIC `SPEC.md`.

The consequence of this framing: OT's enormous machinery for genetics, expression, pathways, animal
models, and text-mined literature is **out of scope** for MeDIC. Only OT's **Drug** and
**pharmacovigilance** subsystems, its **ontology-mapping** layer (OnToma/EFO), and its **engineering
discipline** are comparable.

---

## 2. Data sources & jurisdictional coverage

### What OT ingests for drugs/indications

- **Drug/molecule backbone: ChEMBL** (EMBL-EBI). OT subsets ChEMBL's ~2M compounds down to ~11.6k–22k
  "drugs" (kept if they have ≥1 indication, ≥1 mechanism of action, a DrugBank mapping, or are a
  chemical probe). Molecules are identified by **ChEMBL IDs**, which are the join key across the
  molecule / mechanism-of-action / indication tables.
  ([OT Drug docs](https://platform-docs.opentargets.org/drug), [Drug Index rewrite blog](https://blog.opentargets.org/drug-index-rewrite/))
- **Indications**, in the rewritten "**Clinical Report**" pipeline, come from **six** upstream feeds:
  (1) ClinicalTrials.gov via **AACT**, (2) ChEMBL curated indications, (3) ChEMBL drug warnings,
  (4) **TTD**, (5) **EMA Human Medicines**, (6) **PMDA approvals**.
  ([Clinical Report docs](https://platform-docs.opentargets.org/drug/clinical-report))
- ChEMBL's own `drug_indication` table already aggregates **FDA (DailyMed/Drugs@FDA), EMA, WHO ATC**
  (approved drugs) and **USAN, INN, ClinicalTrials.gov** (clinical candidates), and carries both a
  `mesh_id` and an `efo_id` per indication plus `max_phase`/`max_phase_for_ind`.
  ([ChEMBL FAQ](https://chembl.gitbook.io/chembl-interface-documentation/frequently-asked-questions/drug-and-compound-questions))

### Coverage comparison

| Jurisdiction / source | **MeDIC** | **Open Targets** |
|---|---|---|
| USA — FDA Orange Book (small molecule) | ✅ direct primary | ❌ (FDA only via ChEMBL/DailyMed) |
| USA — FDA Purple Book (biologics) | ✅ direct primary | ❌ |
| USA — DailyMed SPL labels | ✅ direct (SPL XML, v2 API) | ⚠ indirect, via ChEMBL indication refs |
| EU — EMA | ✅ direct primary | ✅ direct ("EMA Human Medicines") |
| Japan — PMDA | ✅ direct primary (consolidated PDF) | ✅ direct ("PMDA approvals") |
| India — CDSCO | ✅ direct primary | ❌ **not ingested** |
| Russia — GRLS | ✅ direct primary | ❌ **not ingested** |
| China — CDE/NMPA | ✅ direct (drug list) | ❌ **not ingested** |
| ClinicalTrials.gov | roadmap (research stage) | ✅ direct via AACT |
| WHO ATC | enrichment (ATC codes) | ✅ via ChEMBL |
| Literature / PubMed | ✅ research associations | ✅ Europe PMC text-mining (down-weighted 0.2) |
| CURE-ID (repurposing case reports) | ✅ | ❌ |

**Key finding:** OT's regulatory ingestion is *narrower* than MeDIC's. It leans on ChEMBL as an
aggregator (which flattens per-jurisdiction provenance) and adds only EMA + PMDA as direct national
feeds. **India, Russia, and China have no representation in OT at all.** MeDIC's multi-jurisdiction,
source-isolated, direct ingestion is a genuine differentiator, not a reinvention.

**Trade-off MeDIC is making explicitly:** ChEMBL gives OT a *curated, deduplicated, already-grounded*
drug + indication table "for free," at the cost of (a) losing the verbatim per-regulator document trail
and (b) inheriting ChEMBL's coverage gaps (no India/Russia/China, US indications filtered through
ChEMBL's own curation choices). MeDIC pays for direct ingestion in engineering effort but gets exactly
the provenance ChEMBL discards — which is the whole point of MeDIC.

---

## 3. Drug identity & grounding

| | **MeDIC** | **Open Targets** |
|---|---|---|
| Canonical drug id | **ChEBI** (Stage-2), with DRON fallback | **ChEMBL ID** |
| Grounding of drug strings | deterministic offline two-stage lexical grounder (SQLite index; exact → normalized → surgery → salt/combination/INN → fuzzy edit-1) | ChEMBL is *already* an entity DB — OT rarely grounds free-text drug strings (exception: LLM extraction of drug names from AACT trial text, since 26.06) |
| ChEBI exposure | canonical | **not surfaced** (ChEMBL↔ChEBI exists upstream via UniChem, but OT does not expose ChEBI ids) ⚠ |
| Salt/parent handling | active-moiety extraction (salt strip rule) | ChEMBL parent/child model (parents aggregate child salt/formulation data) |
| Non-English drug names | **Stage-0 DeepL translation** (Babelon store) for zh/ru | n/a — ChEMBL supplies English INN |

**Observation.** OT sidesteps drug grounding almost entirely by adopting ChEMBL as the drug *registry*
rather than grounding text to an ontology. MeDIC cannot do this without giving up its per-jurisdiction
sources (whose drug strings — Cyrillic, Japanese, Indian formulation noise — are exactly what ChEMBL
doesn't have). MeDIC's choice of **ChEBI** as canonical is more ontology-standard/open than a ChEMBL
id, and interoperates cleanly with the disease side (both OBO ontologies).

**Borrowable idea:** ChEMBL (via its Elasticsearch/REST dumps, or via UniChem) is a strong candidate as
a *supplementary* drug backbone for MeDIC's US/EU/multilateral tail — both as an additional grounding
dictionary and as a source of ChEMBL↔ChEBI↔DrugBank cross-refs to enrich `alternate_ids`. This is the
same role RxNorm already plays (ledger #14), and would compose with it.

---

## 4. Disease grounding — OnToma/EFO vs. MeDIC's grounder

This is the closest technical parallel between the two systems.

### OT: OnToma → EFO

- **Canonical target: EFO**, specifically the **EFO "OTAR slim"** (an OT-specific profile regenerated
  each EFO release). EFO **imports MONDO** (mapped via **OxO**, stored in `imports/mondo_efo_mappings.tsv`)
  and cross-references OMIM/Orphanet/ICD9-10/SNOMED/HPO. So EFO ⊇ MONDO for practical purposes.
  ([EFO3 blog](https://blog.opentargets.org/efo3-a-community-driven-ontology-to-advance-clinical-discoveries/),
  [EBISPOT/efo README](https://github.com/EBISPOT/efo/blob/master/README.md))
- **OnToma** ([repo](https://github.com/opentargets/OnToma)) maps disease/phenotype **strings *and*
  source IDs** (e.g. `OMIM:102900`, `ICD9CM:…`) to EFO. Two generations:
  - *Classic* (v0.0.x): a **deterministic, ordered, short-circuit** cascade — EFO OBO lookup → ZOOMA
    (OT-curated mappings) → ZOOMA API → OLS exact (EFO) → HP OBO → OLS exact (HP) → OLS exact (ORDO)
    → OLS fuzzy (EFO) → OLS fuzzy (HP/ORDO). Results tagged `match`/`fuzzy`/`check` + source. No ML.
  - *Rewrite* (current `master`): entity-agnostic **PySpark** bulk matcher using **Spark NLP**
    normalisation instead of live service calls; can return multiple ids per entity. Used by 7 OT
    data sources. ⚠ (source list from a release summary, not a canonical page)
- **Curation overrides** live as version-controlled TSVs (the `opentargets/curation` repo, per-source
  tables in `evidence_datasource_parsers`, ZOOMA-submitted OT mappings), diffable but
  schema-heterogeneous.

### Side-by-side

| | **MeDIC grounder** | **OnToma** |
|---|---|---|
| Canonical disease id | **MONDO** (>HP>UMLS at Stage-1) | **EFO** (OTAR slim; imports MONDO) |
| Determinism | offline, byte-identical reruns | deterministic (classic); rewrite adds Spark-NLP normalisation |
| Network at resolve time | **none** (local SQLite) | classic hits OLS/ZOOMA/OxO live APIs; rewrite is bulk local |
| Decision persistence | **every** decision → SSSOM store (incl. `NoTermFound`) | winning-step + quality only; curated inputs in git TSVs |
| SSSOM output | ✅ literal + term↔term profiles | ❌ ad-hoc TSVs (SSSOM exists **upstream in MONDO**, consumed not produced) |
| Manual override | hand-editable, locked SSSOM rows survive regeneration | ZOOMA submissions + curated TSVs |
| Ambiguity | never auto-resolved | classic returns single best; rewrite pushes multi-id downstream |

**Assessment.** The two grounders are architecturally similar (curated tables + lexical matching +
id-crosswalks, deterministic). MeDIC's is **offline and fully persisted** (every decision, including
failures, is a diffable SSSOM row), which OnToma is not. MeDIC choosing **MONDO** over EFO is sound:
EFO imports MONDO anyway, MONDO is the more widely-used open disease ontology, and it keeps MeDIC on a
clean OBO stack. There is **no reason for MeDIC to adopt OnToma** — its own grounder is better suited to
MeDIC's provenance goals.

**Borrowable idea:** OT's **curated disease-mapping tables** (`opentargets/curation`,
`evidence_datasource_parsers/*2EFO*.tsv`, ZOOMA mappings) are a free source of high-quality
string→disease and ID→disease mappings. Since EFO↔MONDO equivalences are published (OxO /
`mondo_efo_mappings.tsv`, and MONDO's own SSSOM), MeDIC could **harvest OT's curated mappings, crosswalk
EFO→MONDO, and seed its own SSSOM decision stores** — recovering curation effort MeDIC would otherwise
redo by hand. Worth a scoped spike.

---

## 5. Indications, contraindications, phase

- **Indications.** OT's new **Clinical Report** abstraction is a "single, traceable piece of clinical
  evidence linking a drug to a disease or safety outcome," carrying {drug, disease/outcome, source,
  status}. Heterogeneous source phase labels are harmonised into a **13-tier ladder** (Withdrawal,
  Approval, Phase IV, Preapproval, Phase III, Phase II/III, Phase II, Phase I/II, Phase I, Early Phase I,
  IND, Preclinical, Unknown); reports for the same drug+disease collapse to the **max** stage.
  ([Clinical Report docs](https://platform-docs.opentargets.org/drug/clinical-report))
- **Contraindications.** OT has **no dedicated contraindications dataset**. Its safety surface is
  (a) ChEMBL **drug warnings** (black-box / withdrawn) and (b) FAERS pharmacovigilance. MeDIC's
  DailyMed-derived contraindications (LOINC 34070-3) are **something OT does not model at all**.
- **Approval status.** OT infers approval from ChEMBL `max_phase == 4` + regulatory feeds; MeDIC treats
  a *primary regulatory approval record* (Orange/Purple Book, EPAR, PMDA) as the authoritative "is
  approved" signal, distinct from the indication document — a distinction OT blurs.

**Borrowable idea:** MeDIC's indication model could adopt OT's **explicit phase/stage enum** (a
controlled `max_research_phase` already exists in MeDIC's evidence model; OT's 13-tier ladder is a
well-thought-out reference for the enum values and for harmonising heterogeneous source labels). The
**Clinical Report** framing — one traceable unit per (drug, disease, source, status) — is essentially
what MeDIC's per-source indication records already are, which is reassuring convergent design.

---

## 6. Adverse events / pharmacovigilance

MeDIC's AE pipeline (PVLens/FAERS) is a **stub**. OT has a mature, published methodology MeDIC could
adopt almost wholesale:

- **Source:** FDA **FAERS** via **OpenFDA**, method of Maciejewski et al. 2017 (*eLife* e25818).
- **Statistic:** a **Likelihood Ratio Test (LRT)** (Huang et al. 2011) with **critical values from a
  Monte-Carlo simulation** (openFDA-style empirical null) — *not* MGPS/EBGM. Implicitly bias-corrects
  for how often a drug/event appears.
- **Filtering (4 steps):** health-professional reports only (`primarysource.qualification ∈ {1,2,3}`);
  drop death outcomes (`seriousnessdeath=1`); reporter-attributed-causal only (`drugcharacterization=1`);
  minus a curated blacklist of uninformative events.
- **Drug identity:** FAERS strings matched to ChEMBL molecules across
  `medicinalproduct`/`generic_name`/`brand_name`/`substance_name`.
- **AE coding:** **MedDRA** terms, kept as-is — **not** mapped to HP/MONDO (likely a MedDRA licensing
  constraint). ⚠
  ([Pharmacovigilance docs](https://platform-docs.opentargets.org/drug/pharmacovigilance))

**Gap for MeDIC.** MeDIC's schema *wants* AEs mapped to HP/MONDO for the disease-centric view; OT
demonstrates the disproportionality pipeline but stops at MedDRA. So MeDIC would get the **signal
detection** method from OT for free, but the **MedDRA→HP/MONDO** mapping (via UMLS/OAK) remains MeDIC's
own problem — and a MedDRA licence question to resolve. This is worth a dedicated issue.

---

## 7. Transformation provenance & traceability (MeDIC I-8)

This is where MeDIC is unambiguously ahead of OT.

| Provenance dimension | **MeDIC** | **Open Targets** |
|---|---|---|
| Verbatim source string preserved | ✅ `original_literal` (I-7, never mutated) | ✅ `diseaseFromSource` / `targetFromSourceId` |
| Every transform step recorded in→out | ✅ `Mention.steps` trail, controlled enums (I-8) | ❌ records *winning step + quality* only |
| Every action a controlled enum | ✅ `PreprocessingRuleEnum` etc., enum-first discipline | ❌ ad-hoc; Spark-NLP normalisation not enum'd |
| Replayable from the record alone | ✅ by design | ❌ endpoints kept, not the chain |
| Every decision persisted (incl. failures) | ✅ SSSOM stores, `NoTermFound` rows | ❌ unmatched → dropped/`None` |
| Mention identity | ✅ `MEDICNE:<uuid5>` stable id (I-9) | ❌ none analogous |
| LLM steps auditable | LLM translation is enum'd + Babelon-logged; fuzzy is curator-reviewable | ❌ **26.06 LLM extraction of drug/disease from AACT text has no controlled-transform audit trail** |

The one place genuine SSSOM-grade, provenance-rich mapping exists in OT's orbit is **MONDO's own SSSOM
tables** — which OT/EFO *consume*, not produce. OT's design keeps provenance *endpoints* (source label +
mapped id + a link) but never the *replayable pipeline*. MeDIC's I-8/I-9 invariants are a real
contribution OT has no equivalent of.

**Implication:** MeDIC should not weaken this to look more like OT. If anything, MeDIC's SSSOM decision
stores + Mention trail are exactly the artifact OT (and ChEMBL) lack and that make MeDIC's regulatory
claims auditable in a way theirs are not. Note the cautionary tale: OT's move to **LLM extraction**
(26.06) buys coverage on messy trial text at the cost of the very traceability MeDIC's I-8 protects —
which is precisely why MeDIC keeps LLM translation enum'd and Babelon-logged and keeps fuzzy matches as
curator-reviewable proposals rather than silent trusts.

---

## 8. Source isolation

- **MeDIC:** a **documented, enforced invariant** (`docs/source-isolation.md`, I-1) — no ingester emits
  evidence for a jurisdiction it doesn't originate; cross-jurisdiction merging is confined to one merge
  module.
- **OT:** achieves a *similar separation implicitly* — each source submits evidence against its **own
  source-specific JSON Schema** (`disease_target_evidence.json` defined per source) in the `pis` input
  stage, and harmonisation/merge is a distinct downstream `pts` step. But OT has **no formal
  "isolation" invariant or terminology**; the framing is convergent design, not a stated rule.
  ([json_schema repo](https://github.com/opentargets/json_schema))

Convergent conclusion: both separate ingest from merge. MeDIC states it as a hard invariant with a
documented historical bug; OT enforces it structurally via per-source schemas. MeDIC's explicitness is a
strength for a smaller project where the discipline is easy to erode.

---

## 9. Evidence model & scoring

- **OT.** Association = target–disease pair, backed by **evidence** objects: `datasourceId`,
  `datatypeId` (constrained enum: `genetic_association`, `known_drug`, `literature`, …),
  `diseaseFromSource` + `diseaseFromSourceMappedId` (EFO), `resourceScore` (raw), `score` (harmonised
  [0,1]), `literature` (PMIDs), `urls[]`. Scores aggregate by a **three-level harmonic sum**
  (`Σ score_i / i²`, normalised by π²/6 ≈ 1.644) with **per-datasource weights** (e.g. Europe PMC 0.2,
  IMPC 0.2, OTAR 0.5, else 1.0). OT explicitly warns scores are **heuristics, not probabilities**.
  ([Associations](https://platform-docs.opentargets.org/associations),
  [Evidence](https://platform-docs.opentargets.org/evidence))
- **MeDIC.** `EvidenceItem` with `source_type`, `jurisdiction`, `reference` + title, verifiable
  `snippet`, `support`, `confidence`, `evidence_source`, `approval_status`, `max_research_phase`,
  inlined `curator` Agent. No numeric association scoring — MeDIC's model is categorical/regulatory
  (approved vs off-label vs research), not a continuous target-prioritisation score.

**Assessment.** OT's harmonic-sum scoring is central to *its* prioritisation use case and largely
**inapplicable** to MeDIC: MeDIC's job is to state *what a regulator approved*, which is a fact with a
document, not a score to aggregate. Where MeDIC *does* aggregate weaker signals — the **research
associations** — a lightweight OT-style weighting (e.g. down-weighting text-mined vs. clinical-trial
evidence) could be a reasonable future refinement, but it is not a gap so much as a design choice MeDIC
has correctly declined for regulatory data.

**Anti-hallucination:** OT largely doesn't need it (structured ETL, not generation); it validates via
JSON Schema/Pydantic and links to primary records. MeDIC's **snippet-faithfulness validation**
(`linkml-reference-validator` / `curate_snippets.py`) is genuinely needed precisely because MeDIC *does*
extract text via LLM — again, MeDIC's provenance stance is appropriate to its methods, not overbuilt.

---

## 10. Engineering: schema, pipeline, outputs, releases, licensing

| | **MeDIC** | **Open Targets** |
|---|---|---|
| Schema language | **LinkML** (`src/medic/schema/*.yaml`) | **JSON Schema + Pydantic** (`opentargets/json_schema`) |
| Pipeline | self-contained Python ETL, `just` targets, offline-deterministic | **`opentargets/pipeline`** Python monorepo (`pis`/`pts`/`orchestration`/`croissant`), **Airflow**, `Otter` transform lib, Polars/PySpark/pandas hybrid — **migrated off the legacy Scala/Spark `platform-etl-backend` (now archived)** |
| Determinism | byte-identical offline reruns; caches | reproducible-by-archival (inputs+jar+outputs frozen per release); cloud (Dataproc→Airflow) |
| Releases | `vX.Y.Z` GitHub release (products + exports) | **quarterly, immutable, dated `YY.MM`** (26.06, 26.03, …) |
| Output formats | LinkML YAML products; **legacy CSV/XLSX**; **Biolink KGX**; **SSSOM** | **Parquet only** (+ `p2j` for JSON); **BigQuery** public dataset; **GraphQL API**; FTP/GCS; **Croissant** ML-metadata |
| Graph export | ✅ **KGX (Biolink)** nodes/edges | ❌ **no KGX/Biolink/RDF export** (3rd parties like KG-Hub/RTX-KG2 ingest OT) ⚠ |
| SSSOM | ✅ decision stores + export | ❌ (consumes MONDO's SSSOM upstream) |
| Licensing | open (regulatory public data) | **outputs CC0**, **code Apache-2.0**; ~50 sources under negotiated "use without restriction" agreements; FinnGen R12 gated |

**Notable contrasts:**

1. **OT emits no Biolink KGX / graph** — MeDIC's KGX export is actually *ahead* of OT on
   knowledge-graph interoperability, even though OT is 1000× larger. If MeDIC wants to be consumed by
   the KGX/Biolink ecosystem (KG-Hub, ROBOKOP, RTX-KG2), it is already better positioned than OT's own
   outputs.
2. **OT's release immutability + Croissant metadata** is a discipline MeDIC should copy: dated,
   immutable release snapshots with machine-readable provenance metadata. MeDIC's `just gh-release
   vX.Y.Z` is close; adding a per-release metadata descriptor (Croissant or a MeDIC-native manifest
   pinning source versions + code SHA) would make MeDIC releases as reproducible-on-paper as OT's.
3. **JSON Schema vs LinkML:** MeDIC's LinkML choice is strictly richer (generates JSON Schema, SHACL,
   docs, Python) and better for an ontology-centric project. No change warranted.

---

## 11. Where MeDIC leads, where OT leads

### MeDIC is ahead of OT on

1. **Multi-jurisdiction direct regulatory ingestion** — India, Russia, China are absent from OT entirely;
   Orange/Purple Book and DailyMed are only indirectly present via ChEMBL.
2. **Full replayable transformation provenance (I-8)** and **mention identity (I-9)** — OT keeps
   endpoints, not the chain.
3. **SSSOM decision stores** — every string→id and id→id decision (including failures) persisted,
   diffable, hand-editable. OT produces none of its own.
4. **Offline determinism + anti-hallucination snippet validation** — appropriate to MeDIC's LLM-using
   methods; OT's LLM extraction (26.06) has no equivalent audit.
5. **Contraindications** as a first-class product.
6. **Biolink KGX export** — better KG-interop than OT's own outputs.
7. **Open canonical ids** (ChEBI/MONDO) vs OT's ChEMBL-id drug canonical.

### OT is ahead of MeDIC on

1. **Scale & engineering maturity** — quarterly immutable releases, BigQuery + GraphQL + Parquet, ~50
   integrated sources, industrial funding.
2. **A single curated drug backbone (ChEMBL)** — avoids per-jurisdiction reinvention for the
   US/EU/multilateral core.
3. **Published FAERS pharmacovigilance methodology (LRT + Monte-Carlo null)** — MeDIC's AE pipeline is a
   stub that could adopt this.
4. **Evidence-scoring framework** (harmonic-sum aggregation with per-source weights) — a reference for
   MeDIC's research-association layer if it ever needs continuous confidence.
5. **Clinical Report abstraction + 13-tier phase harmonisation** — a mature reference model for
   normalising heterogeneous indication phase labels.
6. **Release-metadata discipline** (Croissant, per-release immutable snapshots).
7. **A large, curated corpus of disease-mapping tables** MeDIC could harvest as seed mappings.

---

## 12. Recommendations for MeDIC

Ranked by value/effort. Each substantive one should become a tracked issue.

1. **Adopt OT's FAERS/LRT method for the AE stub.** Reimplement Maciejewski 2017 / Huang 2011 LRT with
   a Monte-Carlo empirical null over OpenFDA FAERS, with OT's four filters. Keep MedDRA as the native AE
   code; treat MedDRA→HP/MONDO as a separate mapping problem (and resolve the MedDRA licence question).
   *High value — turns a stub into a real product using a validated, citable method.*
2. **Evaluate ChEMBL as a supplementary drug backbone.** Use ChEMBL (via UniChem/Elasticsearch dumps)
   as (a) an extra offline grounding dictionary for the US/EU tail and (b) a source of
   ChEMBL↔ChEBI↔DrugBank cross-refs for `alternate_ids`. Compose with the existing RxNorm resolver.
   *Preserve MeDIC's direct per-jurisdiction ingestion — ChEMBL supplements, never replaces it,
   precisely because it discards the regulatory document trail MeDIC exists to keep.*
3. **Harvest OT's curated disease-mapping tables** (`opentargets/curation`,
   `evidence_datasource_parsers/*2EFO*`, ZOOMA mappings), crosswalk EFO→MONDO (OxO / MONDO SSSOM), and
   seed MeDIC's SSSOM decision stores. *Recovers curation effort; a scoped spike.*
4. **Add release-metadata discipline.** Emit a per-release manifest (Croissant-style or MeDIC-native)
   pinning each source's version/date + the code SHA + grounding-index hash, so a release is
   reproducible-on-paper the way OT's dated snapshots are. *Low effort, high scientific-credibility
   payoff.*
5. **Reference OT's 13-tier phase ladder** when finalising MeDIC's `max_research_phase` enum and any
   phase-harmonisation logic for research/trial evidence. *Cheap alignment with a well-designed
   controlled vocabulary.*
6. **Do NOT adopt OnToma or EFO.** MeDIC's offline grounder + SSSOM stores + MONDO canonical are better
   suited to MeDIC's provenance goals; EFO imports MONDO anyway. Record this as a deliberate decision so
   it isn't relitigated.
7. **Consider positioning MeDIC as a provenance-rich complement to OT/ChEMBL.** MeDIC's per-regulator,
   per-document, per-jurisdiction indications (esp. India/Russia/China) are exactly what ChEMBL and OT
   lack. There may be a contribution path (feeding curated regulatory indications with document
   provenance upstream to ChEMBL, or being ingested by OT as a datasource).

---

## Appendix — primary sources

**Open Targets docs:** [platform overview](https://platform-docs.opentargets.org/) ·
[Drug](https://platform-docs.opentargets.org/drug) ·
[Clinical Report](https://platform-docs.opentargets.org/drug/clinical-report) ·
[Indications](https://platform-docs.opentargets.org/drug/indications) ·
[Pharmacovigilance](https://platform-docs.opentargets.org/drug/pharmacovigilance) ·
[Disease or Phenotype](https://platform-docs.opentargets.org/disease-or-phenotype) ·
[Evidence](https://platform-docs.opentargets.org/evidence) ·
[Associations](https://platform-docs.opentargets.org/associations) ·
[Datasets/data access](https://platform-docs.opentargets.org/data-access/datasets) ·
[BigQuery](https://platform-docs.opentargets.org/data-access/google-bigquery) ·
[Licence](https://platform-docs.opentargets.org/licence) ·
[Release notes](https://platform-docs.opentargets.org/release-notes)

**GitHub:** [opentargets/pipeline](https://github.com/opentargets/pipeline) ·
[json_schema](https://github.com/opentargets/json_schema) ·
[OnToma](https://github.com/opentargets/OnToma) ·
[curation](https://github.com/opentargets/curation) ·
[evidence_datasource_parsers](https://github.com/opentargets/evidence_datasource_parsers) ·
[EBISPOT/efo](https://github.com/EBISPOT/efo) ·
[gentropy](https://github.com/opentargets/gentropy) ·
[platform-etl-backend (archived)](https://github.com/opentargets/platform-etl-backend)

**Blogs:** [Drug Index rewrite](https://blog.opentargets.org/drug-index-rewrite/) ·
[EFO3](https://blog.opentargets.org/efo3-a-community-driven-ontology-to-advance-clinical-discoveries/) ·
[ETL pipeline (historic)](https://blog.opentargets.org/etl-pipeline/) ·
[25.06 release](https://blog.opentargets.org/open-targets-platform-25-06-release/)

**ChEMBL / other:**
[ChEMBL FAQ — drugs](https://chembl.gitbook.io/chembl-interface-documentation/frequently-asked-questions/drug-and-compound-questions) ·
[UniChem](https://chembl.gitbook.io/unichem)

**Papers:** Koscielny 2017 *NAR* 45:D985 · Ochoa 2021 *NAR* 49:D1302 · Ochoa 2023 *NAR* 51:D1353 ·
Buniello 2025 *NAR* 53:D1467 · Maciejewski 2017 *eLife* e25818 · Huang 2011 (LRT) · SSSOM (Matentzoglu
2022, *Database* baac035).

**Flags:** ChEBI id exposure by OT (believed no); exact MeSH→EFO mapping method inside ChEMBL; whether
OT stores per-document deep links for its direct EMA/PMDA feeds; the "7 sources use OnToma" list; exact
LRT numeric threshold; MedDRA→HP/MONDO mapping absence — all marked ⚠ above and should be re-verified
against source if used as load-bearing claims.
