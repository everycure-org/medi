# Licensing

MeDIC is not one work under one licence. It is code that builds a derived database out of a dozen
upstream sources whose terms differ, and several of those terms are incompatible with each other.
Applying a single permissive licence to the whole repository would misrepresent what a downstream
user is actually allowed to do — most importantly, it would tell them attribution is optional when
for EMA and PMDA it is mandatory.

So the repository is licensed in layers.

## What MeDIC licenses

| Layer | Covers | Licence |
|---|---|---|
| **Software** | `src/`, `tests/`, `scripts/`, `justfile`, `project.justfile`, LinkML schemas in `src/medic/schema/` | [BSD-3-Clause](LICENSE) |
| **Documentation** | `docs/`, `README.md`, `SPEC.md`, this file | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |
| **MeDIC curation** | The mapping *assertions* in `mappings/*.sssom.tsv`, and hand-curated content in `kb/` | Offered as [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/); the **files** are declared [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — see below |
| **Derived data products** | `products/`, `exports/`, GitHub release assets | **No MeDIC licence grant** — see below |

Two caveats on the CC0 layer:

- CC0 covers MeDIC's *contribution* — the decision that string X grounds to CURIE Y, at a given
  confidence, by a given method. It does not cover the `subject_label` and `match_string` columns,
  which reproduce verbatim strings from the sources and remain under the source terms in the table
  below.
- A waiver only works on rights the waiving party holds. MeDIC cannot and does not purport to place
  upstream source content in the public domain.

**Why the SSSOM files declare CC BY 4.0 and not CC0.** A file mixes both layers: MeDIC's assertions
*and* the verbatim source strings in `subject_label` / `match_string`. `license` is the one field a
machine reads, and CC0 in it says "attribution optional" — which is false for a file containing EMA
and PMDA strings, and is exactly the misreading this document warns against at the end. So the set
is declared CC BY 4.0, and the `#comment:` line records that MeDIC's own mapping decisions are still
offered as CC0. The change is about stating the obligation that genuinely attaches to the file, not
about MeDIC wanting credit for the mappings.

## What MeDIC cannot license

The derived data products are built from regulatory sources MeDIC does not own. MeDIC grants no
rights over them and passes the upstream terms through unchanged. If you redistribute a MeDIC data
product, you take on the obligations of every source that contributed to it.

Stated as a passthrough, which is the sentence that travels with every release asset:

> MeDIC grants no rights over upstream source content. The original licence of each contributing
> source remains in force and must be respected; parsing, translation, normalisation and mapping to
> ontology identifiers do not alter it. Where terms conflict, the stricter governs.

In practice this means **attribution is required**, because EMA and PMDA both require it and both
feed the merged on-label product. Ship this notice with any redistribution:

> Contains data from the European Medicines Agency (© EMA), the Pharmaceuticals and Medical Devices
> Agency of Japan (PMDA, <https://www.pmda.go.jp>), the U.S. Food and Drug Administration, and the
> U.S. National Library of Medicine. Data has been edited: source records were parsed, translated,
> normalised, and mapped to ontology identifiers by the MeDIC pipeline. Neither EMA, PMDA, FDA nor
> NLM endorses this derived work.

The "data has been edited" clause is not boilerplate — Japan's Public Data License 1.0 specifically
requires that modified content say so and not be presented as originating from the public body.

## Per-source terms

| Source | Jurisdiction | Upstream terms | Attribution | MeDIC redistributes? |
|---|---|---|---|---|
| [DailyMed](docs/sources/dailymed.md) (NIH/NLM SPLs) | USA | Public domain in practice | Courtesy only | Yes — derived only |
| [Orange Book](docs/sources/orangebook.md) | USA | US Government work, public domain (17 U.S.C. §105) | Courtesy only | Yes |
| [Purple Book](docs/sources/purplebook.md) | USA | US Government work, public domain | Courtesy only | Yes |
| [FAERS](docs/sources/faers.md) | USA | US Government work, public domain — **but carries MedDRA terms** | Courtesy only | Not by default (see MedDRA) |
| [CURE-ID](docs/sources/cureid.md) (FDA/NCATS) | USA | NIH open data, public domain | Courtesy only | Yes |
| [EMA](docs/sources/ema.md) | EU | © EMA. Reproduction permitted, commercial and non-commercial, **provided EMA is acknowledged as the source in each copy**. EU database right also applies. | **Required** | Yes, with attribution |
| [PMDA](docs/sources/pmda.md) | Japan | Japan [Public Data License 1.0](https://www.pmda.go.jp/english/0013.html). Source citation required; edited content must be marked as edited. | **Required** | Yes, with attribution + edit notice |
| [India CDSCO](docs/sources/india.md) | India | No licence declared on cdsco.gov.in. Indian government open data is normally GODL-India (attribution). **Unverified — confirm before commercial redistribution.** | Assume required | Derived only, cautiously |
| [Russia GRLS](docs/sources/russia.md) | Russia | No open licence. State register, bulk export obtained out-of-band. | n/a | Derived records only — **never the source archive** |
| [China CDE/NMPA](docs/sources/china.md) | China | No open licence. Approvals table scraped out-of-band. | n/a | Derived records only — **never the source archive** |
| [EveryCure drug-list](docs/sources/everycure.md) | — | CC BY 4.0 (HuggingFace `everycure/drug-list`) | **Required** | Yes, with attribution |
| [EveryCure disease-list](docs/sources/disease_list.md) | — | CC BY 4.0 (HuggingFace `everycure/disease-list`) | **Required** | Yes, with attribution |
| [PVLens](docs/sources/pvlens.md) | USA | Software is GPL-3.0; MeDIC consumes its CSV *output*, not its code, so no copyleft attaches. Output carries MedDRA terms. | — | Not by default (see MedDRA) |

## Reference vocabularies used for grounding

| Vocabulary | Licence | Consequence for MeDIC |
|---|---|---|
| Mondo | CC BY 4.0 | Attribution; no restriction on use of identifiers |
| ChEBI | CC BY 4.0 | Attribution; no restriction on use of identifiers |
| PubChem | Public domain | None |
| ChEMBL | CC BY-SA 3.0 | Used in `enrichment/atc_smiles.py`. Identifiers and ATC codes are facts; a bulk redistribution of ChEMBL-derived *fields* could attract share-alike. Keep ChEMBL-derived enrichment separable. |
| DrugCentral | CC BY-SA 4.0 (verify) | Same caution as ChEMBL |
| RxNorm | UMLS Metathesaurus Licence. RxNorm-original content is unrestricted; some contributing source vocabularies are not. | Redistribute RxCUIs, not third-party source content |
| **MedDRA** | ICH/MSSO **subscription licence**. Redistribution of dictionary terms is restricted. | **Blocking.** MedDRA reaches MeDIC by *two* routes: FAERS/PVLens (adverse events, not built), and — unnoticed until 2026-08-14 — the **UMLS disease grounding index**, which bundles 113,364 `MDR` atoms. Term text did reach the products; see the UMLS row and I-14. |
| **UMLS Metathesaurus** | UMLS Metathesaurus Licence. The Metathesaurus **bundles MedDRA (`MDR`) and SNOMED CT (`SNOMEDCT_US`)**; individual source vocabularies keep their own terms. | Used to build the disease grounding index (`conf/grounding_sources.yaml` → `background/umls-2021AA-mrconso.zip`). A licence-gated **build input**: not committed, not redistributable, and required to reproduce the index. Matching against it is internal lookup; publishing a restricted vocabulary's term text is not (I-14). |
| **SNOMED CT** | **Global Patient Set** (<https://www.snomed.org/gps>), reached through UMLS. | **Cleared for publication** (2026-08-15). SNOMED term text may be published as a label. Open caveat: the GPS is a *subset* of SNOMED CT while the disease index allowlists the whole `SNOMEDCT_US`, so the GPS licence covers some published SNOMED labels and not necessarily all. Narrowing to GPS members means gating on the GPS concept list — a free download, not yet needed. |
| **WHO ICD-10** (`ICD10`) | WHO copyright, reached through UMLS. **UMLS Appendix 1 Category 3 — publication expressly excluded.** | **Refused for publication** (2026-08-16). Matching against it is internal lookup and stays. It had been supplying published labels — 11 store concepts, incl. `UMLS:C0342919` → "Essential fatty acid [EFA] deficiency" and `UMLS:C0029104` → "Mental and behavioural disorders due to use of opioids, withdrawal state", verbatim rubrics reaching the KGX node names. Removing it costs **zero** unnamed concepts: all 11 fall back to MSH/NCI/SNOMED. Do not confuse with `ICD10CM` below — the SAB names differ by three characters and that is how this was missed. |
| ICD-10-CM (`ICD10CM`) | US federal (CDC/NCHS) — **public domain at source**; also reachable through UMLS. | Published as a label on the strength of the federal public-domain status, not the UMLS route. Open caveat: UMLS Appendix 1 places `ICD10CM` in a category whose *UMLS-route* distribution is US-scoped. Supplies ~37 published labels. Confirm before relying on it more heavily. |
| OMIM | UMLS Appendix 1 **Category 0** — no additional restriction (verified 2026-08-16). | Cleared for publication; supplies ~33 published labels. An earlier review flagged OMIM as licence-gated on the basis of Johns Hopkins copyright; checked against Appendix 1, that concern does not hold for the UMLS route. |
| **WHO ATC/DDD** | WHOCC copyright; the bulk index requires a paid licence. | Scraping whocc.no was deliberately removed (`docs/architecture.md`). ATC codes now come from ChEMBL/PubChem/DrugCentral/RxNorm/ChEBI, where they appear as facts. Do not reintroduce a whocc.no fetch. |

## What is published, and how that is decided

Release assets are enumerated in [`conf/release_assets.yaml`](conf/release_assets.yaml), one entry
per file, each declaring the sources it draws on and whether it may ship. A file in `products/` or
`exports/` with **no entry is refused**, so a new build output cannot become a release asset by
default — `just gh-release` fails until someone decides. Inspect the current plan with
`just release-assets`.

This replaced a glob over `exports/*.{csv,xlsx,jsonl,tsv}`. The glob is how the KGX export silently
became 57 MB of new release assets — including 4.6 MB of verbatim EMA/PMDA/DailyMed label text —
with no licensing decision taken.

The attribution notice is **generated from the manifest**, naming exactly the attribution-required
sources whose data is actually in the release, and is embedded in the outputs rather than only
beside them:

| Output | Where the notice appears |
|---|---|
| **Every release asset** | **`exports/NOTICE.md`** — attribution, the passthrough, and a per-asset table of which sources each file contains |
| KGX | `medic_kgx_metadata.yaml` → `license.attribution_notice` |
| SSSOM (export and `mappings/` stores) | `#comment:` in the mapping-set header, alongside the CC0 carve-out |
| Release | The release body, plus `LICENSING.md` and `NOTICE.md` as assets |

`exports/NOTICE.md` is the general instrument: it is generated from the manifest, covers the whole
release, and is regenerated by `just gh-release` before the assets are resolved, so it always
describes the release it ships in. The per-format notices are convenience copies for the two formats
that can carry one — most of what MeDIC ships is CSV, XLSX and JSONL, none of which can.

The two reliability TSVs carry **no in-band header** on purpose: `docs/reliability.md` documents
reading them with `csv.DictReader`, which a leading `#` comment block would break. `NOTICE.md`
covers them.

### How much verbatim source text actually ships

Measured on the current build. Every excerpt is capped at 500 characters by the extractor (PMDA
988), so this is excerpt-scale, not bulk extraction — which is the relevant question for the EU
database right discussed at the end of this file.

| Source | Excerpts | Verbatim text | Attribution |
|---|---:|---:|---|
| DailyMed | 6,888 | 2.79 MB | courtesy |
| EMA | 3,054 | 1.22 MB | **required** |
| PMDA | 2,620 | 0.57 MB | **required** |
| CDSCO | 132 | 0.03 MB | assumed required |

The same excerpts appear in `products/indication_list.yaml` (~4.9 MB) and, truncated identically, in
`exports/medic_edges.jsonl` (~4.6 MB).

## Open items

- **India CDSCO terms are unverified.** The table assumes attribution is required. Confirm against
  cdsco.gov.in or GODL-India before any commercial redistribution. Note that `exports/india.csv`
  carries *verbatim* CDSCO content — the formulation string, plus a short indication phrase on 101
  of its 112 rows — so it is the more exposed of the two undeclared-licence exports, not
  `russia.csv`.
- **MedDRA blocks the adverse-event products, and reached the on-label ones by a second route.**
  `kb/adverse_events/` holds only `.gitkeep` files today, and `products/adverse_event_list.yaml`
  is held in the release manifest (`ship: false`), so it stays out even once the ingest lands.
  MeDIC also ships **1,443 MedDRA identifiers** as crossreferences on disease records, inherited
  from Mondo — bare identifiers, no term text, a materially weaker exposure.

  **What this section said until 2026-08-14 was wrong.** It claimed "nothing MedDRA-derived is
  committed" and that the shipped MedDRA content was "bare identifiers with no MedDRA term
  text". Both were false. The UMLS Metathesaurus bundles MedDRA and SNOMED CT, the disease
  grounding index allowlisted `MDR` and `SNOMEDCT_US`, and the loader published whichever
  vocabulary's atom came first in file order — which is alphabetical, so MedDRA won constantly.
  MedDRA term text shipped as disease labels: `Crisis addisonian` (an `MDR` `OL` atom) named a
  record whose resolved id was `MONDO:0019801`. Of 2,504 distinct disease labels in the on-label
  products, **217 had no non-restricted source in UMLS that could have supplied them** (an upper
  bound on exposure, not an infringement count — several are generic regulatory phrases MedDRA
  merely also contains).

  Fixed under **I-14**, and the data is clean: normalization carries the canonical label, a
  restricted vocabulary is matched against but never supplies a published label, the index was
  rebuilt, and `scripts/refresh_grounding_labels.py` propagated the change into
  `mappings/disease_grounding.sssom.tsv` (1,242 labels changed, **0** decisions touched).
  MedDRA term text no longer appears anywhere in the on-label products. 28,542 UMLS concepts
  are known to MeDIC only through MedDRA and now ship unnamed; none of them currently carries
  a shipped disease label.
- **`exports/russia.csv` and `exports/india.csv` ship, deliberately.** Both are now explicit
  manifest entries with the reasoning recorded on them, rather than a side effect of a glob.
  `russia.csv` holds 5,885 rows of *DeepL-translated English* drug name + ChEBI id + approval date;
  the Cyrillic originals and the GRLS registration numbers stay in `kb/` and are not exported.
  Revisit `india.csv` before commercial redistribution (see above).
- ~~**The SSSOM headers overstate the position.**~~ RESOLVED. Both the export
  (`exports/medic_drug_mappings.sssom.tsv`) and the in-repo decision stores
  (`mappings/*_grounding.sssom.tsv`) now declare CC BY 4.0 with a `#comment:` recording that
  MeDIC's own mapping decisions are offered as CC0 and that `subject_label` / `match_string`
  carry verbatim source strings. Regenerate the stores on the next grounding run to pick up
  the new header; the normalization and Babelon stores still need the same treatment.
- **DrugCentral's licence is asserted from memory** and was not verified against the source.
- **`.github/workflows/release.yml`** no longer names the deleted `medi/` paths. It creates the
  draft release and ships this file; the data assets are attached from a local build with
  `just gh-release`, because `products/` and `exports/` are gitignored outputs a runner cannot
  rebuild (the manual source archives and API keys are not available to it).

## Why not CC0 for everything

Because it would be both ineffective and misleading. Ineffective, because a rights waiver cannot
reach rights MeDIC never held — the EMA and PMDA material would stay exactly as encumbered as it
was. Misleading, because a downstream user would reasonably read CC0 as "no attribution needed",
redistribute the merged product without acknowledging EMA, and breach EMA's terms on MeDIC's word.

The facts themselves — that a drug was approved for an indication on a date — are largely not
copyrightable in the United States (*Feist*). But the EU sui generis database right protects
substantial investment in obtaining and verifying database contents regardless of originality, and
that is what bites on bulk extraction from EMA. "It's just facts" is a US argument, not a European
one.
