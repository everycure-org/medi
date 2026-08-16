<div align="center">

# MeDIC

**Medicines, Diseases, Indications, and Contraindications**

An open knowledge base of what medicines are approved to treat — built only from government
regulatory sources, and traceable back to the exact sentence a regulator published.

[![CI](https://github.com/monarch-initiative/medic/actions/workflows/ci.yml/badge.svg)](https://github.com/monarch-initiative/medic/actions/workflows/ci.yml)
[![Paper](https://img.shields.io/badge/Nucleic%20Acids%20Research-2026-b31b1b.svg)](https://doi.org/10.1093/nar/gkaf1312)
[![DOI](https://img.shields.io/badge/DOI-10.1093%2Fnar%2Fgkaf1312-blue.svg)](https://doi.org/10.1093/nar/gkaf1312)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://monarch-initiative.github.io/medic/)
[![Schema](https://img.shields.io/badge/schema-LinkML-4b0082.svg)](https://linkml.io)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-green.svg)](LICENSE)

</div>

---

## What this is

Drug repurposing needs a reliable answer to a deceptively simple question: *what is this drug
already approved to treat, and where?* That answer is scattered across national regulators, in
free-text labels, in six languages, under names that rarely match.

MeDIC assembles it from the primary regulatory record — FDA, EMA, PMDA, CDSCO, GRLS, NMPA — grounds
every drug and disease to a public ontology, and keeps the full derivation so any claim can be
walked back to its source.

| | |
|---|---|
| **6,504** drug–disease indication pairs | from **9,716** individual source-document attestations |
| **2,399** contraindication pairs | from **2,978** attestations |
| **4,323** drugs | grounded to ChEBI |
| **23,224** diseases | grounded to Mondo |
| **6 jurisdictions** | USA, EU, Japan, India, Russia, China |

## Provenance is the point

Most drug–disease resources tell you *that* a drug treats a disease. MeDIC tells you **how it
knows** — every step from the string a regulator printed to the ontology term, recorded as a
replayable chain:

```yaml
original_literal: 乳酸环丙沙星氯化钠注射液        # what China's CDE published
resolution:
  output_value: CHEBI:100241                    # ciprofloxacin
  confidence: 0.855
  pipeline:
    - category: EXTRACTION      # read verbatim from the source table
      output_value: 乳酸环丙沙星氯化钠注射液
    - category: TRANSLATION     # DeepL, via babelon 0.3.6
      output_value: Ciprofloxacin Lactate and Sodium Chloride Injection
      flags: [unreviewed_machine]
    - category: GROUNDING       # salt and combination stripped, then matched
      applied_rules: [salt_ester_strip, combination_split]
      output_value: CHEBI:100241
    - category: NORMALIZATION
      output_value: CHEBI:100241
```

Each step names the tool or dated model that produced it and records both its incoming and
outgoing value, so the chain is contiguous by construction. Nothing is asserted without a
recorded reason — including the failures, which are kept rather than dropped.

An indication is scoped the same way. One canonical pair holds one assertion per attesting
**document**, so three regulators agreeing is visible as three independent attestations rather
than collapsed into one row:

```yaml
drug_label: lecanemab
disease_label: mild cognitive impairment
reliability: HIGH
confidence: {method: NOISY_OR, n_assertions: 3, overall: 0.993}
assertions:
  - {source: DAILYMED, jurisdiction: USA,   document: "DailyMed:9d1ff786-…"}
  - {source: EMA,      jurisdiction: EU,    document: "EMA:leqembi"}
  - {source: PMDA,     jurisdiction: JAPAN, document: "PMDA:LECANEMAB#22-20230925"}
```

## Products

| File | Contents |
|---|---|
| `products/indication_list.yaml` | Approved drug–disease indications, per-document provenance |
| `products/contraindication_list.yaml` | Contraindications (currently FDA only) |
| `products/drug_list.yaml` | Drug identities, ChEBI-grounded, with per-authority approvals |
| `products/disease_list.yaml` | Disease list, Mondo-grounded |
| `products/research_list.yaml` | Literature-derived associations (investigational) |
| `exports/` | Flat CSV/TSV for downstream use, including a reliability-filtered subset |

Every string→id and id→id decision also lands in git-tracked [SSSOM](https://mapping-commons.github.io/sssom/)
stores under `mappings/`, so grounding is reviewable and correctable by hand rather than locked
inside the code.

## Quick start

```bash
git clone https://github.com/monarch-initiative/medic.git
cd medic
just setup                      # install dependencies
just restore-manual-sources     # fetch the China + Russia source files
just build-on-label-list        # ingest, ground, merge
just qc                         # reconcile the build against its baseline
```

`just --list` shows everything. Two sources — China's CDE table and Russia's GRLS register —
cannot be fetched automatically and are not redistributable, so their snapshots live out-of-band
rather than in this repo. Set `MEDIC_MANUAL_SOURCES_URL` to the archive location before running the
restore recipe; see [`sources/README.md`](sources/README.md).

## How it works

Four stages, each recorded rather than implied:

1. **Ingest** — every source read from its primary publication, single-path and fail-loud. No
   source ever emits evidence for a jurisdiction it does not itself originate.
2. **Translate** — non-English drug names via DeepL, stored in [Babelon](https://github.com/monarch-initiative/babelon).
3. **Ground** — a deterministic two-stage lexical grounder resolves strings to ChEBI and Mondo,
   persisting every decision, including failures.
4. **Merge** — sources combine into canonical pairs, each retaining its per-document assertions.

Builds are offline, deterministic and byte-identical on re-run. A QC pass reconciles every source
row against the output and fails on unexplained drift.

## Documentation

- [Architecture](https://monarch-initiative.github.io/medic/) — the full pipeline
- [`docs/provenance-walkthrough.md`](docs/provenance-walkthrough.md) — how a record is built, end to end
- [`docs/source-isolation.md`](docs/source-isolation.md) — the hard invariant behind jurisdiction claims
- [`docs/reliability.md`](docs/reliability.md) — how the reliability tiers are computed
- [`docs/related-efforts/`](docs/related-efforts/) — how MeDIC relates to Open Targets and others
- [`SPEC.md`](SPEC.md) — requirements, invariants and the task ledger

## Citing MeDIC

> DeLuca M, Matentzoglu N, Sharp E, Li J, Hempstead C, Lim M, Kaniewski P, Carter EK, Koirala K,
> Ding E, Vijnck L, Brokmeier P, Toro S, Schaper K, Vergine J, Li O, Oprea TI, Fajgenbaum DC,
> Bizon C, Haendel M, Tropsha A.
> **Medicines, Diseases, Indications, and Contraindications (MeDIC): a foundational resource to
> support drug repurposing.**
> *Nucleic Acids Research.* 2026;54(D1):D1477–D1487. doi:[10.1093/nar/gkaf1312](https://doi.org/10.1093/nar/gkaf1312)

<details>
<summary>BibTeX</summary>

```bibtex
@article{deluca2026medic,
  title   = {Medicines, Diseases, Indications, and Contraindications (MeDIC):
             a foundational resource to support drug repurposing},
  author  = {DeLuca, Marcello and Matentzoglu, Nico and Sharp, Elliott and Li, Jane and
             Hempstead, Charlie and Lim, May and Kaniewski, Piotr and Carter, E. Kathleen and
             Koirala, Kushal and Ding, Elvin and Vijnck, Laurens and Brokmeier, Pascal and
             Toro, Sabrina and Schaper, Kevin and Vergine, Jacques and Li, Olivia and
             Oprea, Tudor I. and Fajgenbaum, David C. and Bizon, Christopher and
             Haendel, Melissa and Tropsha, Alexander},
  journal = {Nucleic Acids Research},
  year    = {2026},
  volume  = {54},
  number  = {D1},
  pages   = {D1477--D1487},
  doi     = {10.1093/nar/gkaf1312},
  pmid    = {41385096}
}
```

</details>

## Contributing

Issues and pull requests are welcome at
[monarch-initiative/medic](https://github.com/monarch-initiative/medic/issues). Known gaps are
filed openly rather than hidden — coverage limits, grounding tails and uncalibrated confidence
values all have issues.

The most useful contribution is usually curation: the `mappings/` SSSOM stores are hand-editable,
and a corrected grounding row is honoured on the next build.

## License

The code and schemas are [BSD-3-Clause](LICENSE). The documentation is CC BY 4.0, and MeDIC's own
mapping assertions are CC0.

The **data products are a different matter**: they are derived from a dozen regulatory sources with
incompatible terms, and MeDIC grants no rights over them. EMA and PMDA both require attribution, so
any redistribution of a merged product must carry the notice in [`LICENSING.md`](LICENSING.md) —
which also gives the per-source breakdown and the two sources MeDIC does not redistribute at all.
