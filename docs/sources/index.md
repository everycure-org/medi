# Data Sources

MeDIC integrates data from multiple regulatory, post-market, and research sources. Each source has a dedicated ingest module under `src/medic/ingest/<source>/` that writes schema-validated YAML into `kb/<product>/<source>/` (or `kb/research/`, `kb/diseases/`). Cross-source merging happens later in `src/medic/merge/*.py`.

## Drug sources

- **FDA Orange Book** ([orangebook.md](orangebook.md)) — small-molecule NDA/ANDA approvals (USA)
- **FDA Purple Book** ([purplebook.md](purplebook.md)) — BLA biologics (USA)
- **EMA EPAR** ([ema.md](ema.md)) — EU centrally authorized medicines
- **PMDA** ([pmda.md](pmda.md)) — Japanese new drug approvals
- **GRLS (Russia)** ([russia.md](russia.md)) — Russian State Register of Medicines
- **CDSCO (India)** ([india.md](india.md)) — Indian regulatory approvals
- **CDE (China)** ([china.md](china.md)) — Chinese Center for Drug Evaluation (ingest implemented; no data ingested today)
- **EveryCure** ([everycure.md](everycure.md)) — curated drug list with ATC, drug classes, and property tags

## Indication / contraindication sources

- **FDA DailyMed** ([dailymed.md](dailymed.md)) — FDA SPL indications and contraindications (USA)
- **EMA** ([ema.md](ema.md)) — EU indications from the medicines XLSX; optional contraindications extracted from EPAR Product Information PDFs
- **PMDA** ([pmda.md](pmda.md)) — Japanese indications; optional per-product review-report PDF contraindications
- **CDSCO (India)** ([india.md](india.md)) — India indications only (no contraindication source)

## Adverse event sources

- **PVLens** ([pvlens.md](pvlens.md)) — label-mined adverse events (USA SPLs)
- **FAERS** ([faers.md](faers.md)) — post-market spontaneous reports (USA)

## Research sources

- **CURE-ID** ([cureid.md](cureid.md)) — FDA/NCATS open-data clinician case reports
- **PubMed / deep research** — handled outside the ingest tree (curated under `kb/research/MONDO_*.yaml`)

## Supporting datasets

- **Disease list** ([disease_list.md](disease_list.md)) — canonical disease identifiers and filter flags

## Big-picture analysis

For an overview of which sources are fully raw vs. dependent on v1.0 Kedro intermediates, see [`analysis/sources_analysis.md`](../../analysis/sources_analysis.md).

## Licence

Each source page carries a `## Licence` section giving its upstream terms. The consolidated table,
the required attribution notice, and the open questions live in
[`LICENSING.md`](https://github.com/monarch-initiative/medic/blob/main/LICENSING.md).

The short version: the FDA and NIH sources are public domain; EMA, PMDA, and the two EveryCure
lists require attribution; India is unverified; China and Russia are not redistributable and their
source archives are not in this repository; and anything MedDRA-derived (FAERS, PVLens) is blocked
for release.
