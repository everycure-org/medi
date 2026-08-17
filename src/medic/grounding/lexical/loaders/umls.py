"""Loader for UMLS MRCONSO, streamed directly from the distribution zip.

MRCONSO.RRF is pipe-delimited. Columns used: 0=CUI, 1=LAT, 6=ISPREF, 11=SAB, 14=STR.
Semantic-type filtering needs MRSTY (not in this MRCONSO-only zip); as a draft proxy we
restrict to a disease-relevant SAB allowlist. Preferred atoms (ISPREF=Y) become the
node label / a ``label`` row; all others become ``exactSynonym`` rows.
"""

from __future__ import annotations

import logging
import zipfile
from collections.abc import Iterator

from medic.grounding.lexical.index import LexRow
from medic.grounding.lexical.preprocess import base_normalize

logger = logging.getLogger(__name__)

# Disease-oriented source vocabularies (draft proxy for MRSTY semantic-type filtering).
DEFAULT_DISEASE_SAB = {
    "SNOMEDCT_US", "ICD10CM", "ICD9CM", "ICD10", "MSH", "NCI", "OMIM", "MONDO",
    "HPO", "ORPHANET", "MEDLINEPLUS", "MDR",
}

#: The vocabularies whose term text MeDIC may PUBLISH as a label, each with the decision that
#: permits it. Everything else may still be MATCHED against — that is internal lookup and keeps
#: the recall — but cannot name a concept in anything MeDIC ships. Emitting a string as
#: `object_label` publishes it: the label rides the SSSOM decision stores and every product
#: record downstream. That is the line this map draws (I-14).
#:
#: **This is an allowlist by deliberate reversal (2026-08-16).** It was a blocklist of one
#: prefix, `("MDR",)`, which meant every other licence-gated vocabulary the Metathesaurus
#: bundles passed through unexamined. WHO ICD-10 — whose UMLS licence category *expressly
#: excludes publication* — was shipping rubrics verbatim as KGX node names, e.g. `UMLS:C0029104`
#: → "Mental and behavioural disorders due to use of opioids, withdrawal state". A blocklist has
#: to enumerate what is forbidden, so its failure mode is silence; an allowlist has to enumerate
#: what is permitted, so an unassessed vocabulary fails closed. Adding a vocabulary here is a
#: licensing decision and must be recorded in LICENSING.md at the same time.
LABEL_SAB_DECISIONS: dict[str, str] = {
    "MONDO": "CC BY 4.0.",
    "HPO": "HPO licence, free use with attribution.",
    "MSH": "MeSH, US NLM — public domain.",
    "NCI": "NCI Thesaurus — open, no redistribution restriction.",
    "OMIM": "UMLS licence Category 0 — no additional restriction (verified 2026-08-16 against "
            "UMLS Appendix 1). The earlier concern that OMIM was licence-gated did not hold.",
    "ORPHANET": "CC BY 4.0.",
    "ICD10CM": "US federal (CDC/NCHS) — public domain at source, freely redistributable. "
               "Caveat recorded, not resolved: UMLS Appendix 1 places ICD10CM in a category "
               "whose UMLS-route distribution is US-scoped. MeDIC publishes it on the strength "
               "of the underlying federal public-domain status, not the UMLS route. "
               "Confirm before relying on it more heavily. See LICENSING.md.",
    "ICD9CM": "US federal — public domain at source, same basis as ICD10CM.",
    "MEDLINEPLUS": "US NLM — public domain.",
    "SNOMEDCT_US": "Global Patient Set (<https://www.snomed.org/gps>), decision 2026-08-15. "
                   "Caveat recorded, not resolved: the GPS is a *subset* of SNOMED CT — order "
                   "20k concepts — while this index allowlists the whole `SNOMEDCT_US`, 1.5M "
                   "atoms in UMLS 2021AA. The GPS licence covers some published SNOMED labels "
                   "and not necessarily all. Narrowing means gating on the GPS concept list, a "
                   "free download nobody has needed yet. See LICENSING.md.",
}

#: Vocabularies present in the index that are deliberately refused a publishing decision, with
#: the reason. Purely documentary — absence from `LABEL_SAB_DECISIONS` is what restricts them —
#: but it keeps the reasoning next to the rule instead of only in a commit message.
LABEL_SAB_REFUSED: dict[str, str] = {
    "MDR": "MedDRA, ICH/MSSO subscription licence restricts redistribution of dictionary term "
           "text. Never obtained separately — it ships inside the Metathesaurus, which is how "
           "the grounder picked it up without anyone deciding to include it. Applies to every "
           "language variant (MDRJPN, MDRSPA, ...).",
    "ICD10": "WHO ICD-10. UMLS Appendix 1 Category 3 — publication expressly excluded "
             "(verified 2026-08-16). Distinct from ICD10CM, which is the US clinical "
             "modification and public domain at source; the two were conflated because the SAB "
             "names differ by three characters.",
}

#: Preference order for the published label, best first. Without one, `load_umls` took the
#: first `ISPREF=Y` atom in file order, which is alphabetical by SAB — so `MDR` won constantly
#: and CUIs got labelled with MedDRA strings that had nothing to do with the match. The worked
#: case: `UMLS:C0151467` matched a SNOMED synonym of "Acute adrenocortical insufficiency" and
#: shipped labelled `Crisis addisonian`, a MedDRA `OL` atom.
#:
#: Every member must also appear in `LABEL_SAB_DECISIONS`; a test enforces that.
LABEL_SAB_PREFERENCE = ("MONDO", "HPO", "MSH", "NCI", "OMIM", "ORPHANET",
                        "ICD10CM", "ICD9CM", "MEDLINEPLUS", "SNOMEDCT_US")


def may_publish_label(sab: str) -> bool:
    """May this vocabulary's term text be published as a label? Unknown ⇒ no."""
    return (sab or "") in LABEL_SAB_DECISIONS


def is_restricted_label_sab(sab: str) -> bool:
    """Inverse of :func:`may_publish_label`. Kept as the name I-14 is written in terms of."""
    return not may_publish_label(sab)


def choose_label(atoms: dict[str, str]) -> str:
    """Pick the published label for one CUI from ``{SAB: string}``.

    Preference order first; then any other vocabulary carrying a publishing decision; and if
    the CUI is known *only* to vocabularies without one, **no label at all**. An empty label is
    honest — the id still resolves and the mapping still works — where publishing the string
    would redistribute exactly the term text the licence covers.

    The second loop is sorted so the outcome cannot depend on dict insertion order, i.e. on the
    order atoms happened to appear in MRCONSO (I-2).
    """
    for sab in LABEL_SAB_PREFERENCE:
        if sab in atoms:
            return atoms[sab]
    for sab in sorted(atoms):
        if may_publish_label(sab):
            return atoms[sab]
    return ""


def _row(cui, label, value, field, prefix="UMLS") -> LexRow:
    return LexRow(
        object_id=f"{prefix}:{cui}", object_label=label, string_value=value,
        raw_value=value.strip(), norm_value=base_normalize(value),
        match_field=field, synonym_scope="exact", source_prefix=prefix,
    )


def load_umls(zip_path: str, member: str = "MRCONSO.RRF",
              sab_allow: set[str] | None = DEFAULT_DISEASE_SAB,
              lang: str = "ENG") -> Iterator[LexRow]:
    # First pass: collect the candidate preferred label per CUI, keyed by the vocabulary that
    # supplied it, so `choose_label` can apply a preference order instead of taking whichever
    # atom the file happened to list first.
    candidates: dict[str, dict[str, str]] = {}
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as fh:
        for raw in fh:
            c = raw.decode("utf-8", "replace").rstrip("\n").split("|")
            if len(c) < 15 or c[1] != lang:
                continue
            if sab_allow is not None and c[11] not in sab_allow:
                continue
            if c[6] == "Y":
                candidates.setdefault(c[0], {}).setdefault(c[11], c[14])
    labels: dict[str, str] = {cui: choose_label(atoms) for cui, atoms in candidates.items()}
    # Restricting a vocabulary from labelling has a cost, and it should be visible at build
    # time rather than discovered in the products: these CUIs are known to MeDIC's index only
    # through a restricted vocabulary, so they match but carry no publishable name.
    unlabelled = sum(1 for v in labels.values() if not v)
    if unlabelled:
        logger.warning(
            "%d of %d UMLS concepts have no publishable label — no ISPREF atom came from a "
            "vocabulary carrying a publishing decision (permitted: %s). They still match; they "
            "ship unnamed.",
            unlabelled, len(labels), ", ".join(sorted(LABEL_SAB_DECISIONS)),
        )
    # Second pass: emit rows.
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as fh:
        for raw in fh:
            c = raw.decode("utf-8", "replace").rstrip("\n").split("|")
            if len(c) < 15 or c[1] != lang:
                continue
            if sab_allow is not None and c[11] not in sab_allow:
                continue
            cui, ispref, string = c[0], c[6], c[14]
            field = "label" if ispref == "Y" else "exactSynonym"
            # No fallback to `string`: that reintroduced the restricted term as the label
            # whenever the CUI had no unrestricted atom. An empty label is the honest answer.
            yield _row(cui, labels.get(cui, ""), string, field)
