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

#: Vocabularies whose strings may be MATCHED against but never PUBLISHED as a label.
#:
#: MedDRA (ICH/MSSO subscription) restricts redistribution of its term text. It was never
#: obtained separately — it ships inside the UMLS Metathesaurus, which is why the disease
#: grounder picked it up without anyone deciding to include it.
#:
#: Matching against it is internal lookup and keeps the recall it provides. Emitting one of its
#: strings as `object_label` publishes it: the label rides the SSSOM decision stores and every
#: product record downstream. That is the line this set draws.
#:
#: **SNOMED CT is deliberately not here** (decision 2026-08-15, on the Global Patient Set:
#: <https://www.snomed.org/gps>). One caveat is recorded rather than resolved: the GPS is a
#: *subset* of SNOMED CT — order 20k concepts — while this index allowlists the whole
#: `SNOMEDCT_US`, 1.5M atoms in UMLS 2021AA. So the GPS licence covers some of the SNOMED
#: labels MeDIC publishes, not necessarily all of them. Narrowing to GPS members would mean
#: gating on the GPS concept list, which is a free download nobody has needed yet. See
#: LICENSING.md.
RESTRICTED_LABEL_SAB_PREFIXES = ("MDR",)

#: Preference order for the published label, best first. Without one, `load_umls` took the
#: first `ISPREF=Y` atom in file order, which is alphabetical by SAB — so `MDR` won constantly
#: and CUIs got labelled with MedDRA strings that had nothing to do with the match. The worked
#: case: `UMLS:C0151467` matched a SNOMED synonym of "Acute adrenocortical insufficiency" and
#: shipped labelled `Crisis addisonian`, a MedDRA `OL` atom.
LABEL_SAB_PREFERENCE = ("MONDO", "HPO", "MSH", "NCI", "OMIM", "ORPHANET",
                        "ICD10CM", "ICD10", "ICD9CM", "MEDLINEPLUS", "SNOMEDCT_US")


def is_restricted_label_sab(sab: str) -> bool:
    """May this vocabulary's term text be published as a label?"""
    return (sab or "").startswith(RESTRICTED_LABEL_SAB_PREFIXES)


def choose_label(atoms: dict[str, str]) -> str:
    """Pick the published label for one CUI from ``{SAB: string}``.

    Preference order first; then any other unrestricted vocabulary; and if the CUI is known
    *only* to restricted vocabularies, **no label at all**. An empty label is honest — the id
    still resolves and the mapping still works — where publishing the restricted string would
    redistribute exactly the term text the licence covers.
    """
    for sab in LABEL_SAB_PREFERENCE:
        if sab in atoms:
            return atoms[sab]
    for sab, value in atoms.items():
        if not is_restricted_label_sab(sab):
            return value
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
            "%d of %d UMLS concepts have no publishable label — every ISPREF atom came from a "
            "restricted vocabulary (%s). They still match; they ship unnamed.",
            unlabelled, len(labels), ", ".join(RESTRICTED_LABEL_SAB_PREFIXES),
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
