"""Restricted vocabulary term text must never become a published label (I-14).

MedDRA and SNOMED CT were never obtained separately — both ship inside the UMLS
Metathesaurus, and `DEFAULT_DISEASE_SAB` opted them into the disease index. `load_umls` then
took the first `ISPREF=Y` atom in file order as the CUI's label, which is alphabetical by SAB,
so `MDR` won constantly: `UMLS:C0151467` matched a SNOMED synonym of "Acute adrenocortical
insufficiency" and shipped labelled `Crisis addisonian`, a MedDRA `OL` atom that had nothing
to do with the match.

Matching against these vocabularies is internal lookup and stays. Publishing their strings is
redistribution and does not.

Reversed to an allowlist on 2026-08-16. The guard had been a blocklist of one prefix, `MDR`,
so every other licence-gated vocabulary the Metathesaurus bundles passed through unexamined —
and WHO ICD-10 (UMLS Appendix 1 Category 3, publication expressly excluded) was publishing
rubrics verbatim as KGX node names. `LABEL_SAB_DECISIONS` now enumerates what *may* publish,
each with its reason, so an unassessed vocabulary fails closed instead of silently shipping.
"""

from __future__ import annotations

from pathlib import Path

from medic.grounding.lexical.loaders.umls import (
    LABEL_SAB_DECISIONS,
    LABEL_SAB_PREFERENCE,
    LABEL_SAB_REFUSED,
    choose_label,
    is_restricted_label_sab,
    may_publish_label,
)
from medic.merge.on_label_merge import _canonical_disease_label


# ---------------------------------------------------------------------------
# Which vocabularies may supply a label
# ---------------------------------------------------------------------------
def test_an_unassessed_vocabulary_may_not_label():
    """The point of the allowlist: no decision recorded ⇒ no publishing, by default.

    This is the structural half of the WHO ICD-10 fix. Under the previous blocklist an
    unknown vocabulary published silently, so the only vocabularies ever examined were the
    ones somebody happened to think of.
    """
    for sab in ("CHV", "LNC", "RXNORM", "SOMETHING_NEW", ""):
        assert not may_publish_label(sab), sab


def test_every_meddra_translation_is_refused():
    """MedDRA ships ~20 language variants in UMLS; none may label."""
    for sab in ("MDR", "MDRJPN", "MDRSPA", "MDRGER", "MDRRUS", "MDRCZE"):
        assert is_restricted_label_sab(sab), sab


def test_who_icd10_may_not_label_but_the_us_clinical_modification_may():
    """WHO ICD-10 is UMLS Appendix 1 Category 3 — publication expressly excluded.

    `ICD10` and `ICD10CM` are different vocabularies under near-identical SAB names, which is
    how WHO rubrics reached the KGX node names while everyone believed only the US public-domain
    modification was in play. Recorded as a test so the two cannot be conflated again.
    """
    assert not may_publish_label("ICD10")
    assert may_publish_label("ICD10CM")


def test_open_vocabularies_may_label():
    for sab in ("MONDO", "HPO", "MSH", "NCI", "OMIM", "ORPHANET", "ICD10CM"):
        assert may_publish_label(sab), sab


def test_snomed_may_label_under_the_gps_decision():
    """Decision 2026-08-15, on the SNOMED Global Patient Set.

    Recorded as a test so flipping it back is a deliberate edit rather than a silent one.
    The open caveat is scope, not permission: the GPS is a subset of SNOMED CT while the
    index allowlists the whole `SNOMEDCT_US`. See LICENSING.md.
    """
    assert may_publish_label("SNOMEDCT_US")


def test_no_restricted_vocabulary_is_in_the_preference_order():
    assert not [s for s in LABEL_SAB_PREFERENCE if is_restricted_label_sab(s)]


def test_every_preferred_vocabulary_carries_a_recorded_decision():
    """A vocabulary cannot be preferred for labelling without someone deciding it may."""
    missing = [s for s in LABEL_SAB_PREFERENCE if s not in LABEL_SAB_DECISIONS]
    assert not missing, f"in the preference order with no recorded decision: {missing}"


def test_every_decision_states_a_reason():
    """The map is the record. An entry with no reason is an undocumented licensing call."""
    for sab, reason in {**LABEL_SAB_DECISIONS, **LABEL_SAB_REFUSED}.items():
        # "CC BY 4.0." is a complete answer; "" and "TODO" are not.
        assert len(reason.strip()) >= 8, f"{sab} has no reason recorded"
        assert "TODO" not in reason.upper(), f"{sab} decision is unresolved"


def test_refused_and_permitted_do_not_overlap():
    assert not set(LABEL_SAB_REFUSED) & set(LABEL_SAB_DECISIONS)


# ---------------------------------------------------------------------------
# Label selection
# ---------------------------------------------------------------------------
def test_the_worked_case_no_longer_publishes_the_meddra_string():
    """UMLS:C0151467 — the record that shipped as `Crisis addisonian`."""
    atoms = {
        "MDR": "Crisis addisonian",
        "SNOMEDCT_US": "Acute adrenal insufficiency",
        "NCI": "Adrenal Crisis",
        "OMIM": "Addisonian crisis",
    }
    assert choose_label(atoms) == "Adrenal Crisis"  # NCI outranks OMIM in the preference order


def test_snomed_labels_a_concept_no_open_vocabulary_names():
    """The 450 labels the GPS decision recovers: SNOMED-only CUIs that were shipping unnamed."""
    assert choose_label({"MDR": "Embolism pulmonary",
                         "SNOMEDCT_US": "Pulmonary embolism"}) == "Pulmonary embolism"


def test_preference_order_beats_file_order():
    assert choose_label({"MDR": "Cholecystitis acute", "MSH": "Cholecystitis, Acute"}) == \
        "Cholecystitis, Acute"


def test_an_unassessed_vocabulary_does_not_get_to_label_by_default():
    """Was: "an unlisted but unrestricted vocabulary can still label", asserting the opposite.

    That permissiveness is exactly what let WHO ICD-10 publish. A CUI known only to
    vocabularies without a recorded decision now ships unnamed.
    """
    assert choose_label({"MDR": "Embolism pulmonary", "CHV": "pulmonary embolism"}) == ""


def test_who_icd10_no_longer_supplies_the_label():
    """The worked case: `UMLS:C0342919`, which shipped the verbatim WHO rubric."""
    atoms = {"ICD10": "Essential fatty acid [EFA] deficiency",
             "MSH": "Fatty Acid Deficiency, Essential"}
    assert choose_label(atoms) == "Fatty Acid Deficiency, Essential"


def test_a_cui_known_only_to_who_icd10_ships_unnamed():
    assert choose_label({"ICD10": "Mental and behavioural disorders due to use of opioids"}) == ""


def test_a_cui_known_only_to_meddra_gets_no_label():
    """An empty label is honest: the id still resolves, and nothing restricted is published."""
    assert choose_label({"MDR": "Crisis addisonian", "MDRSPA": "Crisis addisoniana"}) == ""


def test_no_atoms_at_all_is_empty_not_an_error():
    assert choose_label({}) == ""


# ---------------------------------------------------------------------------
# The label follows the canonical id at merge
# ---------------------------------------------------------------------------
def test_a_mondo_record_takes_the_mondo_label():
    labels = {"MONDO:0019801": "adrenal crisis"}
    assert _canonical_disease_label("MONDO:0019801", "Crisis addisonian", labels) == \
        "adrenal crisis"


def test_an_unknown_mondo_id_keeps_what_it_had():
    assert _canonical_disease_label("MONDO:9999999", "Something", {"MONDO:1": "x"}) == \
        "Something"


def test_a_non_mondo_record_keeps_its_label_when_the_store_has_no_opinion():
    """Stage 1 can legitimately rest at UMLS/HP; with no store row there is nothing better."""
    labels = {"MONDO:0019801": "adrenal crisis"}
    assert _canonical_disease_label("UMLS:C0151467", "Adrenal Crisis", labels) == "Adrenal Crisis"


def test_a_non_mondo_record_takes_the_store_label_over_its_own():
    """The store is authoritative for the label of a concept the record rests on (I-4)."""
    assert _canonical_disease_label(
        "UMLS:C0151467", "Crisis addisonian", None, "Adrenal Crisis") == "Adrenal Crisis"


def test_a_concept_the_store_ships_unnamed_ships_unnamed_here():
    """I-14 rule 2. ``""`` from the store is a decision, not a miss.

    This is the case that shipped 104 MedDRA strings into the KGX export: the store had
    already blanked the concept and the record kept the label its ingest run attached.
    """
    assert _canonical_disease_label(
        "UMLS:C0278488", "Breast carcinoma stage IV", None, "") == ""


def test_no_label_map_is_a_no_op():
    assert _canonical_disease_label("MONDO:0019801", "Crisis addisonian", None) == \
        "Crisis addisonian"


# ---------------------------------------------------------------------------
# The committed stores, checked directly
# ---------------------------------------------------------------------------
#: Verbatim WHO ICD-10 rubrics that were published as `object_label` before the allowlist
#: reversal (2026-08-16). Listed by hand because they are the actual regression: each one
#: shipped, and each is recognisable as WHO ICD-10 by its rubric conventions — British
#: spelling, "unspecified"/NOS tails, bracketed abbreviations.
_WHO_ICD10_RUBRICS_THAT_SHIPPED = (
    "Mental and behavioural disorders due to use of opioids, withdrawal state",
    "Essential fatty acid [EFA] deficiency",
    "Acute ischaemic heart disease, unspecified",
    "Vomiting of pregnancy, unspecified",
    "Anogenital pruritus, unspecified",
    "Hypertensive heart disease without (congestive) heart failure",
)


def _committed_store_labels(path: Path) -> list[str]:
    lines = [ln for ln in path.read_text().splitlines() if not ln.startswith("#")]
    if not lines:
        return []
    cols = lines[0].split("\t")
    if "object_label" not in cols:
        return []
    i = cols.index("object_label")
    return [f[i] for ln in lines[1:] if len(f := ln.split("\t")) > i]


def test_no_who_icd10_rubric_survives_in_a_committed_store():
    """Reads the artefacts that actually ship, not a temp copy.

    A hardcoded list of paths is what let the licence-header gap survive a fix written to
    close it (#48), so this globs `mappings/` — a new store cannot dodge the check.
    """
    stores = sorted(Path("mappings").glob("*.tsv"))
    assert stores, "no mapping stores found — wrong working directory?"
    for store in stores:
        labels = set(_committed_store_labels(store))
        for rubric in _WHO_ICD10_RUBRICS_THAT_SHIPPED:
            assert rubric not in labels, f"{store.name} still publishes the WHO ICD-10 rubric {rubric!r}"
