"""Restricted vocabulary term text must never become a published label (I-14).

MedDRA and SNOMED CT were never obtained separately — both ship inside the UMLS
Metathesaurus, and `DEFAULT_DISEASE_SAB` opted them into the disease index. `load_umls` then
took the first `ISPREF=Y` atom in file order as the CUI's label, which is alphabetical by SAB,
so `MDR` won constantly: `UMLS:C0151467` matched a SNOMED synonym of "Acute adrenocortical
insufficiency" and shipped labelled `Crisis addisonian`, a MedDRA `OL` atom that had nothing
to do with the match.

Matching against these vocabularies is internal lookup and stays. Publishing their strings is
redistribution and does not.
"""

from __future__ import annotations

from medic.grounding.lexical.loaders.umls import (
    LABEL_SAB_PREFERENCE,
    choose_label,
    is_restricted_label_sab,
)
from medic.merge.on_label_merge import _canonical_disease_label


# ---------------------------------------------------------------------------
# Which vocabularies may supply a label
# ---------------------------------------------------------------------------
def test_every_meddra_translation_is_restricted():
    """MedDRA ships ~20 language variants in UMLS; the prefix has to catch all of them."""
    for sab in ("MDR", "MDRJPN", "MDRSPA", "MDRGER", "MDRRUS", "MDRCZE"):
        assert is_restricted_label_sab(sab), sab


def test_open_vocabularies_are_not_restricted():
    for sab in ("MONDO", "HPO", "MSH", "NCI", "OMIM", "ORPHANET", "ICD10CM"):
        assert not is_restricted_label_sab(sab), sab


def test_snomed_may_label_under_the_gps_decision():
    """Decision 2026-08-15, on the SNOMED Global Patient Set.

    Recorded as a test so flipping it back is a deliberate edit rather than a silent one.
    The open caveat is scope, not permission: the GPS is a subset of SNOMED CT while the
    index allowlists the whole `SNOMEDCT_US`. See LICENSING.md.
    """
    assert not is_restricted_label_sab("SNOMEDCT_US")


def test_no_restricted_vocabulary_is_in_the_preference_order():
    assert not [s for s in LABEL_SAB_PREFERENCE if is_restricted_label_sab(s)]


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


def test_an_unlisted_but_unrestricted_vocabulary_can_still_label():
    assert choose_label({"MDR": "Embolism pulmonary", "CHV": "pulmonary embolism"}) == \
        "pulmonary embolism"


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
