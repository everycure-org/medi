"""A decision read back from the store classifies exactly as it did when it was made.

`GroundingStoreView` rebuilds the drug mention on every on-label assertion, so its
`quality()` is what `GroundingStep.quality` publishes for those records. It used to
re-derive the value from `applied_rules` alone, because the view dropped the store's
`match_string` column — and `lexical_exact` vs `lexical_exact_normalized` *is* a comparison
of `match_string` against the trimmed subject. No rules recorded therefore answered
`lexical_exact`, so 10,656 drug steps asserted the string was unchanged when it was not
(`VORICONAZOLE` matched `voriconazole`). The on-label products reported zero
`lexical_exact_normalized` for drugs; `drug_list.yaml`, which goes through
`matcher.quality_of`, reported 2,200 of them.

The fix is to stop having two implementations of one enum: the view delegates to
`quality_of`. These tests pin that the delegation stays honest.
"""

from __future__ import annotations

from medic.grounding.lexical.matcher import quality_of
from medic.grounding.store import GroundingDecision as StoreDecision
from medic.grounding_store_view import GroundingDecision

LEXICAL = "semapv:LexicalMatching"


def _view(**kw) -> GroundingDecision:
    base = dict(
        subject_label="VORICONAZOLE", object_id="CHEBI:10023", object_label="voriconazole",
        predicate_id="skos:exactMatch", mapping_justification=LEXICAL,
        match_string="voriconazole", applied_rules=(),
    )
    return GroundingDecision(**{**base, **kw})


def test_a_match_that_needed_normalization_is_not_reported_as_unchanged():
    """The regression: `VORICONAZOLE` -> `voriconazole` is normalized, not exact."""
    assert _view().quality() == "lexical_exact_normalized"


def test_a_genuinely_unchanged_string_is_still_exact():
    assert _view(subject_label="voriconazole").quality() == "lexical_exact"


def test_surrounding_whitespace_does_not_make_it_normalized():
    """`quality_of` compares against the *trimmed* subject."""
    assert _view(subject_label="  voriconazole  ").quality() == "lexical_exact"


def test_applied_rules_still_mean_surgery():
    assert _view(applied_rules=("salt_ester_strip",)).quality() == "lexical_exact_surgery"


def test_rxnorm_proposals_keep_their_own_quality():
    """Marked by the preprocessing rule, not the justification — the justification slot is an
    SSSOM enum and only accepts `semapv:` terms."""
    dec = _view(mapping_justification="semapv:UnspecifiedMatching",
                applied_rules=("rxnorm_resolve",))
    assert dec.quality() == "rxnorm_proposed"


def test_the_view_and_the_grounder_agree_on_every_case():
    """One enum, one implementation. If these ever diverge, the products carry two answers."""
    cases = [
        dict(subject_label="VORICONAZOLE", match_string="voriconazole", applied_rules=()),
        dict(subject_label="voriconazole", match_string="voriconazole", applied_rules=()),
        dict(subject_label="Aspirin 100mg", match_string="aspirin",
             applied_rules=("formulation_strip",)),
    ]
    for case in cases:
        view = _view(**case)
        store = StoreDecision(
            subject_label=case["subject_label"], entity_type="drugs",
            predicate_id="skos:exactMatch", object_id="CHEBI:10023",
            object_label="voriconazole", object_match_field="label",
            mapping_justification=LEXICAL,
            subject_preprocessing=list(case["applied_rules"]),
            match_string=case["match_string"],
        )
        assert view.quality() == quality_of(store), case
