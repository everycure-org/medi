"""The flags the reliability gates read must actually be emitted.

`hallucination`, `truncated_snippet`, `coreference_ambiguity`, `over_extraction` and
`wrong_pairing` were declared in `provenance.yaml` and read by `reliability.py`, but nothing
in `src/` ever wrote them — so `_recognition_gate` returned None for every record in the
products and `_assertion_gate` collapsed to a confidence threshold. Zero flags across 12,694
assertions.

These tests pin the two detectors that now exist, and the boundaries of what they claim.
"""

from __future__ import annotations

from medic.merge.on_label_merge import _polarity_flags
from medic.spans import SNIPPET_CHAR_CAP, is_truncated


# ---------------------------------------------------------------------------
# truncated_snippet (recognition, FAILURE_MODES 5.6)
# ---------------------------------------------------------------------------
def test_a_span_on_the_cap_is_truncated():
    assert is_truncated("x" * SNIPPET_CHAR_CAP)


def test_a_short_span_is_not_truncated():
    assert not is_truncated("indicated for the treatment of hypertension")


def test_the_cap_is_shared_with_the_ingester_that_slices():
    """If EMA's slice and the merge's detector drift apart, detection silently stops."""
    source = (__import__("pathlib").Path(__file__).resolve().parents[1]
              / "src/medic/ingest/ema/__main__.py").read_text()
    assert "[:SNIPPET_CHAR_CAP]" in source
    assert "[:500]" not in source


# ---------------------------------------------------------------------------
# polarity: negated_inversion (EXCLUDED) vs over_extraction (LOW)
# ---------------------------------------------------------------------------
def _spans(*pairs):
    return [{"role": role, "text": text, "document": "d"} for role, text in pairs]


def test_a_fully_negated_full_phrase_is_an_inversion():
    text = "This product is not indicated for asthma."
    negated, flags = _polarity_flags("asthma", text, _spans(("SECTION_TEXT", text)), 0)
    assert negated is True
    assert flags == []


def test_a_positive_claim_is_neither_negated_nor_flagged():
    text = "Indicated for the treatment of hypertension in adults."
    negated, flags = _polarity_flags("hypertension", text, _spans(("SECTION_TEXT", text)), 0)
    assert negated is False
    assert flags == []


def test_the_raloxifene_case_is_over_extraction_not_an_inversion():
    """The regression that shipped at reliability HIGH, confidence 0.999.

    "vertebral, but not hip fractures" is an efficacy sentence, not an indication for
    vertebral fractures. The full phrase never occurs, so the strict check cannot see it and
    must not — head-word matching is too loose to EXCLUDE a record on. It is over-extraction:
    the entity is in the text, but not in this relationship to the drug.
    """
    text = ("Evista is indicated for the treatment and prevention of osteoporosis in "
            "post-menopausal women. A significant reduction in the incidence of vertebral, "
            "but not hip fractures has been demonstrated.")
    negated, flags = _polarity_flags(
        "vertebral fractures", text, _spans(("STRUCTURED_FIELD", text)), 0)
    assert negated is False
    assert flags == ["over_extraction"]


def test_a_disease_negated_in_its_own_right_is_still_an_inversion():
    """"not hip fractures" is a full-phrase match under a cue — a true inversion, not a
    head-word artefact."""
    text = "Reduces the incidence of vertebral, but not hip fractures."
    negated, _ = _polarity_flags(
        "hip fractures", text, _spans(("STRUCTURED_FIELD", text)), 0)
    assert negated is True


def test_a_positive_mention_is_not_dragged_down_by_a_shared_head_word():
    """The reason the strict check refuses head-word anchoring: "fractures" is negated in
    one sentence and indicated in another, and the claim is about the indicated one."""
    text = ("Indicated for the treatment of vertebral fractures. "
            "Not indicated for hip fractures.")
    negated, flags = _polarity_flags(
        "vertebral fractures", text, _spans(("STRUCTURED_FIELD", text)), 0)
    assert negated is False
    assert flags == []


# ---------------------------------------------------------------------------
# the Limitations-of-Use blind spot
# ---------------------------------------------------------------------------
def test_a_disease_only_negated_in_a_limitation_span_is_an_inversion():
    """The span filter that keeps limitations out of the claim also kept them out of the
    negation check — so the place inversions most often hide was the one place nothing looked."""
    spans = _spans(
        ("SECTION_TEXT", "Indicated for the treatment of migraine."),
        ("SUBSECTION_HEADER", "Limitations of Use"),
        ("LIMITATION_STATEMENT", "Not indicated for the prophylaxis of cluster headache."),
    )
    negated, _ = _polarity_flags("cluster headache", spans[0]["text"], spans, 0)
    assert negated is True


def test_a_scope_restriction_on_a_real_indication_is_not_an_inversion():
    """Labels routinely say "indicated for X" AND "Limitations of Use: not indicated for X
    in <subgroup>". That is a restriction, not an inversion, and must not be excluded."""
    spans = _spans(
        ("SECTION_TEXT", "Indicated for the treatment of migraine in adults."),
        ("SUBSECTION_HEADER", "Limitations of Use"),
        ("LIMITATION_STATEMENT", "Not indicated for the treatment of migraine in children."),
    )
    negated, _ = _polarity_flags("migraine", spans[0]["text"], spans, 0)
    assert negated is False
