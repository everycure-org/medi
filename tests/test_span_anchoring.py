"""The extraction step names which span it read (design spec I-8b, D5)."""

import pytest

from medic.provenance_build import build_mention, validate_mention_chain

SPANS = [
    {"role": "SECTION_HEADER", "text": "INDICATIONS AND USAGE", "document": "DailyMed:x"},
    {"role": "SECTION_TEXT", "document": "DailyMed:x",
     "text": "UBRELVY is indicated for the acute treatment of migraine with aura in adults."},
    {"role": "SUBSECTION_HEADER", "text": "Limitations of Use", "document": "DailyMed:x"},
    {"role": "LIMITATION_STATEMENT", "document": "DailyMed:x",
     "text": "UBRELVY is not indicated for the preventive treatment of migraine."},
]

GROUNDING = {"original_string": "migraine with aura", "grounded_id": "MONDO:0005475",
             "grounded_label": "migraine with aura", "grounding_quality": "lexical_exact",
             "confidence": 1.0}


def _mention(**kw):
    extraction = {"span_index": 1, "output_value": "migraine with aura",
                  "method": "LLM", "confidence": 0.85}
    extraction.update(kw)
    return build_mention(
        "migraine with aura", "disease", spans=SPANS, extraction=extraction,
        grounding=GROUNDING, resolved_id="MONDO:0005475")


def test_the_extraction_input_is_the_named_span_not_a_concatenation():
    step = _mention()["resolution"]["pipeline"][0]
    assert step["input_value"] == SPANS[1]["text"]
    assert step["span_index"] == 1
    assert step["span_role"] == "SECTION_TEXT"


def test_char_offsets_locate_the_output_inside_the_span():
    step = _mention()["resolution"]["pipeline"][0]
    span_text = SPANS[1]["text"]
    assert span_text[step["char_start"]:step["char_end"]] == step["output_value"]


def test_offsets_are_absent_when_the_output_is_not_a_substring():
    """A synonym normalization has no offsets; inventing them would be a lie."""
    mention = build_mention(
        "high blood pressure", "disease",
        spans=[{"role": "SECTION_TEXT", "text": "Indicated for hypertension.",
                "document": "d"}],
        extraction={"span_index": 0, "output_value": "high blood pressure",
                    "method": "LLM", "confidence": 0.7, "quality": "synonym"},
    )
    step = mention["resolution"]["pipeline"][0]
    assert "char_start" not in step and "char_end" not in step


def test_spans_are_written_to_the_mention():
    assert _mention()["source_spans"] == SPANS


def test_the_chain_still_validates():
    assert validate_mention_chain(_mention()) == []


def test_an_out_of_range_span_index_raises_rather_than_silently_mis_anchoring():
    with pytest.raises(IndexError, match="span_index"):
        build_mention("x", "disease", spans=SPANS,
                      extraction={"span_index": 99, "output_value": "x", "method": "LLM"})


def test_without_spans_the_legacy_quote_path_still_works():
    """Plan 3 removes source_spans; until then both paths must build a valid chain."""
    mention = build_mention(
        "migraine with aura", "disease",
        source_spans=[{"text": "Indicated for migraine with aura."}],
        extraction={"supporting_quote": "Indicated for migraine with aura.",
                    "output_value": "migraine with aura", "method": "LLM", "confidence": 1.0},
        grounding=GROUNDING, resolved_id="MONDO:0005475")
    step = mention["resolution"]["pipeline"][0]
    assert step["input_value"] == "Indicated for migraine with aura."
    assert "span_index" not in step
    assert validate_mention_chain(mention) == []


# --- co-mentions (D5) ---------------------------------------------------------------------

VFEND = (
    "Voriconazole is indicated as follows: treatment of invasive aspergillosis; treatment of "
    "candidaemia in non-neutropenic patients."
)


def _vfend(co_mentions):
    return build_mention(
        "invasive aspergillosis", "disease",
        spans=[{"role": "SECTION_TEXT", "text": VFEND, "document": "EMA:vfend"}],
        extraction={"span_index": 0, "output_value": "invasive aspergillosis",
                    "method": "LLM", "confidence": 1.0, "co_mentions": co_mentions},
        grounding={"original_string": "invasive aspergillosis",
                   "grounded_id": "MONDO:0000240", "grounding_quality": "lexical_exact",
                   "confidence": 1.0},
        resolved_id="MONDO:0000240",
    )


def test_co_mentions_carry_ids_and_offsets_without_touching_the_chain():
    mention = _vfend([{"value": "candidaemia", "entity_type": "disease"}])
    step = mention["resolution"]["pipeline"][0]
    assert step["output_value"] == "invasive aspergillosis"      # chain unchanged
    assert step["mention_index"] == 1
    assert step["mention_total"] == 2
    co = step["co_mentions"][0]
    assert co["value"] == "candidaemia"
    assert co["mention_id"].startswith("MEDICNE:")
    assert VFEND[co["char_start"]:co["char_end"]] == "candidaemia"
    assert validate_mention_chain(mention) == []


def test_mention_total_counts_only_the_same_entity_type():
    mention = _vfend([{"value": "candidaemia", "entity_type": "disease"},
                      {"value": "Voriconazole", "entity_type": "drug"}])
    step = mention["resolution"]["pipeline"][0]
    assert step["mention_total"] == 2          # 2 diseases; the drug is not counted
    assert len(step["co_mentions"]) == 2       # but both are recorded


def test_a_co_mention_id_matches_the_ingest_mint_convention():
    """Sibling records must be joinable, so the id must equal what ingest would mint."""
    from medic.mention import mint_mention_id

    mention = _vfend([{"value": "candidaemia", "entity_type": "disease"}])
    co = mention["resolution"]["pipeline"][0]["co_mentions"][0]
    assert co["mention_id"] == mint_mention_id("candidaemia", "diseases")


def test_no_co_mentions_means_no_noise_fields():
    step = _vfend([])["resolution"]["pipeline"][0]
    assert "co_mentions" not in step
    assert "mention_total" not in step
