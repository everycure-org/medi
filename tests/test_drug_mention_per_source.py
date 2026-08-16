"""The drug mention comes from the document that asserted it (design spec D4).

Replaces the merge-elected identity, which stamped one source's trail onto every association
naming that drug — how a Cyrillic drug string ended up on an Indian indication.
"""

from medic.grounding_store_view import GroundingStoreView
from medic.merge.on_label_merge import _build_drug_mention

STORE = GroundingStoreView("mappings/drug_grounding.sssom.tsv", "drug").load()


def test_the_literal_is_the_sources_string_not_the_canonical_label():
    """The bug this replaces: 462 records recorded the CHEBI label as original_literal."""
    mention = _build_drug_mention(
        {"source": "DAILYMED"},
        {"original_drug_label": "THIOSULFATE ION"},
        "CHEBI:16094", "thiosulfate(2-)", store=STORE,
    )
    assert mention["original_literal"] == "THIOSULFATE ION"
    assert mention["original_literal"] != "thiosulfate(2-)"
    assert mention["resolved_id"] == "CHEBI:16094"


def test_the_mention_records_its_source():
    mention = _build_drug_mention(
        {"source": "DAILYMED"}, {"original_drug_label": "ALBUTEROL"},
        "CHEBI:2549", "albuterol", store=STORE)
    assert mention["mention_source"] == "DAILYMED"


def test_two_sources_naming_the_same_drug_get_different_literals_and_ids():
    a = _build_drug_mention({"source": "DAILYMED"},
                            {"original_drug_label": "ALBUTEROL"},
                            "CHEBI:2549", "albuterol", store=STORE)
    b = _build_drug_mention({"source": "EMA"},
                            {"original_drug_label": "albuterol sulfate"},
                            "CHEBI:2550", "albuterol sulfate", store=STORE)
    assert a["original_literal"] != b["original_literal"]
    assert a["id"] != b["id"]


def test_the_mention_id_matches_the_ingest_mint_convention():
    from medic.mention import mint_mention_id

    mention = _build_drug_mention({"source": "DAILYMED"},
                                  {"original_drug_label": "ALBUTEROL"},
                                  "CHEBI:2549", "albuterol", store=STORE)
    assert mention["id"] == mint_mention_id("ALBUTEROL", "drugs")


def test_a_trail_is_recovered_from_the_store():
    mention = _build_drug_mention({"source": "DAILYMED"},
                                  {"original_drug_label": "ALBUTEROL"},
                                  "CHEBI:2549", "albuterol", store=STORE)
    steps = [s["category"] for s in mention["resolution"]["pipeline"]]
    assert "GROUNDING" in steps
    assert mention["resolution"]["output_value"] == "CHEBI:2549"


def test_an_unknown_literal_records_source_asserted_rather_than_inventing_a_match():
    mention = _build_drug_mention(
        {"source": "PMDA"}, {"original_drug_label": "Zzzz Unmatchable 999"},
        "CHEBI:99999", "zzzz", store=STORE)
    grounding = [s for s in mention["resolution"]["pipeline"]
                 if s["category"] == "GROUNDING"][0]
    assert grounding["method"] == "SOURCE_ASSERTED"
    assert grounding["quality"] == "source_asserted"


def test_no_evidence_label_falls_back_but_admits_nothing_was_matched():
    """Falling back to the canonical label is only tolerable if the record says so."""
    mention = _build_drug_mention({"source": "PMDA"}, {}, "CHEBI:2549", "albuterol",
                                  store=STORE)
    assert mention["resolved_id"] == "CHEBI:2549"
    grounding = [s for s in mention["resolution"]["pipeline"]
                 if s["category"] == "GROUNDING"][0]
    assert grounding["quality"] == "source_asserted"


def test_the_chain_validates():
    from medic.provenance_build import validate_mention_chain

    mention = _build_drug_mention({"source": "DAILYMED"},
                                  {"original_drug_label": "ALBUTEROL"},
                                  "CHEBI:2549", "albuterol", store=STORE)
    assert validate_mention_chain(mention) == []
