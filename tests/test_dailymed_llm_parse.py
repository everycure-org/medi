"""Robustness tests for the DailyMed disease-list LLM parser.

These guard against a real bug that produced garbage in
`kb/indications/{dailymed,pmda}/indications.yaml`:

The LLM occasionally returns prose-style refusals like
``"None\\n\\nThe text lists diagnostic procedures..."`` for non-disease
indications (e.g. contrast-imaging agents). The original parser only
matched the exact string ``"none"`` (case-insensitive), so the prose
was kept as a single ~200-char "disease" and sent to the grounder.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

from medic.ingest.dailymed.__main__ import (
    LOINC_CONTRAINDICATIONS,
    LOINC_INDICATIONS,
    _extract_ingredients_from_root,
    _extract_section_text_from_root,
    _parse_llm_disease_list,
    _row_from_spl_root,
    mine_spl_labels,
)

# A minimal but structurally faithful SPL document: setid in the `root`
# attribute, an active moiety, and Indications (34067-9) + Contraindications
# (34070-3) sections with nested markup that must be stripped to plain text.
_SPL_XML = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <setId root="11111111-2222-3333-4444-555555555555"/>
  <component><structuredBody><component><section>
    <code code="34067-9" codeSystem="2.16.840.1.113883.6.1"/>
    <text><paragraph>Widgetol is <content>indicated</content> for
      <list><item>type 2 diabetes mellitus</item></list>.</paragraph></text>
    <ingredient><ingredientSubstance>
      <activeMoiety><activeMoiety><name>WIDGETOL</name></activeMoiety></activeMoiety>
    </ingredientSubstance></ingredient>
  </section></component>
  <component><section>
    <code code="34070-3" codeSystem="2.16.840.1.113883.6.1"/>
    <text><paragraph>Contraindicated in severe hepatic impairment.</paragraph></text>
  </section></component>
  </structuredBody></component>
</document>
"""


# ---------------------------------------------------------------------------
# _parse_llm_disease_list
# ---------------------------------------------------------------------------


def test_parse_empty_input_is_empty():
    assert _parse_llm_disease_list("") == []
    assert _parse_llm_disease_list("   ") == []


def test_parse_literal_none_is_empty():
    assert _parse_llm_disease_list("None") == []
    assert _parse_llm_disease_list("none") == []
    assert _parse_llm_disease_list("NONE") == []
    assert _parse_llm_disease_list("  None  ") == []


def test_parse_prose_refusal_is_empty():
    """The exact failure observed in production — model returns 'None' followed
    by an explanation. Must not be kept as a disease."""
    refusal = (
        "None\n\nThe text lists diagnostic procedures (angiocardiography, "
        "angiography, computed tomography, urography) rather than diseases "
        "or therapeutic indications. These are imaging techniques, not "
        "conditions being treated."
    )
    assert _parse_llm_disease_list(refusal) == []


def test_parse_normal_pipe_list():
    out = _parse_llm_disease_list("type 2 diabetes mellitus|hypertension")
    assert out == ["type 2 diabetes mellitus", "hypertension"]


def test_parse_strips_whitespace_and_inner_none():
    out = _parse_llm_disease_list("  asthma | None | COPD  ")
    assert out == ["asthma", "COPD"]


def test_parse_drops_overlong_items():
    """Length cap defends against any other prose-shaped output that doesn't
    start with 'None'. Real disease names don't run 200+ chars."""
    long_prose = "x" * 250
    out = _parse_llm_disease_list(f"asthma|{long_prose}|COPD")
    assert out == ["asthma", "COPD"]


# ---------------------------------------------------------------------------
# SPL XML mining
# ---------------------------------------------------------------------------


def test_extract_section_text_strips_markup():
    root = ET.fromstring(_SPL_XML)
    ind = _extract_section_text_from_root(root, LOINC_INDICATIONS)
    assert "type 2 diabetes mellitus" in ind
    assert "<" not in ind  # nested tags stripped
    con = _extract_section_text_from_root(root, LOINC_CONTRAINDICATIONS)
    assert "severe hepatic impairment" in con


def test_extract_ingredients_from_root():
    root = ET.fromstring(_SPL_XML)
    assert _extract_ingredients_from_root(root) == ["WIDGETOL"]


def test_row_from_spl_root_reads_setid_from_root_attr():
    root = ET.fromstring(_SPL_XML)
    row = _row_from_spl_root(root)
    assert row is not None
    # setid lives in the `root` attribute of <setId>, not `extension`
    assert row["set_id"] == "11111111-2222-3333-4444-555555555555"
    assert row["drug_names"] == ["WIDGETOL"]
    assert "type 2 diabetes mellitus" in row["indications_text"]


def test_row_from_spl_root_none_without_ingredient_or_text():
    empty = '<document xmlns="urn:hl7-org:v3"><setId root="x"/></document>'
    assert _row_from_spl_root(ET.fromstring(empty)) is None


def test_mine_spl_labels_reads_xml_files(tmp_path):
    (tmp_path / "label1.xml").write_text(_SPL_XML)
    df = mine_spl_labels(tmp_path)
    assert len(df) == 1
    assert df.iloc[0]["set_id"] == "11111111-2222-3333-4444-555555555555"


def test_mine_spl_labels_empty_dir_returns_empty(tmp_path):
    df = mine_spl_labels(tmp_path)
    assert df.empty
