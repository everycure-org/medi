"""conf/section_warrants.yaml is a schema-governed record list, not a map keyed by identifiers."""

import subprocess
from pathlib import Path

from linkml_runtime import SchemaView

from medic.merge.on_label_merge import _load_section_warrants, _warrant_for

PROVENANCE_SCHEMA = "src/medic/schema/provenance.yaml"
WARRANTS = Path("conf/section_warrants.yaml")


def test_warrant_classes_exist():
    sv = SchemaView(PROVENANCE_SCHEMA)
    slots = set(sv.class_slots("SectionWarrant"))
    assert {"source", "section_code", "relationship", "description"} <= slots
    assert sv.induced_slot("source", "SectionWarrant").required is True
    assert sv.induced_slot("section_code", "SectionWarrant").required is not True


def test_the_config_validates_against_its_linkml_class():
    out = subprocess.run(
        ["uv", "run", "linkml-validate", "--schema", PROVENANCE_SCHEMA,
         "--target-class", "SectionWarrantSet", str(WARRANTS)],
        capture_output=True, text=True,
    )
    assert out.returncode == 0, f"validate failed:\n{out.stdout}\n{out.stderr}"


def test_the_composite_key_is_built_at_load_not_stored():
    """The file stores source and section_code separately; the key exists only in memory."""
    raw = WARRANTS.read_text()
    assert "DAILYMED/LOINC" not in raw
    warrants = _load_section_warrants()
    assert warrants["DAILYMED/LOINC:34067-9"] == "INDICATION"
    assert warrants["DAILYMED/LOINC:34070-3"] == "CONTRAINDICATION"
    assert warrants["EMA"] == "INDICATION"


def test_all_expected_sources_survive_the_conversion():
    warrants = _load_section_warrants()
    assert set(warrants) == {
        "DAILYMED/LOINC:34067-9", "DAILYMED/LOINC:34070-3",
        "EMA", "PMDA", "INDIA", "CDSCO",
    }


def test_a_section_warrant_beats_the_bare_source_warrant():
    warrants = _load_section_warrants()
    assert _warrant_for(warrants, "DAILYMED", "LOINC:34067-9", "INDICATION") == \
        "DAILYMED/LOINC:34067-9"


def test_an_indication_warrant_never_licenses_a_contraindication():
    warrants = _load_section_warrants()
    assert _warrant_for(warrants, "EMA", "", "CONTRAINDICATION") is None
    assert _warrant_for(warrants, "DAILYMED", "LOINC:34067-9", "CONTRAINDICATION") is None


def test_an_unwarranted_source_returns_nothing():
    warrants = _load_section_warrants()
    assert _warrant_for(warrants, "RUSSIA", "", "INDICATION") is None
