"""ConfidenceBasis enum and the scope_narrowed extraction flag (spec §5, D7)."""

from linkml_runtime import SchemaView

PROVENANCE_SCHEMA = "src/medic/schema/provenance.yaml"


def test_confidence_basis_values():
    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("ConfidenceBasis").permissible_values)
    assert vals == {"MEASURED", "DETERMINISTIC", "PRIOR"}


def test_scope_narrowed_is_an_extraction_flag():
    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("ExtractionFlag").permissible_values)
    assert "scope_narrowed" in vals


def test_extraction_flag_code_map_matches_schema():
    """provenance_build._EXTRACTION_FLAGS must mirror the schema enum exactly."""
    from medic.provenance_build import _EXTRACTION_FLAGS

    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("ExtractionFlag").permissible_values)
    assert set(_EXTRACTION_FLAGS) == vals
