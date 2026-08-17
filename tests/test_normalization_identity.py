"""The identity normalization quality value, and the legacy `none` alias.

`none` read as missing data and was also persisted in the normalization stores, so the rename
to `identity` is a data migration as well as a schema change.
"""

from linkml_runtime import SchemaView

PROVENANCE_SCHEMA = "src/medic/schema/provenance.yaml"


def test_identity_replaces_none_in_enum():
    sv = SchemaView(PROVENANCE_SCHEMA)
    vals = set(sv.get_enum("NormalizationQualityEnum").permissible_values)
    assert "identity" in vals
    assert "none" not in vals


def test_legacy_none_is_read_as_identity():
    """A store row still carrying the pre-migration `none` must not leak through."""
    from medic.normalization.store import _quality_from_comment

    assert _quality_from_comment("none") == "identity"
    assert _quality_from_comment("") == "identity"
    assert _quality_from_comment(None) == "identity"
    assert _quality_from_comment("asserted_exact") == "asserted_exact"


def test_no_none_remains_in_the_stores():
    for path in ("mappings/drug_normalization.sssom.tsv",
                 "mappings/disease_normalization.sssom.tsv"):
        with open(path) as fh:
            header = None
            for line in fh:
                if line.startswith("#"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if header is None:
                    header = parts
                    continue
                idx = header.index("comment")
                assert parts[idx] != "none", f"{path} still carries a `none` comment"


def test_a_legacy_none_from_a_kb_record_becomes_identity_in_the_step():
    """kb/ records written before the rename still carry `none`; the step must not emit it.

    The store loader's alias does not cover this path — a normalization object funneled in
    from kb/indications/*.yaml goes straight to the step builder.
    """
    from medic.provenance_build import _normalization_step

    step = _normalization_step(
        {"original_id": "MONDO:0005148", "normalized_id": "MONDO:0005148",
         "normalization_quality": "none"},
        fallback_input=None,
    )
    assert step["quality"] == "identity"


def test_an_absent_normalization_quality_also_becomes_identity():
    from medic.provenance_build import _normalization_step

    step = _normalization_step(
        {"original_id": "MONDO:0005148", "normalized_id": "MONDO:0005148"},
        fallback_input=None,
    )
    assert step["quality"] == "identity"


def test_a_real_normalization_quality_is_preserved():
    from medic.provenance_build import _normalization_step

    step = _normalization_step(
        {"original_id": "HP:0000822", "normalized_id": "MONDO:0005044",
         "normalization_quality": "asserted_exact"},
        fallback_input=None,
    )
    assert step["quality"] == "asserted_exact"
