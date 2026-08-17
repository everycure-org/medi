"""Tests for LinkML schema validity."""

from pathlib import Path


SCHEMA_DIR = Path("src/medic/schema")


def test_schema_files_exist():
    """All expected schema files exist."""
    expected = [
        "medic.yaml",
        "evidence.yaml",
        "drug.yaml",
        "disease.yaml",
        "indication.yaml",
        "adverse_event.yaml",
        "drug_source.yaml",
        "on_label_source.yaml",
        "adverse_event_source.yaml",
        "research_source.yaml",
        "authority.yaml",
    ]
    for schema_file in expected:
        assert (SCHEMA_DIR / schema_file).exists(), f"Missing schema: {schema_file}"


def test_schema_files_are_valid_yaml():
    """All schema files are valid YAML."""
    import yaml

    for yaml_file in SCHEMA_DIR.glob("*.yaml"):
        with open(yaml_file) as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict), f"{yaml_file.name} is not a dict"
        assert "id" in data, f"{yaml_file.name} missing 'id' field"
