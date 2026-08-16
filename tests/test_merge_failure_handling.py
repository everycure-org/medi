"""The merge must not lose records quietly.

The bare `except Exception` around the whole per-file record loop meant one bad record
discarded every record after it in that file — up to 4,024 for DailyMed — logged a message
with no traceback, and exited 0. These tests pin the two halves of the fix: a per-record
failure is contained to that record, and a build that lost anything raises.
"""

from __future__ import annotations

import pytest

from medic.merge.on_label_merge import MergeFailed, _report_failures


def test_no_failures_is_silent():
    assert _report_failures([]) is None


def test_any_failure_raises():
    with pytest.raises(MergeFailed):
        _report_failures(["DAILYMED: record 12 of indications.yaml (CHEBI:1 -> MONDO:1)"])


def test_the_message_counts_failures_per_source():
    """A releaser needs to know *which* source lost rows, not just that something did."""
    with pytest.raises(MergeFailed) as excinfo:
        _report_failures([
            "DAILYMED: record 12 of indications.yaml (CHEBI:1 -> MONDO:1)",
            "DAILYMED: record 13 of indications.yaml (CHEBI:2 -> MONDO:2)",
            "PMDA: unreadable file kb/indications/pmda/indications.yaml",
        ])
    message = str(excinfo.value)
    assert "3 merge failure(s)" in message
    assert "DAILYMED=2" in message
    assert "PMDA=1" in message


def test_the_message_says_not_to_release_the_build():
    with pytest.raises(MergeFailed, match="do not release this build"):
        _report_failures(["EMA: record 1 of indications.yaml (? -> ?)"])


def test_an_invariant_violation_is_a_merge_failure():
    """SPEC §4 calls the invariants hard rules; breaking one must not exit 0."""
    with pytest.raises(MergeFailed):
        _report_failures(["7 provenance invariant violation(s)"])


def test_one_bad_record_does_not_take_the_rest_of_the_file(tmp_path, monkeypatch):
    """The actual regression: a record that blows up mid-file must cost one record, not the tail."""
    import shutil
    from pathlib import Path

    import yaml

    from medic.merge import on_label_merge as m

    # The merge reads conf/ by relative path (confidence priors, section warrants), so the
    # sandbox needs its own copy rather than inheriting the repo's cwd.
    shutil.copytree(Path(__file__).resolve().parents[1] / "conf", tmp_path / "conf")

    kb = tmp_path / "kb" / "indications" / "dailymed"
    kb.mkdir(parents=True)

    def _record(disease_id: str) -> dict:
        return {
            "final_normalized_drug_id": "CHEBI:1",
            "final_normalized_drug_label": "aspirin",
            "final_normalized_disease_id": disease_id,
            "final_normalized_disease_label": "thing",
            "relationship_type": "INDICATION",
            "evidence": [{"jurisdiction": "USA", "source_type": "REGULATORY",
                          "reference": "https://dailymed.nlm.nih.gov/x",
                          "original_disease_label": "thing", "snippet": "indicated for thing"}],
        }

    records = [_record("MONDO:1"), _record("MONDO:2"), _record("MONDO:3")]
    (kb / "indications.yaml").write_text(yaml.dump(records))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(m, "KB_INDICATIONS_DIR", kb.parent)
    monkeypatch.setattr(m, "INDICATION_OUTPUT", tmp_path / "products" / "indication_list.yaml")
    monkeypatch.setattr(m, "CONTRAINDICATION_OUTPUT",
                        tmp_path / "products" / "contraindication_list.yaml")

    # Blow up on the middle record only.
    real = m._build_source_assertions

    def exploding(record, *args, **kwargs):
        if m._get_disease_id(record) == "MONDO:2":
            raise ValueError("boom")
        return real(record, *args, **kwargs)

    monkeypatch.setattr(m, "_build_source_assertions", exploding)

    with pytest.raises(MergeFailed):
        m.merge_on_label()

    written = yaml.safe_load((tmp_path / "products" / "indication_list.yaml").read_text())
    ids = sorted(a["disease_id"] for a in written["associations"])
    # The record after the failure survived — that is the whole point.
    assert ids == ["MONDO:1", "MONDO:3"]
