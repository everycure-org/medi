"""Source-isolation invariant tests.

Each ingester must emit evidence rows only for the jurisdiction it itself
originates. See `docs/source-isolation.md` for the full rule.

These tests assert the invariant on the *current kb yamls* — they will fail if
any ingester regresses and starts emitting cross-jurisdiction evidence.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

KB_INDICATIONS = Path("kb/indications")

# (kb subdir, allowed jurisdictions for evidence rows in that subdir's yamls).
# A list of values — most sources are single-jurisdiction. Empty list means
# the kb dir hasn't been populated yet (.gitkeep only).
EXPECTED_JURISDICTIONS = {
    "dailymed": {"USA"},
    "ema": {"EU"},
    "pmda": {"JAPAN"},
    "india": {"INDIA"},
}


def _evidence_jurisdictions(yaml_path: Path) -> set[str]:
    """Return the set of distinct evidence-row jurisdictions in a kb yaml.

    Skips files that aren't indication-list-shaped (e.g. setid_lookup_report.yaml).
    """
    with open(yaml_path) as f:
        data = yaml.safe_load(f) or []
    juris: set[str] = set()
    if not isinstance(data, list):
        return juris
    for record in data:
        if not isinstance(record, dict):
            return juris  # not an indication-list yaml; skip the whole file
        for ev in record.get("evidence", []) or []:
            j = ev.get("jurisdiction")
            if j:
                juris.add(j)
    return juris


@pytest.mark.parametrize("source,allowed", sorted(EXPECTED_JURISDICTIONS.items()))
def test_kb_yaml_jurisdiction_isolation(source: str, allowed: set[str]):
    """Each kb/indications/<source>/*.yaml emits only rows for `allowed` jurisdictions.

    Regressions from this test indicate a source is bleeding cross-jurisdiction
    evidence rows. See docs/source-isolation.md for the rule and remediation.
    """
    src_dir = KB_INDICATIONS / source
    if not src_dir.exists():
        pytest.skip(f"{src_dir} does not exist yet")
    yaml_files = [f for f in src_dir.glob("*.yaml")
                  if f.name in ("indications.yaml", "contraindications.yaml")]
    if not yaml_files:
        pytest.skip(f"{src_dir} has no indication-list yaml files")
    seen: set[str] = set()
    for yf in yaml_files:
        seen |= _evidence_jurisdictions(yf)
    illegal = seen - allowed
    assert not illegal, (
        f"{source} kb yamls emit evidence with disallowed jurisdiction(s) "
        f"{sorted(illegal)}; only {sorted(allowed)} permitted. "
        f"See docs/source-isolation.md."
    )


def test_dailymed_does_not_emit_ema_evidence():
    """Specific regression test for the historical DailyMed→EMA/PMDA bleeding."""
    src_dir = KB_INDICATIONS / "dailymed"
    if not src_dir.exists():
        pytest.skip(f"{src_dir} does not exist yet")
    for yf in src_dir.glob("*.yaml"):
        if yf.name not in ("indications.yaml", "contraindications.yaml"):
            continue
        with open(yf) as f:
            data = yaml.safe_load(f) or []
        for record in data:
            for ev in record.get("evidence", []) or []:
                ref = (ev.get("reference") or "").lower()
                explanation = (ev.get("explanation") or "").lower()
                assert "ema.europa.eu" not in ref, f"DailyMed→EMA bleed in {yf}"
                assert "pmda.go.jp" not in ref, f"DailyMed→PMDA bleed in {yf}"
                assert "ema-approved" not in explanation, (
                    f"DailyMed→EMA bleed in {yf}"
                )
                assert "pmda-approved" not in explanation, (
                    f"DailyMed→PMDA bleed in {yf}"
                )


def test_dailymed_spl_evidence_is_usa_only():
    """The DailyMed SPL-XML path hardcodes ``jurisdiction: USA`` on every evidence
    row it emits. This guards the source-isolation invariant structurally: there
    is no code path in the DailyMed ingester that can emit a non-USA jurisdiction.
    """
    import inspect

    from medic.ingest.dailymed import __main__ as dailymed_main

    src = inspect.getsource(dailymed_main._process_spl_data)
    # Every jurisdiction literal in the SPL processor must be "USA".
    juris_literals = re.findall(r'"jurisdiction":\s*"([A-Z]+)"', src)
    assert juris_literals, "expected at least one jurisdiction literal in _process_spl_data"
    assert set(juris_literals) == {"USA"}, (
        f"DailyMed _process_spl_data emits non-USA jurisdictions {set(juris_literals) - {'USA'}}; "
        "see docs/source-isolation.md."
    )


# ---------------------------------------------------------------------------
# The shared I-1 table (review #36, items D2/D3)
# ---------------------------------------------------------------------------
def test_a_source_may_not_claim_another_jurisdiction():
    from medic.source_isolation import violation

    assert violation("DAILYMED", "EU")
    assert violation("EMA", "USA")
    assert violation("DAILYMED", "USA") is None
    assert violation("PMDA", "JAPAN") is None


def test_the_ingester_name_and_the_authority_both_resolve():
    """The KGX gate keyed CDSCO while the exporter writes INDIA, exempting every India edge."""
    from medic.source_isolation import expected_jurisdiction

    assert expected_jurisdiction("INDIA") == "INDIA"
    assert expected_jurisdiction("CDSCO") == "INDIA"
    assert expected_jurisdiction("GRLS") == expected_jurisdiction("MOH_RUSSIA") == "RUSSIA"


def test_an_unknown_source_is_a_violation_not_a_pass():
    from medic.source_isolation import violation

    assert "not in SOURCE_JURISDICTION" in (violation("NEW_REGISTRY", "USA") or "")


def test_jurisdiction_free_sources_are_declared_not_defaulted():
    from medic.source_isolation import is_known, violation

    assert is_known("PUBMED")
    assert violation("PUBMED", "USA") is None


def test_the_kgx_export_uses_the_same_table():
    """One statement of I-1. The export kept its own copy and it drifted."""
    from medic.export.kgx import biolink as bl
    from medic.source_isolation import SOURCE_JURISDICTION

    assert bl.SOURCE_JURISDICTION is SOURCE_JURISDICTION
