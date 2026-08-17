"""Tests for the contra-tuned disease extractor.

Verifies that `extract_contraindicated_diseases_from_text` exists, has its
own cache, and is correctly imported by the EMA and PMDA contra paths.
"""

from __future__ import annotations

import inspect
from pathlib import Path


def test_contra_extractor_function_exists():
    """The contra-tuned sister function must exist with the expected signature."""
    from medic.ingest.dailymed.__main__ import extract_contraindicated_diseases_from_text
    sig = inspect.signature(extract_contraindicated_diseases_from_text)
    params = list(sig.parameters.values())
    assert len(params) == 1
    assert params[0].name == "contraindication_text"
    assert params[0].annotation in (str, "str")


def test_contra_cache_is_separate_from_indication_cache():
    """The contra extractor must use a different cache file than the indication
    extractor — otherwise contra and indication results collide on identical
    source text."""
    from medic.ingest.dailymed.__main__ import (
        DISEASE_CACHE_PATH,
        CONTRA_DISEASE_CACHE_PATH,
    )
    assert DISEASE_CACHE_PATH != CONTRA_DISEASE_CACHE_PATH, (
        "Contraindication disease cache must not share a file with the "
        "indication disease cache."
    )
    assert "contra" in str(CONTRA_DISEASE_CACHE_PATH).lower(), (
        f"Cache path should be self-documenting; got {CONTRA_DISEASE_CACHE_PATH!r}"
    )


def test_contra_extractor_returns_empty_on_empty_input():
    """Defensive: empty input must short-circuit before any LLM call."""
    from medic.ingest.dailymed.__main__ import extract_contraindicated_diseases_from_text
    assert extract_contraindicated_diseases_from_text("") == []
    assert extract_contraindicated_diseases_from_text(None) == []  # type: ignore[arg-type]


def test_dailymed_contra_path_uses_contra_extractor():
    """The DailyMed contraindication path must call the contra-tuned function,
    not the indication-tuned one."""
    src = Path("src/medic/ingest/dailymed/__main__.py").read_text()
    # The contra path must call the contra-tuned extractor on the contra text...
    assert "extract_contraindicated_diseases_from_text(contras_text)" in src, (
        "DailyMed contra extraction call site not found"
    )
    # ...and must NOT feed contra text to the indication-tuned extractor.
    assert "extract_diseases_from_text(contras_text)" not in src, (
        "DailyMed contra path uses the indication-tuned extractor on contra text"
    )


def test_pmda_contra_path_uses_contra_extractor():
    """The PMDA contraindication path must call the contra-tuned function."""
    src = Path("src/medic/ingest/pmda/__main__.py").read_text()
    # The contra extraction lives inside `_build_pmda_contraindication_records`.
    in_contras = False
    found = False
    for ln in src.splitlines():
        if "_build_pmda_contraindication_records" in ln:
            in_contras = True
        if in_contras and "section_text" in ln and "extract_" in ln:
            assert "extract_contraindicated_diseases_from_text" in ln, (
                f"PMDA contra path still uses indication-tuned extractor: {ln!r}"
            )
            found = True
    assert found, "PMDA contra extraction call site not found"


def test_ema_contra_path_uses_contra_extractor():
    """The EMA contraindication path must call the contra-tuned function."""
    src = Path("src/medic/ingest/ema/__main__.py").read_text()
    # The LLM call assigns to `raw_diseases =` (per the agent that built
    # the EMA contra path). Match on that.
    raw_disease_lines = [ln for ln in src.splitlines() if "raw_diseases =" in ln]
    assert raw_disease_lines, "EMA contra LLM call site not found"
    for ln in raw_disease_lines:
        assert "extract_contraindicated_diseases_from_text" in ln, (
            f"EMA contra path still uses indication-tuned extractor: {ln!r}"
        )
