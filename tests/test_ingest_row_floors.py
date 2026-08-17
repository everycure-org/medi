"""Every ingester must refuse to publish an empty/truncated source (SPEC §3.1).

`check_row_floor` already existed and was wired into Russia and China, but EMA and
PMDA parse into `[]` on upstream *layout* drift — a renamed column or a changed PDF
header map is a warning, not an exception — and then wrote empty `kb/` files and
exited 0. A release with zero EU or zero Japanese data would have been green.

These tests drive `main()` with the parse stubbed to the drift result, so they fail
if the floor call is ever removed again.
"""

from __future__ import annotations


import pytest

from medic.ingest import sanity
from medic.ingest.sanity import ROW_FLOORS, SourceSanityError


def test_floors_are_declared_for_every_primary_source():
    for source in ("ema", "pmda", "russia", "china", "orangebook", "purplebook", "india"):
        assert ROW_FLOORS.get(source, 0) > 0, f"{source} has no row floor"


def test_ema_main_raises_when_parse_yields_no_records(monkeypatch, tmp_path):
    """Upstream column rename -> parse_ema returns [] -> must not write empty kb/."""
    from medic.ingest.ema import __main__ as ema_main

    fake = tmp_path / "ema_medicines.xlsx"
    fake.write_bytes(b"stub")

    monkeypatch.setattr(ema_main, "load_source_urls", lambda: {"ema": {"url": "http://x"}})
    monkeypatch.setattr(ema_main, "download_file", lambda url, dest, force=False: fake)
    monkeypatch.setattr(ema_main, "parse_ema", lambda path: [])

    def _boom(*a, **k):  # pragma: no cover - must never be reached
        raise AssertionError("ingest continued past an empty parse")

    monkeypatch.setattr(ema_main, "write_drug_source_yaml", _boom)
    monkeypatch.setattr(sanity, "record_source", lambda *a, **k: None)
    monkeypatch.setattr(ema_main, "record_source", lambda *a, **k: None)

    with pytest.raises(SourceSanityError):
        ema_main.main(
            grounding_backend="lexical",
            force_download=False,
            skip_indications=True,
            extract_contras=False,
        )


def test_pmda_main_raises_when_parse_yields_no_records(monkeypatch, tmp_path):
    """PDF header-map drift -> parse_pmda_pdf returns [] -> must not write empty kb/."""
    from medic.ingest.pmda import __main__ as pmda_main
    from medic.ingest.pmda import fetch_primary, parse_pdf

    fake = tmp_path / "pmda.pdf"
    fake.write_bytes(b"stub")

    # main() imports these lazily inside the function, so patch them at source.
    monkeypatch.setattr(fetch_primary, "fetch_primary_pdf", lambda force=False: fake)
    monkeypatch.setattr(parse_pdf, "parse_pmda_pdf", lambda path: [])
    monkeypatch.setattr(parse_pdf, "deduplicate_by_ingredient", lambda rows: [])

    def _boom(*a, **k):  # pragma: no cover - must never be reached
        raise AssertionError("ingest continued past an empty parse")

    monkeypatch.setattr(pmda_main, "write_drug_source_yaml", _boom)
    monkeypatch.setattr(pmda_main, "record_source", lambda *a, **k: None)

    with pytest.raises(SourceSanityError):
        pmda_main.main(grounding_backend="lexical", force_download=False)


def test_floor_bypass_is_explicit(monkeypatch):
    """The floor may only be skipped deliberately, never by accident."""
    monkeypatch.delenv("MEDIC_SKIP_ROW_FLOORS", raising=False)
    with pytest.raises(SourceSanityError):
        sanity.check_row_floor("ema", 0)
    assert sanity.check_row_floor("ema", 0, limited=True) == 0
    monkeypatch.setenv("MEDIC_SKIP_ROW_FLOORS", "1")
    assert sanity.check_row_floor("ema", 0) == 0


def test_dailymed_uncached_extraction_is_not_silently_dropped(monkeypatch):
    """A cache miss under MEDIC_SKIP_EXPENSIVE_CALLS used to return [] silently, so the
    'cheap' rebuild path shipped fewer indications and still exited 0."""
    from medic.ingest.dailymed import __main__ as dm

    monkeypatch.setattr(dm, "_skipped_uncached", {}, raising=False)
    monkeypatch.setenv("MEDIC_SKIP_EXPENSIVE_CALLS", "1")
    monkeypatch.delenv("MEDIC_ALLOW_UNCACHED_DROPS", raising=False)

    class _EmptyCache:
        def get(self, key):
            return None

    monkeypatch.setattr(dm, "_get_disease_cache", lambda: _EmptyCache())
    assert dm.extract_diseases_from_text("Indicated for widget deficiency.") == []

    with pytest.raises(RuntimeError, match="silently dropped|not in the committed cache"):
        dm._raise_if_rows_were_silently_dropped()

    # ...but a partial build can still be requested deliberately.
    monkeypatch.setenv("MEDIC_ALLOW_UNCACHED_DROPS", "1")
    dm._raise_if_rows_were_silently_dropped()
