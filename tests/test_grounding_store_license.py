"""The in-repo SSSOM decision stores carry the same licence position as the export.

`mappings/*_grounding.sssom.tsv` declared a blanket CC0, but `subject_label` and
`match_string` reproduce verbatim source strings (e.g. `"Golden Star" Balm`) that stay under
the source terms. Same defect as the export header, same fix.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from medic.grounding.store import LiteralMappingStore

#: The stores as actually committed. Tests that only ever inspect a freshly written temp file
#: cannot see a stale artefact — which is exactly what happened: the writer was changed to
#: CC BY, the committed stores kept their CC0 header for weeks, and this suite stayed green
#: the whole time. Every assertion about the licence now runs against both.
COMMITTED_STORES = [
    Path("mappings/disease_grounding.sssom.tsv"),
    Path("mappings/drug_grounding.sssom.tsv"),
]


def _header(tmp_path):
    store = LiteralMappingStore(str(tmp_path / "s.sssom.tsv"), entity_type="drugs")
    store.save()
    return (tmp_path / "s.sssom.tsv").read_text()


def _committed_header(path: Path) -> str:
    lines = []
    with open(path) as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            lines.append(line)
    return "".join(lines)


@pytest.mark.parametrize("path", COMMITTED_STORES, ids=lambda p: p.name)
def test_committed_store_declares_cc_by_not_cc0(path):
    """The artefact, not the writer. CC0 would waive rights over verbatim source strings."""
    header = _committed_header(path)
    assert "# license: https://creativecommons.org/licenses/by/4.0/" in header
    assert "publicdomain/zero" not in header.split("# comment:")[0]


@pytest.mark.parametrize("path", COMMITTED_STORES, ids=lambda p: p.name)
def test_committed_store_matches_what_the_writer_would_emit(path):
    """Pins writer and artefact together, so changing one without regenerating the other fails."""
    from medic.grounding.store import _license_header

    assert "".join(_license_header()) in _committed_header(path)


def test_store_declares_cc_by_not_cc0(tmp_path):
    assert "# license: https://creativecommons.org/licenses/by/4.0/" in _header(tmp_path)


def test_store_records_the_verbatim_string_carve_out(tmp_path):
    header = _header(tmp_path)
    assert "subject_label" in header
    assert "remains in force" in header


def test_store_header_lines_are_comments(tmp_path):
    for line in _header(tmp_path).splitlines():
        if line.startswith("subject_type"):
            break
        assert line.startswith("#"), line
