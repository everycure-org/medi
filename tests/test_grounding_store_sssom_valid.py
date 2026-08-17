"""The committed decision stores must be readable by a standard SSSOM tool (I-4).

I-4 calls the stores "a complete, diffable, hand-editable audit ... nothing is silently
dropped", and SPEC §2 calls them the SSSOM literal profile. Two conformance breaks meant
neither was true when a curator opened one with `sssom-py`:

1. `sssom:NoTermFound` was written into `predicate_id` with an empty `object_id`. SSSOM
   defines `NoTermFound` as a value standing *in place of* a subject/object id, so every
   unresolved row was malformed and dropped on read — `parse_sssom_table` took 21,186 rows
   of the drug store and returned 11,301. The 12,444 rows lost across the two stores are
   precisely the failures the curation surface exists to expose.
2. RxNorm proposals carried `mapping_justification: RXNORM`, which is not a `semapv:` term.
   `validate` raised 460 errors on it.

These tests read the artefacts that actually ship, and glob `mappings/` rather than naming
paths — a hardcoded list is what let #48 survive a fix written to close it.
"""

from __future__ import annotations

import csv
import warnings
from pathlib import Path

import pytest

warnings.filterwarnings("ignore")

STORES = sorted(Path("mappings").glob("*.sssom.tsv"))


def _csv_rows(path: Path) -> list[dict]:
    lines = [ln for ln in open(path, newline="") if not ln.startswith("#")]
    return list(csv.DictReader(lines, delimiter="\t"))


def test_there_are_stores_to_check():
    assert STORES, "no *.sssom.tsv found under mappings/ — wrong working directory?"


@pytest.mark.parametrize("path", STORES, ids=lambda p: p.name)
def test_every_row_survives_a_standard_sssom_read(path: Path):
    """The regression: a reader must not silently drop the unresolved decisions."""
    from sssom.parsers import parse_sssom_table

    expected = len(_csv_rows(path))
    got = len(parse_sssom_table(path).df)
    assert got == expected, f"{path.name}: {expected - got} of {expected} rows dropped on read"


@pytest.mark.parametrize("path", STORES, ids=lambda p: p.name)
def test_every_store_passes_sssom_schema_validation(path: Path):
    from sssom.constants import SchemaValidationType
    from sssom.parsers import parse_sssom_table
    from sssom.validators import validate

    validate(parse_sssom_table(path), [SchemaValidationType.JsonSchema])


@pytest.mark.parametrize("path", STORES, ids=lambda p: p.name)
def test_no_term_found_is_an_object_not_a_predicate(path: Path):
    """`NoTermFound` belongs in `object_id`; a predicate slot is not a place to put it."""
    for row in _csv_rows(path):
        assert row.get("predicate_id") != "sssom:NoTermFound", (
            f"{path.name}: sssom:NoTermFound used as a predicate for "
            f"{row.get('subject_label')!r}"
        )


@pytest.mark.parametrize("path", STORES, ids=lambda p: p.name)
def test_every_mapping_justification_is_a_semapv_term(path: Path):
    for row in _csv_rows(path):
        j = (row.get("mapping_justification") or "").strip()
        if j:
            assert j.startswith("semapv:"), f"{path.name}: illegal justification {j!r}"


def test_unresolved_rows_are_still_present_and_findable():
    """The whole point: a curator can still see what failed to ground."""
    rows = _csv_rows(Path("mappings/disease_grounding.sssom.tsv"))
    unresolved = [r for r in rows if r["object_id"] == "sssom:NoTermFound"]
    assert unresolved, "no unresolved rows — the curation surface lost its residue"
    assert all(r["predicate_id"] for r in unresolved), "unresolved row with no predicate"


# ---------------------------------------------------------------------------
# One id root, not two (B13)
# ---------------------------------------------------------------------------
def test_every_mapping_set_id_hangs_off_the_one_medic_root():
    """There were two roots: the schemas and `MEDICNE:` used
    `w3id.org/monarch-initiative/medic`, the store writers `w3id.org/medic`. Nothing failed —
    both 404 until the redirect is registered (#35) — but an id scheme is the hardest thing to
    change after a tag, and these ids are already in five stores, every Mention and every KGX
    node. Registering the redirect can wait; disagreeing roots cannot.
    """
    from medic.curie_utils import MEDIC_W3ID_ROOT

    for path in STORES:
        header = next((ln for ln in open(path) if ln.startswith("# mapping_set_id:")), "")
        assert header, f"{path.name} declares no mapping_set_id"
        set_id = header.split(":", 1)[1].strip()
        assert set_id.startswith(MEDIC_W3ID_ROOT), f"{path.name}: {set_id!r} is off-root"


def test_the_medicne_prefix_uses_the_same_root():
    from medic.curie_utils import MEDIC_W3ID_ROOT, get_converter

    expanded = get_converter().expand("MEDICNE:abc123")
    assert expanded and expanded.startswith(MEDIC_W3ID_ROOT), expanded
