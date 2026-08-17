"""Every grounding store row carries its MEDICNE subject id (invariant I-4)."""

import csv

import pytest

from medic.mention import mint_mention_id

STORES = [
    ("mappings/drug_grounding.sssom.tsv", "drugs", "mappings/drug_translation.babelon.tsv"),
    ("mappings/disease_grounding.sssom.tsv", "diseases", None),
]


def _rows(path):
    with open(path, newline="") as fh:
        yield from csv.DictReader(
            (ln for ln in fh if not ln.startswith("#")), delimiter="\t")


def _translation_index(path):
    if not path:
        return {}
    out = {}
    for row in _rows(path):
        value = (row.get("translation_value") or "").strip()
        subject = (row.get("subject_id") or "").strip()
        if value and subject:
            out.setdefault(value, subject)
    return out


@pytest.mark.parametrize("path,entity_type,translations", STORES)
def test_no_row_is_missing_its_subject_id(path, entity_type, translations):
    blanks = total = 0
    for row in _rows(path):
        total += 1
        if not (row.get("subject_id") or "").strip():
            blanks += 1
    assert blanks == 0, f"{path}: {blanks}/{total} rows have no subject_id"


@pytest.mark.parametrize("path,entity_type,translations", STORES)
def test_every_subject_id_is_explainable(path, entity_type, translations):
    """An id is the mint of its label, pinned to a foreign original, or provably ambiguous.

    For a translated drug the grounder saw the ENGLISH string, so `subject_label` is English
    while `subject_id` belongs to the *foreign* source literal — that is what makes the trail
    join up, and it is why many drug rows differ from mint(subject_label).

    The residue is rows whose English label was produced by **more than one** distinct source
    literal ("Ofloxacin" comes from two different Cyrillic spellings), so a label -> id join
    genuinely cannot pick one. That is a property of the data, not a defect, so this asserts it
    directly instead of pinning a magic count — the previous ceiling of 43 broke when the
    identity fix made ids more granular and *increased* the ambiguous set to 46.
    """
    index, ambiguous = {}, set()
    if translations:
        by_translation = {}
        for row in _rows(translations):
            value = (row.get("translation_value") or "").strip()
            literal = (row.get("source_value") or "").strip()
            subject = (row.get("subject_id") or "").strip()
            if value and subject:
                index.setdefault(value, subject)
            if value and literal:
                by_translation.setdefault(value, set()).add(literal)
        ambiguous = {v for v, literals in by_translation.items() if len(literals) > 1}

    unexplained = []
    for row in _rows(path):
        label = (row.get("subject_label") or "").strip()
        subject = (row.get("subject_id") or "").strip()
        if not label or not subject:
            continue
        if subject == mint_mention_id(label, entity_type):
            continue
        if index.get(label) == subject:
            continue
        if label in ambiguous:
            continue          # several source literals share this English label
        unexplained.append((label, subject))

    assert not unexplained, \
        f"{path}: {len(unexplained)} subject_ids explained by nothing, e.g. {unexplained[:3]}"


def test_the_disease_store_mints_cleanly():
    """No disease row is translated, so every id must be exactly the mint of its label."""
    for row in _rows("mappings/disease_grounding.sssom.tsv"):
        label = (row.get("subject_label") or "").strip()
        subject = (row.get("subject_id") or "").strip()
        if label and subject:
            assert subject == mint_mention_id(label, "diseases"), label
