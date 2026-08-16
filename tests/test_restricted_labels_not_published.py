"""I-14 rule 2, checked against the artefacts rather than against a function.

The in-memory tests in `test_restricted_vocabulary_labels.py` pin the label *policy*. They
cannot see whether the policy was ever applied to the files that ship, and that gap is the
whole defect this file exists for: `76a13c4` implemented the policy and regenerated
`mappings/disease_grounding.sssom.tsv`, but `scripts/refresh_grounding_labels.py` was scoped to
that one store, so `disease_normalization.sssom.tsv` kept its labels for 238 of the 240 blanked
concepts and the merge read the label from there. 104 MedDRA strings reached
`exports/medic_nodes.jsonl` with every unit test passing.

The invariant is one sentence: **a concept the grounding store ships unnamed must not carry a
name anywhere else.** The grounding store is where the label policy is applied, so it is the
reference; everything derived from it has to agree.

Same shape as `test_grounding_store_license.py` — read what is committed, not what a writer
would produce given a fresh temp file.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
GROUNDING = ROOT / "mappings" / "disease_grounding.sssom.tsv"
DERIVED_STORES = [
    ROOT / "mappings" / "disease_normalization.sssom.tsv",
]
PRODUCTS = [
    ROOT / "products" / "indication_list.yaml",
    ROOT / "products" / "contraindication_list.yaml",
]
NODES = ROOT / "exports" / "medic_nodes.jsonl"


def _rows(path: Path) -> list[dict]:
    with open(path, newline="") as fh:
        lines = [ln for ln in fh if not ln.startswith("#")]
    return list(csv.DictReader(lines, delimiter="\t"))


def _unnamed_concepts() -> set[str]:
    """Ids the grounding store resolves to but deliberately publishes no label for."""
    if not GROUNDING.exists():
        pytest.skip(f"{GROUNDING} not present")
    return {
        r["object_id"] for r in _rows(GROUNDING)
        if r.get("object_id") and not (r.get("object_label") or "").strip()
    }


def test_the_grounding_store_actually_withholds_some_labels():
    """Guard the guard: if this is empty every assertion below passes vacuously."""
    assert len(_unnamed_concepts()) > 0


@pytest.mark.parametrize("store", DERIVED_STORES, ids=lambda p: p.name)
def test_derived_stores_name_nothing_the_grounding_store_withholds(store: Path):
    if not store.exists():
        pytest.skip(f"{store} not built")
    unnamed = _unnamed_concepts()
    leaked = {
        r["object_id"]: r["object_label"] for r in _rows(store)
        if r.get("object_id") in unnamed and (r.get("object_label") or "").strip()
    }
    assert not leaked, (
        f"{store.name} publishes a label for {len(leaked)} concept(s) the grounding store "
        f"ships unnamed, e.g. {dict(list(leaked.items())[:3])}. Run "
        f"`uv run python scripts/refresh_grounding_labels.py`."
    )


@pytest.mark.parametrize("product", PRODUCTS, ids=lambda p: p.name)
def test_products_name_nothing_the_grounding_store_withholds(product: Path):
    if not product.exists():
        pytest.skip(f"{product} not built")
    unnamed = _unnamed_concepts()
    doc = yaml.safe_load(product.read_text())
    leaked: dict[str, str] = {}
    for pair in doc.get("associations", []) or []:
        did, label = pair.get("disease_id", ""), (pair.get("disease_label") or "").strip()
        if did in unnamed and label:
            leaked[did] = label
    assert not leaked, (
        f"{product.name} publishes a label for {len(leaked)} concept(s) the grounding store "
        f"ships unnamed, e.g. {dict(list(leaked.items())[:3])}. Rebuild after refreshing the "
        f"mapping stores."
    )


def test_kgx_nodes_name_nothing_the_grounding_store_withholds():
    if not NODES.exists():
        pytest.skip(f"{NODES} not built")
    unnamed = _unnamed_concepts()
    leaked: dict[str, str] = {}
    with open(NODES) as fh:
        for line in fh:
            node = json.loads(line)
            name = (node.get("name") or "").strip()
            if node.get("id") in unnamed and name:
                leaked[node["id"]] = name
    assert not leaked, (
        f"medic_nodes.jsonl names {len(leaked)} concept(s) the grounding store ships unnamed, "
        f"e.g. {dict(list(leaked.items())[:3])}. This is restricted term text in a release "
        f"asset (I-14 rule 2)."
    )
