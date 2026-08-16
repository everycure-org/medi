"""Deterministic JSONL serialization.

Records are written with sorted keys and no trailing whitespace, and the caller has already
sorted the records themselves. Two builds of unchanged products therefore produce
byte-identical files, which is what makes a release diff readable and is checked by a test.
No timestamp is written into the node or edge files; build time belongs in the metadata file,
where it does not rewrite 30 MB on every run.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml


def write_jsonl(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")


def write_yaml(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as handle:
        yaml.safe_dump(data, handle, sort_keys=True, default_flow_style=False)


def load_product(path: Path, key: str) -> list[dict]:
    """Read one product file, returning its record list (empty when the file is absent).

    Uses libyaml's C loader when available — the indication product is ~86 MB and the pure
    Python loader turns a 20-second read into several minutes.
    """
    if not path.exists():
        return []
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    with open(path) as handle:
        data = yaml.load(handle, Loader=loader)
    if isinstance(data, dict):
        records = data.get(key) or []
    else:
        records = data or []
    return [r for r in records if isinstance(r, dict)]
