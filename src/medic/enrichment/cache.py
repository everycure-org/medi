"""Simple JSON-file cache for enrichment results. GitHub-diffable."""

import json
from pathlib import Path


class EnrichmentCache:
    """Sorted, deterministic JSON cache for enrichment results."""

    def __init__(self, cache_path: Path):
        self._path = cache_path
        self._data: dict | None = None

    def _load(self) -> dict:
        if self._data is not None:
            return self._data
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text())
            except (json.JSONDecodeError, OSError):
                self._data = {}
        else:
            self._data = {}
        return self._data

    def get(self, key: str) -> dict | None:
        return self._load().get(key)

    def put(self, key: str, value: dict) -> None:
        self._load()[key] = value

    def flush(self) -> None:
        if self._data is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Sorted keys, no timestamps, deterministic output
        self._path.write_text(
            json.dumps(self._data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        )
