"""Persistent disk-based grounding cache."""

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_VERSION = 1


class GroundingCache:
    """File-backed grounding cache. One JSON file per source."""

    def __init__(self, cache_dir: Path | None = None):
        self._dir = cache_dir or Path("cache/grounding")
        self._dir.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, dict[str, dict]] = {}

    def _clean_key(self, name: str) -> str:
        return re.sub(r"\W+", " ", name).strip().lower()

    def _source_path(self, source: str) -> Path:
        return self._dir / f"{source}.json"

    def _load_source(self, source: str) -> dict[str, dict]:
        if source in self._data:
            return self._data[source]
        path = self._source_path(source)
        if path.exists():
            try:
                raw = json.loads(path.read_text())
                if raw.get("_version") == CACHE_VERSION:
                    entries = {k: v for k, v in raw.items() if not k.startswith("_")}
                    self._data[source] = entries
                    return entries
                else:
                    logger.info("Cache version mismatch for %s, ignoring", source)
            except (json.JSONDecodeError, KeyError):
                logger.warning("Corrupt cache file %s, ignoring", path)
        self._data[source] = {}
        return self._data[source]

    def get(self, name: str, source: str) -> dict | None:
        entries = self._load_source(source)
        return entries.get(self._clean_key(name))

    def put(self, name: str, source: str, entry: dict) -> None:
        entries = self._load_source(source)
        entries[self._clean_key(name)] = entry

    def flush(self, source: str | None = None) -> None:
        sources = [source] if source else list(self._data.keys())
        for src in sources:
            if src not in self._data:
                continue
            path = self._source_path(src)
            data = {"_version": CACHE_VERSION, **self._data[src]}
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    def flush_all(self) -> None:
        self.flush()
