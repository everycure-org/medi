"""Source sanity: row-count floors + a version/fingerprint stamp per ingest.

We already fail loud when a source file is *absent*. This closes the adjacent hole
(FAILURE_MODES §1): a source that is *stale, truncated, or the wrong file* currently
ingests as a smaller-but-valid dataset. Two cheap, deterministic controls:

* :func:`check_row_floor` — assert an ingester produced at least a known-good floor of
  records; a truncated/partial source trips instead of silently under-populating.
* :func:`record_source` — stamp a manifest (``data/source_manifest.json``) with the
  source file's fingerprint (sha256 + size + mtime date) and the row count, so *which*
  snapshot produced a build is auditable after the fact.

Floors are ~2/3 of the last known-good count — low enough not to trip on normal
variation, high enough to catch a half-download. Update ``ROW_FLOORS`` when a source's
true scale changes. Bypass a floor with ``MEDIC_SKIP_ROW_FLOORS=1`` or ``limited=True``
(an intentional ``--limit`` run).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Minimum expected record count per source (~2/3 of the last known-good scale).
# Known-good (redesign): china~1521 russia~5885 orangebook~2725 purplebook~642
# ema~995 pmda~1174 india~112 dailymed~1819 SPLs.
ROW_FLOORS: dict[str, int] = {
    "china": 1000,
    "russia": 4000,
    "orangebook": 1800,
    "purplebook": 400,
    "ema": 650,
    "pmda": 750,
    "india": 70,
    "dailymed": 1200,
}

DEFAULT_MANIFEST = "data/source_manifest.json"


class SourceSanityError(RuntimeError):
    """A source produced fewer records than its sanity floor (likely truncated/stale)."""


def _skip_floors() -> bool:
    return os.environ.get("MEDIC_SKIP_ROW_FLOORS", "").strip() in ("1", "true", "yes")


def check_row_floor(
    source: str,
    count: int,
    *,
    floor: int | None = None,
    limited: bool = False,
    strict: bool = True,
) -> int:
    """Assert ``count`` records meets ``source``'s floor; raise (or warn) if not.

    ``floor`` overrides ``ROW_FLOORS[source]``. Returns ``count`` for chaining. Skips the
    check for an intentional ``--limit`` run, when ``MEDIC_SKIP_ROW_FLOORS`` is set, or
    when no floor is known for the source.
    """
    resolved = floor if floor is not None else ROW_FLOORS.get(source)
    if resolved is None or limited or _skip_floors():
        return count
    if count < resolved:
        msg = (
            f"{source} ingest: {count} records parsed, expected floor {resolved} — "
            f"possible truncated/stale source. Re-provide the source or, if this is "
            f"genuinely correct, lower ROW_FLOORS['{source}'] "
            f"(or set MEDIC_SKIP_ROW_FLOORS=1 for a one-off run)."
        )
        if strict:
            raise SourceSanityError(msg)
        logger.warning(msg)
    return count


def source_fingerprint(path: str) -> dict:
    """Deterministic fingerprint of a source file: sha256 (short), size, mtime date."""
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
            size += len(chunk)
    return {
        "file": os.path.basename(path),
        "bytes": size,
        "sha256": h.hexdigest()[:16],
        "modified": datetime.fromtimestamp(os.path.getmtime(path)).date().isoformat(),
    }


def record_source(
    source: str,
    path: str,
    row_count: int,
    *,
    manifest_path: str = DEFAULT_MANIFEST,
    extra: dict | None = None,
) -> dict:
    """Stamp ``source``'s fingerprint + row count into the manifest, and return the entry.

    The manifest is a single JSON object keyed by source, sorted and indented so it
    diffs cleanly in version control.
    """
    entry = {**source_fingerprint(path), "row_count": row_count}
    if extra:
        entry.update(extra)

    manifest: dict = {}
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path) as fh:
                manifest = json.load(fh) or {}
        except (json.JSONDecodeError, OSError):
            manifest = {}
    manifest[source] = entry

    os.makedirs(os.path.dirname(manifest_path) or ".", exist_ok=True)
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")
    logger.info("Source manifest: %s -> %s", source, entry)
    return entry
