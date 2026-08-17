"""Verify the locally-restored manual sources against the recorded fingerprints.

China (CDE) and Russia (GRLS) cannot be fetched — see ``sources/README.md``. Neither file is
redistributable (see ``LICENSING.md``), so the archive is hosted out-of-band rather than kept in
the repo, and ``just restore-manual-sources`` downloads it into gitignored ``background/``.

That makes the check conditional: a clean CI checkout has no copy of the files and cannot get
one, so their absence is reported and skipped rather than failed. What is always checked is the
manifest itself — every source it claims must carry a usable fingerprint.

When the files *are* present, this catches the way the pair silently rots: someone refreshes
``background/`` without re-running the ingest, so a build's recorded provenance no longer
describes its actual input. Fingerprinting reuses ``medic.ingest.sanity.source_fingerprint`` so
the hash is computed exactly the way the ingesters compute it.
"""

from __future__ import annotations

import json
import sys
import tempfile
import zipfile
from pathlib import Path

from medic.ingest.sanity import DEFAULT_MANIFEST, source_fingerprint

BACKGROUND = Path("background")
ARCHIVE = BACKGROUND / "manual-sources.zip"

#: Restored filename -> the manifest key whose fingerprint should describe it.
MEMBERS = {
    "cder_drugs_final_all.csv": "china",
    "grls.zip": "russia",
}


def _resolve(tmp: str) -> dict[str, Path]:
    """Locate each member, preferring the restored file over the downloaded archive."""
    found = {name: BACKGROUND / name for name in MEMBERS if (BACKGROUND / name).is_file()}
    if len(found) == len(MEMBERS) or not ARCHIVE.is_file():
        return found
    with zipfile.ZipFile(ARCHIVE) as zf:
        names = set(zf.namelist())
        for name in MEMBERS:
            if name not in found and name in names:
                found[name] = Path(zf.extract(name, tmp))
    return found


def main() -> int:
    manifest_path = Path(DEFAULT_MANIFEST)
    if not manifest_path.exists():
        print(f"ERROR: {manifest_path} is missing. Run an ingest to stamp it "
              f"('just ingest-china', 'just ingest-russia').", file=sys.stderr)
        return 1
    manifest = json.loads(manifest_path.read_text())

    problems: list[str] = []
    for source in MEMBERS.values():
        recorded = manifest.get(source)
        if recorded is None:
            problems.append(f"{source}: no entry in {manifest_path}")
        elif not recorded.get("sha256"):
            problems.append(f"{source}: entry in {manifest_path} has no sha256")

    with tempfile.TemporaryDirectory() as tmp:
        found = _resolve(tmp)
        for member, source in MEMBERS.items():
            path = found.get(member)
            if path is None:
                print(f"  SKIP {source:8s} {member}  not restored locally "
                      f"('just restore-manual-sources')")
                continue
            recorded = manifest.get(source) or {}
            local = source_fingerprint(str(path))
            if local["sha256"] != recorded.get("sha256"):
                problems.append(
                    f"{source}: local sha256 {local['sha256']} != manifest "
                    f"{recorded.get('sha256')} — re-run the ingest so the manifest describes "
                    f"the file you actually built from, or restore the recorded snapshot")
            else:
                print(f"  OK   {source:8s} {member}  sha256={local['sha256']} "
                      f"rows={recorded.get('row_count')}")

    if problems:
        print("\nmanual-source check FAILED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1
    print("\nmanual sources are consistent with the recorded fingerprints.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
