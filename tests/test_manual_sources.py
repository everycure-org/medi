"""The out-of-band manual-source archive (China CDE + Russia GRLS).

Neither file can be fetched — the CDE table has no bulk export and GRLS is IP-blocked for
anonymous non-Russian sessions — and neither is redistributable, so the archive is hosted
out-of-band and downloaded into gitignored `background/` by `just restore-manual-sources`.

These tests guard both halves of that arrangement: the archive must stay *out* of the repo, and
the fingerprints in `data/source_manifest.json` must keep describing whatever a machine actually
restored. A clean checkout has no copy of the files, so the fingerprint tests skip there.
"""

import json
import subprocess
import zipfile
from pathlib import Path

import pytest

from medic.ingest.sanity import source_fingerprint

BACKGROUND = Path("background")
ARCHIVE = BACKGROUND / "manual-sources.zip"
MANIFEST = Path("data/source_manifest.json")
MEMBERS = {"cder_drugs_final_all.csv": "china", "grls.zip": "russia"}


def _restored(member: str, tmp_path: Path) -> Path | None:
    """The restored file, extracting from a downloaded archive if that is all there is."""
    direct = BACKGROUND / member
    if direct.is_file():
        return direct
    if ARCHIVE.is_file():
        with zipfile.ZipFile(ARCHIVE) as zf:
            if member in zf.namelist():
                return Path(zf.extract(member, tmp_path))
    return None


def test_the_archive_is_not_tracked_in_git():
    """It is not redistributable (LICENSING.md); it must never come back into the repo."""
    tracked = subprocess.run(
        ["git", "ls-files", "--", "sources/manual-sources.zip", "background/manual-sources.zip"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    assert not tracked, (
        f"the manual-source archive is tracked again ({tracked}); it holds unredistributable "
        f"CDE and GRLS data and must stay out of git")


def test_the_archive_is_flat_so_restore_lands_in_background():
    """`just restore-manual-sources` unzips straight into background/; a nested path breaks it."""
    if not ARCHIVE.is_file():
        pytest.skip(f"{ARCHIVE} not downloaded ('just restore-manual-sources')")
    with zipfile.ZipFile(ARCHIVE) as zf:
        assert set(zf.namelist()) == set(MEMBERS)
        for name in zf.namelist():
            assert "/" not in name, f"{name} is nested; the ingesters read background/<file>"


@pytest.mark.parametrize("source", sorted(MEMBERS.values()))
def test_the_manifest_fingerprints_every_manual_source(source):
    """The manifest is committed, so this holds on a clean checkout too."""
    manifest = json.loads(MANIFEST.read_text())
    assert source in manifest, f"{source} has no entry in {MANIFEST}"
    assert manifest[source].get("sha256"), f"{source} has no sha256 in {MANIFEST}"


@pytest.mark.parametrize("member,source", sorted(MEMBERS.items()))
def test_each_restored_file_matches_the_recorded_fingerprint(member, source, tmp_path):
    """Catches a locally-refreshed file and the committed manifest drifting apart."""
    restored = _restored(member, tmp_path)
    if restored is None:
        pytest.skip(f"{member} not restored ('just restore-manual-sources')")
    manifest = json.loads(MANIFEST.read_text())
    assert source_fingerprint(str(restored))["sha256"] == manifest[source]["sha256"], (
        f"{source}: the restored file is not the one {MANIFEST} fingerprints — re-run the "
        f"ingest so the manifest describes the file you built from")


@pytest.mark.parametrize("source", sorted(MEMBERS.values()))
def test_the_manifest_records_a_row_count_above_the_sanity_floor(source):
    from medic.ingest.sanity import ROW_FLOORS

    manifest = json.loads(MANIFEST.read_text())
    assert manifest[source]["row_count"] >= ROW_FLOORS[source]


def test_the_ingesters_point_at_the_restore_recipe_when_a_file_is_missing():
    """A clean run must say what to do, not just that something is absent."""
    from medic.ingest.china.locate_source import locate_cde_csv
    from medic.ingest.russia.locate_source import locate_grls_zip

    for locate in (locate_cde_csv, locate_grls_zip):
        with pytest.raises(FileNotFoundError) as exc:
            locate(Path("background/deliberately-absent"))
        assert "just restore-manual-sources" in str(exc.value)
