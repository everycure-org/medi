"""Locate the manually-provided GRLS export.

GRLS (the Russian State Register of Medicines, https://grls.rosminzdrav.ru) is
IP-blocked for anonymous, non-Russian sessions: its search endpoints return an
empty form shell rather than result rows unless you authenticate from a Russian
IP (see the module docstring in ``__main__.py`` for the full investigation).

There is therefore **no live fetch** for Russia. Instead the user manually
downloads the GRLS bulk export and places it at the stable, date-free path
``background/grls.zip``. This module's only job is to locate that file and fail
loudly with an actionable message when it is missing — per SPEC, manual-
acquisition sources must raise a clear error when the local file is absent.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Canonical, date-free path. The user overwrites this file on each rebuild.
# NEVER read a dated filename here — the export inside the zip carries a date in
# its member filenames, but the container path must remain stable.
GRLS_ZIP_PATH = Path("background/grls.zip")


def locate_grls_zip(path: Path = GRLS_ZIP_PATH) -> Path:
    """Return the path to the GRLS export zip, or raise a clear error.

    Args:
        path: Location of the manually-provided GRLS export. Defaults to the
            canonical ``background/grls.zip``.

    Returns:
        The path to the existing zip.

    Raises:
        FileNotFoundError: If the export is missing, with instructions on how to
            obtain and place it.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"GRLS export not found at '{path}'.\n"
            "\n"
            "Russia (GRLS) is a manual-acquisition source: the registry at\n"
            "https://grls.rosminzdrav.ru is IP-blocked for anonymous, non-Russian\n"
            "sessions, so there is no automated fetch.\n"
            "\n"
            "FIRST TRY:  just restore-manual-sources\n"
            "\n"
            "That downloads the manual-source archive into background/. GRLS is not\n"
            "redistributable, so the archive is hosted out-of-band rather than kept in\n"
            "the repo — set MEDIC_MANUAL_SOURCES_URL first (see sources/README.md).\n"
            "\n"
            "Only if you are replacing the snapshot with a fresher export:\n"
            "  1. Obtain the GRLS bulk export (a .zip containing 8 register\n"
            "     .xlsx files with Cyrillic names) from a Russian-IP session.\n"
            f"  2. Place it at the stable path '{path}' (overwrite any existing\n"
            "     file — do NOT use a dated filename).\n"
            "  3. Re-run `python -m medic.ingest.russia`.\n"
            "  4. `just refresh-manual-sources` to repack the archive, re-upload it,\n"
            "     then commit the updated data/source_manifest.json on its own.\n"
            "\n"
            "See sources/README.md and src/medic/ingest/russia/README.md for details."
        )
    logger.info("Found GRLS export at %s", path)
    return path
