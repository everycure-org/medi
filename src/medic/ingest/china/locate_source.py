"""Locate the manually-provided China (CDE/NMPA) scrape.

China's approved-drug list is scraped from the CDE (Center for Drug Evaluation,
https://www.cde.org.cn) paginated approvals table. There is no live fetch in
this repo: the scrape is produced out-of-band and the resulting CSV is placed
manually at the stable, date-free path ``background/cder_drugs_final_all.csv``.

This module's only job is to locate that file and fail loudly with an
actionable message when it is missing — per SPEC, manual-acquisition sources
must raise a clear error when the local file is absent.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Canonical, date-free path. The user overwrites this file on each rebuild.
CDE_CSV_PATH = Path("background/cder_drugs_final_all.csv")


def locate_cde_csv(path: Path = CDE_CSV_PATH) -> Path:
    """Return the path to the CDE scrape CSV, or raise a clear error.

    Args:
        path: Location of the manually-provided CDE scrape. Defaults to the
            canonical ``background/cder_drugs_final_all.csv``.

    Returns:
        The path to the existing CSV.

    Raises:
        FileNotFoundError: If the scrape is missing, with instructions on how to
            obtain and place it.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"China (CDE) scrape not found at '{path}'.\n"
            "\n"
            "China (CDE/NMPA) is a manual-acquisition source: the approvals\n"
            "table at https://www.cde.org.cn is scraped out-of-band, not fetched\n"
            "automatically by this repo.\n"
            "\n"
            "FIRST TRY:  just restore-manual-sources\n"
            "\n"
            "That downloads the manual-source archive into background/. The CDE table is\n"
            "not redistributable, so the archive is hosted out-of-band rather than kept in\n"
            "the repo — set MEDIC_MANUAL_SOURCES_URL first (see sources/README.md).\n"
            "\n"
            "Only if you are replacing the snapshot with a fresher scrape:\n"
            "  1. Obtain the CDE scrape — a 2-column CSV with a `drug_name`\n"
            "     column (Chinese drug names, with formulation suffixes) and an\n"
            "     `approval_date` column.\n"
            f"  2. Place it at the stable path '{path}' (overwrite any existing\n"
            "     file — do NOT use a dated filename).\n"
            "  3. Re-run `python -m medic.ingest.china`.\n"
            "  4. `just refresh-manual-sources` to repack the archive, re-upload it,\n"
            "     then commit the updated data/source_manifest.json on its own.\n"
            "\n"
            "See sources/README.md and src/medic/ingest/china/README.md for details."
        )
    logger.info("Found CDE scrape at %s", path)
    return path
