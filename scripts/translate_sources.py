"""Translate ALL China (zh) and Russia (ru) source mentions to English via DeepL.

Populates the Babelon store ``mappings/drug_translation.babelon.tsv`` — the
git-tracked, deterministic cache for the Stage-0 translation stage. Runs the real
DeepL translation (through the ``babelon`` translator service) over every unique
source name, in chunks so the store is saved frequently and the run is resumable
(already-translated rows are skipped on rerun).

Usage:  uv run python scripts/translate_sources.py [chunk_size]
"""

from __future__ import annotations

import logging
import sys

from medic.ingest.china.locate_source import CDE_CSV_PATH, locate_cde_csv
from medic.ingest.china.parse_cde import parse_cde_csv
from medic.ingest.russia.locate_source import GRLS_ZIP_PATH, locate_grls_zip
from medic.ingest.russia.parse_grls import parse_grls_zip
from medic.mention import ORIGINAL_LITERAL_KEY
from medic.translation import DRUG_TRANSLATION_STORE, TranslationService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("translate_sources")


def _chunks(items, n):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _run(language: str, literals: list[str], chunk_size: int) -> None:
    svc = TranslationService(DRUG_TRANSLATION_STORE, language)
    uniq = sorted({(x or "").strip() for x in literals if (x or "").strip()})
    already = sum(1 for x in uniq if svc.translated(x))
    logger.info("%s: %d unique names (%d already translated)", language, len(uniq), already)
    done = 0
    for chunk in _chunks(uniq, chunk_size):
        svc.translate(chunk)  # registers + translates pending + saves the store
        done += len(chunk)
        translated = sum(1 for x in uniq if svc.translated(x))
        logger.info("%s: processed %d/%d  (translated so far: %d)",
                    language, done, len(uniq), translated)
    final = sum(1 for x in uniq if svc.translated(x))
    logger.info("%s DONE: %d/%d translated", language, final, len(uniq))


def main() -> None:
    chunk_size = int(sys.argv[1]) if len(sys.argv) > 1 else 200

    logger.info("=== China (zh) ===")
    china = parse_cde_csv(locate_cde_csv(CDE_CSV_PATH))
    _run("zh", [r.get(ORIGINAL_LITERAL_KEY) or r.get("source_name") for r in china], chunk_size)

    logger.info("=== Russia (ru) ===")
    russia = parse_grls_zip(locate_grls_zip(GRLS_ZIP_PATH))
    _run("ru", [r.get(ORIGINAL_LITERAL_KEY) or r.get("source_name") for r in russia], chunk_size)

    logger.info("ALL DONE -> %s", DRUG_TRANSLATION_STORE)


if __name__ == "__main__":
    main()
