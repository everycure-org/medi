"""Translation stage — a first-class step alongside grounding and normalization.

Non-English source names (China zh, Russia ru, …) are translated to English
**before** the deterministic lexical grounder sees them. Every translation is
recorded in a Babelon table (``mappings/*_translation.babelon.tsv``) keyed by the
mention's ``MEDICNE`` id, using the ``babelon`` translator service (DeepL). The
git-tracked Babelon table is the authoritative, deterministic cache: once a
mention is translated, reruns skip it (offline, byte-identical).
"""

from medic.translation.service import (
    DISEASE_TRANSLATION_STORE,
    DRUG_TRANSLATION_STORE,
    TranslationService,
    translate_records,
)
from medic.translation.store import BABELON_COLUMNS, TranslationStore

__all__ = [
    "TranslationService",
    "TranslationStore",
    "translate_records",
    "BABELON_COLUMNS",
    "DRUG_TRANSLATION_STORE",
    "DISEASE_TRANSLATION_STORE",
]
