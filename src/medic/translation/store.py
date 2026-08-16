"""Babelon translation store — one hand-editable TSV per entity type.

A minimal `Babelon <https://github.com/monarch-initiative/babelon>`_ profile: one
row per translated mention, keyed by the mention's ``MEDICNE`` subject id. The
file is a plain header-row TSV (Babelon convention — no YAML front-matter) so it
round-trips cleanly through ``babelon.translate.translate_profile``.

The store is the **deterministic cache** for the translation stage: a row whose
``translation_value`` is already filled is never re-translated (mirrors the
grounding/normalization SSSOM stores and the RxNorm enrichment cache). Manual
edits survive regeneration — hand-fix a bad machine translation and it sticks.
"""

from __future__ import annotations

import os

import pandas as pd

# Minimal Babelon slots we persist (subset of the babelon schema's translation
# class). Order is the on-disk column order.
BABELON_COLUMNS = [
    "subject_id",            # MEDICNE:<uuid5> — the minted mention id
    "predicate_id",          # property whose value is translated (rdfs:label)
    "source_language",       # ISO code of the source value (zh, ru, …)
    "source_value",          # verbatim foreign-language string (I-7 faithful)
    "translation_language",  # ISO code of the translation (en)
    "translation_value",     # English translation fed to the grounder
    "translator",            # wikidata id of the translator (DeepL: Q116709136)
    "translator_expertise",  # ALGORITHM for machine translation
    "translation_status",    # NOT_TRANSLATED | CANDIDATE | ...
    "translation_date",      # YYYY-MM-DD the translation was produced
    "comment",               # free text (the translator model name)
]

NOT_TRANSLATED = "NOT_TRANSLATED"
DEFAULT_PREDICATE = "rdfs:label"


def _empty(value) -> bool:
    return value is None or str(value).strip() == "" or str(value).strip().lower() == "nan"


class TranslationStore:
    """A Babelon translation table addressed by ``MEDICNE`` subject id."""

    def __init__(self, path: str):
        self.path = path
        self._rows: dict[str, dict] = {}

    # -- io -----------------------------------------------------------------
    def load(self) -> None:
        self._rows.clear()
        if not os.path.exists(self.path):
            return
        df = pd.read_csv(self.path, sep="\t", dtype=str, keep_default_na=False)
        for _, row in df.iterrows():
            subject_id = str(row.get("subject_id", "")).strip()
            if subject_id:
                self._rows[subject_id] = {c: str(row.get(c, "")) for c in BABELON_COLUMNS}

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.to_dataframe().to_csv(self.path, sep="\t", index=False)

    # -- content ------------------------------------------------------------
    def upsert_source(
        self,
        subject_id: str,
        source_value: str,
        source_language: str,
        translation_language: str = "en",
        predicate_id: str = DEFAULT_PREDICATE,
    ) -> None:
        """Register a mention to be translated (no-op if already present).

        Never overwrites an existing row, so a filled ``translation_value``
        (machine or hand-curated) is preserved across reruns.
        """
        if subject_id in self._rows:
            return
        self._rows[subject_id] = {
            "subject_id": subject_id,
            "predicate_id": predicate_id,
            "source_language": source_language,
            "source_value": source_value,
            "translation_language": translation_language,
            "translation_value": "",
            "translator": "",
            "translator_expertise": "",
            "translation_status": NOT_TRANSLATED,
            "translation_date": "",
            "comment": "",
        }

    def set_translation(
        self,
        subject_id: str,
        translation_value: str,
        translator: str = "",
        translator_expertise: str = "ALGORITHM",
        translation_status: str = "CANDIDATE",
    ) -> None:
        """Fill a mention's translation (used for curation and offline seeding)."""
        row = self._rows.get(subject_id)
        if row is None:
            return
        row["translation_value"] = translation_value
        row["translator"] = translator
        row["translator_expertise"] = translator_expertise
        row["translation_status"] = translation_status

    def get(self, subject_id: str) -> dict | None:
        return self._rows.get(subject_id)

    def translation_value(self, subject_id: str) -> str:
        row = self._rows.get(subject_id)
        if not row:
            return ""
        value = row.get("translation_value", "")
        return "" if _empty(value) else str(value).strip()

    def untranslated_ids(self) -> list[str]:
        return [sid for sid, row in self._rows.items() if _empty(row.get("translation_value"))]

    def to_dataframe(self) -> pd.DataFrame:
        rows = [self._rows[sid] for sid in sorted(self._rows)]
        return pd.DataFrame(rows, columns=BABELON_COLUMNS)

    def update_from_dataframe(self, df: pd.DataFrame) -> None:
        """Merge translated rows back in (matched by ``subject_id``)."""
        for _, row in df.iterrows():
            subject_id = str(row.get("subject_id", "")).strip()
            if subject_id in self._rows:
                self._rows[subject_id].update(
                    {c: str(row.get(c, "")) for c in BABELON_COLUMNS if c in row}
                )
