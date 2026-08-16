"""Translation service — mint MEDICNE ids and batch-translate via Babelon/DeepL.

Wraps the ``babelon`` translator service. The flow for a source ingester:

    svc = TranslationService("mappings/drug_translation.babelon.tsv", "zh")
    svc.translate(list_of_unique_source_names)      # batch, cached, resumable
    obj = svc.translation_object(one_source_name)   # -> record's ``translation`` slot
    english = svc.translated(one_source_name)        # -> string handed to the grounder

Determinism / cost: the git-tracked Babelon TSV is the authoritative cache. Only
rows without a ``translation_value`` are sent to DeepL; a committed table replays
offline and byte-identically. ``MEDIC_SKIP_EXPENSIVE_CALLS`` bypasses DeepL
entirely (rows stay ``NOT_TRANSLATED`` and will not ground) so plumbing can be
validated offline.
"""

from __future__ import annotations

import inspect
import logging

from medic.ingest.common import should_skip_expensive_calls
from medic.mention import MENTION_ID_KEY, ORIGINAL_LITERAL_KEY, mint_mention_id
from medic.translation.store import DEFAULT_PREDICATE, TranslationStore

logger = logging.getLogger(__name__)

# Wikidata id of the DeepL translator (what babelon stamps into ``translator``).
DEEPL_WIKIDATA = "wikidata:Q116709136"

# One Babelon store per entity type (China + Russia share the drug store).
DRUG_TRANSLATION_STORE = "mappings/drug_translation.babelon.tsv"
DISEASE_TRANSLATION_STORE = "mappings/disease_translation.babelon.tsv"


class TranslationService:
    """Mint MEDICNE ids for source mentions and translate them to English."""

    def __init__(
        self,
        store_path: str,
        source_language: str,
        translation_language: str = "en-us",
        entity_type: str = "drugs",
        model: str = "deepl",
        predicate_id: str = DEFAULT_PREDICATE,
    ):
        self.store = TranslationStore(store_path)
        self.store.load()
        self.source_language = source_language
        self.translation_language = translation_language
        self.entity_type = entity_type
        self.model = model
        self.predicate_id = predicate_id

    def mention_id(self, source_value: str) -> str:
        """The stable MEDICNE id for a source string (deterministic, offline)."""
        return mint_mention_id(source_value, self.entity_type)

    def translate(self, source_values: list[str]) -> None:
        """Register + batch-translate a list of source strings (deduped by id).

        Registers every mention in the Babelon store, then sends only the rows
        that are still untranslated to DeepL (via ``babelon.translate``). Saves
        the store once at the end (``translate_profile`` also checkpoints per row
        to the same path, so an interrupted run stays resumable).
        """
        for source_value in source_values:
            sv = (source_value or "").strip()
            if not sv:
                continue
            self.store.upsert_source(
                self.mention_id(sv), sv, self.source_language,
                self.translation_language, self.predicate_id,
            )

        pending = self.store.untranslated_ids()
        if not pending:
            self.store.save()
            return

        if should_skip_expensive_calls():
            logger.warning(
                "MEDIC_SKIP_EXPENSIVE_CALLS set: %d %s mention(s) left untranslated "
                "(they will NOT ground).", len(pending), self.source_language,
            )
            self.store.save()
            return

        logger.info(
            "Translating %d %s mention(s) -> %s via %s...",
            len(pending), self.source_language, self.translation_language, self.model,
        )
        # Import lazily: constructing the DeepL translator reads DEEPL_API_KEY, so
        # keep offline/test imports free of that requirement.
        from medic.llm import _load_env_keys  # noqa: PLC2701 - shared .env loader
        from babelon.translate import translate_profile

        _load_env_keys()  # ensure DEEPL_API_KEY from .env is in the environment
        kwargs = dict(language_code=self.translation_language, model=self.model)
        # ``checkpoint_path`` (per-row resilience) exists in newer babelon only;
        # fall back gracefully for the published release. We save the store below
        # either way, so correctness does not depend on it.
        if "checkpoint_path" in inspect.signature(translate_profile).parameters:
            kwargs["checkpoint_path"] = self.store.path
        translated = translate_profile(self.store.to_dataframe(), **kwargs)
        self.store.update_from_dataframe(translated)
        self.store.save()

    def translated(self, source_value: str) -> str:
        """English translation for a source string ('' if untranslated)."""
        return self.store.translation_value(self.mention_id((source_value or "").strip()))

    def translation_object(self, source_value: str) -> dict | None:
        """The record-level ``translation`` object for a source string.

        Returns ``None`` when the mention was never registered. A registered but
        untranslated mention still returns an object (``translation_status:
        NOT_TRANSLATED``) so the failed step is visible in the trail (I-8).
        """
        sv = (source_value or "").strip()
        row = self.store.get(self.mention_id(sv))
        if row is None:
            return None
        return {
            "subject_id": row["subject_id"],
            "predicate_id": row["predicate_id"] or self.predicate_id,
            "source_language": row["source_language"],
            "source_value": row["source_value"],
            "translation_language": row["translation_language"] or self.translation_language,
            "translation_value": self.store.translation_value(row["subject_id"]),
            "translator": row.get("translator") or "",
            "translator_expertise": row.get("translator_expertise") or "",
            "translation_status": row.get("translation_status") or "NOT_TRANSLATED",
        }


def translate_records(
    records: list[dict],
    source_language: str,
    *,
    name_key: str = "source_name",
    entity_type: str = "drugs",
    store_path: str = DRUG_TRANSLATION_STORE,
    translation_key: str = "translation",
    translation_service: "TranslationService | None" = None,
) -> "TranslationService":
    """Translate each record's original source literal to English in place.

    Translates the record's ``original_literal`` (the verbatim source string
    minted at extraction; falls back to ``name_key`` if absent), attaches the
    Babelon ``translation`` object under ``translation_key``, and — when a
    translation exists — **replaces** ``name_key`` with the English value so the
    downstream grounder sees English. The mention's ``mention_id`` (minted at
    extraction, I-9) is preserved; it is set here only if the record arrived
    without one. The verbatim source string is preserved in the translation
    object's ``source_value`` and the caller's ``original_literal``, so I-7 holds.
    Names that fail to translate keep their original value (Russia's Cyrillic
    transliteration ladder can still catch those).
    """
    svc = translation_service or TranslationService(
        store_path, source_language, entity_type=entity_type
    )

    def _literal(record: dict) -> str:
        return (record.get(ORIGINAL_LITERAL_KEY) or record.get(name_key) or "").strip()

    svc.translate([_literal(r) for r in records if _literal(r)])
    for record in records:
        literal = _literal(record)
        if not literal:
            continue
        record.setdefault(MENTION_ID_KEY, svc.mention_id(literal))
        record[translation_key] = svc.translation_object(literal)
        english = svc.translated(literal)
        if english:
            record[name_key] = english
    return svc
