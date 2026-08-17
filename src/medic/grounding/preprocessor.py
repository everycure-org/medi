"""LLM-based drug name preprocessing."""

import json
import logging
import os
from pathlib import Path

from medic.enrichment.cache import EnrichmentCache

logger = logging.getLogger(__name__)

_cache = EnrichmentCache(Path("cache/grounding/preprocessor.json"))


def _should_skip() -> bool:
    return os.environ.get("MEDIC_SKIP_EXPENSIVE_CALLS", "").strip() in ("1", "true", "yes")


def preprocess_drug_name(name: str) -> dict:
    """Extract active moiety, detect combinations, translate if needed.

    Returns dict with keys: active_moiety, is_combination, components, confidence.
    """
    if _should_skip() or not name:
        return {
            "active_moiety": name,
            "is_combination": False,
            "components": [],
            "confidence": "skipped",
        }

    cache_key = name.strip().lower()

    # Check cache first
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        from medic.llm import llm_call
        result_text = llm_call(
            f"Drug name: {name}",
            task="grounding_preprocess",
            max_tokens=200,
            system=(
                "You are a pharmaceutical expert. Extract the active moiety "
                "from drug names. Strip salt forms, dosages, formulations, "
                "routes of administration. Translate non-English names to "
                "the English INN (International Nonproprietary Name). "
                "Return JSON only: {active_moiety, is_combination, components[], confidence}"
            ),
        )
        try:
            result = json.loads(result_text)
        except json.JSONDecodeError:
            raise ValueError(f"Non-JSON response from LLM: {result_text}")
        _cache.put(cache_key, result)
        _cache.flush()
        return result
    except Exception:
        logger.warning("LLM preprocessing failed for '%s', using raw name", name)
        return {
            "active_moiety": name,
            "is_combination": False,
            "components": [],
            "confidence": "error",
        }
