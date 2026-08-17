"""Enrichment: Combination therapy detection."""

import logging
from pathlib import Path

from medic import product_view as pv
from medic.enrichment.cache import EnrichmentCache
from medic.ingest.common import should_skip_expensive_calls

logger = logging.getLogger(__name__)

_cache = EnrichmentCache(Path("cache/enrichment/combination.json"))


def detect_combinations(drugs: list[dict]) -> None:
    """Detect combination therapies using LLM.

    For each drug, sets:
    - is_combination_therapy: bool
    - combination_therapy_ingredients: list[str]
    - combination_therapy_ingredients_curies: list[str]

    When should_skip_expensive_calls(): sets is_combination_therapy to False.
    """
    skip = should_skip_expensive_calls()

    for i, drug in enumerate(drugs):
        curie = pv.drug_id(drug)

        if skip:
            drug["is_combination_therapy"] = False
            drug["combination_therapy_ingredients"] = []
            drug["combination_therapy_ingredients_curies"] = []
            continue

        # Check cache first
        cached = _cache.get(curie) if curie else None
        if cached is not None:
            drug["is_combination_therapy"] = cached.get("is_combination_therapy", False)
            drug["combination_therapy_ingredients"] = cached.get("combination_therapy_ingredients", [])
            drug["combination_therapy_ingredients_curies"] = cached.get("combination_therapy_ingredients_curies", [])
            continue

        drug_label = pv.drug_label(drug)
        if not drug_label:
            drug["is_combination_therapy"] = False
            drug["combination_therapy_ingredients"] = []
            drug["combination_therapy_ingredients_curies"] = []
            continue

        try:
            from medic.llm import llm_call

            prompt = (
                f"Is '{drug_label}' a combination therapy? "
                f"If yes, list the individual active ingredients.\n"
                f"Reply in exactly this format:\n"
                f"IS_COMBINATION: YES/NO\n"
                f"INGREDIENTS: ingredient1, ingredient2, ... (or NONE)"
            )

            text = llm_call(prompt, task="classification", max_tokens=200)
            is_combo = "IS_COMBINATION: YES" in text.upper()

            ingredients = []
            for line in text.split("\n"):
                if line.upper().startswith("INGREDIENTS:"):
                    raw = line.split(":", 1)[1].strip()
                    if raw.upper() != "NONE":
                        ingredients = [
                            i.strip() for i in raw.split(",") if i.strip()
                        ]

            drug["is_combination_therapy"] = is_combo
            drug["combination_therapy_ingredients"] = ingredients
            drug["combination_therapy_ingredients_curies"] = []

        except Exception:
            logger.debug(
                "Combination detection failed for %s", drug_label
            )
            drug["is_combination_therapy"] = False
            drug["combination_therapy_ingredients"] = []
            drug["combination_therapy_ingredients_curies"] = []

        # Store in cache
        if curie:
            _cache.put(curie, {
                "is_combination_therapy": drug["is_combination_therapy"],
                "combination_therapy_ingredients": drug["combination_therapy_ingredients"],
                "combination_therapy_ingredients_curies": drug["combination_therapy_ingredients_curies"],
            })
        if (i + 1) % 100 == 0:
            _cache.flush()

    _cache.flush()
