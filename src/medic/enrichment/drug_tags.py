"""Enrichment: Drug classification tags from ATC codes and LLM fallback."""

import logging
from pathlib import Path

from medic import product_view as pv
from medic.enrichment.cache import EnrichmentCache
from medic.ingest.common import should_skip_expensive_calls

logger = logging.getLogger(__name__)

_llm_cache = EnrichmentCache(Path("cache/enrichment/drug_tags_llm.json"))

ATC_CLASSIFICATION_MAP = {
    "is_steroid": ["H02", "D07", "R01AD", "R03BA", "S01BA", "S01CB", "S02BA"],
    "is_antimicrobial": ["J01", "J02", "J04", "J05", "P01", "P02", "D01"],
    "is_chemotherapy": ["L01"],
    "is_glucose_regulator": ["A10"],
    "is_vaccine_or_antigen": ["J07"],
    "is_allergen": ["V01"],
    "is_radioisotope_or_diagnostic_agent": ["V09", "V08", "V04"],
    "is_cancer_drug": ["L01", "L02"],
    "is_cardiovascular": ["C"],
}


def classify_from_atc(atc_codes: list[str]) -> dict[str, bool]:
    """Deterministic classification from ATC prefixes.

    For each tag in ATC_CLASSIFICATION_MAP, checks if any ATC code starts
    with any of the mapped prefixes.

    Returns:
        Dict of all boolean flags.
    """
    result = {}
    for tag, prefixes in ATC_CLASSIFICATION_MAP.items():
        result[tag] = any(
            any(code.upper().startswith(p.upper()) for p in prefixes)
            for code in atc_codes
        )
    return result


def classify_with_llm(
    drug_label: str, atc_codes: list[str], smiles: str
) -> dict[str, bool]:
    """LLM fallback for is_no_therapeutic_value and is_metallic_salt.

    When should_skip_expensive_calls(): returns False for both.
    """
    if should_skip_expensive_calls():
        return {"is_no_therapeutic_value": False, "is_metallic_salt": False}

    try:
        from medic.llm import llm_call

        atc_str = ", ".join(atc_codes) if atc_codes else "none"

        prompt = (
            f"Given the drug '{drug_label}' with ATC codes [{atc_str}] "
            f"and SMILES '{smiles}', answer two questions:\n"
            f"1. Is this drug of no therapeutic value (e.g., a placebo, "
            f"vehicle, diluent, or inactive ingredient)? Answer YES or NO.\n"
            f"2. Is this drug a metallic salt (contains a metal ion as "
            f"the active component)? Answer YES or NO.\n"
            f"Reply in exactly this format:\n"
            f"THERAPEUTIC_VALUE: YES/NO\n"
            f"METALLIC_SALT: YES/NO"
        )

        text = llm_call(prompt, task="classification", max_tokens=50).upper()
        no_therapeutic = "THERAPEUTIC_VALUE: YES" in text
        metallic = "METALLIC_SALT: YES" in text

        return {
            "is_no_therapeutic_value": no_therapeutic,
            "is_metallic_salt": metallic,
        }
    except Exception:
        logger.debug("LLM classification failed for %s", drug_label)
        return {"is_no_therapeutic_value": False, "is_metallic_salt": False}


def classify_drugs(drugs: list[dict]) -> None:
    """Classify drugs using ATC-derived tags and LLM fallback.

    Modifies dicts in-place with boolean classification flags.
    """
    for i, drug in enumerate(drugs):
        atc_codes = drug.get("atc_codes", [])

        # ATC-derived tags
        atc_tags = classify_from_atc(atc_codes)
        drug.update(atc_tags)

        # LLM fallback for remaining tags — check cache first
        curie = pv.drug_id(drug)
        cached = _llm_cache.get(curie) if curie else None
        if cached is not None:
            drug.update(cached)
            continue

        drug_label = pv.drug_label(drug)
        smiles = drug.get("smiles", "")
        llm_tags = classify_with_llm(drug_label, atc_codes, smiles)
        drug.update(llm_tags)

        # Store in cache
        if curie:
            _llm_cache.put(curie, {
                "is_no_therapeutic_value": llm_tags["is_no_therapeutic_value"],
                "is_metallic_salt": llm_tags["is_metallic_salt"],
            })
        # Flush every 100 to make progress durable across crashes
        if (i + 1) % 100 == 0:
            _llm_cache.flush()

    _llm_cache.flush()
