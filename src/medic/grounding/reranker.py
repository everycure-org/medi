"""LLM-based candidate reranking for ambiguous grounding results."""

import json
import logging
import os
from pathlib import Path

from medic.enrichment.cache import EnrichmentCache
from medic.grounding.base import GroundingResult

logger = logging.getLogger(__name__)

_cache = EnrichmentCache(Path("cache/grounding/reranker.json"))


def _should_skip() -> bool:
    return os.environ.get("MEDIC_SKIP_EXPENSIVE_CALLS", "").strip() in ("1", "true", "yes")


def rerank_candidates(
    drug_name: str,
    candidates: list[GroundingResult],
    max_candidates: int = 30,
) -> GroundingResult | None:
    """Use LLM to pick the best candidate from an ambiguous set.

    Returns the selected candidate with updated score, or None if no candidates.
    """
    if not candidates:
        return None

    if _should_skip():
        return candidates[0]

    options = candidates[:max_candidates]

    # Build cache key from drug name and candidate IDs
    candidate_ids = "|".join(r.id for r in options)
    cache_key = f"{drug_name}|{candidate_ids}"

    # Check cache first
    cached = _cache.get(cache_key)
    if cached is not None:
        return GroundingResult(
            id=cached["selected_id"],
            label=cached["selected_label"],
            score=cached["selected_score"],
            alternate_ids=[],
            source_name=drug_name,
            service=cached["selected_service"],
        )

    options_text = "\n".join(
        f"{i+1}. {r.id} ({r.label}) [score={r.score:.2f}, via {r.service}]"
        for i, r in enumerate(options)
    )

    try:
        from medic.llm import llm_call
        result_text = llm_call(
            f"Drug: {drug_name}\n\nCandidates:\n{options_text}",
            task="grounding_rerank",
            max_tokens=300,
            system=(
                "You are a pharmaceutical expert. Select the best ontology "
                "identifier for the given drug from the candidates. "
                "Prefer the simplest form (active moiety over salt form). "
                "Return JSON only: {selected_index, reasoning, confidence}"
            ),
        )
        try:
            result = json.loads(result_text)
        except json.JSONDecodeError:
            raise ValueError(f"Non-JSON response from LLM: {result_text}")
        idx = int(result.get("selected_index", 1)) - 1
        if 0 <= idx < len(options):
            selected = options[idx]
            grounding_result = GroundingResult(
                id=selected.id,
                label=selected.label,
                score=max(selected.score, 0.85),
                alternate_ids=selected.alternate_ids,
                source_name=selected.source_name,
                service=f"{selected.service}+llm_rerank",
            )
            _cache.put(cache_key, {
                "selected_id": grounding_result.id,
                "selected_label": grounding_result.label,
                "selected_score": grounding_result.score,
                "selected_service": grounding_result.service,
            })
            _cache.flush()
            return grounding_result
    except Exception:
        logger.warning("LLM reranking failed for '%s'", drug_name)

    return candidates[0]
