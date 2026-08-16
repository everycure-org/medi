"""Confidence scoring and tier classification for grounding results."""

from enum import Enum

import jellyfish


class ConfidenceTier(str, Enum):
    AUTO_ACCEPT = "accepted"
    REVIEW_RECOMMENDED = "review_recommended"
    LLM_RERANK = "llm_rerank"
    UNRESOLVED = "unresolved"


def jaro_winkler_confidence(query: str, label: str) -> float:
    """Compute Jaro-Winkler similarity as a confidence score.

    Returns 0.0 for empty strings, otherwise a float in [0, 1].
    """
    if not query or not label:
        return 0.0
    return jellyfish.jaro_winkler_similarity(
        query.lower().strip(), label.lower().strip()
    )


def confidence_tier(score: float) -> ConfidenceTier:
    """Classify a confidence score into an action tier."""
    if score >= 0.95:
        return ConfidenceTier.AUTO_ACCEPT
    elif score >= 0.80:
        return ConfidenceTier.REVIEW_RECOMMENDED
    elif score >= 0.50:
        return ConfidenceTier.LLM_RERANK
    else:
        return ConfidenceTier.UNRESOLVED
