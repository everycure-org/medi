"""Tests for confidence scoring."""

from medic.grounding.confidence import (
    jaro_winkler_confidence,
    confidence_tier,
    ConfidenceTier,
)


def test_jaro_winkler_exact_match():
    assert jaro_winkler_confidence("aspirin", "aspirin") == 1.0


def test_jaro_winkler_close_match():
    score = jaro_winkler_confidence("aspirin", "Aspirin")
    assert score > 0.9


def test_jaro_winkler_poor_match():
    score = jaro_winkler_confidence("aspirin", "metformin")
    assert score < 0.7


def test_jaro_winkler_empty():
    assert jaro_winkler_confidence("", "") == 0.0
    assert jaro_winkler_confidence("aspirin", "") == 0.0


def test_confidence_tier_auto_accept():
    assert confidence_tier(0.98) == ConfidenceTier.AUTO_ACCEPT


def test_confidence_tier_review():
    assert confidence_tier(0.87) == ConfidenceTier.REVIEW_RECOMMENDED


def test_confidence_tier_rerank():
    assert confidence_tier(0.65) == ConfidenceTier.LLM_RERANK


def test_confidence_tier_unresolved():
    assert confidence_tier(0.3) == ConfidenceTier.UNRESOLVED
