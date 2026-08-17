"""Tests for cascade grounding orchestrator."""

from unittest.mock import MagicMock

from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.cascade import CascadeGrounding


def _mock_backend(name: str, results: list[GroundingResult]) -> GroundingService:
    backend = MagicMock(spec=GroundingService)
    backend.ground_drug.return_value = results
    backend.ground_disease.return_value = results
    backend.normalize.return_value = None
    return backend


def test_cascade_accepts_high_confidence():
    high = GroundingResult(id="CHEBI:1", label="aspirin", score=0.98, service="oak")
    cascade = CascadeGrounding(backends=[_mock_backend("oak", [high])])
    results = cascade.ground_drug("aspirin")
    assert len(results) >= 1
    assert results[0].id == "CHEBI:1"


def test_cascade_skips_low_confidence_backend():
    low = GroundingResult(id="CHEBI:1", label="wrong", score=0.3, service="oak")
    high = GroundingResult(id="CHEBI:2", label="aspirin", score=0.96, service="gilda")
    cascade = CascadeGrounding(backends=[
        _mock_backend("oak", [low]),
        _mock_backend("gilda", [high]),
    ])
    results = cascade.ground_drug("aspirin")
    assert results[0].id == "CHEBI:2"


def test_cascade_collects_all_when_none_pass():
    low1 = GroundingResult(id="CHEBI:1", label="x", score=0.4, service="oak")
    low2 = GroundingResult(id="CHEBI:2", label="y", score=0.5, service="gilda")
    cascade = CascadeGrounding(backends=[
        _mock_backend("oak", [low1]),
        _mock_backend("gilda", [low2]),
    ])
    results = cascade.ground_drug("something")
    assert len(results) == 2
    assert results[0].score >= results[1].score
