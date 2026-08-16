"""Tests for the grounding layer."""

from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.factory import get_grounding_service


def test_grounding_result():
    """Test GroundingResult dataclass."""
    result = GroundingResult(
        id="CHEBI:12345",
        label="Aspirin",
        score=0.95,
        source_name="aspirin",
    )
    assert result.id == "CHEBI:12345"
    assert result.label == "Aspirin"
    assert result.score == 0.95
    assert result.alternate_ids == []


def test_grounding_result_service_field():
    result = GroundingResult(
        id="CHEBI:12345",
        label="Aspirin",
        score=0.95,
        source_name="aspirin",
        service="nameres",
    )
    assert result.service == "nameres"


def test_factory_nameres():
    """Test that the factory creates a NameRes backend."""
    service = get_grounding_service("nameres")
    assert isinstance(service, GroundingService)


def test_factory_oak():
    """Test that the factory creates an OAK backend."""
    service = get_grounding_service("oak")
    assert isinstance(service, GroundingService)


def test_factory_ols():
    """Test that the factory creates an OLS backend."""
    service = get_grounding_service("ols")
    assert isinstance(service, GroundingService)


def test_factory_gilda():
    service = get_grounding_service("gilda")
    assert isinstance(service, GroundingService)


def test_factory_cascade():
    service = get_grounding_service("cascade")
    assert isinstance(service, GroundingService)


def test_factory_invalid():
    """Test that the factory raises on invalid backend."""
    try:
        get_grounding_service("invalid")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
