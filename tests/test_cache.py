"""Tests for persistent grounding cache."""

import tempfile
from pathlib import Path

from medic.grounding.cache import GroundingCache


def test_cache_miss():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = GroundingCache(Path(tmpdir))
        assert cache.get("aspirin", "orangebook") is None


def test_cache_put_and_get():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = GroundingCache(Path(tmpdir))
        entry = {
            "query": "aspirin",
            "result_id": "CHEBI:15365",
            "result_label": "acetylsalicylic acid",
            "confidence": 0.95,
            "service": "nameres",
        }
        cache.put("aspirin", "orangebook", entry)
        result = cache.get("aspirin", "orangebook")
        assert result is not None
        assert result["result_id"] == "CHEBI:15365"


def test_cache_persists_to_disk():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache1 = GroundingCache(Path(tmpdir))
        cache1.put("aspirin", "orangebook", {"result_id": "CHEBI:15365"})
        cache1.flush()

        cache2 = GroundingCache(Path(tmpdir))
        result = cache2.get("aspirin", "orangebook")
        assert result is not None
        assert result["result_id"] == "CHEBI:15365"


def test_cache_different_sources():
    with tempfile.TemporaryDirectory() as tmpdir:
        cache = GroundingCache(Path(tmpdir))
        cache.put("aspirin", "orangebook", {"result_id": "CHEBI:15365"})
        cache.put("aspirin", "ema", {"result_id": "CHEBI:15365"})
        assert cache.get("aspirin", "orangebook") is not None
        assert cache.get("aspirin", "ema") is not None
        assert cache.get("aspirin", "pmda") is None
