"""Tests for LLM preprocessor and reranker."""

import os
from unittest.mock import patch

from medic.grounding.preprocessor import preprocess_drug_name
from medic.grounding.reranker import rerank_candidates
from medic.grounding.base import GroundingResult


def test_preprocessor_skip_mode():
    with patch.dict(os.environ, {"MEDIC_SKIP_EXPENSIVE_CALLS": "1"}):
        result = preprocess_drug_name("FENTANYL CITRATE INJECTION 200MCG")
        assert result["active_moiety"] == "FENTANYL CITRATE INJECTION 200MCG"
        assert result["confidence"] == "skipped"


def test_preprocessor_empty_name():
    result = preprocess_drug_name("")
    assert result["active_moiety"] == ""


def test_reranker_skip_mode():
    candidates = [
        GroundingResult(id="CHEBI:1", label="aspirin", score=0.6, service="nameres"),
        GroundingResult(id="CHEBI:2", label="ibuprofen", score=0.5, service="ols"),
    ]
    with patch.dict(os.environ, {"MEDIC_SKIP_EXPENSIVE_CALLS": "1"}):
        result = rerank_candidates("aspirin", candidates)
        assert result.id == "CHEBI:1"


def test_reranker_empty_candidates():
    result = rerank_candidates("aspirin", [])
    assert result is None
