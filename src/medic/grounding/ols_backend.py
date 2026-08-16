"""OLS backend: uses the EBI Ontology Lookup Service (OLS4) API.

Useful for terms not well covered by pre-built OAK SQLite databases.
API: https://www.ebi.ac.uk/ols4/api
"""

import logging

import httpx

from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.confidence import jaro_winkler_confidence

logger = logging.getLogger(__name__)

OLS_SEARCH_URL = "https://www.ebi.ac.uk/ols4/api/search"


class OLSBackend(GroundingService):
    """EBI OLS4 grounding backend."""

    def __init__(self, timeout: float = 30.0):
        self._client = httpx.Client(timeout=timeout)

    def _search(
        self,
        name: str,
        ontology: str | None = None,
        limit: int = 5,
    ) -> list[GroundingResult]:
        """Search OLS for a term."""
        if not name:
            return []

        params: dict = {
            "q": name,
            "rows": limit,
            "exact": "false",
        }
        if ontology:
            params["ontology"] = ontology

        try:
            response = self._client.get(OLS_SEARCH_URL, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception:
            logger.warning("OLS search failed for '%s'", name)
            return []

        results = []
        docs = (
            data.get("response", {}).get("docs", [])
            if "response" in data
            else []
        )
        for doc in docs[:limit]:
            obo_id = doc.get("obo_id", doc.get("short_form", ""))
            label = doc.get("label", "")
            score = jaro_winkler_confidence(name, label)
            results.append(
                GroundingResult(
                    id=obo_id,
                    label=label,
                    score=score,
                    source_name=name,
                    service="ols",
                )
            )
        return results

    def ground_drug(
        self, name: str, limit: int = 5
    ) -> list[GroundingResult]:
        return self._search(name, ontology="chebi", limit=limit)

    def ground_disease(
        self, name: str, limit: int = 5
    ) -> list[GroundingResult]:
        return self._search(name, ontology="mondo", limit=limit)

    def normalize(self, curie: str) -> GroundingResult | None:
        """Look up a CURIE in OLS to get canonical info."""
        results = self._search(curie, limit=1)
        return results[0] if results else None
