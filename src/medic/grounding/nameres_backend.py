"""NameRes backend: wraps the NCATS SRI Name Resolution Service.

This is the default backend, preserving the existing MeDIC behavior.
API: https://name-resolution-sri.renci.org/lookup
Normalization: https://nodenormalization-sri.renci.org/1.5/get_normalized_nodes
"""

import logging
import re

import httpx

from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.confidence import jaro_winkler_confidence

logger = logging.getLogger(__name__)

NAMERES_URL = "https://name-resolution-sri.renci.org/lookup"
NODENORM_URL = "https://nodenormalization-sri.renci.org/1.5/get_normalized_nodes"

# Biolink ChemicalEntity prefix priority order
DRUG_PREFIXES = [
    "CHEBI",
    "UNII",
    "PUBCHEM.COMPOUND",
    "CHEMBL.COMPOUND",
    "DRUGBANK",
    "MESH",
    "RXNORM",
    "DrugCentral",
]

DISEASE_PREFIXES = [
    "MONDO",
    "OMIM",
    "Orphanet",
    "DOID",
    "HP",
    "NCIT",
    "MESH",
]


class NameResBackend(GroundingService):
    """NCATS SRI Name Resolution Service backend."""

    def __init__(self, timeout: float = 30.0):
        self._client = httpx.Client(timeout=timeout)

    def _lookup(
        self, name: str, limit: int = 5, biolink_type: str | None = None
    ) -> list[dict]:
        """Call the NameRes lookup API."""
        if not name or not isinstance(name, str):
            return []
        # Clean name: replace non-word chars with spaces
        cleaned = re.sub(r"\W+", " ", name).strip()
        if not cleaned:
            return []

        params = {
            "string": cleaned,
            "limit": limit,
            "autocomplete": "false",
        }
        if biolink_type:
            params["biolink_type"] = biolink_type

        try:
            response = self._client.get(NAMERES_URL, params=params)
            response.raise_for_status()
            return response.json()
        except Exception:
            logger.warning("NameRes lookup failed for '%s'", name)
            return []

    def _to_results(
        self, records: list[dict], source_name: str
    ) -> list[GroundingResult]:
        """Convert NameRes JSON records to GroundingResult list."""
        results = []
        for rec in records:
            curie = rec.get("curie", "")
            label = rec.get("label", "")
            score = jaro_winkler_confidence(source_name, label)
            results.append(
                GroundingResult(
                    id=curie,
                    label=label,
                    score=score,
                    source_name=source_name,
                    service="nameres",
                )
            )
        results.sort(key=lambda r: r.score, reverse=True)
        return results

    def ground_drug(
        self, name: str, limit: int = 5, mention_id: str | None = None
    ) -> list[GroundingResult]:
        records = self._lookup(
            name, limit=limit, biolink_type="ChemicalEntity"
        )
        return self._to_results(records, source_name=name)

    def ground_disease(
        self, name: str, limit: int = 5, mention_id: str | None = None
    ) -> list[GroundingResult]:
        records = self._lookup(
            name, limit=limit, biolink_type="Disease"
        )
        return self._to_results(records, source_name=name)

    def normalize(self, curie: str) -> GroundingResult | None:
        """Normalize a CURIE via NodeNorm."""
        try:
            response = self._client.get(
                NODENORM_URL, params={"curie": curie, "conflation": "true"}
            )
            response.raise_for_status()
            data = response.json()
        except Exception:
            logger.warning("NodeNorm failed for '%s'", curie)
            return None

        node_data = data.get(curie)
        if not node_data:
            return None

        canonical = node_data.get("id", {})
        alt_ids = [
            eq.get("identifier", "")
            for eq in node_data.get("equivalent_identifiers", [])
        ]

        return GroundingResult(
            id=canonical.get("identifier", curie),
            label=canonical.get("label", ""),
            alternate_ids=alt_ids,
            source_name=curie,
        )
