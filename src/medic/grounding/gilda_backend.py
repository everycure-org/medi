"""Gilda backend: biomedical entity grounding with real confidence scores."""

import logging

from medic.grounding.base import GroundingResult, GroundingService

logger = logging.getLogger(__name__)

try:
    import gilda
    GILDA_AVAILABLE = True
except ImportError:
    GILDA_AVAILABLE = False
    logger.info("Gilda not installed, GildaBackend will return empty results")


class GildaBackend(GroundingService):
    """Gilda grounding service backend."""

    def _ground(self, name: str, limit: int = 5) -> list[GroundingResult]:
        if not GILDA_AVAILABLE or not name:
            return []
        try:
            matches = gilda.ground(name)[:limit]
            return [
                GroundingResult(
                    id=m.term.db + ":" + m.term.id,
                    label=m.term.entry_name,
                    score=m.score,
                    source_name=name,
                    service="gilda",
                )
                for m in matches
            ]
        except Exception:
            logger.warning("Gilda grounding failed for '%s'", name)
            return []

    def ground_drug(self, name: str, limit: int = 5,
                    mention_id: str | None = None) -> list[GroundingResult]:
        return self._ground(name, limit)

    def ground_disease(self, name: str, limit: int = 5,
                       mention_id: str | None = None) -> list[GroundingResult]:
        return self._ground(name, limit)

    def normalize(self, curie: str) -> GroundingResult | None:
        return None  # Gilda doesn't do normalization
