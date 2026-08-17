"""Cascade grounding: tries multiple backends in order."""

import logging

from medic.grounding.base import GroundingResult, GroundingService

logger = logging.getLogger(__name__)

ACCEPT_THRESHOLD = 0.80


class CascadeGrounding(GroundingService):
    """Tries grounding backends in order, accepts first result above threshold."""

    def __init__(
        self,
        backends: list[GroundingService] | None = None,
        threshold: float = ACCEPT_THRESHOLD,
    ):
        if backends is not None:
            self._backends = backends
        else:
            from medic.grounding.factory import get_grounding_service
            self._backends = []
            for name in ["oak", "gilda", "nameres", "ols"]:
                try:
                    self._backends.append(get_grounding_service(name))
                except Exception:
                    logger.info("Skipping unavailable backend: %s", name)
        self._threshold = threshold

    def _cascade(self, name: str, method: str, limit: int) -> list[GroundingResult]:
        all_candidates: list[GroundingResult] = []
        for backend in self._backends:
            try:
                fn = getattr(backend, method)
                results = fn(name, limit=limit)
                if results and results[0].score >= self._threshold:
                    return results
                all_candidates.extend(results)
            except Exception:
                logger.warning("Backend %s failed for '%s'", type(backend).__name__, name)
        return sorted(all_candidates, key=lambda r: r.score, reverse=True)

    def ground_drug(self, name: str, limit: int = 5,
                    mention_id: str | None = None) -> list[GroundingResult]:
        return self._cascade(name, "ground_drug", limit)

    def ground_disease(self, name: str, limit: int = 5,
                       mention_id: str | None = None) -> list[GroundingResult]:
        return self._cascade(name, "ground_disease", limit)

    def normalize(self, curie: str) -> GroundingResult | None:
        for backend in self._backends:
            result = backend.normalize(curie)
            if result:
                return result
        return None
