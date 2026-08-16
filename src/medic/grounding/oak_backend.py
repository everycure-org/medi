"""OAK backend: uses oaklib with local SQLite adapters for grounding.

Offline, fast, and consistent. Uses pre-built OBO SQLite databases.
"""

import logging

from medic.curie_utils import get_prefix
from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.confidence import jaro_winkler_confidence

logger = logging.getLogger(__name__)


class OAKBackend(GroundingService):
    """OAK/oaklib grounding backend using local SQLite adapters."""

    def __init__(self, config_path: str = "conf/oak_config.yaml"):
        self._config_path = config_path
        self._adapters: dict = {}

    def _get_adapter(self, prefix: str):
        """Lazily load an OAK adapter for a given prefix."""
        if prefix not in self._adapters:
            try:
                from oaklib import get_adapter

                adapter_string = self._get_adapter_string(prefix)
                if adapter_string:
                    self._adapters[prefix] = get_adapter(adapter_string)
                else:
                    self._adapters[prefix] = None
            except Exception:
                logger.warning("Failed to load OAK adapter for %s", prefix)
                self._adapters[prefix] = None
        return self._adapters[prefix]

    def _get_adapter_string(self, prefix: str) -> str | None:
        """Look up the adapter string for a prefix from config."""
        import yaml

        try:
            with open(self._config_path) as f:
                config = yaml.safe_load(f)
            adapters = config.get("ontology_adapters", {})
            value = adapters.get(prefix, "")
            return value if value else None
        except Exception:
            return None

    def _search(
        self, name: str, adapter_prefix: str, limit: int = 5
    ) -> list[GroundingResult]:
        """Search for a term using OAK search."""
        adapter = self._get_adapter(adapter_prefix)
        if adapter is None:
            return []

        results = []
        try:
            from oaklib.datamodels.search import SearchConfiguration

            config = SearchConfiguration(limit=limit)
            for curie in adapter.basic_search(name, config=config):
                label = adapter.label(curie) or ""
                if name.lower() == label.lower():
                    score = 1.0
                elif label.lower() in [
                    s.lower()
                    for s in (
                        adapter.entity_aliases(curie)
                        if hasattr(adapter, "entity_aliases")
                        else []
                    )
                ]:
                    score = 0.9
                else:
                    score = jaro_winkler_confidence(name, label)
                results.append(
                    GroundingResult(
                        id=curie,
                        label=label,
                        score=score,
                        source_name=name,
                        service="oak",
                    )
                )
                if len(results) >= limit:
                    break
        except Exception:
            logger.warning(
                "OAK search failed for '%s' in %s", name, adapter_prefix
            )

        return results

    def ground_drug(
        self, name: str, limit: int = 5
    ) -> list[GroundingResult]:
        return self._search(name, "CHEBI", limit=limit)

    def ground_disease(
        self, name: str, limit: int = 5
    ) -> list[GroundingResult]:
        return self._search(name, "MONDO", limit=limit)

    def normalize(self, curie: str) -> GroundingResult | None:
        """Normalize using OAK - look up the term and return its label."""
        prefix = get_prefix(curie)
        adapter = self._get_adapter(prefix)
        if adapter is None:
            return None

        try:
            label = adapter.label(curie)
            if label:
                return GroundingResult(
                    id=curie, label=label, source_name=curie
                )
        except Exception:
            pass
        return None
