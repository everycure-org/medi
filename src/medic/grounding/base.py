"""Abstract base class for grounding services."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class GroundingResult:
    """Result of a grounding operation."""

    id: str
    """Canonical identifier (CURIE format, e.g. CHEBI:12345)."""
    label: str
    """Canonical label."""
    score: float = 0.0
    """Confidence score from the grounding service."""
    alternate_ids: list[str] = field(default_factory=list)
    """Alternative identifiers for the same entity."""
    source_name: str = ""
    """Original name that was grounded."""
    service: str = ""
    """Which backend produced this result."""


class GroundingService(ABC):
    """Abstract base class for entity grounding backends.

    Each backend implements drug and disease grounding against
    different name resolution or ontology services.
    """

    @abstractmethod
    def ground_drug(
        self, name: str, limit: int = 5, mention_id: str | None = None
    ) -> list[GroundingResult]:
        """Ground a drug name to canonical identifiers.

        Args:
            name: Drug name to ground.
            limit: Maximum number of candidate results to return.
            mention_id: The mention's MEDICNE id (I-9), stamped onto any decision row the
                backend persists. Part of the interface rather than the lexical backend's
                own signature so no caller has to know which backend it holds — a caller
                that drops it silently un-anchors the store rows it writes.

        Returns:
            List of GroundingResult candidates, ordered by score descending.
        """

    @abstractmethod
    def ground_disease(
        self, name: str, limit: int = 5, mention_id: str | None = None
    ) -> list[GroundingResult]:
        """Ground a disease name to canonical Mondo identifiers.

        Args:
            name: Disease name to ground.
            limit: Maximum number of candidate results to return.
            mention_id: See :meth:`ground_drug`.

        Returns:
            List of GroundingResult candidates, ordered by score descending.
        """

    @abstractmethod
    def normalize(self, curie: str) -> GroundingResult | None:
        """Normalize a CURIE to its canonical form.

        Args:
            curie: A CURIE to normalize (e.g., DRUGBANK:DB00001).

        Returns:
            GroundingResult with canonical ID and alternate IDs, or None.
        """

    def ground_drug_best(
        self, name: str, mention_id: str | None = None
    ) -> GroundingResult | None:
        """Ground a drug name and return the best result.

        ``mention_id`` is forwarded so a caller using this convenience wrapper still anchors
        its store rows on the mention's MEDICNE id (I-9). It used to be dropped here, which
        is how DailyMed blanked ids the drug-list ingest had written.
        """
        results = self.ground_drug(name, limit=1, mention_id=mention_id)
        return results[0] if results else None

    def ground_disease_best(
        self, name: str, mention_id: str | None = None
    ) -> GroundingResult | None:
        """Ground a disease name and return the best result. See :meth:`ground_drug_best`."""
        results = self.ground_disease(name, limit=1, mention_id=mention_id)
        return results[0] if results else None
