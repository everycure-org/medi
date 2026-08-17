"""Factory for creating grounding service instances."""

from medic.grounding.base import GroundingService

DEFAULT_BACKEND = "lexical"
_INDEX_DIR = "cache/grounding/lexical_index"


def get_grounding_service(backend: str = DEFAULT_BACKEND) -> GroundingService:
    """Create a grounding service instance.

    Args:
        backend: Which backend to use. Default 'lexical' (deterministic, offline).
            Legacy: 'oak', 'ols', 'gilda', 'cascade', 'nameres' (deprecated, non-default).

    Returns:
        A GroundingService instance for the specified backend.

    Raises:
        ValueError: If the backend is not recognized.
    """
    if backend == "lexical":
        from medic.grounding.lexical_backend import LexicalCascadeGrounding

        return LexicalCascadeGrounding(
            disease_db=f"{_INDEX_DIR}/diseases.db",
            drug_db=f"{_INDEX_DIR}/drugs.db",
        )
    elif backend == "nameres":
        from medic.grounding.nameres_backend import NameResBackend

        return NameResBackend()
    elif backend == "oak":
        from medic.grounding.oak_backend import OAKBackend

        return OAKBackend()
    elif backend == "ols":
        from medic.grounding.ols_backend import OLSBackend

        return OLSBackend()
    elif backend == "gilda":
        from medic.grounding.gilda_backend import GildaBackend

        return GildaBackend()
    elif backend == "cascade":
        from medic.grounding.cascade import CascadeGrounding

        return CascadeGrounding()
    else:
        raise ValueError(
            f"Unknown grounding backend: {backend}. "
            f"Choose from: nameres, oak, ols, gilda, cascade"
        )
