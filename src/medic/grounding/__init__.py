"""
Pluggable entity grounding layer for MeDIC.

Supports multiple backends for resolving drug and disease names to
canonical ontology identifiers:
- nameres: NCATS SRI Name Resolution Service (default, current behavior)
- oak: OAK/oaklib with local SQLite adapters (offline, fast)
- ols: EBI Ontology Lookup Service (OLS4)
"""

from medic.grounding.base import GroundingResult, GroundingService
from medic.grounding.factory import get_grounding_service

__all__ = ["GroundingResult", "GroundingService", "get_grounding_service"]
