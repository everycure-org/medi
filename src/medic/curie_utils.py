"""Shared CURIE handling utilities using the curies package.

All CURIE parsing, prefix extraction, and ID extraction in MeDIC
should use this module rather than manual str.split(":").

Uses the same pattern as sssom-py: a bioregistry-backed converter
with additional MeDIC-specific prefixes chained on top via curies.chain().
"""

from functools import lru_cache

import curies
from curies import Converter, ReferenceTuple

#: The single root every MeDIC-minted identifier and mapping-set id hangs off.
#:
#: There were two. The LinkML schemas, ``MEDICNE:`` and the mention-id UUID namespace all used
#: ``https://w3id.org/monarch-initiative/medic/``, while the grounding and normalization store
#: writers emitted ``mapping_set_id: https://w3id.org/medic/...``. Nothing failed — both simply
#: 404 until the redirect is registered — but an id scheme is the hardest thing to change after
#: a release, and these ids are already baked into five git-tracked stores, every product
#: Mention and every KGX node. Reconciled onto the schema root, and defined once here so the
#: two cannot drift apart again.
#:
#: The w3id redirect itself is still unregistered (#35). That is safe to do after the tag —
#: registering it later changes no published string — whereas disagreeing roots are not.
MEDIC_W3ID_ROOT = "https://w3id.org/monarch-initiative/medic"

# MeDIC-specific prefixes not in bioregistry
_MEDIC_EXTRA_PREFIXES = {
    "PHAROS": "https://pharos.nih.gov/ligands/",
    "OMOP": "https://athena.ohdsi.org/search-terms/terms/",
    # MeDIC named entity — a stable id minted for every extracted source mention
    # (see ``medic.mention``). Anchors the translation/grounding/normalization trail.
    "MEDICNE": f"{MEDIC_W3ID_ROOT}/MEDICNE_",
}


@lru_cache(maxsize=1)
def get_converter() -> Converter:
    """Get a CURIE converter backed by the full bioregistry.

    Returns a cached Converter with ~2000+ biomedical prefixes from the
    bioregistry, plus MeDIC-specific extras. Uses curies.chain() to layer
    extras on top, following the same pattern as sssom-py.
    """
    base = curies.get_bioregistry_converter()
    extras = Converter.from_prefix_map(_MEDIC_EXTRA_PREFIXES)
    return curies.chain([extras, base])


def parse_curie(curie: str) -> ReferenceTuple | None:
    """Parse a CURIE string into prefix and local identifier.

    Returns None if the CURIE cannot be parsed.

    >>> parse_curie("CHEBI:15365")
    ReferenceTuple(prefix='CHEBI', identifier='15365')
    >>> parse_curie("DRUGBANK:DB00945")
    ReferenceTuple(prefix='DRUGBANK', identifier='DB00945')
    """
    if not curie or ":" not in curie:
        return None
    return get_converter().parse_curie(curie)


def get_prefix(curie: str) -> str:
    """Extract the prefix from a CURIE.

    >>> get_prefix("CHEBI:15365")
    'CHEBI'
    >>> get_prefix("invalid")
    ''
    """
    ref = parse_curie(curie)
    return ref.prefix if ref else ""


def get_local_id(curie: str) -> str:
    """Extract the local identifier from a CURIE.

    >>> get_local_id("CHEBI:15365")
    '15365'
    >>> get_local_id("CHEMBL.COMPOUND:CHEMBL25")
    'CHEMBL25'
    """
    ref = parse_curie(curie)
    return ref.identifier if ref else ""


def has_prefix(curie: str, prefix: str) -> bool:
    """Check if a CURIE has the given prefix.

    >>> has_prefix("CHEBI:15365", "CHEBI")
    True
    >>> has_prefix("DRUGBANK:DB00945", "CHEBI")
    False
    """
    return get_prefix(curie).upper() == prefix.upper()


def find_by_prefix(curies_list: list[str], prefix: str) -> str | None:
    """Find the first CURIE with the given prefix in a list.

    >>> find_by_prefix(["DRUGBANK:DB00945", "CHEBI:15365"], "CHEBI")
    'CHEBI:15365'
    """
    prefix_upper = prefix.upper()
    for curie in curies_list:
        if get_prefix(curie).upper() == prefix_upper:
            return curie
    return None
