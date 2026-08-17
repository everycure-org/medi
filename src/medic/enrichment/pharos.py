"""Enrichment: PHAROS cross-references."""

import logging
from pathlib import Path

import httpx

from medic import product_view as pv
from medic.enrichment.cache import EnrichmentCache
from medic.ingest.common import should_skip_expensive_calls

logger = logging.getLogger(__name__)

_cache = EnrichmentCache(Path("cache/enrichment/pharos.json"))

PHAROS_GRAPHQL_URL = "https://pharos-api.ncats.io/graphql"


def enrich_pharos(drugs: list[dict]) -> None:
    """Enrich drugs with PHAROS cross-references.

    For each drug, queries the PHAROS GraphQL API and:
    - Extracts cross-reference IDs from synonyms, adds to alternate_ids
    - Adds LyCHI hash as PHAROS:{lychi_hash}

    When should_skip_expensive_calls(): skips all lookups.
    """
    if should_skip_expensive_calls():
        return

    for i, drug in enumerate(drugs):
        curie = pv.drug_id(drug)
        drug_label = pv.drug_label(drug)
        if not drug_label:
            continue

        # Check cache first
        cached = _cache.get(curie) if curie else None
        if cached is not None:
            alt_ids = set(drug.get("alternate_ids", []))
            alt_ids.update(cached.get("pharos_ids", []))
            drug["alternate_ids"] = sorted(alt_ids)
            continue

        try:
            query = """
            {
                ligand(ligid: "%s") {
                    ligid
                    name
                    synonyms {
                        name
                        value
                    }
                }
            }
            """ % drug_label.replace('"', '\\"')

            with httpx.Client(timeout=30.0) as client:
                resp = client.post(
                    PHAROS_GRAPHQL_URL,
                    json={"query": query},
                )
                resp.raise_for_status()
                data = resp.json()

            ligand = (data.get("data") or {}).get("ligand")
            if not ligand:
                if curie:
                    _cache.put(curie, {"pharos_ids": []})
                continue

            alt_ids = set(drug.get("alternate_ids", []))
            new_ids = []

            # Extract synonyms as cross-references
            synonyms = ligand.get("synonyms") or []
            for syn in synonyms:
                name = syn.get("name", "")
                value = syn.get("value", "")
                if name and value:
                    # Add as prefix:value format
                    cross_ref = f"{name}:{value}"
                    alt_ids.add(cross_ref)
                    new_ids.append(cross_ref)

            # Add LyCHI hash if available
            ligid = ligand.get("ligid", "")
            if ligid:
                pharos_id = f"PHAROS:{ligid}"
                alt_ids.add(pharos_id)
                new_ids.append(pharos_id)

            drug["alternate_ids"] = sorted(alt_ids)

            # Store in cache
            if curie:
                _cache.put(curie, {"pharos_ids": sorted(new_ids)})

        except Exception:
            logger.debug("PHAROS lookup failed for %s", drug_label)
        if (i + 1) % 100 == 0:
            _cache.flush()

    _cache.flush()
