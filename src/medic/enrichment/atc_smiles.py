"""Enrichment: ATC codes and SMILES via ChEMBL and PubChem."""

import logging
import re
from pathlib import Path

import httpx

from medic import product_view as pv
from medic.curie_utils import find_by_prefix, get_local_id, get_prefix, parse_curie
from medic.enrichment.cache import EnrichmentCache
from medic.ingest.common import should_skip_expensive_calls

logger = logging.getLogger(__name__)

_cache = EnrichmentCache(Path("cache/enrichment/atc_smiles.json"))


def decompose_atc(code: str) -> dict:
    """Break an ATC code into its hierarchical levels."""
    if not code or len(code) < 1:
        return {}
    return {
        "atc_main": code[0] if len(code) >= 1 else "",
        "atc_level1": code[:3] if len(code) >= 3 else code,
        "atc_level2": code[:4] if len(code) >= 4 else code,
        "atc_level3": code[:5] if len(code) >= 5 else code,
        "atc_level4": code[:7] if len(code) >= 7 else code,
        "atc_level5": code[:7] if len(code) >= 7 else code,
    }


def _find_chembl_id(alternate_ids: list[str]) -> str | None:
    """Extract ChEMBL ID from alternate_ids list."""
    for aid in alternate_ids:
        aid_str = str(aid)
        # Match CHEMBL.COMPOUND:CHEMBL1234 or CHEMBL:CHEMBL1234 or CHEMBL1234
        if aid_str.upper().startswith("CHEMBL.COMPOUND:"):
            return get_local_id(aid_str)
        if aid_str.upper().startswith("CHEMBL:"):
            return get_local_id(aid_str)
        if re.match(r"^CHEMBL\d+$", aid_str, re.IGNORECASE):
            return aid_str
    return None


def _lookup_chembl(chembl_id: str) -> dict:
    """Look up ATC codes and SMILES from ChEMBL API."""
    url = f"https://www.ebi.ac.uk/chembl/api/data/molecule/{chembl_id}.json"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        atc_codes = data.get("atc_classifications", []) or []
        smiles = ""
        structures = data.get("molecule_structures") or {}
        if structures:
            smiles = structures.get("canonical_smiles", "") or ""

        return {"atc_codes": atc_codes, "smiles": smiles}
    except Exception:
        logger.debug("ChEMBL lookup failed for %s", chembl_id)
        return {"atc_codes": [], "smiles": ""}


# UniChem source IDs for various databases
_UNICHEM_SRC_MAP = {
    "CHEBI": 7,
    "PUBCHEM.COMPOUND": 22,
    "DRUGBANK": 2,
}


def _lookup_unichem(curie: str) -> str | None:
    """Map a CURIE to ChEMBL ID via UniChem."""
    ref = parse_curie(curie)
    if ref is None:
        return None

    src_id = _UNICHEM_SRC_MAP.get(ref.prefix.upper())
    if src_id is None:
        return None

    url = f"https://www.ebi.ac.uk/unichem/rest/src_compound_id/{ref.identifier}/{src_id}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        # Look for ChEMBL (src_id=1) in the results
        for entry in data:
            if str(entry.get("src_id")) == "1":
                return entry.get("src_compound_id")
    except Exception:
        logger.debug("UniChem lookup failed for %s", curie)

    return None


def _lookup_smiles_pubchem(cid: str) -> str:
    """Fallback SMILES lookup via PubChem."""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/property/IsomericSMILES/JSON"
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        props = data.get("PropertyTable", {}).get("Properties", [])
        if props:
            return props[0].get("IsomericSMILES", "")
    except Exception:
        logger.debug("PubChem SMILES lookup failed for CID %s", cid)

    return ""


def _extract_pubchem_cid(alternate_ids: list[str]) -> str | None:
    """Extract PubChem CID from alternate_ids."""
    curie = find_by_prefix(alternate_ids, "PUBCHEM.COMPOUND")
    if curie is not None:
        return get_local_id(curie)
    return None


_ATC_CODE_RE = re.compile(r"[A-Z]\d\d[A-Z][A-Z]\d\d")


def _lookup_atc_pubchem_pugview(cid: str) -> list[str]:
    """Look up ATC codes from PubChem PUG View classification data."""
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{cid}/JSON"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        sections = data.get("Record", {}).get("Section", [])
        for section in sections:
            if section.get("TOCHeading") == "Classification":
                for subsection in section.get("Section", []):
                    heading = subsection.get("TOCHeading", "")
                    if "ATC" in heading:
                        # Collect all text from this subsection
                        text_blob = str(subsection)
                        codes = _ATC_CODE_RE.findall(text_blob)
                        if codes:
                            return list(dict.fromkeys(codes))  # deduplicate, preserve order
    except Exception:
        logger.debug("PubChem PUG View ATC lookup failed for CID %s", cid)

    return []


def _extract_drugcentral_id(alternate_ids: list[str]) -> str | None:
    """Extract DrugCentral ID from alternate_ids (prefix DrugCentral:)."""
    curie = find_by_prefix(alternate_ids, "DrugCentral")
    if curie is not None:
        return get_local_id(curie)
    return None


def _lookup_atc_drugcentral(drugcentral_id: str) -> list[str]:
    """Look up ATC codes from DrugCentral API."""
    url = f"https://drugcentral.org/api/drugcentral/structures?q={drugcentral_id}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        if isinstance(data, list) and data:
            annotations = data[0].get("annotations", [])
            codes = [a["value"] for a in annotations if a.get("type") == "ATC" and a.get("value")]
            return codes
    except Exception:
        logger.debug("DrugCentral ATC lookup failed for ID %s", drugcentral_id)

    return []


def _extract_rxcui(alternate_ids: list[str]) -> str | None:
    """Extract RxNorm CUI from alternate_ids (prefix RXCUI:)."""
    curie = find_by_prefix(alternate_ids, "RXCUI")
    if curie is not None:
        return get_local_id(curie)
    return None


def _lookup_atc_rxnorm(rxcui: str) -> list[str]:
    """Look up ATC codes from RxNav API."""
    url = f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/property.json?propName=ATC"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

        group = data.get("propConceptGroup", {}) or {}
        concepts = group.get("propConcept", []) or []
        codes = [c["propValue"] for c in concepts if c.get("propName") == "ATC" and c.get("propValue")]
        return codes
    except Exception:
        logger.debug("RxNorm ATC lookup failed for RXCUI %s", rxcui)

    return []


def _lookup_atc_chebi(chebi_id: str) -> list[str]:
    """Look up ATC codes from ChEBI web service (XML)."""
    import xml.etree.ElementTree as ET

    url = f"https://www.ebi.ac.uk/webservices/chebi/2.0/getCompleteEntity?chebiId=CHEBI:{chebi_id}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()

        root = ET.fromstring(resp.content)
        # Search for ATC cross-references in any namespace
        codes = []
        for elem in root.iter():
            if elem.tag.endswith("DatabaseAccession") or elem.tag.endswith("data"):
                text = elem.text or ""
                if _ATC_CODE_RE.match(text):
                    codes.append(text)
        return list(dict.fromkeys(codes))
    except Exception:
        logger.debug("ChEBI ATC lookup failed for %s", chebi_id)

    return []


def enrich_atc_smiles(drugs: list[dict]) -> None:
    """Enrich drug dicts with ATC codes and SMILES.

    Modifies dicts in-place:
    - atc_codes: list of ATC code strings
    - atc_main, atc_level1..atc_level5: hierarchy from first ATC code
    - smiles: canonical SMILES string
    """
    skip = should_skip_expensive_calls()
    total = len(drugs)

    for i, drug in enumerate(drugs):
        if (i + 1) % 100 == 0 or i == 0:
            logger.info("ATC+SMILES enrichment: %d/%d drugs processed", i + 1, total)
        alt_ids = drug.get("alternate_ids", [])
        curie = pv.drug_id(drug)

        if skip:
            drug.setdefault("atc_codes", [])
            drug.setdefault("smiles", "")
            continue

        # Check cache first
        cached = _cache.get(curie) if curie else None
        if cached is not None:
            drug["atc_codes"] = cached.get("atc_codes", [])
            drug["smiles"] = cached.get("smiles", "")
            if cached.get("atc_codes"):
                levels = decompose_atc(cached["atc_codes"][0])
                drug.update(levels)
            else:
                drug["atc_main"] = cached.get("atc_main", "")
                drug["atc_level1"] = cached.get("atc_level1", "")
                drug["atc_level2"] = cached.get("atc_level2", "")
                drug["atc_level3"] = cached.get("atc_level3", "")
                drug["atc_level4"] = cached.get("atc_level4", "")
                drug["atc_level5"] = cached.get("atc_level5", "")
            continue

        # Step 1: Find ChEMBL ID
        chembl_id = _find_chembl_id(alt_ids)

        if not chembl_id:
            # Try UniChem mapping — only for prefixes UniChem knows about
            for aid in alt_ids:
                aid_str = str(aid)
                prefix = get_prefix(aid_str).upper()
                if prefix in _UNICHEM_SRC_MAP:
                    chembl_id = _lookup_unichem(aid_str)
                    if chembl_id:
                        break
            # Also try the main curie itself
            if not chembl_id and curie:
                chembl_id = _lookup_unichem(curie)

        # Step 2: Query ChEMBL
        result = {"atc_codes": [], "smiles": ""}
        if chembl_id:
            result = _lookup_chembl(chembl_id)

        # Step 2b: ATC fallbacks if ChEMBL didn't return ATC codes
        if not result["atc_codes"]:
            # Try PubChem PUG View
            cid = _extract_pubchem_cid(alt_ids)
            if cid:
                atc = _lookup_atc_pubchem_pugview(cid)
                if atc:
                    result["atc_codes"] = atc

        if not result["atc_codes"]:
            # Try DrugCentral
            dc_id = _extract_drugcentral_id(alt_ids)
            if dc_id:
                atc = _lookup_atc_drugcentral(dc_id)
                if atc:
                    result["atc_codes"] = atc

        if not result["atc_codes"]:
            # Try RxNorm
            rxcui = _extract_rxcui(alt_ids)
            if rxcui:
                atc = _lookup_atc_rxnorm(rxcui)
                if atc:
                    result["atc_codes"] = atc

        if not result["atc_codes"]:
            # Try ChEBI web service
            chebi_curie = find_by_prefix(alt_ids, "CHEBI")
            if not chebi_curie and curie and get_prefix(curie).upper() == "CHEBI":
                chebi_curie = curie
            if chebi_curie:
                atc = _lookup_atc_chebi(get_local_id(chebi_curie))
                if atc:
                    result["atc_codes"] = atc

        # Step 3: Fallback SMILES via PubChem
        if not result["smiles"]:
            cid = _extract_pubchem_cid(alt_ids)
            if cid:
                result["smiles"] = _lookup_smiles_pubchem(cid)

        # Step 4: Set fields on drug dict
        drug["atc_codes"] = result["atc_codes"]
        drug["smiles"] = result["smiles"]

        # Decompose first ATC code
        if result["atc_codes"]:
            levels = decompose_atc(result["atc_codes"][0])
            drug.update(levels)
        else:
            drug["atc_main"] = ""
            drug["atc_level1"] = ""
            drug["atc_level2"] = ""
            drug["atc_level3"] = ""
            drug["atc_level4"] = ""
            drug["atc_level5"] = ""

        # Store in cache
        if curie:
            cache_value = {
                "atc_codes": drug["atc_codes"],
                "smiles": drug["smiles"],
                "atc_main": drug.get("atc_main", ""),
                "atc_level1": drug.get("atc_level1", ""),
                "atc_level2": drug.get("atc_level2", ""),
                "atc_level3": drug.get("atc_level3", ""),
                "atc_level4": drug.get("atc_level4", ""),
                "atc_level5": drug.get("atc_level5", ""),
            }
            _cache.put(curie, cache_value)

        # Flush every 100 drugs to avoid losing progress
        if (i + 1) % 100 == 0:
            _cache.flush()

    _cache.flush()
