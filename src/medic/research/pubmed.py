"""PubMed search and abstract retrieval for the research pipeline.

Uses the NCBI E-utilities API with the NCBI_API_KEY from .env.
"""

import json
import logging
import os
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
CACHE_DIR = Path("references_cache")


def _get_api_key() -> str:
    """Get NCBI API key from environment or .env file."""
    key = os.environ.get("NCBI_API_KEY", "")
    if not key:
        env_path = Path(".env")
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("NCBI_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    return key


def search_pubmed(
    query: str,
    max_results: int = 20,
    sort: str = "relevance",
) -> list[str]:
    """Search PubMed and return a list of PMIDs.

    Args:
        query: PubMed search query string.
        max_results: Maximum number of results to return.
        sort: Sort order ('relevance' or 'date').

    Returns:
        List of PMID strings.
    """
    api_key = _get_api_key()
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": max_results,
        "retmode": "json",
        "sort": sort,
    }
    if api_key:
        params["api_key"] = api_key

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(ESEARCH_URL, params=params)
            response.raise_for_status()
            data = response.json()

        id_list = data.get("esearchresult", {}).get("idlist", [])
        logger.info("PubMed search '%s': %d results", query[:60], len(id_list))
        return id_list
    except Exception:
        logger.warning("PubMed search failed for query: %s", query[:60])
        return []


def fetch_abstracts(pmids: list[str]) -> dict[str, dict]:
    """Fetch abstracts for a list of PMIDs.

    Args:
        pmids: List of PMID strings.

    Returns:
        Dict mapping PMID to {title, abstract, authors, journal, year}.
    """
    if not pmids:
        return {}

    # Check cache first
    results = {}
    uncached = []
    for pmid in pmids:
        cached = _read_cache(pmid)
        if cached:
            results[pmid] = cached
        else:
            uncached.append(pmid)

    if not uncached:
        return results

    # Fetch uncached abstracts in batches of 50
    api_key = _get_api_key()
    for i in range(0, len(uncached), 50):
        batch = uncached[i : i + 50]
        params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
            "rettype": "abstract",
        }
        if api_key:
            params["api_key"] = api_key

        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.get(EFETCH_URL, params=params)
                response.raise_for_status()
                xml_text = response.text

            parsed = _parse_pubmed_xml(xml_text)
            for pmid, data in parsed.items():
                results[pmid] = data
                _write_cache(pmid, data)

        except Exception:
            logger.warning("Failed to fetch batch starting at index %d", i)

        # Rate limit: 10 requests/sec with API key, 3/sec without
        time.sleep(0.1 if api_key else 0.34)

    return results


def _parse_pubmed_xml(xml_text: str) -> dict[str, dict]:
    """Parse PubMed XML response into structured data."""
    import xml.etree.ElementTree as ET

    results = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return results

    for article in root.findall(".//PubmedArticle"):
        pmid_el = article.find(".//PMID")
        if pmid_el is None:
            continue
        pmid = pmid_el.text

        title_el = article.find(".//ArticleTitle")
        title = title_el.text if title_el is not None and title_el.text else ""

        abstract_parts = []
        for abs_el in article.findall(".//AbstractText"):
            label = abs_el.get("Label", "")
            text = abs_el.text or ""
            if label:
                abstract_parts.append(f"{label}: {text}")
            else:
                abstract_parts.append(text)
        abstract = " ".join(abstract_parts)

        # Authors
        authors = []
        for author in article.findall(".//Author"):
            last = author.find("LastName")
            first = author.find("ForeName")
            if last is not None and last.text:
                name = last.text
                if first is not None and first.text:
                    name += f" {first.text}"
                authors.append(name)

        # Journal and year
        journal_el = article.find(".//Journal/Title")
        journal = journal_el.text if journal_el is not None else ""
        year_el = article.find(".//PubDate/Year")
        year = year_el.text if year_el is not None else ""

        results[pmid] = {
            "title": title,
            "abstract": abstract,
            "authors": authors[:5],  # First 5 authors
            "journal": journal,
            "year": year,
        }

    return results


def build_search_query(disease_name: str, disease_synonyms: list[str] | None = None) -> str:
    """Build a PubMed search query for drug-disease pairs.

    Args:
        disease_name: Primary disease name.
        disease_synonyms: Optional synonyms.

    Returns:
        PubMed search query string.
    """
    # Build disease part with OR'ed synonyms
    disease_terms = [f'"{disease_name}"']
    if disease_synonyms:
        for syn in disease_synonyms[:3]:  # Limit to avoid overly long queries
            if syn and len(syn) > 3:
                disease_terms.append(f'"{syn}"')

    disease_part = " OR ".join(disease_terms)

    # Drug treatment terms
    drug_terms = (
        "(drug therapy[MeSH] OR drug repurposing OR therapeutic OR treatment OR "
        "clinical trial OR off-label OR pharmacotherapy)"
    )

    return f"({disease_part}) AND {drug_terms}"


def _cache_path(pmid: str) -> Path:
    """Get the cache file path for a PMID."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"PMID_{pmid}.json"


def _read_cache(pmid: str) -> dict | None:
    """Read a cached abstract."""
    path = _cache_path(pmid)
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _write_cache(pmid: str, data: dict) -> None:
    """Write an abstract to cache."""
    path = _cache_path(pmid)
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception:
        pass
