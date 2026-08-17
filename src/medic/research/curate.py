"""Research curation pipeline for drug-disease pair discovery.

Searches for drug-disease associations using either deep research providers
(Edison/Perplexity) when API keys are available, or PubMed as a fallback.

Usage:
    python -m medic.research.curate --disease MONDO:0007037
    python -m medic.research.curate --disease next
"""

import json
import logging
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

import yaml

from medic.research.pubmed import (
    build_search_query,
    fetch_abstracts,
    search_pubmed,
)

logger = logging.getLogger(__name__)

#: The dated priority-disease snapshot the research axis works through, Mondo-derived and
#: ours to ship. It lived under `background/` — the directory for manually-provided,
#: non-redistributable source archives, which `.gitignore` excludes wholesale — so it was the
#: one input `build-research` needed that a fresh clone could not have.
PRIORITY_DISEASES_PATH = Path("data/priority-diseases-2026-03-11.tsv")
PROGRESS_PATH = Path("cache/research/progress.yaml")
KB_RESEARCH_DIR = Path("kb/research")
DISEASE_CACHE_DIR = Path("cache/research")
RESEARCH_DIR = Path("research")
TEMPLATES_DIR = Path("templates")
TEMPLATE_FILE = TEMPLATES_DIR / "drug_disease_research.md"

# Regex patterns for extracting references from deep research output
PMID_RE = re.compile(r"\bPMID\s*[:#]?\s*(\d{4,9})\b", re.IGNORECASE)
PUBMED_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:pubmed\.ncbi\.nlm\.nih\.gov|pmc\.ncbi\.nlm\.nih\.gov/articles/PMC)(\d{4,9})(?:/|\b)"
)
DOI_RE = re.compile(r"https?://(?:dx\.)?doi\.org/([^\s\],)]+)")

# Deep research provider configuration
PROVIDER_ENV_KEYS = {
    "falcon": "EDISON_API_KEY",
    "perplexity": "PERPLEXITY_API_KEY",
}


def _load_priority_diseases() -> list[dict]:
    """Load the priority disease list.

    Fails loud when the file is missing (SPEC §9, "no silent legacy fallbacks anywhere").
    It used to warn and return an empty list, which made "the snapshot is not checked out"
    indistinguishable from "there is no priority work to do" — the curation run would report
    zero diseases and exit 0.
    """
    if not PRIORITY_DISEASES_PATH.exists():
        raise FileNotFoundError(
            f"Priority disease snapshot not found at {PRIORITY_DISEASES_PATH}. It is tracked "
            f"in the repo; if it is missing, the checkout is incomplete."
        )

    import pandas as pd

    df = pd.read_csv(PRIORITY_DISEASES_PATH, sep="\t")
    diseases = []
    for _, row in df.iterrows():
        diseases.append({
            "mondo_id": str(row.get("mondo id", row.get("mondo_id", ""))),
            "label": str(row.get("mondo label", row.get("mondo_label", ""))),
        })
    return diseases


def _load_progress() -> set[str]:
    """Load the set of already-curated disease IDs."""
    if not PROGRESS_PATH.exists():
        return set()
    try:
        with open(PROGRESS_PATH) as f:
            data = yaml.safe_load(f)
        return set(data.get("curated_diseases", []))
    except Exception:
        return set()


def _save_progress(curated: set[str]) -> None:
    """Save the progress tracker."""
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_PATH, "w") as f:
        yaml.dump(
            {"curated_diseases": sorted(curated), "last_updated": datetime.now().isoformat()},
            f,
        )


def _get_next_disease(curated: set[str]) -> dict | None:
    """Get the next uncurated disease from the priority list."""
    diseases = _load_priority_diseases()
    for d in diseases:
        if d["mondo_id"] not in curated:
            return d
    return None


# ---------------------------------------------------------------------------
# Deep research providers
# ---------------------------------------------------------------------------


def _get_available_providers() -> list[str]:
    """Return list of deep research providers with API keys set."""
    available = []
    for provider, env_key in PROVIDER_ENV_KEYS.items():
        key = os.environ.get(env_key, "")
        if not key:
            # Check .env file
            env_path = Path(".env")
            if env_path.exists():
                for line in env_path.read_text().splitlines():
                    if line.startswith(f"{env_key}="):
                        key = line.split("=", 1)[1].strip()
                        break
        if key:
            available.append(provider)
    return available


def _run_deep_research(
    disease_label: str,
    mondo_id: str,
    provider: str,
) -> Path | None:
    """Run deep-research-client for a single disease and provider.

    Returns path to the output markdown file, or None on failure.
    """
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = disease_label.replace(" ", "_").replace(",", "_").replace("/", "-")
    output_file = RESEARCH_DIR / f"{safe_name}-deep-research-{provider}.md"

    # Skip if already exists with content
    if output_file.exists() and output_file.stat().st_size > 500:
        logger.info("Using existing deep research for %s (%s)", disease_label, provider)
        return output_file

    if not TEMPLATE_FILE.exists():
        logger.warning("Research template not found: %s", TEMPLATE_FILE)
        return None

    provider_arg = ["--use-cborg"] if provider == "cborg" else ["--provider", provider]

    cmd = [
        "uv", "run", "--group", "research", "--python", "3.12",
        "deep-research-client", "research",
        "--template", str(TEMPLATE_FILE),
        "--var", f"disease_name={disease_label}",
        "--var", f"mondo_id={mondo_id}",
        *provider_arg,
        "--output", str(output_file),
        "--separate-citations", f"{output_file}.citations.md",
    ]

    logger.info("Running deep research: %s (%s)", disease_label, provider)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0 and output_file.exists() and output_file.stat().st_size > 500:
            logger.info("Deep research complete: %s -> %s", disease_label, output_file)
            return output_file
        else:
            logger.warning(
                "Deep research failed for %s (%s): exit %d, stderr: %s",
                disease_label, provider, result.returncode, result.stderr[:200],
            )
            return None
    except subprocess.TimeoutExpired:
        logger.warning("Deep research timed out for %s (%s)", disease_label, provider)
        return None


def _extract_references_from_markdown(md_path: Path) -> list[str]:
    """Extract PMID and DOI references from a deep research markdown file."""
    text = md_path.read_text()
    refs = set()

    # Extract PMIDs
    for m in PMID_RE.finditer(text):
        refs.add(f"PMID:{m.group(1)}")

    # Extract PMIDs from PubMed URLs
    for m in PUBMED_URL_RE.finditer(text):
        refs.add(f"PMID:{m.group(1)}")

    # Extract DOIs
    for m in DOI_RE.finditer(text):
        doi = m.group(1).rstrip(".,;)")
        refs.add(f"DOI:{doi}")

    return sorted(refs)


def _extract_drug_mentions(text: str) -> list[str]:
    """Extract potential drug names from text using heuristics.

    Looks for common drug name patterns: capitalized words near treatment
    keywords, words ending in common drug suffixes.
    """
    drug_suffixes = [
        "mab", "nib", "lib", "zib", "mib",  # biologics/kinase inhibitors
        "olol", "pril", "artan",  # cardiovascular
        "azole", "mycin", "cillin", "cycline",  # antimicrobials
        "amine", "pine", "done", "pam", "lam",  # CNS
        "tide", "tase", "stat", "vir",  # various
        "umab", "izumab", "ximab",  # monoclonal antibodies
    ]

    candidates = set()
    words = re.findall(r"\b[A-Z][a-z]{3,}\b", text)
    for word in words:
        lower = word.lower()
        for suffix in drug_suffixes:
            if lower.endswith(suffix):
                candidates.add(word)
                break

    # Also look for words in drug-treatment context
    treatment_pattern = re.compile(
        r"(?:treated with|therapy with|administered|receiving|given)\s+([A-Z][a-z]+(?:\s+[a-z]+)?)",
        re.IGNORECASE,
    )
    for match in treatment_pattern.finditer(text):
        candidates.add(match.group(1).strip())

    # Bold drug names (common in deep research output): **drugname**
    bold_pattern = re.compile(r"\*\*([A-Z][a-z]{3,}(?:\s+[a-z]+)?)\*\*")
    for match in bold_pattern.finditer(text):
        word = match.group(1)
        lower = word.lower()
        for suffix in drug_suffixes:
            if lower.endswith(suffix):
                candidates.add(word)
                break

    return sorted(candidates)


def _find_snippet_for_drug(text: str, drug_name: str) -> str:
    """Find the most relevant sentence mentioning a drug in the text."""
    for sentence in re.split(r"(?<=[.!?])\s+", text):
        if drug_name.lower() in sentence.lower():
            # Clean markdown formatting
            clean = re.sub(r"\*\*([^*]+)\*\*", r"\1", sentence)
            clean = re.sub(r"\[(\d+)\]", "", clean).strip()
            if len(clean) > 20:
                return clean[:500]
    return ""


def _parse_deep_research(
    md_path: Path,
    disease_id: str,
    disease_label: str,
    provider: str,
) -> list[dict]:
    """Parse a deep research markdown file into ResearchAssociation dicts."""
    text = md_path.read_text()

    # Get the output section (after "## Output")
    output_match = re.search(r"^## Output\s*$", text, re.MULTILINE)
    output_text = text[output_match.end():] if output_match else text

    # Extract all references from the full file
    references = _extract_references_from_markdown(md_path)

    # Extract drug mentions from the output section
    drug_names = _extract_drug_mentions(output_text)

    associations = []
    for drug_name in drug_names:
        snippet = _find_snippet_for_drug(output_text, drug_name)

        # Build evidence: one item per reference found near the drug mention,
        # or a single item from the deep research report if no specific refs
        evidence_items = []
        for ref in references:
            evidence_items.append({
                "source_type": "LITERATURE",
                "reference": ref,
                "support": "PARTIAL",
                "confidence": "MEDIUM",
                "evidence_source": "HUMAN_CLINICAL",
            })

        # If no specific references, create a single evidence item from the report
        if not evidence_items:
            evidence_items.append({
                "source_type": "LITERATURE",
                "explanation": f"Deep research report ({provider})",
                "support": "PARTIAL",
                "confidence": "MEDIUM",
            })

        # Add snippet to first evidence item
        if snippet and evidence_items:
            evidence_items[0]["snippet"] = snippet

        association = {
            "drug_id": "",  # To be grounded later
            "drug_label": drug_name,
            "disease_id": disease_id,
            "disease_label": disease_label,
            "curation_status": "DRAFT",
            "curation_date": datetime.now().isoformat(),
            "curator": f"deep-research-{provider}",
            "search_query": f"deep-research-client --provider {provider}",
            "evidence": evidence_items,
            "deep_research_used": True,
        }
        associations.append(association)

    return associations


def curate_disease_deep_research(
    disease_id: str,
    disease_label: str,
) -> list[dict]:
    """Curate drug-disease associations using deep research providers.

    Runs all available deep research providers and merges the results.
    """
    providers = _get_available_providers()
    if not providers:
        return []

    all_associations = []
    seen_drugs: set[str] = set()

    for provider in providers:
        md_path = _run_deep_research(disease_label, disease_id, provider)
        if md_path is None:
            continue

        associations = _parse_deep_research(md_path, disease_id, disease_label, provider)
        # Deduplicate across providers
        for assoc in associations:
            drug_key = assoc["drug_label"].lower()
            if drug_key not in seen_drugs:
                seen_drugs.add(drug_key)
                all_associations.append(assoc)
            else:
                # Merge evidence from additional provider into existing association
                for existing in all_associations:
                    if existing["drug_label"].lower() == drug_key:
                        existing["evidence"].extend(assoc["evidence"])
                        existing["curator"] += f",deep-research-{provider}"
                        break

    logger.info(
        "Deep research found %d drug associations for %s across %d providers",
        len(all_associations), disease_label, len(providers),
    )
    return all_associations


# ---------------------------------------------------------------------------
# PubMed fallback
# ---------------------------------------------------------------------------


def curate_disease_pubmed(
    disease_id: str,
    disease_label: str = "",
    max_pubmed_results: int = 20,
) -> list[dict]:
    """Curate drug-disease associations using PubMed search (fallback)."""
    # Check cache
    cache_path = DISEASE_CACHE_DIR / f"{disease_id.replace(':', '_')}.json"
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            if cached.get("associations"):
                logger.info("Using cached PubMed results for %s", disease_id)
                return cached["associations"]
        except Exception:
            pass

    logger.info("PubMed curating %s (%s)", disease_id, disease_label)

    query = build_search_query(disease_label)
    pmids = search_pubmed(query, max_results=max_pubmed_results)

    if not pmids:
        logger.info("No PubMed results for %s", disease_label)
        _cache_results(cache_path, disease_id, disease_label, query, [])
        return []

    abstracts = fetch_abstracts(pmids)

    associations = []
    seen_drugs: set[str] = set()

    for pmid, article in abstracts.items():
        abstract_text = article.get("abstract", "")
        if not abstract_text:
            continue

        drugs = _extract_drug_mentions(abstract_text)
        for drug_name in drugs:
            if drug_name.lower() in seen_drugs:
                continue
            seen_drugs.add(drug_name.lower())

            snippet = ""
            for sentence in re.split(r"[.!?]+", abstract_text):
                if drug_name.lower() in sentence.lower():
                    snippet = sentence.strip()
                    break

            association = {
                "drug_id": "",
                "drug_label": drug_name,
                "disease_id": disease_id,
                "disease_label": disease_label,
                "curation_status": "DRAFT",
                "curation_date": datetime.now().isoformat(),
                "curator": "medic-research-pipeline",
                "search_query": query,
                "evidence": [
                    {
                        "source_type": "LITERATURE",
                        "reference": f"PMID:{pmid}",
                        "reference_title": article.get("title", ""),
                        "snippet": snippet[:500] if snippet else "",
                        "support": "PARTIAL",
                        "confidence": "LOW",
                        "evidence_source": "HUMAN_CLINICAL",
                    }
                ],
                "deep_research_used": False,
            }
            associations.append(association)

    logger.info("PubMed found %d candidate drug associations for %s", len(associations), disease_label)
    _cache_results(cache_path, disease_id, disease_label, query, associations)
    return associations


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------


def curate_disease(
    disease_id: str,
    disease_label: str = "",
    max_pubmed_results: int = 20,
) -> list[dict]:
    """Curate drug-disease associations for a single disease.

    Uses deep research providers if any API keys are available,
    otherwise falls back to PubMed search.
    """
    providers = _get_available_providers()
    if providers:
        logger.info("Deep research providers available: %s", ", ".join(providers))
        return curate_disease_deep_research(disease_id, disease_label)
    else:
        logger.info("No deep research API keys found, falling back to PubMed")
        return curate_disease_pubmed(disease_id, disease_label, max_pubmed_results)


def _cache_results(
    cache_path: Path,
    disease_id: str,
    disease_label: str,
    query: str,
    associations: list[dict],
) -> None:
    """Cache curated results for a disease."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(
            {
                "disease_id": disease_id,
                "disease_label": disease_label,
                "query": query,
                "associations": associations,
                "timestamp": datetime.now().isoformat(),
            },
            f,
            indent=2,
        )


def write_research_yaml(disease_id: str, associations: list[dict]) -> Path | None:
    """Write research associations to a YAML file in kb/research/."""
    if not associations:
        return None

    KB_RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{disease_id.replace(':', '_')}.yaml"
    output_path = KB_RESEARCH_DIR / filename

    content = yaml.dump(
        {"associations": associations},
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    )
    content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
    with open(output_path, "w") as f:
        f.write(content)

    logger.info("Wrote %d associations to %s", len(associations), output_path)
    return output_path


def run_curate(disease: str = "next", max_results: int = 20) -> None:
    """Run curation for a single disease.

    Args:
        disease: Mondo ID or 'next' for the next uncurated disease.
        max_results: Max PubMed results per disease.
    """
    curated = _load_progress()

    if disease == "next":
        d = _get_next_disease(curated)
        if d is None:
            logger.info("All priority diseases have been curated!")
            return
        disease_id = d["mondo_id"]
        disease_label = d["label"]
    else:
        disease_id = disease
        disease_label = disease  # Will be improved with grounding

    associations = curate_disease(
        disease_id, disease_label, max_pubmed_results=max_results
    )

    if associations:
        write_research_yaml(disease_id, associations)

    curated.add(disease_id)
    _save_progress(curated)


def main():
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="MeDIC research curation")
    parser.add_argument(
        "--disease",
        default="next",
        help="Mondo ID to curate, or 'next' for the next uncurated disease",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=20,
        help="Maximum PubMed search results per disease",
    )
    args = parser.parse_args()

    run_curate(disease=args.disease, max_results=args.max_results)


if __name__ == "__main__":
    main()
