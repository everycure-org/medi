#!/usr/bin/env python3
"""Curate evidence snippets by extracting verified excerpts from PubMed abstracts.

For each PMID-referenced evidence item in a research YAML file, this script:
1. Fetches and caches the PubMed abstract (via linkml-reference-validator)
2. Uses the Anthropic API to identify the best supporting excerpt
3. Populates the `snippet` field with an exact quote from the abstract

Usage:
    python scripts/curate_snippets.py kb/research/MONDO_0001347.yaml
    python scripts/curate_snippets.py kb/research/MONDO_0001347.yaml --dry-run
"""

import argparse
import logging
import os
import re
import subprocess
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

REFERENCES_CACHE_DIR = Path("references_cache")


def _get_anthropic_api_key() -> str:
    """Get Anthropic API key from environment or .env file."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        env_path = Path(".env")
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    return key


def _cache_reference(pmid: str) -> bool:
    """Cache a PMID using linkml-reference-validator."""
    cache_path = REFERENCES_CACHE_DIR / f"PMID_{pmid}.md"
    if cache_path.exists():
        return True
    try:
        result = subprocess.run(
            ["uv", "run", "linkml-reference-validator", "cache", "reference", f"PMID:{pmid}"],
            capture_output=True, text=True, timeout=30,
        )
        return result.returncode == 0
    except Exception:
        return False


def _read_cached_abstract(pmid: str) -> str:
    """Read a cached PubMed abstract. Returns the content section."""
    cache_path = REFERENCES_CACHE_DIR / f"PMID_{pmid}.md"
    if not cache_path.exists():
        return ""
    text = cache_path.read_text()
    # Extract content after "## Content" header
    m = re.search(r"^## Content\s*$", text, re.MULTILINE)
    if m:
        return text[m.end():].strip()
    # Fallback: everything after the YAML frontmatter
    parts = text.split("---", 2)
    if len(parts) >= 3:
        return parts[2].strip()
    return text


def _read_cached_title(pmid: str) -> str:
    """Read the title from a cached reference."""
    cache_path = REFERENCES_CACHE_DIR / f"PMID_{pmid}.md"
    if not cache_path.exists():
        return ""
    text = cache_path.read_text()
    m = re.search(r"^title:\s*(.+)$", text, re.MULTILINE)
    return m.group(1).strip().strip('"') if m else ""


def _extract_snippet_with_llm(
    abstract: str,
    drug_label: str,
    disease_label: str,
    interpreted_text: str,
    api_key: str = "",
) -> str:
    """Use LLM to extract the best supporting excerpt from an abstract.

    Returns an exact substring of the abstract, or empty string if no match.
    """
    from medic.llm import llm_call

    prompt = f"""You are extracting evidence from a PubMed abstract. Given the claim below, find the EXACT excerpt from the abstract that best supports it.

IMPORTANT RULES:
- Return ONLY an exact substring from the abstract text — do not paraphrase, reword, or add anything
- The excerpt should be 1-3 sentences that directly support the claim
- If the abstract does not contain relevant supporting text, respond with exactly: NO_MATCH
- Do not include any explanation, just the exact quote

CLAIM: {interpreted_text}

DRUG: {drug_label}
DISEASE: {disease_label}

ABSTRACT:
{abstract}

EXACT EXCERPT:"""

    try:
        excerpt = llm_call(prompt, task="snippet_curation", max_tokens=500)

        if excerpt == "NO_MATCH" or not excerpt:
            return ""

        # Verify the excerpt is actually a substring of the abstract
        if excerpt in abstract:
            return excerpt

        # Try with minor whitespace normalization
        normalized_abstract = re.sub(r"\s+", " ", abstract)
        normalized_excerpt = re.sub(r"\s+", " ", excerpt)
        if normalized_excerpt in normalized_abstract:
            # The normalized excerpt is what we return; the original offset is not needed.
            return normalized_excerpt

        logger.warning("LLM returned excerpt not found in abstract, skipping")
        return ""

    except Exception as e:
        logger.warning("LLM snippet extraction failed: %s", e)
        return ""


def curate_snippets(yaml_path: Path, dry_run: bool = False) -> dict:
    """Curate snippets for all PMID evidence items in a research YAML file.

    Returns stats dict with counts.
    """
    api_key = _get_anthropic_api_key()
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return {"error": "no API key"}

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    if not data or "associations" not in data:
        return {"skipped": "no associations"}

    stats = {"total_evidence": 0, "pmid_refs": 0, "cached": 0, "snippets_added": 0, "no_match": 0, "already_has_snippet": 0}

    for assoc in data["associations"]:
        drug_label = assoc.get("drug_label", "")
        disease_label = assoc.get("disease_label", "")

        for ev in assoc.get("evidence", []):
            stats["total_evidence"] += 1
            ref = ev.get("reference", "")

            if not ref.startswith("PMID:"):
                continue
            stats["pmid_refs"] += 1

            # Skip if snippet already populated
            if ev.get("snippet"):
                stats["already_has_snippet"] += 1
                continue

            pmid = ref.split(":", 1)[1]
            interpreted = ev.get("explanation", "") or ev.get("interpretation", "") or ev.get("interpreted_text", "")
            if not interpreted:
                continue

            # Fetch and cache abstract
            if not _cache_reference(pmid):
                logger.warning("Failed to cache PMID:%s", pmid)
                continue
            stats["cached"] += 1

            abstract = _read_cached_abstract(pmid)
            if not abstract:
                logger.warning("Empty abstract for PMID:%s", pmid)
                continue

            # Also populate reference_title if missing
            if not ev.get("reference_title"):
                title = _read_cached_title(pmid)
                if title:
                    ev["reference_title"] = title

            if dry_run:
                logger.info("Would extract snippet for %s / PMID:%s", drug_label, pmid)
                continue

            # Extract snippet using LLM
            snippet = _extract_snippet_with_llm(
                abstract, drug_label, disease_label, interpreted, api_key,
            )

            if snippet:
                ev["snippet"] = snippet
                stats["snippets_added"] += 1
                logger.info("Added snippet for %s / PMID:%s: %s...", drug_label, pmid, snippet[:80])
            else:
                stats["no_match"] += 1
                logger.info("No matching snippet for %s / PMID:%s", drug_label, pmid)

    if not dry_run:
        content = yaml.dump(data, default_flow_style=False, allow_unicode=True, width=120)
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        yaml_path.write_text(content)
        logger.info("Updated %s", yaml_path)

    return stats


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Curate evidence snippets from PubMed abstracts")
    parser.add_argument("yaml_file", help="Path to kb/research/*.yaml file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    args = parser.parse_args()

    yaml_path = Path(args.yaml_file)
    stats = curate_snippets(yaml_path, dry_run=args.dry_run)

    print("\nResults:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
