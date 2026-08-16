"""Back-fill snippet, reference_title, and original_* fields on existing
research evidence rows.

For each kb/research/MONDO_*.yaml association:
- Use `explanation` as `snippet` when no snippet is set (best available proxy
  until the curation skill is re-run with snippet capture enabled).
- Look up `reference_title` for PMID/PMC references via NCBI E-utilities
  (cached to cache/enrichment/pmid_titles.json).
- Set `original_drug_label` and `original_disease_label` from the parent
  association's `drug_label` / `disease_label` so the audit fields are
  consistent with regulatory evidence.

Idempotent: only fills missing fields; does not overwrite existing values.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Optional

import httpx
import yaml

from medic.enrichment.cache import EnrichmentCache

logger = logging.getLogger(__name__)

KB_DIR = Path("kb/research")
TITLE_CACHE = Path("cache/enrichment/pmid_titles.json")

ESUMMARY_URL = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
)

_cache: EnrichmentCache | None = None


def _get_cache() -> EnrichmentCache:
    global _cache
    if _cache is None:
        _cache = EnrichmentCache(TITLE_CACHE)
    return _cache


def _fetch_title(pmid: str) -> str:
    """Look up a paper title by PMID via NCBI E-utilities."""
    cache = _get_cache()
    cached = cache.get(pmid)
    if cached is not None:
        return cached.get("title", "")
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get(
                ESUMMARY_URL,
                params={"db": "pubmed", "id": pmid, "retmode": "json"},
            )
            resp.raise_for_status()
            payload = resp.json()
        title = ""
        result = payload.get("result", {})
        entry = result.get(pmid) if isinstance(result, dict) else None
        if isinstance(entry, dict):
            title = (entry.get("title") or "").strip()
    except Exception as e:
        logger.debug("PMID %s lookup failed: %s", pmid, e)
        title = ""
    cache.put(pmid, {"title": title})
    cache.flush()
    # Be polite to NCBI
    time.sleep(0.34)
    return title


def _normalize_pmid(reference: str) -> Optional[str]:
    if not reference:
        return None
    m = re.match(r"PMID:(\d+)", reference, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def enrich_file(path: Path) -> int:
    """Enrich a single MONDO_*.yaml; returns number of rows touched."""
    data = yaml.safe_load(open(path)) or {}
    associations = data.get("associations", [])
    if not isinstance(associations, list):
        return 0
    touched = 0
    for assoc in associations:
        drug_label = assoc.get("drug_label", "")
        disease_label = assoc.get("disease_label", "")
        for ev in assoc.get("evidence", []) or []:
            changed = False
            if not ev.get("snippet"):
                explanation = (ev.get("explanation") or "").strip()
                if explanation:
                    ev["snippet"] = explanation
                    changed = True
            if not ev.get("original_drug_label") and drug_label:
                ev["original_drug_label"] = drug_label
                changed = True
            if not ev.get("original_disease_label") and disease_label:
                ev["original_disease_label"] = disease_label
                changed = True
            if not ev.get("reference_title"):
                pmid = _normalize_pmid(ev.get("reference", ""))
                if pmid:
                    title = _fetch_title(pmid)
                    if title:
                        ev["reference_title"] = title
                        changed = True
            if changed:
                touched += 1

    out = yaml.dump(data, default_flow_style=False, allow_unicode=True, width=1000, sort_keys=False)
    out = "".join(c for c in out if c == "\n" or c == "\t" or ord(c) >= 32)
    path.with_suffix(".yaml.tmp").write_text(out)
    path.with_suffix(".yaml.tmp").replace(path)
    return touched


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    total = 0
    files = sorted(KB_DIR.glob("MONDO_*.yaml"))
    for f in files:
        n = enrich_file(f)
        if n:
            logger.info("%s: %d evidence rows enriched", f.name, n)
        total += n
    logger.info("Total: %d evidence rows enriched across %d files", total, len(files))


if __name__ == "__main__":
    main()
