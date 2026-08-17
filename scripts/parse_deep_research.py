#!/usr/bin/env python3
"""Parse deep research markdown into paper-level drug-disease associations.

Extracts individual drug sections from deep research reports, resolves
numbered citations to URLs/PMIDs, and produces ResearchAssociation entries
with paper-level evidence: one evidence item per cited paper per drug,
each with reference, reference_title, snippet, and explanation.

Usage:
    python scripts/parse_deep_research.py research/facioscapulohumeral_muscular_dystrophy-deep-research-perplexity.md
"""

import json
import re
from datetime import datetime
from pathlib import Path

import yaml

# Regex for extracting citation numbers from text like [5][9][22]
CITE_NUM_RE = re.compile(r"\[(\d{1,3})\]")

# Regex for PMID extraction from URLs
PMC_RE = re.compile(r"pmc\.ncbi\.nlm\.nih\.gov/articles/PMC(\d+)")
PUBMED_RE = re.compile(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d+)")
NCT_RE = re.compile(r"clinicaltrials\.gov/study/(NCT\d+)")
DOI_RE = re.compile(r"doi\.org/([^\s,)]+)")

import httpx  # noqa: E402 - imported after sys.path setup above

REFERENCES_CACHE_DIR = Path("references_cache")
TITLE_CACHE_DIR = Path("cache/titles")

# Headers that are section categories, NOT drug names
CATEGORY_PATTERNS = [
    "current status", "absence of", "limited efficacy", "previously investigated",
    "failed therapies", "comprehensive research", "output", "question",
    "understanding", "research objectives", "required information",
    "citation requirements", "output format", "target disease",
    "approved therapies", "approved drug therapies", "investigational drugs",
    "investigational and pipeline",
    "contraindicated medications", "contraindicated", "combination therapies",
    "adverse events", "drug repurposing", "symptomatic management",
    "symptomatic pharmacological", "pharmacological therapies",
    "current and emerging", "monitoring", "biomarker", "clinical trial infrastructure",
    "outcome measure", "future directions", "global", "exercise",
    "physical rehabilitation", "pain management", "gene therapy approaches",
    "antisense oligonucleotide advantages", "therapeutic mechanisms",
    "dux4-targeting", "anti-myostatin", "small molecule dux4",
    "inflammatory modulation", "combination hormone", "steroidal",
    "non-steroidal", "historical", "overview", "introduction",
    "the biochemical", "pathophysiology", "fda approval",
    "research initiatives", "key concepts", "clinical trial research network",
    "diversity of", "international research", "collaboration",
    "trial approaches", "trial landscape", "regulatory",
    "summary", "conclusion", "references", "limitations",
    "standard of care", "current standard", "novel therapies",
    "emerging therapies", "treatment options", "treatment strategies",
    "therapeutic strategies", "therapeutic options", "therapeutic landscape",
    "management strategies", "management options", "management of",
    "off-label", "receptor antagonist", "partial inhibition",
    "flushing prevention", "controlled drinking",
]


def load_citations(citations_path: Path) -> dict[int, str]:
    """Load the numbered citation list from the citations companion file."""
    citations = {}
    if not citations_path.exists():
        return citations
    text = citations_path.read_text()
    for m in re.finditer(r"^(\d+)\.\s+(.+)$", text, re.MULTILINE):
        num = int(m.group(1))
        url = m.group(2).strip().rstrip(",")
        citations[num] = url
    return citations


_pmc_to_pmid_cache: dict[str, str] = {}


def _resolve_pmc_to_pmid(pmc_numeric: str) -> str:
    """Convert a PMC numeric ID to a PMID via NCBI elink API."""
    if pmc_numeric in _pmc_to_pmid_cache:
        return _pmc_to_pmid_cache[pmc_numeric]

    # Check title cache for a previously resolved mapping
    cache_file = TITLE_CACHE_DIR / f"pmc2pmid_PMC{pmc_numeric}.txt"
    if cache_file.exists():
        pmid = cache_file.read_text().strip()
        _pmc_to_pmid_cache[pmc_numeric] = pmid
        return pmid

    api_key = _get_ncbi_api_key()
    params = {"dbfrom": "pmc", "db": "pubmed", "id": pmc_numeric, "retmode": "json"}
    if api_key:
        params["api_key"] = api_key
    try:
        resp = httpx.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi",
            params=params, timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        linksets = data.get("linksets", [{}])
        for ls in linksets[0].get("linksetdbs", []):
            if ls.get("dbto") == "pubmed":
                pmid = ls.get("links", [""])[0]
                if pmid:
                    _pmc_to_pmid_cache[pmc_numeric] = pmid
                    TITLE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                    cache_file.write_text(pmid)
                    return pmid
    except Exception:
        pass

    _pmc_to_pmid_cache[pmc_numeric] = ""
    return ""


def url_to_reference(url: str) -> tuple[str, str]:
    """Convert a URL to (reference_id, source_type).

    PMC articles are resolved to their corresponding PMIDs so the
    reference validator can check snippets against PubMed abstracts.
    """
    m = PUBMED_RE.search(url)
    if m:
        return f"PMID:{m.group(1)}", "LITERATURE"
    m = PMC_RE.search(url)
    if m:
        pmc_numeric = m.group(1)
        pmid = _resolve_pmc_to_pmid(pmc_numeric)
        if pmid:
            return f"PMID:{pmid}", "LITERATURE"
        return f"PMC:PMC{pmc_numeric}", "LITERATURE"
    m = NCT_RE.search(url)
    if m:
        return m.group(1), "DATABASE"
    m = DOI_RE.search(url)
    if m:
        return f"DOI:{m.group(1).rstrip('.,;)')}", "LITERATURE"
    return url, "DATABASE"


def _get_ncbi_api_key() -> str:
    """Get NCBI API key from environment or .env file."""
    import os
    key = os.environ.get("NCBI_API_KEY", "")
    if not key:
        env_path = Path(".env")
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("NCBI_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    return key


def _read_title_cache(ref_id: str) -> str | None:
    """Read a cached title. Returns None if not cached."""
    safe = ref_id.replace("/", "_").replace(":", "_").replace("?", "_")
    cache_file = TITLE_CACHE_DIR / f"{safe}.txt"
    if cache_file.exists():
        return cache_file.read_text().strip()
    # Also check references_cache for PMIDs
    if ref_id.startswith("PMID:"):
        pmid = ref_id.split(":", 1)[1]
        cache_file = REFERENCES_CACHE_DIR / f"PMID_{pmid}.json"
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text())
                return data.get("title", "")
            except Exception:
                pass
    return None


def _write_title_cache(ref_id: str, title: str) -> None:
    """Cache a resolved title."""
    TITLE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe = ref_id.replace("/", "_").replace(":", "_").replace("?", "_")
    cache_file = TITLE_CACHE_DIR / f"{safe}.txt"
    cache_file.write_text(title)


def _fetch_pubmed_title(pmid: str) -> str:
    """Fetch title from PubMed esummary API."""
    api_key = _get_ncbi_api_key()
    params = {"db": "pubmed", "id": pmid, "retmode": "json"}
    if api_key:
        params["api_key"] = api_key
    try:
        resp = httpx.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            params=params, timeout=15,
        )
        resp.raise_for_status()
        result = resp.json().get("result", {}).get(pmid, {})
        return result.get("title", "")
    except Exception:
        return ""


def _fetch_pmc_title(pmc_id: str) -> str:
    """Fetch title from PMC esummary API."""
    # pmc_id is like "PMC8326894" — strip PMC prefix for the API
    numeric = pmc_id.replace("PMC", "")
    api_key = _get_ncbi_api_key()
    params = {"db": "pmc", "id": numeric, "retmode": "json"}
    if api_key:
        params["api_key"] = api_key
    try:
        resp = httpx.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            params=params, timeout=15,
        )
        resp.raise_for_status()
        result = resp.json().get("result", {}).get(numeric, {})
        return result.get("title", "")
    except Exception:
        return ""


def _fetch_nct_title(nct_id: str) -> str:
    """Fetch brief title from ClinicalTrials.gov API v2."""
    try:
        resp = httpx.get(
            f"https://clinicaltrials.gov/api/v2/studies/{nct_id}",
            params={"fields": "NCTId,BriefTitle"},
            timeout=15,
        )
        resp.raise_for_status()
        return (
            resp.json()
            .get("protocolSection", {})
            .get("identificationModule", {})
            .get("briefTitle", "")
        )
    except Exception:
        return ""


def _fetch_webpage_title(url: str) -> str:
    """Fetch <title> from a webpage. Best-effort."""
    try:
        resp = httpx.get(url, timeout=15, follow_redirects=True)
        if resp.status_code != 200:
            return ""
        m = re.search(r"<title[^>]*>([^<]+)</title>", resp.text, re.IGNORECASE)
        if m:
            title = m.group(1).strip()
            # Clean common suffixes
            for suffix in [" | FSHD Society", " - PubMed", " - PMC", " | Muscular Dystrophy News"]:
                title = title.removesuffix(suffix)
            return title.strip()
    except Exception:
        pass
    return ""


def resolve_title(ref_id: str, url: str = "") -> str:
    """Resolve a document title for a reference. Uses cache, then fetches.

    Args:
        ref_id: Reference identifier (PMID:xxx, PMC:PMCxxx, NCTxxx, URL)
        url: Original URL (used for web scraping fallback)
    """
    # Check cache first
    cached = _read_title_cache(ref_id)
    if cached:
        return cached

    title = ""

    if ref_id.startswith("PMID:"):
        title = _fetch_pubmed_title(ref_id.split(":", 1)[1])
    elif ref_id.startswith("PMC:"):
        pmc_id = ref_id.split(":", 1)[1]
        title = _fetch_pmc_title(pmc_id)
    elif ref_id.startswith("NCT"):
        title = _fetch_nct_title(ref_id)
    elif ref_id.startswith("DOI:"):
        # Try resolving DOI via web scraping of doi.org redirect
        doi_url = f"https://doi.org/{ref_id.split(':', 1)[1]}"
        title = _fetch_webpage_title(doi_url)

    # Fallback: try the original URL if we still have no title
    if not title and url and not url.startswith(("PMID:", "PMC:", "NCT", "DOI:")):
        title = _fetch_webpage_title(url)

    # Cache the result (even empty, to avoid re-fetching)
    if title:
        _write_title_cache(ref_id, title)

    return title


def is_category_header(title: str) -> bool:
    """Check if a header is a category/topic heading, not a specific drug.

    Uses both an explicit pattern list and heuristic rules to distinguish
    therapeutic category headings from actual drug names.
    """
    lower = title.lower()

    # Explicit patterns
    for pat in CATEGORY_PATTERNS:
        if pat in lower:
            return True

    # Short generic words (e.g. "Background", "Methods")
    if len(lower.split()) <= 2 and not re.search(r"[A-Z]{2,}|\d", title):
        return True

    # Heuristic: headings that start with "Approved", "Investigational",
    # "Novel", "Emerging", "Additional", "Alternative" followed by plural
    # therapy/drug nouns are categories, not drug names.
    if re.match(
        r"^(approved|investigational|novel|emerging|additional|alternative|"
        r"adjunctive|adjuvant|first-line|second-line|third-line|"
        r"acute|chronic|conventional|established|experimental|"
        r"potential|promising|repurposed|off-label|other)\b",
        lower,
    ):
        return True

    # Headings ending with plural therapy/class nouns
    if re.search(
        r"\b(therapies|treatments|strategies|approaches|agents|drugs|"
        r"medications|options|considerations|complications|effects|"
        r"inhibitors|antagonists|agonists|modulators|blockers|"
        r"analgesics|antibiotics|anticonvulsants|antiepileptics|"
        r"antipsychotics|antidepressants|antihistamines|antimicrobials|"
        r"antioxidants|outcomes|requirements|surveillance|management|"
        r"directions|risks?|concerns?)$",
        lower,
    ):
        return True

    # Headers with colons that describe a topic: "Category: Description"
    # (but not "DrugCode: DrugName" which is short before the colon)
    if ":" in title:
        before_colon = title.split(":")[0].strip()
        if len(before_colon.split()) >= 3:
            return True

    return False


def extract_drug_name(header: str) -> str:
    """Extract the primary drug name from a header line.

    Tries multiple patterns to pull a concrete drug name out of what may
    be a descriptive heading.  Returns the cleaned name, or the original
    header stripped of citations if no pattern matches.
    """
    header = re.sub(r"\[\d+\]", "", header).strip()

    # "Code (GenericName) from Company" -> "GenericName (Code)"
    m = re.match(r"^([\w][\w-]*)\s+\(([^)]+)\)\s+from\s+", header)
    if m:
        code = m.group(1).strip()
        generic = m.group(2).strip()
        if not any(w in generic.lower() for w in ["inhibitor", "acting", "locally", "receptor"]):
            return f"{generic} ({code})"
        return code

    # "Drug (description)" -> Drug
    m = re.match(r"^([\w][\w\s-]+?)\s*\(", header)
    if m:
        return m.group(1).strip()

    # "Combined X and Y" -> "X and Y"
    if header.lower().startswith("combined "):
        return header[9:].strip()

    # "Drug from Company" -> Drug
    m = re.match(r"^([\w][\w\s-]+?)\s+from\s+", header)
    if m:
        return m.group(1).strip()

    # "DrugName: Descriptive Subtitle" -> DrugName (if short before colon)
    if ":" in header:
        before = header.split(":")[0].strip()
        if len(before.split()) <= 2 and re.search(r"[A-Z]", before):
            return before

    return header.strip()


def _split_sentences_with_citations(section_text: str) -> list[tuple[str, set[int]]]:
    """Split section text into (sentence, citation_numbers) pairs.

    Preserves which citation numbers appear in each sentence.
    """
    results = []
    # Split on sentence boundaries, keeping citation markers attached
    raw_sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z\[])", section_text)

    for sent in raw_sentences:
        sent = sent.strip()
        if not sent:
            continue
        # Extract citation numbers from this sentence
        nums = set()
        for m in CITE_NUM_RE.finditer(sent):
            nums.add(int(m.group(1)))
        # Clean the sentence for display
        clean = re.sub(r"\[\d+\]", "", sent).strip()
        clean = re.sub(r"\*\*([^*]+)\*\*", r"\1", clean)
        if clean and len(clean) > 20:
            results.append((clean, nums))

    return results


def extract_drug_sections(output_text: str) -> list[dict]:
    """Extract drug sections with per-citation sentence mapping."""
    drugs = []
    header_pattern = re.compile(r"^(#{2,4})\s+(.+)$", re.MULTILINE)
    headers = list(header_pattern.finditer(output_text))

    for i, match in enumerate(headers):
        level = len(match.group(1))
        title = match.group(2).strip()

        if is_category_header(title):
            continue

        # Get section text
        start = match.end()
        end = len(output_text)
        for j in range(i + 1, len(headers)):
            next_level = len(headers[j].group(1))
            if next_level <= level:
                end = headers[j].start()
                break

        section_text = output_text[start:end].strip()
        if not section_text:
            continue

        drug_label = extract_drug_name(title)
        if not drug_label or len(drug_label) < 3:
            continue

        # Parse sentences with their citation numbers
        sentences = _split_sentences_with_citations(section_text)

        # Also capture citations from the header
        header_cites = set()
        for m in CITE_NUM_RE.finditer(title):
            header_cites.add(int(m.group(1)))

        # Build per-citation-number -> snippets mapping
        cite_to_snippets: dict[int, list[str]] = {}
        for sent, nums in sentences:
            for n in nums:
                cite_to_snippets.setdefault(n, []).append(sent)
        for n in header_cites:
            cite_to_snippets.setdefault(n, [])

        # Get overall section summary (first substantive sentence)
        section_summary = ""
        first_word = drug_label.lower().split()[0]
        for sent, _ in sentences:
            if len(sent) > 50 and first_word in sent.lower():
                section_summary = sent[:500]
                break
        if not section_summary:
            for sent, _ in sentences:
                if len(sent) > 50:
                    section_summary = sent[:500]
                    break

        drugs.append({
            "drug_label": drug_label,
            "section_title": title,
            "section_summary": section_summary,
            "cite_to_snippets": cite_to_snippets,
        })

    return drugs


def build_associations(
    drug_sections: list[dict],
    citations: dict[int, str],
    disease_id: str,
    disease_label: str,
    provider: str,
    md_path: str = "",
) -> list[dict]:
    """Build ResearchAssociation dicts with paper-level evidence."""
    associations = []

    curator_agent = {
        "curator_type": "AI_AGENT",
        "name": f"Deep research via {provider} provider, parsed by parse_deep_research.py",
    }

    for drug in drug_sections:
        evidence_items = []

        for cite_num, snippets in drug["cite_to_snippets"].items():
            url = citations.get(cite_num, "")
            if not url:
                continue

            ref_id, source_type = url_to_reference(url)

            # Classify by reference type
            if ref_id.startswith(("PMID:", "PMC:", "DOI:")):
                src_type = "LITERATURE"
                conf_drug = "MEDIUM"
                conf_disease = "MEDIUM"
                conf_assoc = "LOW"
            elif ref_id.startswith("NCT"):
                src_type = "DATABASE"
                conf_drug = "MEDIUM"
                conf_disease = "MEDIUM"
                conf_assoc = "MEDIUM"
            else:
                src_type = "DATABASE"
                conf_drug = "LOW"
                conf_disease = "LOW"
                conf_assoc = "LOW"

            # Resolve document title (fetches from PubMed/PMC/NCT/web if not cached)
            doc_title = resolve_title(ref_id, url)

            # The sentences citing this reference are the LLM's interpretation,
            # NOT actual excerpts from the paper. Use as snippet until
            # curate_snippets.py replaces with a verified excerpt.
            interpreted = " ".join(snippets[:2])[:500] if snippets else ""

            ev = {
                "source": {
                    "name": "Perplexity deep research" if provider == "perplexity" else f"{provider.capitalize()} deep research",
                    "description": f"AI-generated literature review from {provider} provider via deep-research-client",
                    "type": src_type,
                    "file": md_path,
                    "url": url,
                },
                "reference": ref_id,
                "snippet": interpreted if interpreted else f"[Deep research citation — see {ref_id}]",
                "confidence_drug": conf_drug,
                "confidence_disease": conf_disease,
                "confidence_association": conf_assoc,
                "evidence_source": "HUMAN_CLINICAL",
                "curator": curator_agent,
            }
            if doc_title:
                ev["reference_title"] = doc_title

            evidence_items.append(ev)

        if not evidence_items:
            continue

        association = {
            "drug_id": "",
            "drug_label": drug["drug_label"],
            "disease_id": disease_id,
            "disease_label": disease_label,
            "curation_status": "DRAFT",
            "curation_date": datetime.now().isoformat(),
            "curator": f"deep-research-{provider}",
            "search_query": f"deep-research-client --provider {provider}",
            "evidence": evidence_items,
            "deep_research_used": True,
            "notes": drug["section_summary"],
        }
        associations.append(association)

    return associations


def parse_deep_research_file(
    md_path: Path,
    disease_id: str,
    disease_label: str,
) -> list[dict]:
    """Parse a deep research markdown file into structured associations."""
    text = md_path.read_text()

    provider = "unknown"
    m = re.search(r"^provider:\s*(\w+)", text, re.MULTILINE)
    if m:
        provider = m.group(1)

    output_match = re.search(r"^## Output\s*$", text, re.MULTILINE)
    output_text = text[output_match.end():] if output_match else text

    citations_path = Path(f"{md_path}.citations.md")
    citations = load_citations(citations_path)

    drug_sections = extract_drug_sections(output_text)

    return build_associations(
        drug_sections, citations, disease_id, disease_label, provider,
        md_path=str(md_path),
    )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("md_file", help="Deep research markdown file")
    parser.add_argument("--disease-id", default="MONDO:0001347")
    parser.add_argument("--disease-label", default="facioscapulohumeral muscular dystrophy")
    parser.add_argument("--write", action="store_true", help="Write to kb/research/")
    args = parser.parse_args()

    md_path = Path(args.md_file)
    associations = parse_deep_research_file(md_path, args.disease_id, args.disease_label)

    # Print summary
    print(f"Extracted {len(associations)} drug associations:\n")
    for a in associations:
        print(f"  {a['drug_label']}")
        for ev in a["evidence"]:
            title = ev.get("reference_title", "")
            ref = ev["reference"]
            snip = ev.get("snippet", "")[:80]
            cd = ev.get("confidence_drug", "?")
            ca = ev.get("confidence_association", "?")
            title_str = f' "{title}"' if title else ""
            print(f"    [drug:{cd} assoc:{ca}] {ref}{title_str}")
            if snip:
                print(f"      snippet: {snip}...")
        print()

    if args.write:
        kb_dir = Path("kb/research")
        kb_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{args.disease_id.replace(':', '_')}.yaml"
        output_path = kb_dir / filename
        content = yaml.dump(
            {"associations": associations},
            default_flow_style=False, allow_unicode=True, width=120,
        )
        content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
        output_path.write_text(content)
        print(f"Wrote to {output_path}")


if __name__ == "__main__":
    main()
