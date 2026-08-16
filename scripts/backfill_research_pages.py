#!/usr/bin/env python
"""Backfill ``page_or_section`` on existing kb/research/MONDO_*.yaml evidence rows.

The medic-research-curation skill was updated to emit ``page_or_section`` for
each evidence row by scanning the matching ``*.citations.md`` file (and the
prose surrounding the citation in the deep-research markdown). The 35 yamls
already on disk were curated *before* that change. This script does a pure
regex backfill — no LLMs, no network — and only adds ``page_or_section`` when
a hint is unambiguously present in a citation entry that resolves to the
evidence row's ``reference``.

Usage:

    uv run python scripts/backfill_research_pages.py [--dry-run] [--mondo MONDO:0007947]

The script is idempotent: if ``page_or_section`` is already set on a row we
leave it alone. We never invent hints — when no match is found, the field stays
absent.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Iterable

from ruamel.yaml import YAML

REPO_ROOT = Path(__file__).resolve().parent.parent
KB_RESEARCH_DIR = REPO_ROOT / "kb" / "research"
RESEARCH_DIR = REPO_ROOT / "research"

# --- Hint regex patterns (precedence-ordered, case-insensitive) -------------
#
# Mirrors the patterns documented in
# .claude/skills/medic-research-curation/SKILL.md Step 3d.1. Order matters:
# the first match wins.

_HINT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("pages_range", re.compile(r"\bpages?\s+(\d+)\s*[-–]\s*(\d+)\b", re.IGNORECASE)),
    ("pages_single", re.compile(r"\bpages?\s+(\d+)\b", re.IGNORECASE)),
    ("p_dot", re.compile(r"\bpp?\.\s*(\d+)(?:\s*[-–]\s*(\d+))?\b", re.IGNORECASE)),
    ("section", re.compile(r"\bsection\s+(\d+(?:\.\d+)*)\b", re.IGNORECASE)),
    ("table", re.compile(r"\btable\s+(\d+[A-Za-z]?)\b", re.IGNORECASE)),
    ("figure", re.compile(r"\bfigure\s+(\d+[A-Za-z]?)\b", re.IGNORECASE)),
    ("box", re.compile(r"\bbox\s+(\d+[A-Za-z]?)\b", re.IGNORECASE)),
]


def extract_hint(text: str) -> str | None:
    """Return the first locator hint found in ``text`` (or None).

    Output form mirrors what the skill emits:
      - ``pages 6-10`` / ``page 47``
      - ``pp. 47-49`` / ``p. 47``
      - ``Section 3.2``
      - ``Table 4`` / ``Figure 1B`` / ``Box 2``
    """

    if not text:
        return None
    for kind, pat in _HINT_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        if kind == "pages_range":
            return f"pages {m.group(1)}-{m.group(2)}"
        if kind == "pages_single":
            return f"page {m.group(1)}"
        if kind == "p_dot":
            # preserve pp. vs p. exactly as written
            unit = m.group(0).lstrip().split(None, 1)[0]
            unit = "pp." if unit.lower().startswith("pp") else "p."
            if m.group(2):
                return f"{unit} {m.group(1)}-{m.group(2)}"
            return f"{unit} {m.group(1)}"
        if kind == "section":
            return f"Section {m.group(1)}"
        if kind == "table":
            return f"Table {m.group(1)}"
        if kind == "figure":
            return f"Figure {m.group(1)}"
        if kind == "box":
            return f"Box {m.group(1)}"
    return None


# --- Citation entry parsing -------------------------------------------------

_NUMBERED_LINE = re.compile(r"^\s*(\d+)\.\s+(.+?)\s*$")
_PMID_RE = re.compile(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d+)")
_PMC_RE = re.compile(r"(?:pmc\.ncbi\.nlm\.nih\.gov/articles/|/pmc/articles/)(PMC\d+)", re.IGNORECASE)
_NCT_RE = re.compile(r"\b(NCT\d{6,})\b")
_DOI_INLINE_RE = re.compile(r"\b10\.\d{3,9}/[\w._\-/():;]+", re.IGNORECASE)


@dataclass
class CitationEntry:
    number: int
    raw: str
    pmids: set[str] = field(default_factory=set)
    pmcs: set[str] = field(default_factory=set)
    ncts: set[str] = field(default_factory=set)
    dois: set[str] = field(default_factory=set)
    urls: set[str] = field(default_factory=set)


def _normalize_doi(doi: str) -> str:
    return doi.rstrip(",.;").lower()


def parse_citations_md(path: Path) -> list[CitationEntry]:
    """Parse a ``*.citations.md`` file into numbered entries.

    Falcon files use ``N. <key> pages X-Y``. Perplexity files use
    ``N. <url>``. Either way we walk top-to-bottom; if a numbered line is
    followed by indented continuation lines we glue them into ``raw`` so that
    page hints split across lines are still captured.
    """

    text = path.read_text(encoding="utf-8", errors="replace")
    entries: list[CitationEntry] = []
    current: CitationEntry | None = None
    for line in text.splitlines():
        m = _NUMBERED_LINE.match(line)
        if m:
            if current is not None:
                entries.append(current)
            current = CitationEntry(number=int(m.group(1)), raw=m.group(2))
            continue
        if current is not None and line.strip():
            # treat indented / continuation lines as part of the entry
            if line.startswith((" ", "\t")) or not _NUMBERED_LINE.match(line):
                # but stop if we hit a markdown heading or blank-then-heading
                if line.lstrip().startswith("#"):
                    entries.append(current)
                    current = None
                    continue
                current.raw = f"{current.raw} {line.strip()}"
    if current is not None:
        entries.append(current)

    for e in entries:
        for m in _PMID_RE.finditer(e.raw):
            e.pmids.add(m.group(1))
        for m in _PMC_RE.finditer(e.raw):
            e.pmcs.add(m.group(1).upper())
        for m in _NCT_RE.finditer(e.raw):
            e.ncts.add(m.group(1))
        for m in _DOI_INLINE_RE.finditer(e.raw):
            e.dois.add(_normalize_doi(m.group(0)))
        for tok in e.raw.split():
            if tok.startswith(("http://", "https://")):
                e.urls.add(tok.rstrip(",.;)>"))

    return entries


# --- Disease → citation file discovery --------------------------------------


def find_citation_files_for_mondo(mondo_id: str) -> list[Path]:
    """Find ``research/*.citations.md`` files whose front-matter mentions
    the given MONDO id (e.g. ``MONDO:0007947``)."""

    matches: list[Path] = []
    if not RESEARCH_DIR.is_dir():
        return matches
    needle = mondo_id
    for p in sorted(RESEARCH_DIR.glob("*.citations.md")):
        try:
            head = p.read_text(encoding="utf-8", errors="replace")[:4096]
        except OSError:
            continue
        if needle in head:
            matches.append(p)
    return matches


def find_prose_files_for_mondo(mondo_id: str) -> list[Path]:
    """Find ``research/*.md`` deep-research prose files for the MONDO id
    (excluding the .citations.md companions)."""

    matches: list[Path] = []
    if not RESEARCH_DIR.is_dir():
        return matches
    for p in sorted(RESEARCH_DIR.glob("*.md")):
        if p.name.endswith(".citations.md"):
            continue
        try:
            head = p.read_text(encoding="utf-8", errors="replace")[:4096]
        except OSError:
            continue
        if mondo_id in head:
            matches.append(p)
    return matches


# --- Reference normalization ------------------------------------------------


def reference_keys(reference: str) -> tuple[set[str], set[str], set[str], set[str], set[str]]:
    """Return (pmids, pmcs, ncts, dois, urls) extracted from an evidence
    row's ``reference`` field. The reference can be a CURIE (PMID:..,
    PMC:.., DOI:..), a bare NCT id, or a full URL."""

    pmids: set[str] = set()
    pmcs: set[str] = set()
    ncts: set[str] = set()
    dois: set[str] = set()
    urls: set[str] = set()

    if not reference:
        return pmids, pmcs, ncts, dois, urls

    ref = reference.strip()
    if ref.startswith("PMID:"):
        pmids.add(ref.split(":", 1)[1].strip())
    elif ref.startswith("PMC:"):
        pmc = ref.split(":", 1)[1].strip().upper()
        if not pmc.startswith("PMC"):
            pmc = f"PMC{pmc}"
        pmcs.add(pmc)
    elif ref.startswith(("DOI:", "doi:")):
        dois.add(_normalize_doi(ref.split(":", 1)[1]))
    elif ref.startswith("NCT"):
        ncts.add(ref.split()[0])
    elif ref.startswith(("http://", "https://")):
        urls.add(ref.rstrip(",.;)>"))
        for m in _PMID_RE.finditer(ref):
            pmids.add(m.group(1))
        for m in _PMC_RE.finditer(ref):
            pmcs.add(m.group(1).upper())
        for m in _NCT_RE.finditer(ref):
            ncts.add(m.group(1))
        for m in _DOI_INLINE_RE.finditer(ref):
            dois.add(_normalize_doi(m.group(0)))
    return pmids, pmcs, ncts, dois, urls


def match_entry(
    entries: list[CitationEntry],
    reference: str,
) -> CitationEntry | None:
    """Return the citation entry whose identifiers overlap with the
    reference's normalized identifiers, else None."""

    rp, rpmc, rnct, rdoi, rurl = reference_keys(reference)
    if not (rp or rpmc or rnct or rdoi or rurl):
        return None
    for e in entries:
        if rp and (rp & e.pmids):
            return e
        if rpmc and (rpmc & e.pmcs):
            return e
        if rnct and (rnct & e.ncts):
            return e
        if rdoi and (rdoi & e.dois):
            return e
        if rurl and (rurl & e.urls):
            return e
    return None


# --- Prose fallback ---------------------------------------------------------


def find_hint_in_prose(prose_text: str, title: str | None) -> str | None:
    """If ``title`` appears in the deep-research prose, scan the surrounding
    window for a hint. We look ±400 chars on either side of the title match.
    """

    if not title or not prose_text:
        return None
    needle = title.strip().rstrip(".").lower()
    if len(needle) < 12:
        return None
    haystack = prose_text.lower()
    idx = haystack.find(needle)
    if idx == -1:
        return None
    start = max(0, idx - 400)
    end = min(len(prose_text), idx + len(needle) + 400)
    return extract_hint(prose_text[start:end])


# --- YAML round-trip --------------------------------------------------------


def make_yaml() -> YAML:
    yaml = YAML(typ="rt")
    yaml.preserve_quotes = True
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=2, offset=0)
    return yaml


# --- Main per-file logic ----------------------------------------------------


@dataclass
class FileStats:
    path: Path
    total: int = 0
    backfilled: int = 0
    already_set: int = 0
    no_hint: int = 0


def process_yaml(yaml_path: Path, dry_run: bool, yaml_io: YAML) -> FileStats:
    stats = FileStats(path=yaml_path)
    with yaml_path.open("r", encoding="utf-8") as f:
        doc = yaml_io.load(f)
    if not doc or "associations" not in doc:
        return stats

    # disease_id is the same on every association row; grab it from the first.
    associations = doc["associations"]
    if not associations:
        return stats
    disease_id = associations[0].get("disease_id")
    if not disease_id:
        return stats

    citation_files = find_citation_files_for_mondo(disease_id)
    prose_files = find_prose_files_for_mondo(disease_id)
    all_entries: list[CitationEntry] = []
    for cf in citation_files:
        all_entries.extend(parse_citations_md(cf))
    prose_blob = "\n".join(
        p.read_text(encoding="utf-8", errors="replace") for p in prose_files
    )

    changed = False
    for assoc in associations:
        for ev in assoc.get("evidence", []) or []:
            stats.total += 1
            if ev.get("page_or_section"):
                stats.already_set += 1
                continue

            hint: str | None = None
            ref = (ev.get("reference") or "").strip()
            entry = match_entry(all_entries, ref) if ref else None
            if entry is not None:
                hint = extract_hint(entry.raw)

            if not hint:
                hint = find_hint_in_prose(prose_blob, ev.get("reference_title"))

            if hint:
                ev["page_or_section"] = hint
                stats.backfilled += 1
                changed = True
            else:
                stats.no_hint += 1

    if changed and not dry_run:
        buf = StringIO()
        yaml_io.dump(doc, buf)
        yaml_path.write_text(buf.getvalue(), encoding="utf-8")

    return stats


def iter_yaml_files(filter_mondo: str | None) -> Iterable[Path]:
    for p in sorted(KB_RESEARCH_DIR.glob("MONDO_*.yaml")):
        if filter_mondo and filter_mondo.replace(":", "_") not in p.name:
            continue
        yield p


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would change without writing files",
    )
    parser.add_argument(
        "--mondo",
        type=str,
        default=None,
        help="restrict to a single MONDO id (e.g. MONDO:0007947 or MONDO_0007947)",
    )
    args = parser.parse_args(argv)

    yaml_io = make_yaml()
    grand_total = grand_filled = grand_no_hint = grand_already = 0

    for yaml_path in iter_yaml_files(args.mondo):
        stats = process_yaml(yaml_path, dry_run=args.dry_run, yaml_io=yaml_io)
        grand_total += stats.total
        grand_filled += stats.backfilled
        grand_no_hint += stats.no_hint
        grand_already += stats.already_set
        print(
            f"{yaml_path.name}: {stats.total} evidence rows, "
            f"{stats.backfilled} backfilled, {stats.no_hint} no hint found"
            + (f", {stats.already_set} already set" if stats.already_set else "")
        )

    print()
    print(
        f"TOTAL: total_evidence_rows={grand_total} "
        f"total_backfilled={grand_filled} "
        f"total_no_hint={grand_no_hint} "
        f"total_already_set={grand_already} "
        f"({'dry-run' if args.dry_run else 'wrote files'})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
