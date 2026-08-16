"""Turn a source's raw extracted text into typed TextSpans (design spec D6).

Sources hand MeDIC one flat string per record. That string is often several semantically
distinct chunks concatenated: a section header, the indication sentence, a subsection header,
and a scope-limiting sentence. Conflating them put a *negated* sentence in scope for the
*positive* claim's negation check — see the UBRELVY case in the design spec §4.3.

Splitting is deliberately conservative. When the structure is not recoverable, one
`SECTION_TEXT` (or the source's default role) is emitted — degraded, not wrong. Every role
returned is a `TextSpanRoleEnum` value; nothing here invents a vocabulary.
"""

from __future__ import annotations

import re

#: How many characters an ingester keeps when it stores a supporting snippet.
#:
#: Shared rather than repeated as a literal, because the merge has to be able to tell a
#: snippet that was *cut* from one that merely ended: a span sitting exactly on the cap is
#: a truncated span, which is the `truncated_snippet` recognition flag (FAILURE_MODES 5.6).
#: If an ingester's slice and this constant drift apart, that detection silently stops.
SNIPPET_CHAR_CAP = 500


def is_truncated(text: str) -> bool:
    """Was this span cut short by :data:`SNIPPET_CHAR_CAP`?

    A span landing exactly on the cap was almost certainly sliced — a source section whose
    length is exactly 500 characters is vanishingly rare, and the cost of the rare false
    positive is one record scored MEDIUM instead of HIGH.
    """
    return len(text or "") >= SNIPPET_CHAR_CAP


#: SPL section titles that appear inline at the head of an extracted section body.
_SPL_HEADERS = ("INDICATIONS AND USAGE", "INDICATIONS & USAGE", "CONTRAINDICATIONS")

#: The subsection marker introducing a scope restriction inside an SPL indications section.
_LIMITATION_MARKER = re.compile(r"\bLimitations? of Use\b")

#: Default span role per ingester, for sources whose extracted text has no inner structure.
_SOURCE_ROLE = {
    "DAILYMED": "SECTION_TEXT",
    "EMA": "STRUCTURED_FIELD",
    "PMDA": "SECTION_TEXT",
    "INDIA": "TABLE_CELL",
    "CDSCO": "TABLE_CELL",
}


def _span(role: str, text: str, document: str, section_code: str) -> dict:
    span = {"role": role, "text": text, "document": document}
    if section_code:
        span["section_code"] = section_code
    return span


def split_dailymed_section(text: str, *, document: str, section_code: str) -> list[dict]:
    """Split an SPL section body into header / body / limitation spans.

    Lossless: joining the returned texts with a single space reproduces the input, so no source
    text is dropped or invented (I-7).
    """
    body = (text or "").strip()
    if not body:
        return []
    spans: list[dict] = []

    for header in _SPL_HEADERS:
        if body.upper().startswith(header):
            spans.append(_span("SECTION_HEADER", body[:len(header)], document, section_code))
            body = body[len(header):].strip()
            break

    match = _LIMITATION_MARKER.search(body)
    if match:
        before = body[:match.start()].strip()
        marker = body[match.start():match.end()]
        after = body[match.end():].strip()
        if before:
            spans.append(_span("SECTION_TEXT", before, document, section_code))
        spans.append(_span("SUBSECTION_HEADER", marker, document, section_code))
        if after:
            spans.append(_span("LIMITATION_STATEMENT", after, document, section_code))
    elif body:
        spans.append(_span("SECTION_TEXT", body, document, section_code))
    return spans


def spans_for_source(
    source: str, text: str, *, document: str, section_code: str
) -> list[dict]:
    """Typed spans for one source record. Empty text yields no spans, never an empty span."""
    src = (source or "").upper()
    body = (text or "").strip()
    if not body:
        return []
    if src == "DAILYMED":
        return split_dailymed_section(body, document=document, section_code=section_code)
    return [_span(_SOURCE_ROLE.get(src, "UNKNOWN"), body, document, section_code)]
