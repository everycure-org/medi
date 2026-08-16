"""Loader for BioPortal's ICD10CM (UMLS2RDF Turtle).

ICD10CM uses SKOS (``skos:prefLabel`` for the code description, ``skos:altLabel`` for
synonyms). The UMLS2RDF output has some non-conformant string literals elsewhere that
break both OWLAPI (robot) and rdflib, so we use a tolerant line-based parser that only
reads the prefLabel/altLabel lines we need. Class IRIs are
``http://purl.bioontology.org/ontology/ICD10CM/<code>``.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

from medic.grounding.lexical.index import LexRow
from medic.grounding.lexical.preprocess import base_normalize

_IRI = "http://purl.bioontology.org/ontology/ICD10CM/"
_SUBJECT = re.compile(rf"^<{re.escape(_IRI)}([^>]+)>")
_QUOTED = re.compile(r'"""(.*?)"""|"((?:[^"\\]|\\.)*)"')


def _values(line: str) -> list[str]:
    out = []
    for triple, single in _QUOTED.findall(line):
        val = triple if triple else single
        if val:
            out.append(val.replace('\\"', '"'))
    return out


def _row(oid, label, value, field, prefix="ICD10CM") -> LexRow:
    return LexRow(
        object_id=oid, object_label=label, string_value=value,
        raw_value=value.strip(), norm_value=base_normalize(value),
        match_field=field, synonym_scope="exact", source_prefix=prefix,
    )


def load_icd10cm(ttl_path: str, prefix: str = "ICD10CM") -> Iterator[LexRow]:
    # First pass: code -> prefLabel; and collect altLabels per code.
    labels: dict[str, str] = {}
    alts: dict[str, list[str]] = {}
    current: str | None = None
    with open(ttl_path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = _SUBJECT.match(line)
            if m:
                current = m.group(1)
                continue
            if current is None:
                continue
            if "skos:prefLabel" in line:
                vals = _values(line)
                if vals:
                    labels[current] = vals[0]
            elif "skos:altLabel" in line:
                alts.setdefault(current, []).extend(_values(line))
    for code, label in labels.items():
        oid = f"{prefix}:{code}"
        yield _row(oid, label, label, "label", prefix)
        for alt in alts.get(code, []):
            yield _row(oid, label, alt, "exactSynonym", prefix)
