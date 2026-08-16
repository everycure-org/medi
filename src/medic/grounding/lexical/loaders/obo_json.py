"""Loader for OBO Graph JSON (downloaded from OBO PURLs, e.g. mondo.json).

Yields one LexRow per rdfs:label and per typed synonym for every node whose id is in
the target prefix's namespace. stdlib json only (oaklib in this repo is too old).
"""

from __future__ import annotations

import json
from collections.abc import Iterator

from medic.grounding.lexical.index import LexRow
from medic.grounding.lexical.preprocess import base_normalize

_OBO = "http://purl.obolibrary.org/obo/"
_SYN_PRED = {
    "hasExactSynonym": ("exactSynonym", "exact"),
    "hasBroadSynonym": ("broadSynonym", "broad"),
    "hasNarrowSynonym": ("narrowSynonym", "narrow"),
    "hasRelatedSynonym": ("relatedSynonym", "related"),
}


def _iri_to_curie(iri: str, prefix: str, iri_prefix: str | None) -> str | None:
    """Map a node IRI to ``prefix:local``.

    Default (``iri_prefix`` None) handles OBO PURLs (``.../obo/PREFIX_local``). For
    non-OBO ontologies (e.g. BioPortal ICD10CM ``.../ontology/ICD10CM/local``), pass the
    IRI stem in ``iri_prefix`` and the local id is whatever follows it.
    """
    if iri_prefix is not None:
        if iri.startswith(iri_prefix):
            return prefix + ":" + iri[len(iri_prefix):]
        return None
    frag = iri[len(_OBO):] if iri.startswith(_OBO) else None
    if frag and frag.startswith(prefix + "_"):
        return prefix + ":" + frag[len(prefix) + 1:]
    return None


def _row(oid, label, value, field, scope, prefix) -> LexRow:
    return LexRow(
        object_id=oid, object_label=label, string_value=value,
        raw_value=value.strip(), norm_value=base_normalize(value),
        match_field=field, synonym_scope=scope, source_prefix=prefix,
    )


def load_obo_json(json_path: str, prefix: str, iri_prefix: str | None = None) -> Iterator[LexRow]:
    with open(json_path) as fh:
        data = json.load(fh)
    for graph in data.get("graphs", []):
        for node in graph.get("nodes", []):
            oid = _iri_to_curie(node.get("id", ""), prefix, iri_prefix)
            if not oid:
                continue
            label = node.get("lbl")
            if not label:
                continue
            yield _row(oid, label, label, "label", "exact", prefix)
            for syn in node.get("meta", {}).get("synonyms", []):
                field_scope = _SYN_PRED.get(syn.get("pred", ""))
                val = syn.get("val")
                if field_scope and val:
                    field, scope = field_scope
                    yield _row(oid, label, val, field, scope, prefix)
