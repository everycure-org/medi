"""Build the Stage-2 normalization index from a target namespace's own assertions.

We use only mappings the target namespace itself publishes: ``skos:exactMatch`` values
(from OBO Graph JSON ``meta.basicPropertyValues``) and obsolete-term ``replaced_by``.
Reverse-mapped to ``other_id -> (target_id, quality, predicate)``.
"""

from __future__ import annotations

import json
import re
import sqlite3

_OBO = "http://purl.obolibrary.org/obo/"
_EXACT = "http://www.w3.org/2004/02/skos/core#exactMatch"
_REPLACED_BY = "http://purl.obolibrary.org/obo/IAO_0100001"
_DEPRECATED = "http://www.w3.org/2002/07/owl#deprecated"

# IRI -> CURIE conversions for the exactMatch objects MONDO publishes.
_IRI_PATTERNS = [
    (re.compile(r"^http://linkedlifedata\.com/resource/umls/id/(.+)$"), "UMLS"),
    (re.compile(r"^http://purl\.bioontology\.org/ontology/ICD10CM/(.+)$"), "ICD10CM"),
    (re.compile(r"^http://identifiers\.org/mesh/(.+)$"), "MESH"),
    (re.compile(r"^http://identifiers\.org/snomedct/(.+)$"), "SNOMEDCT"),
]


def _to_curie(iri: str, target_prefix: str) -> str | None:
    if iri.startswith(_OBO):
        frag = iri[len(_OBO):]
        if "_" in frag:
            pfx, local = frag.split("_", 1)
            return f"{pfx}:{local}"
    for pat, pfx in _IRI_PATTERNS:
        m = pat.match(iri)
        if m:
            return f"{pfx}:{m.group(1)}"
    return None


def build_mapping_index(target_prefix: str, obo_json_path: str, out_db: str) -> int:
    with open(obo_json_path) as fh:
        data = json.load(fh)
    con = sqlite3.connect(out_db)
    con.execute("DROP TABLE IF EXISTS norm")
    con.execute("CREATE TABLE norm (other_id TEXT PRIMARY KEY, target_id TEXT, "
                "quality TEXT, predicate TEXT)")
    n = 0
    for graph in data.get("graphs", []):
        for node in graph.get("nodes", []):
            iri = node.get("id", "")
            if not iri.startswith(_OBO + target_prefix + "_"):
                continue
            target_id = target_prefix + ":" + iri[len(_OBO) + len(target_prefix) + 1:]
            meta = node.get("meta", {})
            deprecated = meta.get("deprecated", False)
            for bpv in meta.get("basicPropertyValues", []):
                pred, val = bpv.get("pred"), bpv.get("val")
                if pred == _EXACT:
                    other = _to_curie(val, target_prefix)
                    if other and other != target_id:
                        con.execute("INSERT OR IGNORE INTO norm VALUES (?,?,?,?)",
                                    (other, target_id, "asserted_exact", "skos:exactMatch"))
                        n += 1
                elif pred == _REPLACED_BY and deprecated:
                    new = _to_curie(val, target_prefix)
                    if new:
                        con.execute("INSERT OR REPLACE INTO norm VALUES (?,?,?,?)",
                                    (target_id, new, "deprecated_replacement", "IAO:0100001"))
                        n += 1
    con.commit()
    con.close()
    return n


def load_mapping_index(db_path: str) -> dict[str, tuple[str, str, str]]:
    con = sqlite3.connect(db_path)
    out = {r[0]: (r[1], r[2], r[3])
           for r in con.execute("SELECT other_id, target_id, quality, predicate FROM norm")}
    con.close()
    return out
