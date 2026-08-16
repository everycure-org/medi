import json

from medic.grounding.lexical.loaders.obo_json import load_obo_json


def _write(tmp_path, nodes):
    p = tmp_path / "ont.json"
    p.write_text(json.dumps({"graphs": [{"nodes": nodes}]}))
    return str(p)


def test_obo_purl_label_and_synonyms(tmp_path):
    path = _write(tmp_path, [{
        "id": "http://purl.obolibrary.org/obo/MONDO_0007947", "lbl": "Marfan syndrome",
        "meta": {"synonyms": [
            {"pred": "hasExactSynonym", "val": "Marfan's syndrome"},
            {"pred": "hasBroadSynonym", "val": "connective tissue disorder"}]},
    }])
    rows = list(load_obo_json(path, "MONDO"))
    got = {(r.object_id, r.match_field) for r in rows}
    assert ("MONDO:0007947", "label") in got
    assert ("MONDO:0007947", "exactSynonym") in got
    assert ("MONDO:0007947", "broadSynonym") in got


def test_bioportal_iri_prefix(tmp_path):
    path = _write(tmp_path, [{
        "id": "http://purl.bioontology.org/ontology/ICD10CM/E11.9",
        "lbl": "Type 2 diabetes mellitus without complications",
    }])
    rows = list(load_obo_json(path, "ICD10CM",
                              iri_prefix="http://purl.bioontology.org/ontology/ICD10CM/"))
    assert rows and rows[0].object_id == "ICD10CM:E11.9" and rows[0].match_field == "label"
