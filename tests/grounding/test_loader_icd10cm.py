from medic.grounding.lexical.loaders.icd10cm import load_icd10cm

_TTL = '''\
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
<http://purl.bioontology.org/ontology/ICD10CM/E11> a owl:Class ;
\tskos:prefLabel """Type 2 diabetes mellitus"""@en ;
\tskos:notation """E11"""^^xsd:string ;
\tskos:altLabel """diabetes NOS"""@en , """insulin resistant diabetes"""@en .
<http://purl.bioontology.org/ontology/ICD10CM/> a owl:Ontology .
'''


def test_icd10cm_labels_and_synonyms(tmp_path):
    p = tmp_path / "icd10cm.ttl"
    p.write_text(_TTL)
    rows = list(load_icd10cm(str(p)))
    by = {(r.object_id, r.match_field, r.string_value) for r in rows}
    assert ("ICD10CM:E11", "label", "Type 2 diabetes mellitus") in by
    assert ("ICD10CM:E11", "exactSynonym", "diabetes NOS") in by
    assert ("ICD10CM:E11", "exactSynonym", "insulin resistant diabetes") in by
    # ontology header node must not produce rows
    assert all(r.object_id != "ICD10CM:" for r in rows)
