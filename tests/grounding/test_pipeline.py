import sqlite3

from medic.grounding.lexical.index import COLS
from medic.grounding.lexical.preprocess import base_normalize
from medic.grounding.lexical_backend import LexicalCascadeGrounding
from medic.grounding.pipeline import attach_grounding
from medic.normalization.normalizer import Normalizer
from medic.normalization.store import NormalizationMappingStore


def _fixture_db(path, rows):
    con = sqlite3.connect(path)
    con.execute(f"CREATE TABLE lex ({', '.join(c + ' TEXT' for c in COLS)})")
    con.executemany(f"INSERT INTO lex VALUES ({', '.join('?' * len(COLS))})", rows)
    con.execute("CREATE INDEX ix_raw ON lex (raw_value, match_field)")
    con.execute("CREATE INDEX ix_norm ON lex (norm_value, match_field)")
    con.commit()
    con.close()


def _r(oid, value):
    return (oid, value, value, value.strip(), base_normalize(value), "label", "exact",
            oid.split(":")[0])


def test_attach_grounding_and_normalization(tmp_path):
    db = tmp_path / "diseases.db"
    _fixture_db(str(db), [_r("UMLS:C0024796", "type 2 diabetes")])
    be = LexicalCascadeGrounding(disease_db=str(db), store_dir=str(tmp_path))
    store = NormalizationMappingStore(str(tmp_path / "disease_normalization.sssom.tsv"), "diseases")
    store.load()
    nz = Normalizer("diseases",
                    {"UMLS:C0024796": ("MONDO:0005148", "asserted_exact", "skos:exactMatch")},
                    store, "MONDO", "medic-normalizer/0")

    recs = [{"original_disease_label": "Type 2 Diabetes"}]
    out = attach_grounding(recs, "original_disease_label", be, nz, "diseases")

    g = out[0]["grounding"]
    n = out[0]["normalization"]
    assert g["original_string"] == "Type 2 Diabetes"          # verbatim (I-7)
    assert g["grounded_id"] == "UMLS:C0024796"
    assert g["grounding_quality"] == "lexical_exact_normalized"
    assert n["normalized_id"] == "MONDO:0005148"
    assert n["normalization_quality"] == "asserted_exact"
