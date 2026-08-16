"""Tests for CURE-ID ingest (no network calls)."""

from pathlib import Path

from medic.ingest.cureid.__main__ import (
    outcome_to_confidence,
    outcome_to_support,
    parse_cureid_tsv,
    aggregate_associations,
)


def test_outcome_to_confidence_improved():
    assert outcome_to_confidence("Patient improved") == "MEDIUM"


def test_outcome_to_confidence_recovered():
    assert outcome_to_confidence("Patient fully recovered") == "MEDIUM"


def test_outcome_to_confidence_unchanged():
    assert outcome_to_confidence("Patient's condition was unchanged") == "LOW"


def test_outcome_to_confidence_died():
    assert outcome_to_confidence("Patient died") == "LOW"


def test_outcome_to_confidence_empty():
    assert outcome_to_confidence("") == "LOW"
    assert outcome_to_confidence(None) == "LOW"


def test_outcome_to_support_improved():
    assert outcome_to_support("Patient improved") == "SUPPORT"


def test_outcome_to_support_recovered():
    assert outcome_to_support("Patient fully recovered") == "SUPPORT"


def test_outcome_to_support_unchanged():
    assert outcome_to_support("Patient's condition was unchanged") == "PARTIAL"


def test_outcome_to_support_died():
    assert outcome_to_support("Patient died") == "PARTIAL"


def test_outcome_to_support_empty():
    assert outcome_to_support("") == "SUPPORT"


_HEADER = (
    "subject_label_original\tsubject_label\tsubject_type\tsubject_final_label\t"
    "subject_final_curie\tsubject_missing_final\tpredicate_raw\tbiolink_predicate\t"
    "association_category\tobject_label_original\tobject_label\tobject_type\t"
    "object_final_label\tobject_final_curie\tobject_missing_final\treport_id\tpmid\tlink\toutcome"
)


def _make_row(
    subj_curie="CHEBI:68478",
    subj_label="Everolimus",
    subj_type="Drug",
    predicate="biolink:applied_to_treat",
    obj_curie="MONDO:0007893",
    obj_label="Noonan syndrome",
    obj_type="Disease",
    report_id="rpt-001",
    pmid="",
    link="https://cure.ncats.io/case/rpt-001",
    outcome="Patient improved",
):
    return (
        f"orig\tlabel\t{subj_type}\t{subj_label}\t{subj_curie}\tN\traw\t{predicate}\t"
        f"biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation\t"
        f"orig\tlabel\t{obj_type}\t{obj_label}\t{obj_curie}\tN\t{report_id}\t{pmid}\t{link}\t{outcome}"
    )


def _write_tsv(tmp_path: Path, rows: list[str]) -> Path:
    tsv = tmp_path / "cureid_data.tsv"
    tsv.write_text(_HEADER + "\n" + "\n".join(rows) + "\n")
    return tsv


def test_parse_filters_to_drug_treatment_edges(tmp_path):
    rows = [
        _make_row(),
        _make_row(subj_type="Gene", subj_curie="NCBIGene:673", predicate="biolink:gene_associated_with_condition"),
        _make_row(predicate="biolink:has_adverse_event", obj_type="AdverseEvent", obj_curie="HP:0000155", obj_label="Oral ulcer"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assert len(records) == 1
    assert records[0]["drug_curie"] == "CHEBI:68478"
    assert records[0]["disease_curie"] == "MONDO:0007893"


def test_parse_includes_phenotypic_feature_treatment(tmp_path):
    rows = [
        _make_row(obj_type="PhenotypicFeature", obj_curie="HP:0003765", obj_label="Psoriasis"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assert len(records) == 1
    assert records[0]["disease_curie"] == "HP:0003765"


def test_parse_captures_pmid(tmp_path):
    rows = [
        _make_row(pmid="39235050"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assert records[0]["pmid"] == "39235050"


def test_parse_handles_empty_pmid(tmp_path):
    rows = [_make_row(pmid="")]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assert records[0]["pmid"] == ""


def test_aggregate_groups_by_drug_disease(tmp_path):
    rows = [
        _make_row(report_id="rpt-001", outcome="Patient improved"),
        _make_row(report_id="rpt-002", outcome="Patient fully recovered"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    assert len(assocs) == 1
    assert assocs[0]["drug_id"] == "CHEBI:68478"
    assert assocs[0]["disease_id"] == "MONDO:0007893"
    assert len(assocs[0]["evidence"]) >= 2


def test_aggregate_different_diseases_separate(tmp_path):
    rows = [
        _make_row(obj_curie="MONDO:0007893", obj_label="Disease A"),
        _make_row(obj_curie="MONDO:0005045", obj_label="Disease B"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    assert len(assocs) == 2


def test_aggregate_evidence_has_database_source(tmp_path):
    rows = [_make_row()]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    db_evidence = [e for e in assocs[0]["evidence"] if e["source_type"] == "DATABASE"]
    assert len(db_evidence) == 1
    assert "CURE-ID" in db_evidence[0]["reference_title"]
    assert db_evidence[0]["jurisdiction"] == "USA"
    assert db_evidence[0]["approval_status"] == "OFF_LABEL"
    assert db_evidence[0]["max_research_phase"] == "CASE_REPORT"


def test_aggregate_evidence_includes_pmid(tmp_path):
    rows = [_make_row(pmid="39235050")]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    lit_evidence = [e for e in assocs[0]["evidence"] if e["source_type"] == "LITERATURE"]
    assert len(lit_evidence) == 1
    assert lit_evidence[0]["reference"] == "PMID:39235050"


def test_aggregate_evidence_no_pmid_no_literature(tmp_path):
    rows = [_make_row(pmid="")]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    lit_evidence = [e for e in assocs[0]["evidence"] if e["source_type"] == "LITERATURE"]
    assert len(lit_evidence) == 0


def test_aggregate_phenotype_edges_folded_into_notes(tmp_path):
    rows = [
        _make_row(report_id="rpt-001", obj_curie="MONDO:0015280", obj_label="CFC syndrome", obj_type="Disease"),
        _make_row(report_id="rpt-001", obj_curie="HP:0000982", obj_label="Palmoplantar keratoderma", obj_type="PhenotypicFeature"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    disease_assocs = [a for a in assocs if a["disease_id"].startswith("MONDO:")]
    assert len(disease_assocs) == 1
    assert "Palmoplantar keratoderma" in disease_assocs[0]["notes"]


def test_aggregate_orphan_phenotype_kept_as_association(tmp_path):
    rows = [
        _make_row(report_id="rpt-001", obj_curie="HP:0003765", obj_label="Psoriasis", obj_type="PhenotypicFeature"),
    ]
    tsv = _write_tsv(tmp_path, rows)
    records = parse_cureid_tsv(tsv)
    assocs = aggregate_associations(records)
    assert len(assocs) == 1
    assert assocs[0]["disease_id"] == "HP:0003765"
