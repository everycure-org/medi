"""Tests for regulatory_document_url deep-linking logic.

Covers:
- on_label_merge `_is_search_url` and FDA URL upgrade
- on_label_merge `_build_fda_url_lookup` (Orange Book + Purple Book)
- DailyMed `_build_regulatory_evidence` with setid passthrough
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from medic.merge.on_label_merge import (
    _build_fda_url_lookup,
    _build_regulatory_status_from_evidence,
    _is_search_url,
)


def test_is_search_url_recognises_dailymed_search():
    assert _is_search_url("https://dailymed.nlm.nih.gov/dailymed/search.cfm?labeltype=all&query=X")


def test_is_search_url_recognises_pmda_search():
    assert _is_search_url("https://www.pmda.go.jp/english/search/search.html?q=X")
    assert _is_search_url("https://www.pmda.go.jp/PmdaSearch/iyakuSearch/")


def test_is_search_url_recognises_ema_search():
    assert _is_search_url("https://www.ema.europa.eu/en/search-results?query=X")
    assert _is_search_url("https://www.ema.europa.eu/en/medicines?search_api_fulltext=X")


def test_is_search_url_recognises_purplebook_landing():
    assert _is_search_url("https://purplebooksearch.fda.gov/?query=125276")
    assert _is_search_url("https://purplebooksearch.fda.gov/results?query=125276")


def test_is_search_url_rejects_specific_urls():
    assert not _is_search_url("https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=020233")
    assert not _is_search_url("https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=abc")
    assert not _is_search_url("https://www.ema.europa.eu/en/medicines/human/EPAR/keppra")
    assert not _is_search_url("")


def test_build_fda_url_lookup_maps_orangebook_nda():
    with tempfile.TemporaryDirectory() as tmp:
        ob_path = Path(tmp) / "orangebook.yaml"
        ob_path.write_text(yaml.dump([
            {
                "normalized_id": "CHEBI:15365",
                "application_number": "020233|020441",
            },
            {
                "normalized_id": "CHEBI:6801",
                "application_number": "017900",
            },
        ]))
        lookup = _build_fda_url_lookup(orangebook_path=ob_path, purplebook_path=Path(tmp) / "missing.yaml")
        assert "020233" in lookup["CHEBI:15365"]
        assert "accessdata.fda.gov" in lookup["CHEBI:15365"]
        assert "017900" in lookup["CHEBI:6801"]


def test_build_fda_url_lookup_maps_purplebook_bla():
    with tempfile.TemporaryDirectory() as tmp:
        pb_path = Path(tmp) / "purplebook.yaml"
        pb_path.write_text(yaml.dump([
            {
                "normalized_id": "DRUGBANK:DB00072",
                "bla_number": "BLA125276",
            },
        ]))
        lookup = _build_fda_url_lookup(orangebook_path=Path(tmp) / "missing.yaml", purplebook_path=pb_path)
        assert "purplebooksearch.fda.gov" in lookup["DRUGBANK:DB00072"]
        assert "BLA125276" in lookup["DRUGBANK:DB00072"]


def test_orangebook_takes_priority_over_purplebook():
    """When same drug appears in both, OB NDA wins (small molecules first)."""
    with tempfile.TemporaryDirectory() as tmp:
        ob_path = Path(tmp) / "orangebook.yaml"
        pb_path = Path(tmp) / "purplebook.yaml"
        ob_path.write_text(yaml.dump([{"normalized_id": "CHEBI:1", "application_number": "999999"}]))
        pb_path.write_text(yaml.dump([{"normalized_id": "CHEBI:1", "bla_number": "BLA000"}]))
        lookup = _build_fda_url_lookup(orangebook_path=ob_path, purplebook_path=pb_path)
        assert "999999" in lookup["CHEBI:1"]
        assert "BLA000" not in lookup["CHEBI:1"]


def test_fda_search_url_upgraded_when_deep_link_known():
    """Generic FDA search URL is replaced by Drugs@FDA NDA URL when available."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/search.cfm?labeltype=all&query=Aspirin",
            "approval_status": "APPROVED",
        }
    ]
    fda_lookup = {
        "CHEBI:15365": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=016030"
    }
    result = _build_regulatory_status_from_evidence(
        evidence, drug_id="CHEBI:15365", fda_url_lookup=fda_lookup
    )
    assert len(result) == 1
    assert "accessdata.fda.gov" in result[0]["regulatory_document_url"]
    assert "ApplNo=016030" in result[0]["regulatory_document_url"]


def test_fda_setid_url_preserved_not_overridden():
    """Already-deep-linked DailyMed setid URLs should NOT be overridden by NDA lookup."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=abc-123",
            "approval_status": "APPROVED",
        }
    ]
    fda_lookup = {"CHEBI:15365": "https://www.accessdata.fda.gov/.../ApplNo=016030"}
    result = _build_regulatory_status_from_evidence(
        evidence, drug_id="CHEBI:15365", fda_url_lookup=fda_lookup
    )
    # Setid URL is specific, should be preserved
    assert "lookup.cfm?setid=abc-123" in result[0]["regulatory_document_url"]


def test_fda_search_url_kept_when_no_deep_link():
    """Search URL stays as fallback when no NDA/BLA available."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/search.cfm?labeltype=all&query=Foo",
            "approval_status": "APPROVED",
        }
    ]
    result = _build_regulatory_status_from_evidence(evidence, drug_id="UNKNOWN:1")
    assert "search.cfm" in result[0]["regulatory_document_url"]


def test_ema_url_unchanged_by_fda_lookup():
    """EMA URLs must not be touched by the FDA URL upgrade logic."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "reference": "https://www.ema.europa.eu/en/search-results?query=X",
            "approval_status": "APPROVED",
        }
    ]
    fda_lookup = {"CHEBI:1": "https://accessdata.fda.gov/.../ApplNo=000001"}
    result = _build_regulatory_status_from_evidence(
        evidence, drug_id="CHEBI:1", fda_url_lookup=fda_lookup
    )
    # EMA reference should stay as the (search) EMA URL
    assert result[0]["authority"] == "EMA"
    assert "ema.europa.eu" in result[0]["regulatory_document_url"]


# ---------------------------------------------------------------------------
# Multi-row regulatory_status (one per authority+source artifact)
# ---------------------------------------------------------------------------


def test_dailymed_and_orangebook_emit_separate_fda_rows():
    """Drug with both DailyMed label and Orange Book NDA should produce TWO FDA rows."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=2c47cd2d-0fdc-4855-8434-3f0e1cd00387",
            "approval_status": "APPROVED",
            "source_role": "INTERMEDIARY",
        }
    ]
    fda_artifacts = {
        "CHEBI:15365": [
            {
                "source": "ORANGEBOOK",
                "source_role": "PRIMARY",
                "application_number": "020233|020441|020746",
                "regulatory_document_url": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=020233",
                "approval_date": "19940214",
            }
        ]
    }
    rows = _build_regulatory_status_from_evidence(
        evidence, drug_id="CHEBI:15365", fda_artifacts=fda_artifacts
    )
    fda_rows = [r for r in rows if r["authority"] == "FDA"]
    assert len(fda_rows) == 2
    sources = {r["source"] for r in fda_rows}
    assert sources == {"DAILYMED", "ORANGEBOOK"}


def test_setid_extracted_into_field():
    """DailyMed lookup URL setid should populate the `setid` field."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=abc-123",
            "approval_status": "APPROVED",
        }
    ]
    rows = _build_regulatory_status_from_evidence(evidence, drug_id="CHEBI:1")
    assert rows[0]["source"] == "DAILYMED"
    assert rows[0]["setid"] == "abc-123"


def test_orangebook_application_number_pipe_joined():
    """All NDAs should be carried, pipe-joined, in application_number field."""
    fda_artifacts = {
        "CHEBI:1": [{
            "source": "ORANGEBOOK",
            "source_role": "PRIMARY",
            "application_number": "020233|020441|077519",
            "regulatory_document_url": "https://www.accessdata.fda.gov/.../ApplNo=020233",
        }]
    }
    rows = _build_regulatory_status_from_evidence(
        evidence_items=[], drug_id="CHEBI:1", fda_artifacts=fda_artifacts
    )
    assert len(rows) == 1
    assert rows[0]["application_number"] == "020233|020441|077519"
    # URL still points to first NDA
    assert "ApplNo=020233" in rows[0]["regulatory_document_url"]


def test_purplebook_emits_separate_row_with_bla():
    """Biologic with both DailyMed setid and Purple Book BLA should produce TWO FDA rows."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=xyz",
            "approval_status": "APPROVED",
        }
    ]
    fda_artifacts = {
        "DRUGBANK:DB00072": [{
            "source": "PURPLEBOOK",
            "source_role": "PRIMARY",
            "bla_number": "125276",
            "regulatory_document_url": "https://purplebooksearch.fda.gov/results?query=125276",
        }]
    }
    rows = _build_regulatory_status_from_evidence(
        evidence, drug_id="DRUGBANK:DB00072", fda_artifacts=fda_artifacts
    )
    fda_rows = [r for r in rows if r["authority"] == "FDA"]
    assert len(fda_rows) == 2
    sources = {r["source"] for r in fda_rows}
    assert sources == {"DAILYMED", "PURPLEBOOK"}
    pb_row = next(r for r in fda_rows if r["source"] == "PURPLEBOOK")
    assert pb_row["bla_number"] == "125276"


def test_source_inferred_from_url():
    from medic.merge.on_label_merge import _identify_source_from_evidence
    assert _identify_source_from_evidence({"reference": "https://dailymed.nlm.nih.gov/.../setid=X"}, "FDA") == "DAILYMED"
    assert _identify_source_from_evidence({"reference": "https://www.accessdata.fda.gov/.../ApplNo=X"}, "FDA") == "ORANGEBOOK"
    assert _identify_source_from_evidence({"reference": "https://purplebooksearch.fda.gov/?query=X"}, "FDA") == "PURPLEBOOK"
    assert _identify_source_from_evidence({"reference": "https://www.ema.europa.eu/.../EPAR/X"}, "EMA") == "EMA_EPAR"
    assert _identify_source_from_evidence({"reference": "https://www.pmda.go.jp/...?q=X"}, "PMDA") == "PMDA"


# ---------------------------------------------------------------------------
# PMDA URL normalisation + source_document_url
# ---------------------------------------------------------------------------


def test_normalize_pmda_url_rewrites_dead_search():
    from medic.merge.on_label_merge import _normalize_pmda_url
    dead = "https://www.pmda.go.jp/english/search/search.html?q=Slinda+28+Tablets"
    assert _normalize_pmda_url(dead) == "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"


def test_normalize_pmda_url_passes_through_pdf():
    from medic.merge.on_label_merge import _normalize_pmda_url
    pdf = "https://www.pmda.go.jp/files/000275874.pdf"
    assert _normalize_pmda_url(pdf) == pdf


def test_normalize_pmda_url_passes_through_iyaku_landing():
    from medic.merge.on_label_merge import _normalize_pmda_url
    landing = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"
    assert _normalize_pmda_url(landing) == landing


def test_pmda_pdf_emits_source_document_url():
    """A PMDA review-report PDF reference should also surface as source_document_url."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "JAPAN",
            "reference": "https://www.pmda.go.jp/files/000275874.pdf",
            "approval_status": "APPROVED",
            "source_role": "PRIMARY",
        }
    ]
    rows = _build_regulatory_status_from_evidence(evidence, drug_id="CHEBI:1")
    assert len(rows) == 1
    assert rows[0]["authority"] == "PMDA"
    assert rows[0]["regulatory_document_url"] == "https://www.pmda.go.jp/files/000275874.pdf"
    assert rows[0]["source_document_url"] == "https://www.pmda.go.jp/files/000275874.pdf"


def test_pmda_dead_url_rewritten_in_regulatory_status():
    """The legacy english/search/search.html?q= URL must not appear in output."""
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "JAPAN",
            "reference": "https://www.pmda.go.jp/english/search/search.html?q=Foo",
            "approval_status": "APPROVED",
            "source_role": "INTERMEDIARY",
        }
    ]
    rows = _build_regulatory_status_from_evidence(evidence, drug_id="CHEBI:1")
    assert len(rows) == 1
    assert "english/search/search.html" not in rows[0]["regulatory_document_url"]
    assert rows[0]["regulatory_document_url"] == "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"
    # Search landing should NOT yield a source_document_url
    assert "source_document_url" not in rows[0]


def test_pmda_evidence_normalisation_in_record():
    """_normalize_pmda_evidence rewrites dead URLs and adds source_document_url for PDFs."""
    from medic.merge.on_label_merge import _normalize_pmda_evidence
    record = {
        "evidence": [
            {
                "source_type": "REGULATORY",
                "jurisdiction": "JAPAN",
                "reference": "https://www.pmda.go.jp/english/search/search.html?q=X",
            },
            {
                "source_type": "REGULATORY",
                "jurisdiction": "JAPAN",
                "reference": "https://www.pmda.go.jp/files/000275874.pdf",
            },
            {
                "source_type": "REGULATORY",
                "jurisdiction": "USA",
                "reference": "https://www.pmda.go.jp/english/search/search.html?q=Skip",
            },
        ]
    }
    _normalize_pmda_evidence(record)
    assert record["evidence"][0]["reference"] == "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/"
    assert "source_document_url" not in record["evidence"][0]
    assert record["evidence"][1]["source_document_url"] == "https://www.pmda.go.jp/files/000275874.pdf"
    # USA-jurisdiction rows are untouched even with a pmda.go.jp URL (defensive).
    assert "english/search" in record["evidence"][2]["reference"]


def test_build_fda_artifact_lookup_returns_richer_per_drug_data():
    """Ensure the artifact lookup carries application_number AND approval_date."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        ob_path = Path(tmp) / "orangebook.yaml"
        ob_path.write_text(yaml.dump([{
            "normalized_id": "CHEBI:15365",
            "application_number": "020233|020441",
            "approval_date": "19940214",
        }]))
        from medic.merge.on_label_merge import _build_fda_artifact_lookup
        artifacts = _build_fda_artifact_lookup(orangebook_path=ob_path, purplebook_path=Path(tmp) / "missing.yaml")
        assert "CHEBI:15365" in artifacts
        ob_entry = artifacts["CHEBI:15365"][0]
        assert ob_entry["source"] == "ORANGEBOOK"
        assert ob_entry["application_number"] == "020233|020441"
        assert ob_entry["approval_date"] == "19940214"


# ---------------------------------------------------------------------------
# PRIMARY-vs-INTERMEDIARY dedup at evidence level
# ---------------------------------------------------------------------------


def test_dedup_drops_intermediary_when_primary_exists_for_same_jurisdiction_source():
    """EMA PRIMARY (deep EPAR) supersedes EMA INTERMEDIARY (search URL)."""
    from medic.merge.on_label_merge import _dedup_evidence_prefer_primary
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "source_role": "INTERMEDIARY",
            "reference": "https://www.ema.europa.eu/en/medicines?search_api_fulltext=keppra",
            "original_drug_label": "Levetiracetam",
            "original_disease_label": "epilepsy",
        },
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "source_role": "PRIMARY",
            "reference": "https://www.ema.europa.eu/en/medicines/human/EPAR/keppra",
        },
    ]
    out = _dedup_evidence_prefer_primary(evidence)
    assert len(out) == 1
    assert out[0]["source_role"] == "PRIMARY"
    # Audit fields carried over from the dropped INTERMEDIARY.
    assert out[0]["original_drug_label"] == "Levetiracetam"
    assert out[0]["original_disease_label"] == "epilepsy"


def test_dedup_keeps_intermediary_when_no_primary_counterpart():
    """INTERMEDIARY survives when no PRIMARY exists for the same key."""
    from medic.merge.on_label_merge import _dedup_evidence_prefer_primary
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "source_role": "INTERMEDIARY",
            "reference": "https://www.ema.europa.eu/en/medicines?search_api_fulltext=foo",
        },
    ]
    out = _dedup_evidence_prefer_primary(evidence)
    assert len(out) == 1
    assert out[0]["source_role"] == "INTERMEDIARY"


def test_dedup_does_not_collapse_dailymed_and_orangebook():
    """DAILYMED INTERMEDIARY and ORANGEBOOK PRIMARY have different (jur, source) keys."""
    from medic.merge.on_label_merge import _dedup_evidence_prefer_primary
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "source_role": "INTERMEDIARY",
            "reference": "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=abc",
        },
        {
            "source_type": "REGULATORY",
            "jurisdiction": "USA",
            "source_role": "PRIMARY",
            "reference": "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=020233",
        },
    ]
    out = _dedup_evidence_prefer_primary(evidence)
    # Both rows kept because they describe different artifacts (label vs marketing approval).
    assert len(out) == 2


def test_dedup_does_not_drop_primary_when_audit_field_already_present():
    """Existing original_* fields on PRIMARY are not overwritten by carryover."""
    from medic.merge.on_label_merge import _dedup_evidence_prefer_primary
    evidence = [
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "source_role": "INTERMEDIARY",
            "reference": "https://www.ema.europa.eu/en/medicines?search_api_fulltext=x",
            "original_drug_label": "RAW_FROM_INTERMEDIARY",
        },
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "source_role": "PRIMARY",
            "reference": "https://www.ema.europa.eu/en/medicines/human/EPAR/x",
            "original_drug_label": "RAW_FROM_PRIMARY",
        },
    ]
    out = _dedup_evidence_prefer_primary(evidence)
    assert len(out) == 1
    assert out[0]["original_drug_label"] == "RAW_FROM_PRIMARY"


def test_dedup_passes_through_non_regulatory_evidence():
    """LITERATURE evidence is never affected by the PRIMARY/INTERMEDIARY dedup."""
    from medic.merge.on_label_merge import _dedup_evidence_prefer_primary
    evidence = [
        {
            "source_type": "LITERATURE",
            "reference": "https://pubmed.ncbi.nlm.nih.gov/12345",
        },
        {
            "source_type": "REGULATORY",
            "jurisdiction": "EU",
            "source_role": "PRIMARY",
            "reference": "https://www.ema.europa.eu/en/medicines/human/EPAR/y",
        },
    ]
    out = _dedup_evidence_prefer_primary(evidence)
    assert len(out) == 2
