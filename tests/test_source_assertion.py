"""One assertion per source document (design spec D2, D3)."""

from medic.merge.on_label_merge import _document_for, _make_key, _pair_key


def _rec(**kw):
    base = {"final_normalized_drug_id": "CHEBI:2549",
            "final_normalized_disease_id": "MONDO:0005002",
            "relationship_type": "INDICATION", "source": "DAILYMED"}
    base.update(kw)
    return base


def test_the_pair_key_ignores_the_source():
    assert _pair_key(_rec()) == "CHEBI:2549|MONDO:0005002|INDICATION"
    assert _pair_key(_rec(source="EMA")) == _pair_key(_rec(source="DAILYMED"))


def test_the_record_key_separates_documents():
    """667 DailyMed pairs come from two different SPLs; the old key collapsed them."""
    a = _make_key(_rec(set_id="aaa"))
    b = _make_key(_rec(set_id="bbb"))
    assert a != b
    assert a.startswith("CHEBI:2549|MONDO:0005002|INDICATION|DAILYMED|")


def test_the_record_key_separates_sources():
    assert _make_key(_rec(source="DAILYMED", set_id="x")) != \
        _make_key(_rec(source="EMA", set_id="x"))


def test_a_missing_id_still_yields_no_key():
    assert _make_key(_rec(final_normalized_drug_id="")) is None
    assert _make_key(_rec(final_normalized_disease_id="")) is None
    assert _pair_key(_rec(final_normalized_drug_id="")) is None


def test_an_error_id_is_still_rejected():
    assert _make_key(_rec(final_normalized_drug_id="Error: no match")) is None


def test_the_document_comes_from_the_evidence_then_the_record():
    assert _document_for({"source": "DAILYMED"}, {"setid": "abc"}) == "DailyMed:abc"
    assert _document_for({"source": "DAILYMED", "set_id": "def"}, {}) == "DailyMed:def"
    assert _document_for(
        {"source": "EMA"},
        {"reference": "https://www.ema.europa.eu/en/medicines/human/EPAR/keppra"},
    ) == "EMA:keppra"


def test_a_document_is_always_produced_so_the_key_is_never_ambiguous():
    doc = _document_for({"source": "PMDA", "final_normalized_drug_id": "CHEBI:1"}, {})
    assert doc and doc.startswith("PMDA:")


def test_two_drugs_from_one_registry_do_not_collide():
    a = _document_for({"source": "GRLS", "final_normalized_drug_id": "CHEBI:1"}, {})
    b = _document_for({"source": "GRLS", "final_normalized_drug_id": "CHEBI:2"}, {})
    assert a != b


# --- artifacts are drug-level facts, not claim attestations -------------------------------

def _dailymed_record():
    return {
        "final_normalized_drug_id": "CHEBI:2549",
        "final_normalized_drug_label": "albuterol",
        "final_normalized_disease_id": "MONDO:0005002",
        "final_normalized_disease_label": "COPD",
        "relationship_type": "INDICATION",
        "source": "DAILYMED",
        "set_id": "abc123",
        "indications_text": "FOO is indicated for the treatment of COPD.",
        "evidence": [{
            "source_type": "REGULATORY", "jurisdiction": "USA", "source_role": "INTERMEDIARY",
            "original_disease_label": "COPD", "original_drug_label": "ALBUTEROL",
            "snippet": "FOO is indicated for the treatment of COPD.", "setid": "abc123",
            "approval_status": "APPROVED"}],
    }


#: An Orange Book NDA and a Russian registration for the same drug. Neither says anything
#: about COPD; both are already recorded on the drug in drug_list.yaml.
ARTIFACTS = {"CHEBI:2549": [
    {"source": "ORANGEBOOK", "source_role": "PRIMARY", "application_number": "012345",
     "regulatory_document_url": "https://accessdata.fda.gov/x"},
    {"source": "GRLS", "authority": "MOH_RUSSIA", "source_role": "PRIMARY",
     "regulatory_document_url": "https://grls.rosminzdrav.ru/Default.aspx"},
]}


def test_a_record_with_one_evidence_row_yields_exactly_one_assertion():
    from medic.merge.on_label_merge import _build_source_assertions

    out = _build_source_assertions(_dailymed_record(), fda_artifacts=ARTIFACTS)
    assert len(out) == 1
    assert out[0]["source"] == "DAILYMED"
    assert out[0]["document"] == "DailyMed:abc123"


def test_drug_level_artifacts_never_become_assertions():
    """Orange Book attests a drug approval, not the drug-disease claim.

    Fabricating an ORANGEBOOK assertion produced a relabelled copy of the DailyMed one —
    a disease mention_source of ORANGEBOOK for a disease Orange Book never saw, on a
    DailyMed document. The artifacts already live on Drug.approvals.
    """
    from medic.merge.on_label_merge import _build_source_assertions

    sources = {a["source"] for a in _build_source_assertions(
        _dailymed_record(), fda_artifacts=ARTIFACTS)}
    assert sources == {"DAILYMED"}
    assert not sources & {"ORANGEBOOK", "PURPLEBOOK", "GRLS", "CDE_CHINA"}


def test_an_assertions_regulatory_status_describes_its_own_document_only():
    from medic.merge.on_label_merge import _build_source_assertions

    a = _build_source_assertions(_dailymed_record(), fda_artifacts=ARTIFACTS)[0]
    rs = a.get("regulatory_status")
    assert isinstance(rs, dict)
    assert rs["authority"] == "FDA"          # from the DailyMed row's own USA jurisdiction
    assert rs.get("source") == "DAILYMED"    # not ORANGEBOOK


def test_no_foreign_jurisdiction_leaks_into_the_assertion():
    """I-1: a Russian registration is not evidence for a US indication."""
    from medic.merge.on_label_merge import _build_source_assertions

    a = _build_source_assertions(_dailymed_record(), fda_artifacts=ARTIFACTS)[0]
    assert a.get("jurisdiction") == "USA"
    assert (a.get("evidence") or {}).get("jurisdiction") == "USA"


def test_every_span_carries_the_assertions_document_whatever_the_source():
    """This used to hardcode the DailyMed setid, so EMA/PMDA/India spans had no document."""
    from medic.merge.on_label_merge import _build_source_assertions

    ema = {
        "final_normalized_drug_id": "CHEBI:6437", "final_normalized_drug_label": "levetiracetam",
        "final_normalized_disease_id": "MONDO:0005027", "final_normalized_disease_label": "epilepsy",
        "relationship_type": "INDICATION", "source": "EMA",
        "indications_text": "Keppra is indicated as monotherapy for epilepsy.",
        "evidence": [{"source_type": "REGULATORY", "jurisdiction": "EU", "source_role": "PRIMARY",
                      "original_disease_label": "epilepsy", "original_drug_label": "levetiracetam",
                      "snippet": "Keppra is indicated as monotherapy for epilepsy.",
                      "reference": "https://www.ema.europa.eu/en/medicines/human/EPAR/keppra"}],
    }
    a = _build_source_assertions(ema)[0]
    assert a["document"] == "EMA:keppra"
    assert a["spans"], "EMA record should produce a span"
    for s in a["spans"]:
        assert s["document"] == a["document"]


def test_an_explicit_document_id_from_the_ingester_wins():
    """PMDA names the exact approval row it read; that must key the assertion."""
    rec = {"final_normalized_drug_id": "CHEBI:1", "final_normalized_disease_id": "MONDO:1",
           "relationship_type": "INDICATION", "source": "PMDA"}
    assert _document_for(rec, {"document_id": "PMDA:PEMBROLIZUMAB#9-20211125"}) == \
        "PMDA:PEMBROLIZUMAB#9-20211125"


def test_two_approvals_of_one_drug_are_separate_documents():
    rec = {"final_normalized_drug_id": "CHEBI:1", "final_normalized_disease_id": "MONDO:1",
           "relationship_type": "INDICATION", "source": "PMDA"}
    a = _make_key({**rec, "evidence": [{"document_id": "PMDA:X#3-20160928"}]})
    b = _make_key({**rec, "evidence": [{"document_id": "PMDA:X#9-20211125"}]})
    assert a != b
