"""Read-side accessors for the transformation-provenance product shape.

The v2.0 products model a drug's identity as a :class:`Mention` (``mention.resolved_id`` /
``mention.resolved_label``) and its approvals as a ``RegulatoryStatus`` list
(``approvals[].authority``); an association references the drug as a light ``DrugRef``
(``drug.id`` / ``drug.label``) and inlines the disease as a ``Mention``
(``disease.resolved_id``). These helpers give every consumer (exports, coverage,
reliability) one place to read those fields, so the flat→provenance mapping lives here and
not smeared across modules. Each accessor tolerates a partially-populated record (returns
``""`` / empty set) rather than raising.
"""

from __future__ import annotations

# Authority enum (authority.yaml RegulatoryAuthorityEnum) -> the legacy jurisdiction slug
# used by the flat ``approved_<jurisdiction>`` columns and reliability jurisdictions.
AUTHORITY_TO_JURISDICTION = {
    "FDA": "usa",
    "EMA": "europe",
    "PMDA": "japan",
    "CDSCO": "india",
    "MOH_RUSSIA": "russia",
    "NMPA_CHINA": "china",
}


def _mention(record: dict) -> dict:
    m = record.get("identity")
    return m if isinstance(m, dict) else {}


def drug_id(drug: dict) -> str:
    """Resolved canonical id of a Drug record (``identity.resolved_id``)."""
    return _mention(drug).get("resolved_id") or ""


def drug_label(drug: dict) -> str:
    """Resolved canonical label of a Drug record (``identity.resolved_label``)."""
    return _mention(drug).get("resolved_label") or ""


def approvals(drug: dict) -> list[dict]:
    return [a for a in (drug.get("approvals") or []) if isinstance(a, dict)]


def approved_authorities(drug: dict) -> set[str]:
    """Set of RegulatoryAuthorityEnum values this drug carries an approval for."""
    return {a.get("authority") for a in approvals(drug) if a.get("authority")}


def approved_jurisdictions(drug: dict) -> set[str]:
    """Set of legacy jurisdiction slugs (usa/europe/japan/india/russia/china)."""
    return {
        AUTHORITY_TO_JURISDICTION[a]
        for a in approved_authorities(drug)
        if a in AUTHORITY_TO_JURISDICTION
    }


def is_approved_anywhere(drug: dict) -> bool:
    return bool(approvals(drug))


def marketing_status_usa(drug: dict) -> str:
    """Most permissive FDA marketing status across the drug's US approvals."""
    order = {"OTC": 3, "RX": 2, "DISCN": 1, "NONE": 0}
    best, best_rank = "", -1
    for a in approvals(drug):
        if a.get("authority") != "FDA":
            continue
        status = (a.get("marketing_status") or "").strip()
        rank = order.get(status.upper(), -1)
        if rank > best_rank:
            best, best_rank = status, rank
    return best


def earliest_approval_date(drug: dict) -> str:
    """Earliest approval date (YYYYMMDD) across the drug's approvals, or ''."""
    dates = [
        (a.get("approval_date") or "").strip()
        for a in approvals(drug)
        if (a.get("approval_date") or "").strip()
    ]
    return min(dates) if dates else ""


def application_numbers(drug: dict) -> list[str]:
    """All regulatory application/BLA numbers across the drug's approvals."""
    out: list[str] = []
    for a in approvals(drug):
        for key in ("application_number", "bla_number"):
            val = (a.get(key) or "").strip()
            if val:
                out.append(val)
    return out


# --- IndicationAssociation accessors -------------------------------------------------
# v3.0: the pair carries identity at the top level and all provenance in `assertions`, one per
# source document. These accessors keep every consumer on one read path, so the flat ->
# two-level migration lives here and not smeared across modules.
def assoc_assertions(assoc: dict) -> list[dict]:
    """The pair's SourceAssertions, skipping anything malformed."""
    return [a for a in (assoc.get("assertions") or []) if isinstance(a, dict)]


def assoc_drug_id(assoc: dict) -> str:
    return assoc.get("drug_id") or ""


def assoc_drug_label(assoc: dict) -> str:
    return assoc.get("drug_label") or ""


def assoc_disease_id(assoc: dict) -> str:
    return assoc.get("disease_id") or ""


def assoc_disease_label(assoc: dict) -> str:
    return assoc.get("disease_label") or ""


def assoc_evidence(assoc: dict) -> list[dict]:
    """Every evidence row backing a record, in assertion order.

    On-label pairs keep evidence on ``assertions[].evidence``, one row per source document.
    The research and adverse-event products were never two-level and keep a flat ``evidence``
    list, so that shape is read too — this accessor is what lets `reliability` and
    `reliability_export` stay statement-type agnostic.
    """
    out = []
    for a in assoc_assertions(assoc):
        ev = a.get("evidence")
        if isinstance(ev, dict):
            out.append(ev)
    if out:
        return out
    flat = assoc.get("evidence")
    if isinstance(flat, list):
        return [e for e in flat if isinstance(e, dict)]
    if isinstance(flat, dict):
        return [flat]
    return []


def assoc_authorities(assoc: dict) -> set[str]:
    """RegulatoryAuthorityEnum values backing a pair, across all its assertions."""
    out = set()
    for a in assoc_assertions(assoc):
        rs = a.get("regulatory_status")
        if isinstance(rs, dict) and rs.get("authority"):
            out.add(rs["authority"])
    return out


def assoc_jurisdictions(assoc: dict) -> set[str]:
    """Jurisdictions attesting a pair — the input to cross-source reliability."""
    return {e["jurisdiction"] for e in assoc_evidence(assoc) if e.get("jurisdiction")}


def assoc_mentions(record: dict) -> list[dict]:
    """Every Mention on a record, whatever its shape.

    A Drug carries `identity`. An on-label pair carries `drug` + `disease` on each of its
    assertions. Older/other products inline them at the top level. Consumers that score
    resolution quality (reliability) need all of them without knowing which shape they have.
    """
    out = []
    for key in ("identity", "drug", "disease"):
        m = record.get(key)
        if isinstance(m, dict):
            out.append(m)
    for a in assoc_assertions(record):
        for key in ("drug", "disease"):
            m = a.get(key)
            if isinstance(m, dict):
                out.append(m)
    return out


def assoc_claims(record: dict) -> list[dict]:
    """Every Assertion object on a record — one per source assertion, or one at the top."""
    out = []
    top = record.get("assertion")
    if isinstance(top, dict):
        out.append(top)
    for a in assoc_assertions(record):
        claim = a.get("assertion")
        if isinstance(claim, dict):
            out.append(claim)
    return out
