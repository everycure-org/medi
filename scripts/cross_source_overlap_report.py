"""Cross-source overlap report for indication and drug lists.

Reports per-authority counts, multi-authority associations, and regulatory
document URL coverage.
"""

from collections import Counter
from pathlib import Path

import yaml


def report_indications(path: Path) -> None:
    if not path.exists():
        print(f"SKIP: {path} not found")
        return
    with open(path) as f:
        data = yaml.safe_load(f)
    associations = data.get("associations", [])
    total = len(associations)

    authority_pairs: dict[str, set] = {}
    multi_authority = 0
    has_url = 0
    has_regulatory_status = 0
    primary_count = 0
    intermediary_count = 0

    for assoc in associations:
        authorities = set()
        # New schema: regulatory_status list
        for status in assoc.get("regulatory_status", []) or []:
            auth = status.get("authority")
            if auth:
                authorities.add(auth)
                if status.get("regulatory_document_url", "").startswith("http"):
                    has_url += 1
                    break  # count each association once
        # Legacy booleans
        if assoc.get("fda"):
            authorities.add("FDA")
        if assoc.get("ema"):
            authorities.add("EMA")
        if assoc.get("pmda"):
            authorities.add("PMDA")
        if assoc.get("regulatory_status"):
            has_regulatory_status += 1
        for auth in authorities:
            authority_pairs.setdefault(auth, set()).add(
                (assoc.get("final_normalized_drug_id", ""), assoc.get("final_normalized_disease_id", ""))
            )
        if len(authorities) > 1:
            multi_authority += 1

        for ev in assoc.get("evidence", []) or []:
            role = ev.get("source_role")
            if role == "PRIMARY":
                primary_count += 1
            elif role == "INTERMEDIARY":
                intermediary_count += 1

    print(f"\n=== {path} ===")
    print(f"Total associations: {total}")
    print("Per authority (drug-disease pair counts):")
    for auth in ("FDA", "EMA", "PMDA", "CDSCO", "MOH_RUSSIA", "NMPA_CHINA"):
        if auth in authority_pairs:
            print(f"  {auth}: {len(authority_pairs[auth])}")
    print(f"Multi-authority associations: {multi_authority}")
    print(f"With regulatory_status field: {has_regulatory_status} ({100*has_regulatory_status/total:.1f}%)")
    print(f"With direct regulatory document URL: {has_url} ({100*has_url/total:.1f}%)")
    print(f"Evidence with source_role=PRIMARY: {primary_count}")
    print(f"Evidence with source_role=INTERMEDIARY: {intermediary_count}")

    if "FDA" in authority_pairs and "EMA" in authority_pairs:
        overlap_fe = authority_pairs["FDA"] & authority_pairs["EMA"]
        print(f"FDA-EMA pair overlap: {len(overlap_fe)}")
    if "FDA" in authority_pairs and "PMDA" in authority_pairs:
        overlap_fp = authority_pairs["FDA"] & authority_pairs["PMDA"]
        print(f"FDA-PMDA pair overlap: {len(overlap_fp)}")
    if "EMA" in authority_pairs and "PMDA" in authority_pairs:
        overlap_ep = authority_pairs["EMA"] & authority_pairs["PMDA"]
        print(f"EMA-PMDA pair overlap: {len(overlap_ep)}")

    # Disease ID prefix breakdown
    prefixes = Counter()
    for assoc in associations:
        did = str(assoc.get("final_normalized_disease_id", ""))
        if did:
            prefixes[did.split(":")[0]] += 1
    print("Disease ID prefixes:")
    for prefix, count in prefixes.most_common(8):
        print(f"  {prefix}: {count} ({100*count/total:.1f}%)")


def report_drugs(path: Path) -> None:
    if not path.exists():
        print(f"SKIP: {path} not found")
        return
    with open(path) as f:
        data = yaml.safe_load(f)
    drugs = data.get("drugs", [])
    total = len(drugs)

    has_evidence = sum(1 for d in drugs if d.get("evidence"))
    has_url_evidence = sum(
        1 for d in drugs
        if any(str(ev.get("reference", "")).startswith("http") for ev in d.get("evidence", []) or [])
    )
    fda_url = sum(
        1 for d in drugs
        if any("accessdata.fda.gov" in str(ev.get("reference", "")) for ev in d.get("evidence", []) or [])
    )
    epar_url = sum(
        1 for d in drugs
        if any("/medicines/human/EPAR/" in str(ev.get("reference", "")) for ev in d.get("evidence", []) or [])
    )

    print(f"\n=== {path} ===")
    print(f"Total drugs: {total}")
    print(f"With evidence: {has_evidence} ({100*has_evidence/total:.1f}%)")
    print(f"With URL evidence: {has_url_evidence} ({100*has_url_evidence/total:.1f}%)")
    print(f"With FDA accessdata URL (NDA-based): {fda_url}")
    print(f"With EMA EPAR URL: {epar_url}")


if __name__ == "__main__":
    report_indications(Path("products/indication_list.yaml"))
    report_indications(Path("products/contraindication_list.yaml"))
    report_drugs(Path("products/drug_list.yaml"))
