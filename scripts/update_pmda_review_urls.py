"""In-place update of PMDA evidence URLs from the (improved) review-URL index.

Use after improving `pmda.review_lookup.build_index` heuristics. Avoids
re-running the full PMDA ingest (which redoes LLM disease extraction).

Reads kb/indications/pmda/indications.yaml, looks up each drug's brand_name
or normalized_label in the review index, attaches `product_id` + the deep-link
`reference` URL where one is available; leaves search-URL fallbacks unchanged.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from medic.ingest.pmda.review_lookup import build_index, lookup_review

logger = logging.getLogger(__name__)

INDICATIONS_PATH = Path("kb/indications/pmda/indications.yaml")
DRUGS_PATH = Path("kb/drugs/pmda/pmda.yaml")


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    # Force a fresh index build using the latest scraping logic
    build_index(force=True)

    # Build curie -> brand_name map from kb/drugs/pmda
    brand_map: dict[str, str] = {}
    if DRUGS_PATH.exists():
        recs = yaml.safe_load(open(DRUGS_PATH)) or []
        for r in recs:
            curie = r.get("normalized_id", "")
            if not curie:
                continue
            brand = (r.get("brand_name", "") or "").split(" | ")[0].strip()
            if brand:
                brand_map[curie] = brand

    if not INDICATIONS_PATH.exists():
        logger.error("Missing %s", INDICATIONS_PATH)
        return

    records = yaml.safe_load(open(INDICATIONS_PATH)) or []
    if not isinstance(records, list):
        logger.error("Expected a list at %s", INDICATIONS_PATH)
        return

    upgraded = 0
    already_deep_linked = 0
    no_match = 0

    for rec in records:
        drug_id = rec.get("final_normalized_drug_id", "")
        drug_label = rec.get("final_normalized_drug_label", "")
        evidence_items = rec.get("evidence", []) or []

        # Try brand name first, then INN
        brand = brand_map.get(drug_id, "")
        review = lookup_review(brand) if brand else {}
        if not review:
            review = lookup_review(drug_label) or {}

        for ev in evidence_items:
            if (ev.get("source_type") or "").upper() != "REGULATORY":
                continue
            if (ev.get("jurisdiction") or "").upper() != "JAPAN":
                continue
            ref = ev.get("reference", "")
            if review and review.get("url"):
                if ref != review["url"] or ev.get("product_id") != review.get("product_id"):
                    ev["reference"] = review["url"]
                    ev["product_id"] = review.get("product_id", "")
                    ev["confidence"] = "HIGH"
                    ev["explanation"] = "PMDA-approved indication; per-product review report"
                    upgraded += 1
                else:
                    already_deep_linked += 1
            else:
                if "search" in ref:
                    no_match += 1

    out = yaml.dump(records, default_flow_style=False, allow_unicode=True, width=1000)
    out = "".join(c for c in out if c == "\n" or c == "\t" or ord(c) >= 32)
    INDICATIONS_PATH.with_suffix(".yaml.tmp").write_text(out)
    INDICATIONS_PATH.with_suffix(".yaml.tmp").replace(INDICATIONS_PATH)

    logger.info(
        "PMDA URL update: %d upgraded, %d already deep-linked, %d still on search",
        upgraded, already_deep_linked, no_match,
    )


if __name__ == "__main__":
    main()
