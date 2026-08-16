#!/usr/bin/env python3
"""Write a research association YAML file from JSON input on stdin.

Reads a JSON object with the structure:
{
    "disease_id": "MONDO:0008087",
    "disease_label": "hereditary neuropathy with liability to pressure palsies",
    "drugs": [
        {
            "drug_label": "pregabalin",
            "notes": "First-line gabapentinoid for HNPP neuropathic pain.",
            "evidence": [
                {
                    "reference": "PMID:39839199",
                    "source_type": "LITERATURE",
                    "explanation": "Meta-analysis showed pregabalin superior...",
                    "confidence": "MEDIUM",
                    "evidence_source": "HUMAN_CLINICAL"
                }
            ]
        }
    ]
}

Resolves drug IDs via the cascade grounding service and writes
kb/research/MONDO_XXXX.yaml with proper YAML formatting.

Usage:
    echo '{"disease_id":"MONDO:0008087",...}' | python scripts/write_research_yaml.py
    python scripts/write_research_yaml.py < input.json
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import yaml

COMMIT_HASH = "f60076dc17a9c62e51e4acfaf83f6566b7b0b1cb"
CURATOR_ID = f"https://github.com/monarch-initiative/medic/blob/{COMMIT_HASH}/.claude/skills/medic-research-curation/SKILL.md"
KB_DIR = Path("kb/research")


def ground_drugs(drug_names: list[str]) -> dict[str, tuple[str, str]]:
    """Resolve drug names to (id, label) via cascade grounding."""
    from medic.grounding.factory import get_grounding_service

    svc = get_grounding_service("cascade")
    results = {}
    for name in drug_names:
        result = svc.ground_drug_best(name)
        if result:
            results[name] = (result.id, result.label)
        else:
            results[name] = ("", name)
    return results


def build_yaml(data: dict, grounded: dict[str, tuple[str, str]]) -> dict:
    """Build the ResearchAssociationList dict."""
    disease_id = data["disease_id"]
    disease_label = data["disease_label"]
    provider = data.get("provider", "Perplexity")

    curator_agent = {
        "curator_id": CURATOR_ID,
        "curator_type": "AI_AGENT",
        "name": f"MEDIC research skill extracting evidence from {provider} deep research",
    }

    associations = []
    for drug in data["drugs"]:
        drug_name = drug["drug_label"]
        drug_id, canonical_label = grounded.get(drug_name, ("", drug_name))

        evidence_items = []
        for ev in drug.get("evidence", []):
            item = {
                "source_type": ev.get("source_type", "DATABASE"),
                "reference": ev["reference"],
                "explanation": ev.get("explanation", ""),
                "confidence": ev.get("confidence", "LOW"),
                "evidence_source": ev.get("evidence_source", "HUMAN_CLINICAL"),
                "curator": curator_agent,
            }
            if ev.get("reference_title"):
                item["reference_title"] = ev["reference_title"]
            evidence_items.append(item)

        assoc = {
            "drug_id": drug_id,
            "drug_label": canonical_label if drug_id else drug_name,
            "disease_id": disease_id,
            "disease_label": disease_label,
            "curation_status": "DRAFT",
            "curation_date": datetime.now().isoformat(),
            "curator": "medic-research-skill",
            "deep_research_used": True,
            "notes": drug.get("notes", ""),
            "evidence": evidence_items,
        }
        associations.append(assoc)

    return {"associations": associations}


def main():
    data = json.load(sys.stdin)
    disease_id = data["disease_id"]

    # Ground all drug names
    drug_names = [d["drug_label"] for d in data["drugs"]]
    print(f"Grounding {len(drug_names)} drugs for {disease_id}...", file=sys.stderr)
    grounded = ground_drugs(drug_names)

    for name, (did, label) in grounded.items():
        status = did if did else "UNRESOLVED"
        print(f"  {name} -> {status}", file=sys.stderr)

    # Build and write YAML
    result = build_yaml(data, grounded)
    KB_DIR.mkdir(parents=True, exist_ok=True)
    filename = disease_id.replace(":", "_") + ".yaml"
    output_path = KB_DIR / filename

    content = yaml.dump(
        result, default_flow_style=False, allow_unicode=True, width=120,
    )
    # Strip non-printable characters
    content = "".join(c for c in content if c == "\n" or c == "\t" or ord(c) >= 32)
    output_path.write_text(content)

    n_grounded = sum(1 for _, (did, _) in grounded.items() if did)
    print(f"Written {len(data['drugs'])} associations to {output_path} ({n_grounded}/{len(drug_names)} grounded)")


if __name__ == "__main__":
    main()
