#!/usr/bin/env python3
"""Ground drug names to CHEBI IDs using the cascade grounding service.

Usage:
    python scripts/ground_drugs.py pregabalin gabapentin venlafaxine
    python scripts/ground_drugs.py "ascorbic acid" "lithium carbonate"
"""
import sys

from medic.grounding.factory import get_grounding_service

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/ground_drugs.py DRUG1 DRUG2 ...", file=sys.stderr)
        sys.exit(1)

    svc = get_grounding_service("cascade")
    for drug in sys.argv[1:]:
        result = svc.ground_drug_best(drug)
        if result:
            print(f"{drug}\t{result.id}\t{result.label}\t{result.score}")
        else:
            print(f"{drug}\tUNRESOLVED\t\t")


if __name__ == "__main__":
    main()
