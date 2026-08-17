#!/usr/bin/env bash
# obtain_raw_sources.sh — Download/extract all raw source data for MeDIC drug pipeline
#
# Sources that can be downloaded programmatically:
#   - Orange Book: FDA ZIP download
#   - Purple Book: FDA CSV download (URL contains date, may need updating)
#   - EMA: EMA XLSX download
#
# Sources that use pre-grounded data from previous MeDIC pipeline (main branch):
#   - PMDA (Japan): git show main:medi/data/drugs/02_intermediate/pmda/pmda_norm.xlsx
#   - Russia: git show main:medi/data/drugs/02_intermediate/russia/russia_norm.csv
#   - India: git show main:medi/data/drugs/02_intermediate/india/india_norm.csv
#
# Usage:
#   bash scripts/obtain_raw_sources.sh
#
# Prerequisites:
#   - git (with access to main branch)
#   - The medi/ submodule data on the main branch

set -euo pipefail

echo "=== MeDIC Raw Source Data Acquisition ==="
echo ""

# Create directories
mkdir -p data/raw/{orangebook,purplebook,ema,pmda,russia,india}

# --- Orange Book (FDA) ---
echo "--- Orange Book ---"
echo "  Downloaded automatically by the ingest module from:"
echo "  https://www.fda.gov/media/76860/download"
echo "  -> data/raw/orangebook/products.txt"
echo ""

# --- Purple Book (FDA) ---
echo "--- Purple Book ---"
echo "  NOTE: The FDA Purple Book CSV URL contains a date and changes periodically."
echo "  The current URL in conf/source_urls.yaml may be stale."
echo "  Downloaded automatically by the ingest module, with fallback to pre-grounded data."
echo ""

# --- EMA ---
echo "--- EMA ---"
echo "  Downloaded automatically by the ingest module from:"
echo "  https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"
echo "  -> cache/downloads/ema/ema_medicines.xlsx"
echo "  NOTE: The EMA XLSX has metadata rows 0-7; the parser auto-detects the header row."
echo ""

# --- PMDA (Japan) - Pre-grounded from main branch ---
echo "--- PMDA (Japan) ---"
if [ -f data/raw/pmda/pmda_norm.xlsx ]; then
    echo "  Already exists: data/raw/pmda/pmda_norm.xlsx"
else
    echo "  Extracting from main branch..."
    git show main:medi/data/drugs/02_intermediate/pmda/pmda_norm.xlsx > data/raw/pmda/pmda_norm.xlsx
    echo "  -> data/raw/pmda/pmda_norm.xlsx ($(wc -c < data/raw/pmda/pmda_norm.xlsx) bytes)"
fi
echo "  Source: Pre-grounded via NameRes/NodeNorm in previous MeDIC pipeline"
echo "  Original upstream: https://www.pmda.go.jp/files/000278243.pdf (PDF, would need parsing)"
echo ""

# --- Russia - Pre-grounded from main branch ---
echo "--- Russia ---"
if [ -f data/raw/russia/russia_norm.csv ]; then
    echo "  Already exists: data/raw/russia/russia_norm.csv"
else
    echo "  Extracting from main branch..."
    git show main:medi/data/drugs/02_intermediate/russia/russia_norm.csv > data/raw/russia/russia_norm.csv
    echo "  -> data/raw/russia/russia_norm.csv ($(wc -l < data/raw/russia/russia_norm.csv) lines)"
fi
echo "  Source: Pre-grounded + translated in previous MeDIC pipeline"
echo "  Original upstream: https://grls.rosminzdrav.ru/GRLS.aspx (manual Excel export, Russian)"
echo ""

# --- India - Pre-grounded from main branch ---
echo "--- India ---"
if [ -f data/raw/india/india_norm.csv ]; then
    echo "  Already exists: data/raw/india/india_norm.csv"
else
    echo "  Extracting from main branch..."
    git show main:medi/data/drugs/02_intermediate/india/india_norm.csv > data/raw/india/india_norm.csv
    echo "  -> data/raw/india/india_norm.csv ($(wc -l < data/raw/india/india_norm.csv) lines)"
fi
echo "  Source: Pre-grounded in previous MeDIC pipeline"
echo "  Original upstream: https://cdsco.gov.in/opencms/opencms/en/Approval_new/Approved-New-Drugs/ (year-by-year PDFs)"
echo ""

echo "=== Done ==="
echo ""
echo "To run the full pipeline:"
echo "  MEDIC_SKIP_EXPENSIVE_CALLS=1 just build-drug-list"
