#!/usr/bin/env bash
# Run the full MeDIC drug pipeline with all enrichment APIs enabled.
# Sources .env for API keys (ANTHROPIC_API_KEY).
set -euo pipefail

cd "$(dirname "$0")/.."

# Load API keys
if [ -f .env ]; then
    set -a
    source .env
    set +a
    echo "Loaded .env"
fi

echo "ANTHROPIC_API_KEY: $([ -n "${ANTHROPIC_API_KEY:-}" ] && echo SET || echo NOT SET)"

echo ""
echo "=== Running merge + enrichment ==="
uv run python -m medic.merge.drug_merge

echo ""
echo "=== Running legacy export ==="
uv run python -m medic.export.legacy

echo ""
echo "=== Generating comparison report ==="
uv run python tmp/generate_report.py

echo ""
echo "=== Done ==="
