#!/usr/bin/env bash
set -euo pipefail

# Fetch sites from the IHS/IoT API and write the JSON response to a file in the repo root.
#
# Prereqs:
#   export IHS_API_BASE_URL="https://<host>/api"
#   export IHS_API_TOKEN="..."
#
# Usage:
#   bash scripts/fetch_sites.sh

: "${IHS_API_BASE_URL:?IHS_API_BASE_URL is required}"
: "${IHS_API_TOKEN:?IHS_API_TOKEN is required}"

OUT_FILE="${OUT_FILE:-./ihs_sites.json}"
PAGE="${PAGE:-1}"
PER_PAGE="${PER_PAGE:-100}"

curl -sS -G \
  "${IHS_API_BASE_URL%/}/sites" \
  --data-urlencode "page=${PAGE}" \
  --data-urlencode "per_page=${PER_PAGE}" \
  --data-urlencode "X-Access-Token=${IHS_API_TOKEN}" \
  -o "${OUT_FILE}"

echo "Wrote sites response to ${OUT_FILE}"
