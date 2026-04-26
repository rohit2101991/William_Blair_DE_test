#!/usr/bin/env bash
# Same metadata as CLI materializations: ~/.dagster (not Dagster's temp dev folder).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export DAGSTER_HOME="${HOME}/.dagster"
mkdir -p "$DAGSTER_HOME"
cd "$ROOT"
exec "$ROOT/.venv/bin/dagster" dev -h 127.0.0.1 -p 3333 "$@"
