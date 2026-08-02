#!/usr/bin/env bash
# Demo-host environment for macOS Homebrew Python + object-store / SQL Server.
# Usage: source apps/api/scripts/demo_host_env.sh
# Then start the API or run pytest from the same shell.
set -euo pipefail

EXPAT_LIB="$(brew --prefix expat 2>/dev/null)/lib"
if [[ -d "${EXPAT_LIB}" ]]; then
  export DYLD_LIBRARY_PATH="${EXPAT_LIB}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
  echo "demo_host_env: DYLD_LIBRARY_PATH includes Homebrew expat (${EXPAT_LIB})"
else
  echo "demo_host_env: WARNING — Homebrew expat not found; S3/GCS/ADLS may fail pyexpat" >&2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "${ROOT}/.venv/bin/python" ]]; then
  # shellcheck disable=SC1091
  # Prefer the API venv when present.
  export PATH="${ROOT}/.venv/bin:${PATH}"
  echo "demo_host_env: PATH prefers ${ROOT}/.venv/bin"
fi
