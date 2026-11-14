#!/usr/bin/env bash
# Install Microsoft ODBC Driver 18 for SQL Server on an Ubuntu runner and prove
# pyodbc can see it.
#
# The product dials SQL Server through pyodbc + "ODBC Driver 18 for SQL Server"
# and falls back to pymssql (FreeTDS) without it. FreeTDS cannot honour
# Encrypt / TrustServerCertificate, so the SQL Server matrix template
# (encrypt=yes) is refused on a runner without the driver and every cell
# skips as "not reachable". A job that proves SQL Server must therefore fail
# here when the driver is missing, not skip later.
#
# The apt source follows the runner's own release (ubuntu-latest is 24.04
# today); the old hard-coded jammy source installed 22.04 packages on noble.
# Usage: install_msodbcsql18.sh            (run after `pip install` to also
#                                           check pyodbc; set SKIP_PYODBC_CHECK=1 to skip)
set -euo pipefail

DRIVER="ODBC Driver 18 for SQL Server"

if ! odbcinst -q -d -n "$DRIVER" >/dev/null 2>&1; then
  # shellcheck disable=SC1091
  . /etc/os-release
  if ! apt-cache policy msodbcsql18 2>/dev/null | grep -q 'Candidate: [0-9]'; then
    # --batch/--yes: runners have no TTY; gpg otherwise prompts and fails.
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/${VERSION_ID}/prod ${VERSION_CODENAME} main" \
      | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null
  fi
  sudo apt-get update -q
  sudo ACCEPT_EULA=Y apt-get install -y -q msodbcsql18 unixodbc-dev
fi

odbcinst -q -d -n "$DRIVER" >/dev/null || {
  echo "$DRIVER is not registered with unixODBC after install" >&2
  exit 1
}
echo "unixODBC: $DRIVER registered"

if [ "${SKIP_PYODBC_CHECK:-0}" != "1" ]; then
  python - <<'PY'
import sys

try:
    import pyodbc
except ImportError as exc:  # requirements.txt pins pyodbc; missing means a broken install
    sys.exit(f"pyodbc is not importable: {exc}")
drivers = pyodbc.drivers()
if "ODBC Driver 18 for SQL Server" not in drivers:
    sys.exit(f"pyodbc does not list ODBC Driver 18 for SQL Server (sees {drivers!r})")
print("pyodbc: ODBC Driver 18 for SQL Server visible")
PY
fi
