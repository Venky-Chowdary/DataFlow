"""start_ci_databases.sh reports an engine ready only when its real database answers.

PR 422 CI (base script): `start_ci_databases.sh postgres-cdc` printed
"Postgres CDC ready", then psql failed with FATAL: database "dataflow" does not
exist. pg_isready over the container's unix socket answers while the image's
init runs a temporary server (listen_addresses='', before CREATE DATABASE). So
the check passed before the database existed. Every engine now waits for its
real database / user (bounded), not just for the server process.

The functional tests run the script against a stub `docker` that reproduces
the race: the unix-socket probe is "ready" at once, while the database answers
over TCP only from the third probe on.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
SCRIPT = REPO / ".github" / "scripts" / "start_ci_databases.sh"


def _function(name: str) -> str:
    text = SCRIPT.read_text(encoding="utf-8")
    match = re.search(rf"^{name}\(\) \{{\n(.*?)^\}}", text, flags=re.MULTILINE | re.DOTALL)
    assert match, name
    return match.group(1)


@pytest.mark.parametrize(
    ("starter", "probe"),
    [
        ("start_postgres", "pg_db_ready dataflow-ci-postgres"),
        ("start_postgres_cdc", "pg_db_ready dataflow-postgres-cdc"),
        ("start_mysql", "mysql_db_ready"),
        ("start_mongo", "mongo_db_ready"),
        ("start_redis", "redis_db_ready"),
        ("start_sqlserver", "mssql_sqlcmd -d dataflow"),
        ("start_bigquery", "/datasets/dataflow"),
    ],
)
def test_every_engine_waits_for_its_real_database(starter: str, probe: str) -> None:
    body = _function(starter)
    assert probe in body, f"{starter} must wait for {probe!r}"
    assert "wait_for" in body


def test_the_postgres_probe_uses_tcp_and_the_dataflow_database() -> None:
    body = _function("pg_db_ready")
    assert "pg_isready -h 127.0.0.1" in body and "-d dataflow" in body
    assert "psql -h 127.0.0.1 -U dataflow -d dataflow" in body and "SELECT 1" in body


def test_the_mysql_probe_logs_in_as_the_app_user_to_its_database() -> None:
    body = _function("mysql_db_ready")
    assert "-u dataflow" in body and "-D dataflow" in body and "SELECT 1" in body


# ── functional: the race, against a stub docker ───────────────────────────

STUB = r"""#!/usr/bin/env bash
# Stub docker: the DB answers over TCP only from the 3rd probe on.
state="$STUB_STATE"
echo "$*" >> "$state/calls"
case "$1" in
  pull|tag|build|rm|run|logs|inspect) exit 0 ;;
  exec) ;;
  *) exit 0 ;;
esac
args="$*"
n=$(cat "$state/n" 2>/dev/null || echo 0)
if [[ "$args" == *pg_isready* && "$args" != *"-h 127.0.0.1"* ]]; then
  exit 0                                # the unix socket says "ready" at once
fi
if [[ "$args" == *pg_isready* ]]; then
  echo $((n + 1)) > "$state/n"
  [ "$n" -ge 2 ] && exit 0 || exit 2
fi
if [[ "$args" == *"SHOW wal_level"* ]]; then
  [ "$n" -ge 3 ] && { echo logical; exit 0; }
  echo 'FATAL:  database "dataflow" does not exist' >&2; exit 2
fi
if [[ "$args" == *"SELECT 1"* ]]; then
  [ "$n" -ge 3 ] && { echo 1; exit 0; }
  echo 'FATAL:  database "dataflow" does not exist' >&2; exit 2
fi
exit 0
"""


def _run(tmp_path: Path, *services: str) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("docker", STUB), ("sleep", "#!/bin/sh\nexit 0\n")):
        path = bin_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    state = tmp_path / "state"
    state.mkdir()
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STUB_STATE": str(state)}
    return subprocess.run(
        ["bash", str(SCRIPT), *services], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
    )


@pytest.mark.parametrize(("service", "label"), [("postgres-cdc", "Postgres CDC"), ("postgres", "Postgres")])
def test_postgres_is_ready_only_once_the_dataflow_database_answers(tmp_path: Path, service: str, label: str) -> None:
    proc = _run(tmp_path, service)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert f"{label} ready" in proc.stdout
    calls = (tmp_path / "state" / "calls").read_text(encoding="utf-8").splitlines()
    tcp_probes = [c for c in calls if "pg_isready -h 127.0.0.1" in c]
    assert len(tcp_probes) >= 3, "ready was claimed before the database answered over TCP"
    if service == "postgres-cdc":
        assert "wal_level=logical" in proc.stdout


def test_a_database_that_never_appears_fails_bounded(tmp_path: Path) -> None:
    stub = tmp_path / "never"
    stub.mkdir()
    proc = _run(stub, "postgres")
    assert proc.returncode == 0  # the stub's DB appears on probe 3; now make it never appear
    never = tmp_path / "never2"
    never.mkdir()
    bin_dir = never / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\ncase "$1" in exec) exit 2 ;; *) exit 0 ;; esac\n', encoding="utf-8"
    )
    (bin_dir / "sleep").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    for p in bin_dir.iterdir():
        p.chmod(p.stat().st_mode | stat.S_IXUSR)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    proc = subprocess.run(
        ["bash", str(SCRIPT), "postgres"], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
    )
    assert proc.returncode != 0
    assert "Postgres failed to become ready" in proc.stderr
