"""start_ci_databases.sh reports an engine ready only when the tests' database answers.

PR #428 fixed PostgreSQL: the image's init runs a temporary server on the Unix
socket only, before POSTGRES_DB exists, so `pg_isready` passed early and the
next query failed with 'database "dataflow" does not exist'. Its readiness is
`pg_serving`: SELECT 1 on that database over TCP.

The other engines had the same gap:
- MySQL: the image's init also runs a temporary server (--skip-networking).
- Mongo: the shell's ping answered before the replica-set URI the tests use did.
- Redis: `redis-cli ping` exits 0 on an error reply such as "LOADING".
- SQL Server: master answered before the tests' "dataflow" database, which
  the CT init creates.
- BigQuery: the emulator's discovery document answered before the
  --dataset=dataflow the tests write to.

Each now has one `<engine>_serving` check in pg_serving's style.

The functional tests run the script against a stub `docker` (and `curl`). The
server-level probes answer at once, while each engine's serving probe fails
until its third call, which reproduces the race. "ready" may only be printed
after the third serving probe, and the steps that need the database (grants,
replica-set reads, the CDC init) may only run after it.
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
    match = re.search(rf"^{name}\(\) \{{[^\n]*\n(.*?)^\}}", text, flags=re.MULTILINE | re.DOTALL)
    assert match, name
    return match.group(1)


@pytest.mark.parametrize(
    ("starter", "check"),
    [
        ("start_postgres", "pg_serving dataflow-ci-postgres"),
        ("start_postgres_cdc", "pg_serving dataflow-postgres-cdc"),
        ("start_mysql", "mysql_serving"),
        ("start_mongo", "mongo_serving"),
        ("start_redis", "redis_serving"),
        ("start_sqlserver", "mssql_serving"),
        ("start_bigquery", "bigquery_serving"),
    ],
)
def test_every_engine_waits_on_its_serving_check(starter: str, check: str) -> None:
    body = _function(starter)
    assert re.search(rf"wait_for [^\n]*{re.escape(check)}", body), f"{starter} must wait_for {check}"


@pytest.mark.parametrize(
    ("helper", "probe"),
    [
        ("mysql_serving", ["-h 127.0.0.1", "-u dataflow", "-D dataflow", "SELECT 1"]),
        ("mongo_serving", ["mongodb://localhost:27017/dataflow?replicaSet=rs0", "isWritablePrimary"]),
        ("redis_serving", ["-n 0 ping", "PONG"]),
        ("mssql_serving", ["-d dataflow", "SELECT 1"]),
        ("bigquery_serving", ["/projects/dataflow-test/datasets/dataflow"]),
    ],
)
def test_each_serving_check_asks_the_tests_database(helper: str, probe: list[str]) -> None:
    body = _function(helper)
    for part in probe:
        assert part in body, f"{helper} must use {part!r}"


def test_postgres_readiness_is_428s_pg_serving_only() -> None:
    """One PostgreSQL implementation (PR #428), not a second copy."""
    text = SCRIPT.read_text(encoding="utf-8")
    assert text.count("pg_serving() {") == 1
    assert "pg_db_ready" not in text


def test_no_password_is_on_a_command_line() -> None:
    body = _function("mysql_serving")
    assert "-pdataflow" not in body and "MYSQL_PWD" in body


# ── functional: the race, against a stub docker / curl ────────────────────

STUB = r"""#!/usr/bin/env bash
# Every call is logged. A call matching $STUB_SERVING fails until its 3rd try;
# every other call succeeds at once (the server process is "up").
state="$STUB_STATE"
args="$(basename "$0") $*"
echo "$args" >> "$state/calls"
case "$1" in pull|tag|build|rm|run|logs|inspect|cp) exit 0 ;; esac
if [[ "$args" == *"$STUB_SERVING"* ]]; then
  n=$(( $(cat "$state/n" 2>/dev/null || echo 0) + 1 ))
  echo "$n" > "$state/n"
  echo "SERVING-PROBE $n" >> "$state/calls"
  if [ "$n" -ge 3 ]; then
    [[ "$args" == *ping* ]] && echo PONG
    [[ "$args" == *sqlcmd* || "$args" == *mysql* ]] && echo 1
    exit 0
  fi
  if [[ "$args" == *redis-cli* ]]; then
    echo "LOADING Redis is loading the dataset in memory"; exit 0   # redis-cli exits 0 on an error reply
  fi
  exit 1
fi
[[ "$args" == *dm_server_services* ]] && echo Running
exit 0
"""


def _run(tmp_path: Path, service: str, serving: str, extra_env: dict | None = None) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("docker", STUB), ("curl", STUB), ("sleep", "#!/bin/sh\nexit 0\n")):
        path = bin_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    state = tmp_path / "state"
    state.mkdir()
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STUB_STATE": str(state),
           "STUB_SERVING": serving, **(extra_env or {})}
    env.pop("GITHUB_ENV", None)
    proc = subprocess.run(
        ["bash", str(SCRIPT), service], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
    )
    calls = (state / "calls").read_text(encoding="utf-8").splitlines() if (state / "calls").exists() else []
    return proc, calls


@pytest.mark.parametrize(
    ("service", "serving", "label", "needs_db", "extra_env"),
    [
        ("mysql", "-D dataflow", "MySQL", "GRANT", None),
        ("mongo", "replicaSet=rs0", "Mongo", "printjson", None),
        ("redis", "-n 0 ping", "Redis", None, None),
        ("bigquery", "/datasets/dataflow", "BigQuery", "inspect", None),
        ("sqlserver", "-d dataflow", "SQL Server dataflow database", None, {"MSSQL_SA_PASSWORD": "stub-Passw0rd"}),
    ],
)
def test_ready_only_once_the_tests_database_answers(
    tmp_path: Path, service: str, serving: str, label: str, needs_db: str | None, extra_env: dict | None
) -> None:
    proc, calls = _run(tmp_path, service, serving, extra_env)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert f"{label} ready" in proc.stdout, proc.stdout
    probes = [i for i, c in enumerate(calls) if c.startswith("SERVING-PROBE")]
    assert len(probes) >= 3, f"{label} was reported ready before its database answered: {calls}"
    if needs_db:
        later = [i for i, c in enumerate(calls) if needs_db in c and not c.startswith("SERVING-PROBE")]
        assert later and min(later) > probes[2], f"{needs_db!r} ran before {label} was serving"


def test_a_database_that_never_answers_fails_bounded(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\ncase "$1" in exec) exit 1 ;; *) exit 0 ;; esac\n', encoding="utf-8"
    )
    (bin_dir / "sleep").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    for p in bin_dir.iterdir():
        p.chmod(p.stat().st_mode | stat.S_IXUSR)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    env.pop("GITHUB_ENV", None)
    proc = subprocess.run(
        ["bash", str(SCRIPT), "mysql"], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
    )
    assert proc.returncode != 0
    assert "MySQL failed to become ready" in proc.stderr
