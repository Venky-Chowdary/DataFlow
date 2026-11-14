"""cdc-sqlserver proves SQL Server through both DBAPI drivers.

Production (Dockerfile.api) ships unixODBC but no Microsoft ODBC driver, so
``connectors.generic_sql._mssql_drivername`` picks ``mssql+pymssql`` there.
CI used to install ODBC Driver 18 and therefore only ever proved pyodbc.
The job now runs once per driver; the pymssql leg runs with no Microsoft
driver registered, exactly like production.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
if str(API) not in sys.path:
    sys.path.insert(0, str(API))
if str(API / "tests") not in sys.path:
    sys.path.insert(0, str(API / "tests"))

from connectors.generic_sql import _mssql_connect_query  # noqa: E402
from test_execute_tracked_universal_matrix import _build_db_endpoint  # noqa: E402


def _sqlserver_extra(tmp_path: Path) -> dict:
    return dict(_build_db_endpoint("sqlserver", tmp_path, "source", "x").extra or {})


def test_default_matrix_template_requires_encryption(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("DATAFLOW_TEST_MSSQL_ENCRYPT", raising=False)
    extra = _sqlserver_extra(tmp_path)
    assert extra.get("encrypt") == "yes"
    assert extra.get("trust_server_certificate") is True
    # FreeTDS cannot honour Encrypt=yes: the product refuses rather than
    # pretending, so the pymssql leg must not declare it.
    with pytest.raises(ValueError, match="ODBC driver"):
        _mssql_connect_query(extra, "mssql+pymssql")


def test_pymssql_leg_template_leaves_encryption_to_freetds(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("DATAFLOW_TEST_MSSQL_ENCRYPT", "")
    extra = _sqlserver_extra(tmp_path)
    assert "encrypt" not in extra
    assert _mssql_connect_query(extra, "mssql+pymssql") == {}


def _job() -> dict:
    ci = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    return ci["jobs"]["cdc-sqlserver"]


def test_cdc_sqlserver_runs_once_per_driver() -> None:
    job = _job()
    assert job["strategy"]["matrix"]["driver"] == ["pyodbc", "pymssql"]
    assert job["strategy"]["fail-fast"] is False
    steps = job["steps"]
    install = next(s for s in steps if "install_msodbcsql18.sh" in str(s.get("run", "")))
    assert install.get("if") == "${{ !cancelled() && matrix.driver == 'pyodbc' }}"
    assert any("_mssql_drivername" in str(s.get("run", "")) for s in steps), "no driver-in-use assertion"
    checker = next(s for s in steps if "check_live_skips.py" in str(s.get("run", "")))
    assert "--label driver=${{ matrix.driver }}" in checker["run"]


def test_pymssql_leg_declares_no_encrypt() -> None:
    env = _job()["env"]
    assert env["DATAFLOW_TEST_MSSQL_ENCRYPT"] == "${{ matrix.driver == 'pyodbc' && 'yes' || '' }}"


# ── PR 424 CI (bdfcf0f2c), cdc-sqlserver (pymssql) ──────────────────────────
# 1. CDC init raced SQL Server Agent: sp_cdc_enable_table -> sp_cdc_add_job
#    failed with 14258 "Cannot perform this operation while SQLServerAgent is
#    starting" (surfaced as Msg 22832) although Agent already read Running.
# 2. That failed the start step, so setup-python / requirements were skipped,
#    and the `!cancelled()` pytest steps ran on /usr/bin/python: "No module
#    named pytest", 0 executed.


def test_python_setup_runs_in_both_legs_even_after_an_earlier_failure() -> None:
    steps = _job()["steps"]
    by_name = {s.get("name", ""): s for s in steps}
    setup = next(s for s in steps if s.get("uses", "").startswith("actions/setup-python"))
    install = next(s for s in steps if "requirements-dev.txt" in str(s.get("run", "")))
    assert setup.get("if") == "${{ !cancelled() }}"
    assert install.get("if") == "${{ !cancelled() }}"
    check = next(s for s in steps if 'import pytest, pymssql' in str(s.get("run", "")))
    assert check.get("id") == "python_ready"
    for name, step in by_name.items():
        if "python -m pytest" in str(step.get("run", "")):
            assert "steps.python_ready.outcome == 'success'" in str(step.get("if", "")), name


def test_cdc_init_waits_for_agent_to_accept_job_operations() -> None:
    script = (REPO / ".github" / "scripts" / "start_ci_databases.sh").read_text(encoding="utf-8")
    start = script.split("start_sqlserver() {", 1)[1].split("\n}\n", 1)[0]
    # Agent must read Running before the CDC init runs ...
    assert start.index('wait_for "SQL Server Agent"') < start.index("mssql_cdc_init")
    assert "-i /tmp/cdc_init.sql" not in start
    # ... which retries while Agent still refuses job operations, bounded at 120 s.
    init = script.split("mssql_cdc_init() {", 1)[1].split("\n}\n", 1)[0]
    assert "-i /tmp/cdc_init.sql" in init
    assert "14258" in init and "22832" in init
    assert "SECONDS + 120" in init
    assert "SQL Server Agent did not accept CDC job operations" in init
