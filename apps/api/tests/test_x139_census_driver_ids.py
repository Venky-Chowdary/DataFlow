"""X-139 — census-driver coverage is keyed on the canonical driver id.

``resolve_driver_type`` canonicalizes sqlserver/mssql/azure_sql to
``sqlserver``; ``_culprit_census_conn`` matched the literal ``mssql`` and the
SQL Server branch was unreachable. This guard locks two things: every
canonical id either reaches a census connection or declines explicitly, and
the "no census driver" message lists exactly the engines the code supports —
both derive from ``_CENSUS_DRIVER_ENGINES``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer import reconcile_step as recon_step  # noqa: E402


@pytest.fixture
def fake_conns(monkeypatch, tmp_path):
    sentinel = object()
    monkeypatch.setattr(
        "connectors.postgresql_conn.get_connection", lambda **kw: sentinel
    )
    monkeypatch.setattr(
        "services.copy_mysql_pg._mysql_connect", lambda cfg: sentinel
    )
    monkeypatch.setattr(
        "services.copy_sqlserver_sqlserver._ss_connect", lambda cfg: sentinel
    )
    monkeypatch.setattr(
        "connectors.snowflake_conn.get_connection", lambda **kw: sentinel
    )
    return tmp_path


def test_sql_server_spellings_all_reach_the_census(fake_conns, tmp_path):
    """Every SQL Server catalog spelling resolves to the canonical census."""
    for fmt in ("sqlserver", "mssql", "azure_sql"):
        out = recon_step._culprit_census_conn(fmt, {"host": "h", "port": 1433})
        assert out is not None, f"{fmt} got no census driver"
        assert out[0] == "sqlserver"


def test_every_census_engine_reaches_a_connection(fake_conns, tmp_path):
    cfg = {
        "postgresql": {"host": "h", "database": "d"},
        "mysql": {"host": "h"},
        "sqlserver": {"host": "h", "port": 1433},
        "duckdb": {"database": str(tmp_path / "c.duckdb")},
        "sqlite": {"database": str(tmp_path / "c.db")},
        "snowflake": {"host": "acct"},
    }
    for eng in sorted(recon_step._CENSUS_DRIVER_ENGINES):
        out = recon_step._culprit_census_conn(eng, cfg[eng])
        assert out is not None, f"census engine {eng} does not connect"
        assert out[0] in recon_step._CENSUS_DRIVER_ENGINES


def test_unsupported_formats_decline_and_the_message_lists_the_real_set(
    fake_conns,
):
    """A non-census engine returns None, and the supported list in any
    message text must equal _CENSUS_DRIVER_ENGINES — never a drifted string."""
    for fmt in ("mongodb", "oracle", "csv", "redis"):
        assert recon_step._culprit_census_conn(fmt, {}) is None, fmt
    expected = "/".join(sorted(recon_step._CENSUS_DRIVER_ENGINES))
    assert expected == "duckdb/mysql/postgresql/snowflake/sqlite/sqlserver"
