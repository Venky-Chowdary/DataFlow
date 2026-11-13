"""CI exports every connection-setting spelling the live tests read (decisions 5, 6).

The suite reads a dozen names per engine (PGHOST, PG_HOST, P2_PG_*,
DATAFLOW_PG_TEST_DSN, DATAFLOW_TEST_MYSQL_*, DATAFLOW_TEST_PGVECTOR_PORT, ...).
CI set a few, so live PostgreSQL / MySQL / Mongo tests skipped next to a
running engine. ``.github/ci-live-engines.vars`` is the one list; the jobs that
start those engines load it.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
ENV_FILE = REPO / ".github" / "ci-live-engines.vars"
if str(API / "tests") not in sys.path:
    sys.path.insert(0, str(API / "tests"))


def _env() -> dict[str, str]:
    out = {}
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            out[key] = value
    return out


def test_env_file_covers_the_names_gated_tests_read() -> None:
    env = _env()
    for name in (
        "PGHOST", "DATAFLOW_PG_TEST_DSN", "DATAFLOW_TEST_PG_DSN", "PG_HOST", "P2_PG_HOST",
        "DATAFLOW_PG_HOST", "R11_PG_LOGICAL_HOST", "DATAFLOW_TEST_PGVECTOR_PORT",
        "MYSQL_PASSWORD", "DATAFLOW_TEST_MYSQL_HOST", "DATAFLOW_MYSQL_USER", "P2_MYSQL_HOST",
        "DATAFLOW_TEST_MONGO_URI", "DATAFLOW_LIVE_MONGO_URI", "MONGO_URI", "P2_MONGO_URI",
    ):
        assert env.get(name), name


def test_env_file_never_switches_product_configuration() -> None:
    # The product reads these as its own stores / lease backend / auth state;
    # setting them would change every TestClient test in the shard.
    env = _env()
    for name in ("MONGODB_URI", "REDIS_URL", "DATAFLOW_REDIS_URL", "DATAFLOW_CDC_LEASE_REDIS_URL", "DATABASE_URL"):
        assert name not in env, name
    assert not any("PASSWORD" in k and "MSSQL" in k for k in env), "SQL Server password is per-run"


def test_jobs_that_start_the_engines_load_the_env_file() -> None:
    action = (REPO / ".github" / "actions" / "ci-services" / "action.yml").read_text(encoding="utf-8")
    assert ".github/ci-live-engines.vars" in action and "GITHUB_ENV" in action
    nightly = yaml.safe_load((REPO / ".github" / "workflows" / "full-suite.yml").read_text(encoding="utf-8"))
    steps = nightly["jobs"]["engine-suite"]["steps"]
    load = [i for i, s in enumerate(steps) if ".github/ci-live-engines.vars" in str(s.get("run", ""))]
    suite = [i for i, s in enumerate(steps) if "python -m pytest tests" in str(s.get("run", ""))]
    assert load and suite and load[0] < suite[0]


@pytest.mark.parametrize(
    ("module", "attr", "env", "value"),
    [
        ("test_pgvector_m7_live", "_PG_PORT", "DATAFLOW_TEST_PGVECTOR_PORT", "5432"),
        ("test_cdc_postgres_vector_live", "PGVECTOR_PORT", "DATAFLOW_TEST_PGVECTOR_PORT", "5432"),
        ("test_mx3_20_cdc_catchup_false_failure", "_MYSQL_PORT", "DATAFLOW_TEST_MYSQL_PORT", "3306"),
    ],
)
def test_live_modules_take_the_port_from_the_environment(module, attr, env, value, monkeypatch) -> None:
    """Compose-lab ports (pgvector 5434, MySQL 3307) were hard-coded; CI serves
    pgvector and MySQL on 5432 / 3306, so these modules skipped there."""
    monkeypatch.setenv(env, value)
    sys.modules.pop(module, None)
    mod = importlib.import_module(module)
    try:
        assert getattr(mod, attr) == int(value)
    finally:
        sys.modules.pop(module, None)
