"""D30: a Snowflake route with no ``snowflake-sqlalchemy`` is refused loudly, never degraded.

Keyed modes, mirror/SCD2, FK carry, the physical-state diff and the key probes
all reach Snowflake through the SQLAlchemy dialect. ``driver_available`` only
looked for ``snowflake.connector``, so a host without the dialect still passed
Validate; each of those checks then caught the missing-dialect error and
skipped itself while the run reported success. The dialect is part of the
Snowflake driver: its absence is an environment gap reported up front.

(Snowflake is fakesnow-emulated in these tests.)
"""
from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from src.transfer import connector_capabilities as caps

_REPO = Path(__file__).resolve().parents[3]


@pytest.fixture
def dialect_missing(monkeypatch):
    real = caps._module_is_installed

    def installed(name):
        return False if name == "snowflake.sqlalchemy" else real(name)

    monkeypatch.setattr(caps, "_module_is_installed", installed)


def test_snowflake_driver_needs_the_sqlalchemy_dialect(dialect_missing):
    assert caps.driver_available("snowflake") is False


def test_missing_dialect_is_reported_with_the_package_to_install(dialect_missing):
    allowed, reason = caps.endpoint_allowed_for_role("snowflake", "destination")
    assert allowed is False
    assert "snowflake-sqlalchemy" in reason


def test_missing_dialect_fails_the_run_instead_of_skipping_checks(dialect_missing, tmp_path, monkeypatch):
    pytest.importorskip("fakesnow")
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src = tmp_path / "s.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "a"), (2, "b")])
    conn.commit()
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(
            kind="database", format="snowflake", host="localhost", port=443,
            database="dataflow", username="test", password="test", schema="public",
            table="d30_" + uuid.uuid4().hex[:8],
        ),
        sync_mode="full_refresh_append",
        validation_mode="balanced",
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is False
    assert "snowflake-sqlalchemy" in (result.error or "")


def test_production_images_install_the_pinned_requirements():
    """Every API-image service builds Dockerfile.api, which pip-installs requirements.txt."""
    dockerfile = (_REPO / "Dockerfile.api").read_text()
    assert "COPY apps/api/requirements.txt /app/requirements.txt" in dockerfile
    assert "pip install -r /app/requirements.txt" in dockerfile
    for service in ("api", "worker", "scheduler"):
        toml = (_REPO / "deploy" / "railway" / f"{service}.toml").read_text()
        assert 'dockerfilePath = "Dockerfile.api"' in toml, service
    pins = [
        line for line in (_REPO / "apps/api/requirements.txt").read_text().splitlines()
        if line.lower().startswith("snowflake-sqlalchemy")
    ]
    assert pins and "<2" in pins[0]
