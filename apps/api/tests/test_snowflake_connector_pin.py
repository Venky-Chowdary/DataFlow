"""snowflake-connector-python 5.0.0 removed snowflake.connector.converter.

fakesnow 0.11.22 imports that module while loading conn.py. The API image
(Dockerfile.api, Railway deploy/railway/api.toml) installs requirements.txt
with no lockfile, so the upper bound is what keeps production and CI on 4.x.
"""

from __future__ import annotations

import re
from pathlib import Path

from connectors.snowflake_conn import fakesnow_availability_error
from services.desktop_lab import LabBackends, _bind

REQUIREMENTS = Path(__file__).resolve().parents[1] / "requirements.txt"


def _snowflake_requirement() -> str:
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("snowflake-connector-python"):
            return stripped
    raise AssertionError("snowflake-connector-python missing from requirements.txt")


def test_requirements_keep_snowflake_connector_below_5():
    line = _snowflake_requirement()
    assert re.search(r">=3\.6\.0,<5\b", line), line


def test_fakesnow_and_pinned_connector_import_together():
    import snowflake.connector
    import snowflake.connector.converter as converter

    import fakesnow
    import fakesnow.cursor  # noqa: F401

    major = int(str(snowflake.connector.__version__).split(".", 1)[0])
    assert major < 5, snowflake.connector.__version__
    assert hasattr(converter, "SnowflakeConverter")
    assert fakesnow_availability_error() is None
    assert fakesnow.__name__ == "fakesnow"


def test_snowflake_bind_reports_the_import_error(monkeypatch, tmp_path):
    message = "ModuleNotFoundError: No module named 'snowflake.connector.converter'"

    def boom() -> str:
        return message

    monkeypatch.setattr("connectors.snowflake_conn.fakesnow_availability_error", boom)
    reason = _bind(
        {"id": "snowflake", "family": "snowflake"},
        LabBackends(root=tmp_path),
        table="orders",
    )
    assert reason == message
    assert "fakesnow package not installed" not in str(reason)
