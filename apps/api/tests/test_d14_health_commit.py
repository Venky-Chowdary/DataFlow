"""D-14: / and /health must report the deployed git SHA, not only the
hardcoded "1.0.0" — a healthcheck can't tell a pre-fix image from post-fix.
"""

import asyncio

from src.main import _BUILD_COMMIT, health_check, root


def test_root_reports_commit():
    payload = asyncio.run(root())
    assert payload["version"] == "1.0.0"
    assert isinstance(payload["commit"], str) and payload["commit"]
    # Local checkout resolves HEAD; deployed images get DATAFLOW_COMMIT_SHA /
    # RAILWAY_GIT_COMMIT_SHA / SOURCE_COMMIT — only bare "unknown" is allowed
    # outside a git context.
    assert payload["commit"] != "unknown" or _BUILD_COMMIT == "unknown"


def test_health_reports_commit():
    payload = asyncio.run(health_check())
    assert payload["version"] == "1.0.0"
    assert isinstance(payload["commit"], str) and payload["commit"]
    assert payload["commit"] == _BUILD_COMMIT


def test_env_commit_sha_wins(monkeypatch):
    monkeypatch.setenv("DATAFLOW_COMMIT_SHA", "deadbeefcafe1234")
    from src.main import _build_commit

    assert _build_commit() == "deadbeefcafe"[:12] or _build_commit() == "deadbeefcafe"
    # 12-char short SHA
    assert len(_build_commit()) <= 12
