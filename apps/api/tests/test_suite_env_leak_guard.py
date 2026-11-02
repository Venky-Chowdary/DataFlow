"""Regression guard for the env/app-state isolation fixtures in conftest.

``_isolate_process_env`` restores ``os.environ`` and ``_isolate_app_state``
restores ``app.state`` after every test. If either regresses, the second
test below fails — and so would the ~dozens of later tests that 503 or hit
``SecretVaultError`` under a leaked production env.
"""

from __future__ import annotations

import os


def test_zz_deliberate_leak_writes_env_and_app_state() -> None:
    os.environ["DATAFLOW_SUITE_LEAK_PROBE"] = "leaked"
    from src.main import app

    app.state.config_errors = ["leaked"]
    app.state.leak_probe = "leaked"


def test_zz_env_and_app_state_are_clean_after_leaker() -> None:
    assert "DATAFLOW_SUITE_LEAK_PROBE" not in os.environ
    from src.main import app

    assert getattr(app.state, "config_errors", None) in (None, [])
    assert getattr(app.state, "leak_probe", None) is None
