"""Guard: no test run may write to the real apps/api/data state.

The autouse session fixture in conftest snapshots ``schedules.json`` and
``cdc_incremental_snapshots.json`` and fails the run on teardown if either
changed. This test is the visible half: it proves the DATAFLOW_DATA_DIR
sandbox is actually in force, so a deleted env var or a bypassed conftest
cannot silently re-arm the hazard.
"""

import os
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]


def test_data_dir_is_sandboxed_not_the_real_apps_api_data():
    redirect = os.environ.get("DATAFLOW_DATA_DIR")
    assert redirect, "DATAFLOW_DATA_DIR must point tests at a sandbox"
    assert Path(redirect).resolve() != (_API_ROOT / "data").resolve()


def test_sandbox_is_writable_so_store_paths_land_inside_it():
    from services import schedule_store

    path = schedule_store._store_path()
    assert Path(path).resolve() != (_API_ROOT / "data" / "schedules.json").resolve()


def test_env_dir_still_wins_when_store_path_is_not_patched(monkeypatch, tmp_path):
    """The lazy-path contract: in production DATAFLOW_DATA_DIR picks the dir.

    Only a monkeypatched ``STORE_PATH`` overrides it — nothing else may drag
    the store back to a stale import-time constant.
    """
    from services import schedule_store

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    assert schedule_store._store_path() == tmp_path / "schedules.json"


def test_patched_store_path_wins_over_the_env_dir(monkeypatch, tmp_path):
    from services import schedule_store

    own = tmp_path / "own.json"
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "env"))
    monkeypatch.setattr(schedule_store, "STORE_PATH", own)
    assert schedule_store._store_path() == own
