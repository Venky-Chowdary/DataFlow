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


def test_every_runtime_state_writer_resolves_inside_the_sandbox(monkeypatch, tmp_path):
    """Each module-relative writer must honor DATAFLOW_DATA_DIR.

    A writer that resolves ``parents[n] / "data"`` instead writes into the
    real apps/api/data in every test run AND in any deployment that sets
    DATAFLOW_DATA_DIR — both are wrong.
    """
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    # Clear every writer-specific override another test may have leaked via a
    # raw os.environ set — the env name always ends in one of these suffixes
    # under either brand (DATAFLOW_* / DATAWRAP_*).
    for key in list(os.environ):
        if key.endswith(
            (
                "_MEMORY_PATH",
                "_ACK_PATH",
                "_RESULTS_PATH",
                "VECTOR_STORE_DIR",
                "TRAINING_OUTPUT_DIR",
                "CONTRACTS_PATH",
            )
        ):
            monkeypatch.delenv(key, raising=False)
    root = _API_ROOT / "data"

    from services import agentic_repair, cdc_incremental_snapshot, contract_file_store, pair_assurance
    from src.ai.copilot import ack_ledger, result_store, working_memory
    from src.ai.rag import vector_store
    from src.ai.training import fine_tuning, training_agent

    resolved = {
        "cdc_incremental_snapshot": Path(cdc_incremental_snapshot._snap_path()),
        "agentic_repair.audit": Path(agentic_repair._audit_path()),
        "agentic_repair.proposals": Path(agentic_repair._proposals_path()),
        "contract_file_store": Path(contract_file_store._default_path()),
        "pair_assurance": Path(pair_assurance._default_proof_dir()),
        "working_memory": Path(working_memory._default_path()),
        "ack_ledger": Path(ack_ledger._default_path()),
        "result_store": Path(result_store._default_path()),
        "vector_store": Path(vector_store._default_persist_dir()),
        "training_agent": Path(training_agent.DataTransferTrainingAgent._default_output_dir()),
        "fine_tuning": Path(fine_tuning.DataTransferFineTuningPipeline._default_output_dir()),
    }
    for name, path in resolved.items():
        assert tmp_path in path.resolve().parents or path.resolve() == tmp_path, (
            f"{name} resolves {path} — outside DATAFLOW_DATA_DIR"
        )
        assert root not in path.resolve().parents, (
            f"{name} resolves inside real apps/api/data: {path}"
        )
