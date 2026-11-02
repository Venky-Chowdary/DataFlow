"""X-DD: every persistent writer resolves under DATAFLOW_DATA_DIR.

Repo-state JSON files (Pilot memory/acks/results, vector store, training
output, CDC leases) must land under the platform data dir so the app dir
stays read-only-safe and the artifact volume controls every write. Each
writer has a guard test; the source scan stops regressions back to
``Path(__file__).parents[3] / "data"`` hardcodes.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def isolated_dir(monkeypatch, tmp_path):
    dd = tmp_path / "dataflow_data"
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(dd))
    # data_dir() is cached in some environments; drop a cached resolve if any.
    try:
        from services import platform_config

        platform_config.data_dir.cache_clear()
    except (AttributeError, ImportError):
        pass
    yield dd
    try:
        from services import platform_config

        platform_config.data_dir.cache_clear()
    except (AttributeError, ImportError):
        pass


def test_working_memory_resolves_under_data_dir(isolated_dir):
    from src.ai.copilot.working_memory import _default_path

    p = _default_path()
    assert p.parent == isolated_dir
    assert p.name == "pilot_memory.json"


def test_ack_ledger_resolves_under_data_dir(isolated_dir):
    from src.ai.copilot.ack_ledger import _default_path

    p = _default_path()
    assert p.parent == isolated_dir
    assert p.name == "pilot_acks.json"


def test_result_store_resolves_under_data_dir(isolated_dir):
    from src.ai.copilot.result_store import _default_path

    p = _default_path()
    assert p.parent == isolated_dir
    assert p.name == "pilot_results.json"


def test_vector_store_resolves_under_data_dir(isolated_dir, monkeypatch):
    from src.ai.rag.vector_store import _default_persist_dir

    monkeypatch.delenv("DATAFLOW_VECTOR_STORE_DIR", raising=False)
    monkeypatch.delenv("VECTOR_STORE_DIR", raising=False)
    assert Path(_default_persist_dir()) == isolated_dir / "vector_store"


def test_vector_store_explicit_override_still_wins(isolated_dir, monkeypatch):
    from src.ai.rag.vector_store import _default_persist_dir

    monkeypatch.setenv("DATAFLOW_VECTOR_STORE_DIR", "/tmp/custom_vs")
    assert _default_persist_dir() == "/tmp/custom_vs"


def test_training_agent_resolves_under_data_dir(isolated_dir):
    from src.ai.training.training_agent import DataTransferTrainingAgent

    assert Path(DataTransferTrainingAgent._output_dir()) == isolated_dir / "training" / "copilot"


def test_fine_tuning_resolves_under_data_dir(isolated_dir):
    from src.ai.training.fine_tuning import DataTransferFineTuningPipeline

    assert Path(DataTransferFineTuningPipeline._default_output_dir()) == isolated_dir / "training"


def test_cdc_lease_store_resolves_under_data_dir(isolated_dir):
    from services.cdc_lease_store import FileLeaseStore

    monkey = FileLeaseStore()
    assert monkey.data_dir == str(isolated_dir)


# --- source-scan guard: no writer may regress to a hardcoded ../data path ---
WRITERS = [
    "src/ai/copilot/working_memory.py",
    "src/ai/copilot/ack_ledger.py",
    "src/ai/copilot/result_store.py",
    "src/ai/rag/vector_store.py",
    "src/ai/training/training_agent.py",
    "src/ai/training/fine_tuning.py",
]


@pytest.mark.parametrize("relpath", WRITERS)
def test_writer_calls_shared_resolver(relpath):
    src = (API_ROOT / relpath).read_text(encoding="utf-8")
    assert "platform_config import data_dir" in src or "data_dir(" in src, (
        f"{relpath} must resolve paths through services.platform_config.data_dir()"
    )
    assert 'parents[3]' not in src, (
        f"{relpath} still hardcodes a repo-relative data path — use data_dir()"
    )
    assert '".."  ,  ".."' not in src
    assert not (
        'os.path.dirname(__file__), "..", "..", "..", "data"' in src
    ), f"{relpath} still hardcodes ../../../data — use data_dir()"


def test_no_new_hardcoded_data_paths_in_ai_tree():
    """Writers under src/ai/ that persist state must not embed repo-relative
    data paths that bypass DATAFLOW_DATA_DIR."""
    hits = []
    for root, _dirs, files in os.walk(API_ROOT / "src" / "ai"):
        for fn in files:
            if not fn.endswith(".py"):
                continue
            rel = os.path.relpath(os.path.join(root, fn), API_ROOT)
            text = (API_ROOT / rel).read_text(encoding="utf-8")
            if 'parents[3] / "data"' in text or '"..", "..", "..", "data"' in text:
                hits.append(rel)
    assert hits == [], f"hardcoded repo data paths bypass DATAFLOW_DATA_DIR: {hits}"
