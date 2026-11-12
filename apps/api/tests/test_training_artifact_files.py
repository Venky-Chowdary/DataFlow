"""Training artifacts: atomic writes keep file modes; stale temp files are swept.

PR 410 review L1 + L2. ``atomic_text_file`` used ``mkstemp``, which creates
0600 files, so every artifact (and any 0644 file it replaced) became 0600.
And a daemon writer killed at interpreter exit left its ``.<name>.<rand>.tmp``
sibling behind (27.8 MB in the probe) with nothing to ever remove it.
"""

from __future__ import annotations

import os
import stat
import time
from pathlib import Path

import pytest

from services.atomic_file import STALE_TEMP_SECONDS, atomic_text_file


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


@pytest.fixture
def umask_022():
    previous = os.umask(0o022)
    try:
        yield
    finally:
        os.umask(previous)


@pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
def test_new_artifact_gets_the_mode_open_would_give(tmp_path, umask_022):
    plain = tmp_path / "plain.json"
    plain.write_text("x", encoding="utf-8")
    atomic = tmp_path / "atomic.json"

    with atomic_text_file(str(atomic)) as handle:
        handle.write("x")

    assert _mode(plain) == 0o644
    assert _mode(atomic) == _mode(plain)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["atomic.json", "plain.json"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")
@pytest.mark.parametrize("existing_mode", [0o644, 0o640, 0o600])
def test_rewrite_keeps_the_existing_file_mode(tmp_path, umask_022, existing_mode):
    target = tmp_path / "conversations.jsonl"
    target.write_text("old\n", encoding="utf-8")
    os.chmod(target, existing_mode)

    with atomic_text_file(str(target)) as handle:
        handle.write("new\n")

    assert target.read_text(encoding="utf-8") == "new\n"
    assert _mode(target) == existing_mode


def test_training_writers_use_the_shared_atomic_helper():
    """One implementation: ``services.atomic_file``, not a copy in training."""
    import services.atomic_file as atomic_file
    import src.ai.training.fine_tuning as fine_tuning
    import src.ai.training.training_agent as training_agent
    import src.ai.training.training_runtime as training_runtime

    assert training_agent.atomic_text_file is atomic_file.atomic_text_file
    assert fine_tuning.atomic_text_file is atomic_file.atomic_text_file
    assert not hasattr(training_runtime, "atomic_text_file")


def test_agent_init_sweeps_stale_temp_files_only(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    copilot_dir = tmp_path / "training" / "copilot"
    finetune_dir = tmp_path / "training"
    copilot_dir.mkdir(parents=True)
    old = time.time() - STALE_TEMP_SECONDS - 60

    stale = [
        copilot_dir / ".conversations.jsonl.xegst8zp.tmp",
        copilot_dir / ".training_checkpoint.json.ab12cd34.tmp",
        finetune_dir / ".openai_finetune.jsonl.k3j4h5g6.tmp",
    ]
    kept = [
        copilot_dir / ".vector_documents.jsonl.fresh123.tmp",  # a writer may still own it
        copilot_dir / "conversations.jsonl",
        copilot_dir / ".hidden_notes",
        finetune_dir / "column_mapping.jsonl",
        finetune_dir / "notes.tmp",  # not one of ours: no leading dot
    ]
    for path in stale + kept:
        path.write_text("x", encoding="utf-8")
    for path in stale + kept[1:]:
        os.utime(path, (old, old))

    from src.ai.training.training_agent import DataTransferTrainingAgent

    DataTransferTrainingAgent()

    assert [p.name for p in stale if p.exists()] == []
    assert [p.name for p in kept if not p.exists()] == []
