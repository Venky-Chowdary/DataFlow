"""forget_job_runs syncs the profile directory even when a rewrite fails midway."""

from __future__ import annotations

import json

import pytest

from services import data_quality_history as dq


def test_the_directory_is_fsynced_even_if_a_later_rewrite_fails(tmp_path, monkeypatch):
    monkeypatch.delenv("DATAWRAP_QUALITY_PROFILES_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_QUALITY_PROFILES_DIR", str(tmp_path))
    monkeypatch.setattr(dq, "_profile_collection", lambda: None)
    for name in ("a", "b"):
        (tmp_path / f"{name}.json").write_text(json.dumps({
            "job_id": "keep",
            "runs": [{"job_id": "keep", "captured_at": "t1"}, {"job_id": "forget", "captured_at": "t2"}],
        }))
    real_write = dq.write_json_atomic
    writes: list[str] = []

    def second_write_fails(path, data, **kwargs):
        writes.append(path.name)
        if len(writes) == 2:
            raise OSError(28, "No space left on device")
        return real_write(path, data, **kwargs)

    synced: list[str] = []
    monkeypatch.setattr(dq, "write_json_atomic", second_write_fails)
    monkeypatch.setattr(dq, "fsync_directory", lambda d: synced.append(str(d)))
    with pytest.raises(OSError):
        dq.forget_job_runs({"forget"})
    assert writes == ["a.json", "b.json"]
    assert synced == [str(tmp_path)], "the rewrite that landed is made durable"
