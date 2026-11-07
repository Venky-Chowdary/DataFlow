"""D69 — strict mode refused text containing a zero-width space.

U+200B, ZWJ / ZWNJ (emoji sequences, Persian and Indic scripts), soft hyphen
and BOM are Unicode format characters (category Cf): valid text that every
UTF-8 destination stores and returns byte for byte. They are worth a warning
(they can break joins on visually equal keys) but not a refusal. Real damage
stays blocking in strict: U+FFFD (lost encoding) and C0/C1 control bytes
such as NUL, which destinations reject.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.data_integrity import run_integrity_audit  # noqa: E402


def _encoding_check(value: str, mode: str = "strict") -> dict:
    report = run_integrity_audit(
        source_columns=["title"],
        mappings=[
            {"source": "title", "target": "title", "confidence": 0.99, "target_type": "TEXT"}
        ],
        sample_rows=[{"title": value}],
        validation_mode=mode,
    )
    return report, next(c for c in report["checks"] if c["check"] == "encoding_anomalies")


@pytest.mark.parametrize(
    "value",
    [
        "hello​world",          # zero-width space
        "\U0001F468‍\U0001F469‍\U0001F467",  # ZWJ family emoji
        "می‌خواهم",  # ZWNJ in Persian
        "co­operate",            # soft hyphen
        "﻿leading bom",
    ],
)
def test_format_characters_warn_but_do_not_block_strict(value):
    report, enc = _encoding_check(value)
    assert enc["blocks_transfer"] is False, enc
    assert not enc["issues"]
    assert any("strip_controls" in w for w in enc["warnings"]), enc
    assert report["blocks_transfer"] is False


@pytest.mark.parametrize("value", ["bad\x00byte", "bell\x07here", "lost � encoding"])
def test_control_bytes_and_replacement_char_still_block_strict(value):
    _report, enc = _encoding_check(value)
    assert enc["blocks_transfer"] is True, enc


def test_mixed_cell_blocks_on_its_control_byte():
    _report, enc = _encoding_check("zw​sp and nul\x00")
    assert enc["blocks_transfer"] is True
    assert any("U+0000" in str(i) for i in enc["issues"])


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.data_quality_history as dqh
    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(dqh, "_profile_collection", lambda: None)
    monkeypatch.setenv("DATAFLOW_QUALITY_PROFILES_DIR", str(tmp_path / "profiles"))
    return fake


def test_strict_sqlite_transfer_keeps_zero_width_text_verbatim(tmp_path, isolated):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    values = [(i, f"name{i}") for i in range(1, 9)] + [(9, "hello​world")]
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, title TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", values)
    conn.close()

    job_id = "d69" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
        ),
        job_id,
    )
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute("SELECT title FROM t WHERE id = 9").fetchone()[0] == "hello​world"
    finally:
        conn.close()
