"""X-86: zero-length binary lands as zero-length bytes — never NULL —
and ``b''`` vs ``NULL`` must fail the fingerprint.

Live probe at 616df100 (pre-X-46): Parquet ``binary`` b'' landed as NULL
on MySQL/SQLite/PG while reporting verified/full_checksum — the fingerprint
folded ``''`` and NULL together — and MSSQL/DuckDB refused with "Empty
value cannot coerce to binary". On the X-46 tree the sqlite leg writes
``blob`` length 0; this file pins the rule hermetically and leaves the
5-engine legs live-gated.
"""

import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402


def test_x86_empty_binary_fingerprint_distinct_from_null():
    """'' (empty base64 / empty bytes) and NULL are different fingerprints."""
    from services.reconciliation import fingerprint_for_reconcile, normalize_cell

    empty_fp = fingerprint_for_reconcile(b"", ddl_type="BLOB")
    null_fp = fingerprint_for_reconcile(None, ddl_type="BLOB")
    assert empty_fp != null_fp
    assert empty_fp == ""
    assert null_fp != ""
    # Raw bytes and empty-string wire forms agree.
    assert normalize_cell(b"") == normalize_cell("")


def test_x86_parquet_empty_binary_lands_as_blob_not_null(tmp_path):
    """file-source parquet b'' -> sqlite writes blob length 0; NULL stays NULL."""
    import sqlite3

    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    pq.write_table(
        pa.table(
            {
                "id": [1, 2, 3, 4],
                "t": [b"\x00\xff\x10", b"\xde\xad", None, b""],
            }
        ),
        tmp_path / "bin.parquet",
    )
    dest = EndpointConfig(
        kind="database", format="sqlite",
        database=str(tmp_path / "dest.db"), table="t_dst",
    )
    written, _ddl, _summary, _cols = stream_file_to_database(
        str(tmp_path / "bin.parquet"), "bin.parquet",
        dest,
        [
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "t", "target": "t", "target_type": "BLOB"},
        ],
        {"id": "INTEGER", "t": "BLOB"},
        job_id="x86" + uuid.uuid4().hex[:15],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    assert written == 4
    conn = sqlite3.connect(tmp_path / "dest.db")
    try:
        rows = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT id, typeof(t), length(t) FROM t_dst ORDER BY id"
            )
        }
    finally:
        conn.close()
    assert rows[1] == ("blob", 3)
    assert rows[2] == ("blob", 2)
    assert rows[3] == ("null", None)  # NULL is NULL
    assert rows[4] == ("blob", 0)  # b'' is zero-length bytes, not NULL


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X86_LIVE") != "1",
    reason="live engines required — DATAFLOW_X86_LIVE=1",
)
def test_x86_empty_binary_clean_and_tamper_live():
    """Parquet binary + MySQL VARBINARY b'' -> MySQL/SQLite/MSSQL/PG/DuckDB:
    clean lands zero-length bytes exact; tamper b'' -> NULL fails naming id 1
    ('' vs NULL must fail the fingerprint on every engine)."""
    pytest.fail("live leg — runs on the user's engine matrix")
