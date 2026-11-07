"""Path-based file streaming: billion-row style loads without loading bytes."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest


_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from src.transfer.file_stream import (  # noqa: E402
    _iter_csv_batches,
    peek_file_source,
    prepare_stream_content,
    stream_file_to_database,
)
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.reconcile_step import is_count_proof_token  # noqa: E402


def _write_csv_file(path: Path, rows: int) -> list[str]:
    headers = ["id", "name", "amount"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(headers) + "\n")
        for i in range(1, rows + 1):
            f.write(f"{i},row{i},{i * 1.5}\n")
    return headers


def test_peek_file_source_from_path():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "large.csv"
        _write_csv_file(p, 12000)
        columns, schema, total, sample = peek_file_source(str(p), "large.csv")
        assert columns == ["id", "name", "amount"]
        assert total == 12000
        assert len(sample) == 100
        assert schema.get("id") in {"INTEGER", "INT"}


def test_iter_csv_batches_from_path():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "data.csv"
        _write_csv_file(p, 500)
        batches = list(_iter_csv_batches(str(p), 100))
        assert len(batches) == 5
        assert sum(len(b) for b in batches) == 500
        assert batches[0][0] == {"id": "1", "name": "row1", "amount": "1.5"}


@pytest.mark.parametrize("copy_fast_path", ["1", "0"])
def test_stream_file_to_database_from_path(monkeypatch, copy_fast_path):
    # Two proof contracts share this route. The local CSV COPY still reports a
    # ``dest_count:<n>`` cardinality token beside the value digest. When the
    # second parse aligns with that write-pass identity, both paths stamp
    # ``source_reread``. A forced-off re-read keeps ``inline_write_pass``.
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", copy_fast_path)
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        p = tmp / "payments.csv"
        with open(p, "w", encoding="utf-8", newline="") as f:
            f.write("id,amount\n")
            f.write("1,1000.00\n")
            f.write("2,2000.50\n")

        dest = EndpointConfig(kind="database", format="sqlite", database=str(tmp / "out.db"), table="payments")
        rows, ddl, summary, columns = stream_file_to_database(
            str(p),
            "payments.csv",
            dest,
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            schema={"id": "INTEGER", "amount": "DECIMAL"},
        )
        assert rows == 2
        assert summary.get("checksum")
        if copy_fast_path == "1":
            assert summary.get("copy_fast_path") == "used"
            # Full-refresh fast path fingerprints mapped rows during the write
            # pass (real value digest) while the engine count fields stay
            # cardinality proof — the two must never be graded against each other.
            assert len(summary["checksum"]) == 64
            assert not is_count_proof_token(summary["checksum"])
            assert summary.get("checksum_mode") == "source_reread"
            assert summary.get("source_independently_reread") is True
            assert summary.get("identity_hash_aligned") is True
            assert is_count_proof_token(summary["engine_source_checksum"])
            assert is_count_proof_token(summary["engine_target_checksum"])
            assert "dest_count_equals_source_snapshot" in str(summary.get("proof_scope"))
        else:
            assert summary.get("copy_fast_path") != "used"
            assert summary.get("checksum_mode") == "source_reread"
            assert summary.get("source_independently_reread") is True
            assert summary.get("identity_hash_aligned") is True
            versions = summary.get("connector_versions") or {}
            assert any(ch.isdigit() for ch in str(versions.get("source") or ""))
            assert any(ch.isdigit() for ch in str(versions.get("destination") or ""))
            assert not is_count_proof_token(summary["checksum"])
        assert columns == ["id", "amount"]


def test_ten_row_csv_reread_aligns_identity_and_captures_versions(monkeypatch):
    """Named fixture: 10 source rows, second parse, identity match, release strings.

    This is the file→warehouse earn path. It does not re-execute a live
    Postgres load. ``migration_proven`` still requires Gate-8 dest read-back
    to match this digest; the stream stamps the three proofs the pack checks.
    """
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    monkeypatch.delenv("DATAFLOW_RECONCILE_SOURCE_REREAD", raising=False)
    monkeypatch.delenv("DATAWRAP_RECONCILE_SOURCE_REREAD", raising=False)
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        p = tmp / "datawrap_ten.csv"
        with open(p, "w", encoding="utf-8", newline="") as f:
            f.write("id,name,amount\n")
            for i in range(1, 11):
                f.write(f"{i},row{i},{i * 10}.00\n")
        dest = EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(tmp / "out.db"),
            table="datawrap_ten",
        )
        rows, _ddl, summary, columns = stream_file_to_database(
            str(p),
            "datawrap_ten.csv",
            dest,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {"source": "amount", "target": "amount"},
            ],
            schema={"id": "INTEGER", "name": "VARCHAR", "amount": "DECIMAL"},
        )
        assert rows == 10
        assert columns == ["id", "name", "amount"]
        assert summary.get("checksum_mode") == "source_reread"
        assert summary.get("source_independently_reread") is True
        assert summary.get("identity_hash_aligned") is True
        alignment = summary.get("identity_alignment") or {}
        assert alignment.get("write_pass_rows") == 10
        assert alignment.get("reread_rows") == 10
        assert alignment.get("write_pass_identity_digest") == alignment.get(
            "reread_identity_digest"
        )
        assert len(summary.get("checksum") or "") == 64
        assert summary.get("checksum") == summary.get("write_pass_checksum")
        versions = summary.get("connector_versions") or {}
        assert "python-csv" in str(versions.get("source"))
        assert any(ch.isdigit() for ch in str(versions.get("source")))
        assert "sqlite3" in str(versions.get("destination"))
        assert any(ch.isdigit() for ch in str(versions.get("destination")))
        phases = {
            str(p.get("phase"))
            for p in (summary.get("phase_profile") or {}).get("phases") or []
        }
        assert "transform_write" in phases
        assert "checksum" in phases
        assert float((summary.get("phase_profile") or {}).get("busy_seconds") or 0) > 0

        from services.signed_proof_pack import build_signed_proof_pack
        from src.transfer.reconcile_step import run_reconciliation

        report = run_reconciliation(
            endpoint=dest,
            records=[],
            columns=columns,
            rows_written=10,
            writer_checksum=summary["checksum"],
            dest_summary=summary,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {"source": "amount", "target": "amount"},
            ],
            source_schema={"id": "INTEGER", "name": "VARCHAR", "amount": "DECIMAL"},
            validation_mode="strict",
        )
        assert report["source_checksum_provenance"] == "independent_source_reread"
        assert report["coverage"] == "full_checksum"
        assert report["checksum_match"] is True
        assert report["source_checksum"] == report["target_checksum"]
        pack = build_signed_proof_pack(
            job_id="datawrap-ten",
            job_success=True,
            reconciliation=report,
            ddl_hash="ddl-datawrap-ten",
            mapping_hash="map-datawrap-ten",
            connector_versions=versions,
        )
        assert pack["assurance"]["migration_proven"] is True
        assert pack["connector_versions_honesty"] == "provided"


def test_file_reread_env_off_keeps_write_pass(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    monkeypatch.setenv("DATAFLOW_RECONCILE_SOURCE_REREAD", "0")
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        p = tmp / "two.csv"
        p.write_text("id,amount\n1,1000.00\n2,2000.50\n", encoding="utf-8")
        dest = EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(tmp / "out.db"),
            table="payments",
        )
        _rows, _ddl, summary, _columns = stream_file_to_database(
            str(p),
            "two.csv",
            dest,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            schema={"id": "INTEGER", "amount": "DECIMAL"},
        )
        assert summary.get("checksum_mode") == "inline_write_pass"
        assert summary.get("source_independently_reread") is False
        assert summary.get("identity_hash_aligned") is False


def test_prepare_stream_content_spills_large_payload():
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        original = tmp / "original.csv"
        original.write_bytes(b"id\n" + b"\n".join(f"{i}".encode() for i in range(1000)))
        result = prepare_stream_content(
            content=original.read_bytes(),
            filename="big.csv",
            source_path="",
        )
        if len(original.read_bytes()) > 50 * 1024 * 1024:
            assert isinstance(result, (str, Path))
            assert Path(result).exists()
        else:
            # Below the default 50 MB threshold, bytes are returned as-is.
            assert isinstance(result, bytes)
