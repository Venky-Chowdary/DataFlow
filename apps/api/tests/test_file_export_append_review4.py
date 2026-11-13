"""Appending to a CSV / TSV export an earlier build wrote (review 4, H-3).

Earlier builds wrote a file-source CSV / TSV export with LF line endings, and
a TSV that never quoted. This build writes CRLF and quotes (the C04 dialect).
A scheduled append onto such a file compared the headers byte for byte and
refused (``id,v`` is not ``id,v\\r``). The headers are now compared whatever
the line ending, and the rows are appended in the existing file's line ending,
and in its quoting where it can be detected, so the file stays one dialect.
"""

from __future__ import annotations

import uuid

import pytest

from services.file_export_append import append_payload

#: What this build writes (CRLF; NULL empty, '' quoted, a cell with a delimiter, quote or line break quoted).
HEAD_CSV = b'id,v,w\r\n3,,"x\r\ny"\r\n4,"","q""z"\r\n'
HEAD_TSV = b'id\tv\tw\r\n3\t\t"a\tb"\r\n4\t""\t"q""z"\r\n'


def _append(path, fmt, data):
    with open(path, "ab") as fh:
        fh.write(append_payload(fmt, str(path), data))
    return path.read_bytes()


def test_h3_an_lf_csv_written_by_an_earlier_build_takes_the_append_in_lf(tmp_path):
    path = tmp_path / "old.csv"
    path.write_bytes(b'id,v,w\n1,"",a\n2,"","q""x"\n')  # the earlier build's file-source CSV
    out = _append(path, "csv", HEAD_CSV)
    # One header, every row; record ends in LF like the rest of the file, a line break inside a value kept as is.
    assert out == b'id,v,w\n1,"",a\n2,"","q""x"\n3,,"x\r\ny"\n4,"","q""z"\n'


def test_h3_a_crlf_csv_still_takes_the_append_unchanged(tmp_path):
    path = tmp_path / "new.csv"
    path.write_bytes(b"id,v,w\r\n1,,a\r\n")
    assert _append(path, "csv", HEAD_CSV) == b'id,v,w\r\n1,,a\r\n3,,"x\r\ny"\r\n4,"","q""z"\r\n'


def test_h3_an_unquoted_lf_tsv_takes_the_append_in_its_own_quoting(tmp_path):
    path = tmp_path / "old.tsv"
    path.write_bytes(b'id\tv\tw\n1\t\tplain\n2\t\tq"x\n')  # the earlier build never quoted a TSV
    out = _append(path, "tsv", HEAD_TSV)
    # Unquoted like the file; a value holding a tab still has to be quoted to stay one cell.
    assert out == b'id\tv\tw\n1\t\tplain\n2\t\tq"x\n3\t\t"a\tb"\n4\t\tq"z\n'


def test_h3_a_header_that_really_differs_is_still_refused(tmp_path):
    path = tmp_path / "old.csv"
    path.write_bytes(b"id,v\n1,a\n")
    with pytest.raises(ValueError, match="does not match this run's columns"):
        append_payload("csv", str(path), b"id,w\r\n2,b\r\n")


def test_h3_an_append_run_onto_an_lf_export_from_an_earlier_build(tmp_path, monkeypatch):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path))
    target = tmp_path / "nightly.csv"
    target.write_bytes(b"id,name\n1,alice\n2,bob\n")  # written by the earlier build (LF)
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="file_export", format="csv", output_path="nightly.csv"),
        source_filename="users.csv", source_content=b"id,name\n3,carol\n4,dan\n",
        sync_mode="full_refresh_append", skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert target.read_bytes() == b"id,name\n1,alice\n2,bob\n3,carol\n4,dan\n"


def test_h3_a_database_source_appends_onto_an_lf_export_and_a_jsonl_append_balances(tmp_path, monkeypatch):
    # The run's own rows are the delta: an append onto an existing export is not read as a surplus.
    import sqlite3

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path))
    src = tmp_path / "s.sqlite"
    conn = sqlite3.connect(src)
    with conn:
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO users VALUES (?, ?)", [(3, "carol"), (4, "dan")])
    conn.close()
    csv_target, jsonl_target = tmp_path / "db.csv", tmp_path / "db.jsonl"
    csv_target.write_bytes(b"id,name\n1,alice\n2,bob\n")
    jsonl_target.write_bytes(b'{"id": 1, "name": "alice"}\n{"id": 2, "name": "bob"}\n')
    for fmt, name in (("csv", "db.csv"), ("jsonl", "db.jsonl")):
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="users"),
            destination=EndpointConfig(kind="file_export", format=fmt, output_path=name),
            sync_mode="full_refresh_append", skip_preflight=True,
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, (fmt, result.error)
        ledger = result.row_accounting or {}
        assert ledger.get("balanced") is True and ledger.get("dest_count_before") == 2, (fmt, ledger)
        assert ledger.get("conservation_kind") == "append_delta" and ledger.get("rows_written") == 2, ledger
    assert csv_target.read_bytes() == b"id,name\n1,alice\n2,bob\n3,carol\n4,dan\n"
    assert jsonl_target.read_bytes().count(b"\n") >= 3 and b'"carol"' in jsonl_target.read_bytes()
