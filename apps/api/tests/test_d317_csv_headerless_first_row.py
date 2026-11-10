"""D317 — a header-less CSV silently lost its first record.

QA: a 30-row SFTP CSV with no header line (``1,Alice,5,10.5,true,2024-01-02``)
landed 29 rows, the columns named after that first record, and the job said
"row fidelity verified": COUNT and the Gate-8 checksum read the file through the
same "row 1 is the header" rule, so they matched the loss.

Root cause: ``services.tabular_window.header_and_rows`` took physical row 1 as
the header whenever ``header_row`` was the default, with no look at the data.
Every CSV consumer (preview, write stream, COUNT, Gate-8 dicts) calls it.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.csv_profiler import count_csv_rows, iter_csv_dicts  # noqa: E402
from services.read_options import ReadOptions, ReadOptionsError  # noqa: E402


def _headerless(n: int = 30, eol: str = "\n") -> bytes:
    names = ["Alice", "Bob", "Chloé", "Dmitri", "Eve"]
    lines = [
        f"{i},{names[i % 5]},{i % 9},{i}.5,{'true' if i % 2 else 'false'},2024-01-{(i % 28) + 1:02d}"
        for i in range(1, n + 1)
    ]
    lines[0] = "1,Alice,5,10.5,true,2024-01-02"  # QA's first line
    return (eol.join(lines) + eol).encode("utf-8")


def _with_header(eol: str = "\n") -> bytes:
    return b"id,name,qty,amount,active,day" + eol.encode() + _headerless(eol=eol)


def test_headerless_csv_keeps_every_row_under_positional_names():
    rows = list(iter_csv_dicts(_headerless()))
    assert len(rows) == 30
    assert list(rows[0]) == ["col_0", "col_1", "col_2", "col_3", "col_4", "col_5"]
    assert rows[0]["col_1"] == "Alice" and rows[0]["col_3"] == "10.5"
    # The verifier's source COUNT sees the first record too.
    assert count_csv_rows(_headerless()) == 30


def test_header_present_is_still_the_header():
    rows = list(iter_csv_dicts(_with_header()))
    assert len(rows) == 30
    assert list(rows[0]) == ["id", "name", "qty", "amount", "active", "day"]
    assert count_csv_rows(_with_header()) == 30


@pytest.mark.parametrize(
    "payload", [_headerless(eol="\r\n"), _with_header(eol="\r\n")], ids=["no_header", "header"]
)
def test_crlf_files_decide_the_same_way(payload):
    from src.transfer.file_stream import peek_file_source

    headers, _schema, total, sample = peek_file_source(payload, "drop.csv")
    assert total == 30
    assert sample[0][headers[1]] == "Alice"
    assert all("\r" not in h for h in headers)


def test_ambiguous_all_text_file_keeps_row_one_as_header():
    data = b"alpha,beta\ngamma,delta\nepsilon,zeta\n"
    rows = list(iter_csv_dicts(data))
    assert list(rows[0]) == ["alpha", "beta"] and len(rows) == 2


def test_integer_pivot_header_is_not_mistaken_for_data():
    data = b"region,2023,2024\nNorth,10,12\nSouth,7,9\n"
    rows = list(iter_csv_dicts(data))
    assert list(rows[0]) == ["region", "2023", "2024"] and len(rows) == 2


def test_has_header_option_overrides_the_sniff():
    forced = ReadOptions.from_dict({"has_header": True})
    assert forced.header_row == 1 and not forced.sniff_header
    assert count_csv_rows(_headerless(), options=forced) == 29

    absent = ReadOptions.from_dict({"has_header": "false"})
    assert absent.header_row == 0
    assert count_csv_rows(_with_header(), options=absent) == 31

    assert ReadOptions.from_dict({"has_header": "auto"}) == ReadOptions()
    with pytest.raises(ReadOptionsError):
        ReadOptions.from_dict({"has_header": False, "header_row": 2})


@pytest.fixture()
def fake_mongo(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


@pytest.mark.parametrize(
    "payload,first_col",
    [(_headerless(), "col_0"), (_with_header(), "id")],
    ids=["no_header", "header"],
)
def test_sftp_csv_to_sqlite_lands_all_thirty_rows(tmp_path: Path, fake_mongo, payload, first_col):
    pytest.importorskip("paramiko")
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "srv"
    root.mkdir()
    (root / "people.csv").write_bytes(payload)
    server, runner = start_sftp_server(str(root))
    dst = tmp_path / "dst.sqlite"
    try:
        cfg = server.endpoint_config("/people.csv")
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
                username=cfg["username"], password=cfg["password"],
                database=cfg["database"], table=cfg["table"],
                extra={"host_key": cfg["host_key"]},
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="people"
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=False,
            validation_mode="strict",
        )
        job_id = "d317" + uuid.uuid4().hex[:16]
        fake_mongo.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
    finally:
        runner.stop()

    assert result.success, result.error
    assert result.records_transferred == 30
    conn = sqlite3.connect(dst)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(people)")]
        assert cols[0] == first_col and len(cols) == 6, cols
        assert conn.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 30
        name_col = cols[1]
        names = {r[0] for r in conn.execute(f'SELECT "{name_col}" FROM people')}
        assert "Alice" in names
    finally:
        conn.close()
