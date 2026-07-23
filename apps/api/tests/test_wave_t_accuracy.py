"""Wave T accuracy: vector identity, email/SFTP policy, atomic SFTP, Couchbase order."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_pinecone_blank_id_is_deterministic_and_distinct():
    from connectors.pinecone_writer import build_pinecone_vectors

    rows = [
        {
            "id": "",
            "source_id": "doc-a",
            "chunk_index": 0,
            "content": "alpha",
            "embedding": [0.1, 0.2, 0.3],
        },
        {
            "id": "",
            "source_id": "doc-b",
            "chunk_index": 0,
            "content": "beta",
            "embedding": [0.4, 0.5, 0.6],
        },
    ]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert rejected == []
    assert len(vectors) == 2
    assert vectors[0]["id"] != vectors[1]["id"]
    assert all(v["id"] for v in vectors)

    again, _ = build_pinecone_vectors(rows[:1], dimension=3)
    assert again[0]["id"] == vectors[0]["id"]


def test_pinecone_blank_id_without_identity_quarantines():
    from connectors.pinecone_writer import build_pinecone_vectors

    vectors, rejected = build_pinecone_vectors(
        [{"id": "", "content": "", "source_id": "", "embedding": [0.1, 0.2, 0.3]}],
        dimension=3,
    )
    assert vectors == []
    assert rejected
    assert "missing id" in (rejected[0].get("reason") or "").lower()


def test_weaviate_blank_id_is_deterministic():
    from connectors.weaviate_writer import build_weaviate_objects

    row = {
        "id": "",
        "source_id": "s1",
        "chunk_index": 0,
        "content": "hello",
        "embedding": [0.1, 0.2],
    }
    a, ra = build_weaviate_objects([row], class_name="DataflowChunk", dimension=2)
    b, rb = build_weaviate_objects([row], class_name="DataflowChunk", dimension=2)
    assert ra == [] and rb == []
    assert a[0]["id"] == b[0]["id"]


def test_weaviate_blank_id_without_identity_quarantines():
    from connectors.weaviate_writer import build_weaviate_objects

    objects, rejected = build_weaviate_objects(
        [{"id": "", "content": "", "source_id": "", "embedding": [0.1, 0.2]}],
        class_name="DataflowChunk",
        dimension=2,
    )
    assert objects == []
    assert rejected
    assert "missing id" in (rejected[0].get("reason") or "").lower()


def test_weaviate_object_uuid_refuses_empty():
    from connectors.weaviate_writer import _object_uuid

    with pytest.raises(ValueError, match="refuse random UUID"):
        _object_uuid("")


def test_email_fail_policy_skips_smtp():
    from connectors import email as email_connector

    with patch("connectors.email.smtplib") as mock_smtplib:
        result = email_connector.write_mapped_rows(
            host="localhost",
            port=1025,
            username="u",
            password="p",
            database="to@example.com",
            table_name="payments",
            headers=["id", "amount"],
            data_rows=[["1", "not-a-number"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "INTEGER", "amount": "DECIMAL"},
            error_policy="fail",
        )
    assert result.ok is False
    assert "Transform errors" in (result.error or "")
    mock_smtplib.SMTP.assert_not_called()
    mock_smtplib.SMTP_SSL.assert_not_called()


def test_email_quarantine_policy_still_sends_valid_rows():
    from connectors import email as email_connector

    server = MagicMock()
    with patch("connectors.email.smtplib") as mock_smtplib:
        mock_smtplib.SMTP.return_value.__enter__ = MagicMock(return_value=server)
        mock_smtplib.SMTP.return_value.__exit__ = MagicMock(return_value=False)
        result = email_connector.write_mapped_rows(
            host="localhost",
            port=1025,
            username="u",
            password="p",
            database="to@example.com",
            table_name="payments",
            headers=["id", "amount"],
            data_rows=[["1", "10.5"], ["2", "bad"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "INTEGER", "amount": "DECIMAL"},
            error_policy="quarantine",
        )
    assert result.ok is True
    assert result.rows_written == 2  # quarantine keeps row with nullified cell
    assert result.rejected_details
    server.sendmail.assert_called_once()


def test_sftp_writes_temp_then_posix_rename():
    from connectors.sftp_writer import write_mapped_rows as write_sftp_rows

    sftp = MagicMock()
    file_handle = MagicMock()
    file_handle.__enter__ = MagicMock(return_value=file_handle)
    file_handle.__exit__ = MagicMock(return_value=False)
    sftp.file.return_value = file_handle
    transport = MagicMock()

    with patch("connectors.sftp_writer.connect_sftp", return_value=(transport, sftp)):
        result = write_sftp_rows(
            connection_string="sftp://u:p@host/data/out.csv",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
        )

    assert result.ok is True
    written_path = sftp.file.call_args[0][0]
    assert written_path.startswith("/data/out.csv.dataflow-")
    assert written_path.endswith(".tmp")
    sftp.posix_rename.assert_called_once_with(written_path, "/data/out.csv")


def test_sftp_rename_failure_is_fail_closed():
    from connectors.sftp_writer import write_mapped_rows as write_sftp_rows

    sftp = MagicMock()
    file_handle = MagicMock()
    file_handle.__enter__ = MagicMock(return_value=file_handle)
    file_handle.__exit__ = MagicMock(return_value=False)
    sftp.file.return_value = file_handle
    sftp.posix_rename.side_effect = OSError("rename failed")
    transport = MagicMock()

    with patch("connectors.sftp_writer.connect_sftp", return_value=(transport, sftp)):
        result = write_sftp_rows(
            connection_string="sftp://u:p@host/data/out.csv",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
        )

    assert result.ok is False
    assert "SFTP write failed" in (result.error or "")
    sftp.remove.assert_called()  # best-effort temp cleanup


def test_couchbase_offset_orders_by_meta_id():
    from connectors.couchbase import read_object

    seen: list[str] = []

    def fake_n1ql(_url, _user, _password, statement):
        seen.append(statement)
        return {"results": []}

    with patch("connectors.couchbase._n1ql", side_effect=fake_n1ql):
        batch = read_object(
            cfg={"host": "localhost", "port": 8093, "username": "u", "password": "p"},
            object="travel-sample",
            limit=50,
            offset=100,
        )

    assert len(seen) == 1
    stmt = seen[0]
    assert "ORDER BY META().id" in stmt
    assert stmt.index("ORDER BY META().id") < stmt.index("LIMIT")
    assert "OFFSET 100" in stmt
    assert batch.total_rows is None
