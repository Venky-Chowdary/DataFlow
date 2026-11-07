"""A file transfer reads ``source_path`` only from the upload/staging roots."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import dataset_file  # noqa: E402
from services.dataset_file import confine_source_path  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.file_stream import prepare_stream_content  # noqa: E402
from src.transfer.models import (  # noqa: E402
    EndpointConfig,
    TransferRequest,
    transfer_request_from_dict,
    transfer_request_to_dict,
)


@pytest.fixture
def uploads(tmp_path, monkeypatch):
    root = tmp_path / "uploads"
    root.mkdir()
    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [root])
    monkeypatch.delenv("DATAFLOW_SOURCE_PATH_ROOTS", raising=False)
    monkeypatch.delenv("DATAWRAP_SOURCE_PATH_ROOTS", raising=False)
    return root


@pytest.fixture
def outside(tmp_path):
    secret = tmp_path / "host" / "secret.csv"
    secret.parent.mkdir()
    secret.write_text("user,token\nroot,s3cr3t\n")
    return secret


def _file_request(path: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="t"),
        source_filename="data.csv",
        source_path=path,
    )


def test_etc_passwd_is_refused_everywhere(uploads):
    with pytest.raises(ValueError, match="outside the upload directory"):
        confine_source_path("/etc/passwd")
    with pytest.raises(ValueError, match="outside the upload directory"):
        prepare_stream_content(source_path="/etc/passwd")
    with pytest.raises(ValueError, match="outside the upload directory"):
        UniversalTransferEngine()._read_source(_file_request("/etc/passwd"))


def test_outside_path_refused_before_existence_is_checked(uploads, outside):
    missing = outside.parent / "does-not-exist.csv"
    for path in (outside, missing):
        with pytest.raises(ValueError, match="outside the upload directory"):
            prepare_stream_content(content=b"a\n1\n", source_path=str(path))


def test_dotdot_traversal_out_of_uploads_is_refused(uploads, outside):
    sneaky = uploads / ".." / "host" / "secret.csv"
    assert sneaky.is_file()
    with pytest.raises(ValueError, match="outside the upload directory"):
        prepare_stream_content(source_path=str(sneaky))


def test_symlink_inside_uploads_pointing_out_is_refused(uploads, outside):
    link = uploads / "innocent.csv"
    try:
        os.symlink(outside, link)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    with pytest.raises(ValueError, match="outside the upload directory"):
        prepare_stream_content(source_path=str(link))
    with pytest.raises(ValueError, match="outside the upload directory"):
        UniversalTransferEngine()._read_source(_file_request(str(link)))


def test_serialized_request_drops_outside_path(uploads, outside):
    payload = transfer_request_to_dict(_file_request(str(outside)))
    assert transfer_request_from_dict(payload).source_path == ""
    payload["source_path"] = "/etc/passwd"
    assert transfer_request_from_dict(payload).source_path == ""


def test_staged_upload_is_still_read(uploads):
    staged = uploads / "xfer_abc123_data.csv"
    staged.write_text("id,name\n1,a\n2,b\n")
    assert prepare_stream_content(source_path=str(staged)) == str(staged.resolve())
    rows, columns, _ = UniversalTransferEngine()._read_source(_file_request(str(staged)))
    assert columns == ["id", "name"]
    assert len(rows) == 2
    payload = transfer_request_to_dict(_file_request(str(staged)))
    assert transfer_request_from_dict(payload).source_path == str(staged.resolve())


def test_missing_file_under_uploads_falls_back_to_content(uploads):
    gone = uploads / "xfer_gone_data.csv"
    assert prepare_stream_content(content=b"a\n1\n", source_path=str(gone)) == b"a\n1\n"


def test_operator_declared_root_is_honored(uploads, outside, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(outside.parent))
    assert prepare_stream_content(source_path=str(outside)) == str(outside.resolve())
