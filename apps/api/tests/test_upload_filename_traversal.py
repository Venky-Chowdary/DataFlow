"""Upload filenames never steer a filesystem write outside the upload directory."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import file_parser as fp  # noqa: E402

TRAVERSAL_NAMES = [
    "../probe.csv",
    "../../../../tmp/probe.csv",
    "../services/evil.py",
    "..\\..\\probe.csv",
    "/etc/probe.csv",
    "C:\\Windows\\probe.csv",
    "sub/dir/../../probe.csv",
    "..",
    ".",
    "",
    "probe\x00.csv",
]


@pytest.fixture()
def isolated_upload_dir(tmp_path, monkeypatch):
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(fp, "UPLOAD_DIR", uploads)
    monkeypatch.setattr(fp, "REGISTRY_PATH", tmp_path / "reg.json")
    monkeypatch.setattr(fp, "upload_dir", lambda: uploads)
    monkeypatch.setattr(fp, "_save_registry", lambda: None)
    return uploads


def _files_outside(root: Path, base: Path) -> list[Path]:
    return [p for p in base.rglob("*") if p.is_file() and root not in p.parents and p.name != "reg.json"]


@pytest.mark.parametrize("name", TRAVERSAL_NAMES)
def test_store_upload_stays_inside_upload_dir(isolated_upload_dir, name):
    record = fp.store_upload(name, b"a,b\n1,2\n")
    path = Path(record["path"])
    assert path.parent == isolated_upload_dir.resolve()
    assert path.read_bytes() == b"a,b\n1,2\n"
    assert "/" not in record["filename"] and "\\" not in record["filename"]
    assert _files_outside(isolated_upload_dir, isolated_upload_dir.parent) == []


def test_store_upload_keeps_plain_display_name(isolated_upload_dir):
    record = fp.store_upload("Q3 sales.csv", b"a\n1\n")
    assert record["filename"] == "Q3 sales.csv"
    assert Path(record["path"]).name == f"{record['file_id']}_Q3_sales.csv"


@pytest.mark.parametrize("name", TRAVERSAL_NAMES)
def test_get_file_materialize_stays_inside_upload_dir(isolated_upload_dir, monkeypatch, name):
    import services.object_store as object_store

    written: list[Path] = []

    def fake_materialize(uri, dest):
        written.append(Path(dest))
        Path(dest).write_bytes(b"x\n")
        return dest

    monkeypatch.setattr(object_store, "materialize_local", fake_materialize)
    fid = "deadbeefcafef00d"
    fp._file_registry[fid] = {
        "file_id": fid,
        "filename": name,
        "path": str(isolated_upload_dir / "missing.csv"),
        "object_uri": "s3://bucket/uploads/x",
    }
    try:
        record = fp.get_file(fid)
    finally:
        fp._file_registry.pop(fid, None)
    assert record is not None
    assert len(written) == 1
    assert written[0].parent == isolated_upload_dir.resolve()
    assert _files_outside(isolated_upload_dir, isolated_upload_dir.parent) == []


def test_upload_path_for_refuses_escaping_file_id(tmp_path):
    path = fp.upload_path_for(tmp_path, "../../x", "a.csv")
    assert path.parent == tmp_path.resolve()


@pytest.mark.parametrize(
    ("raw", "safe"),
    [
        ("../../etc/passwd", "passwd"),
        ("..\\..\\boot.ini", "boot.ini"),
        (".env", "env"),
        ("..", "upload.bin"),
        ("my file (1).csv", "my_file__1_.csv"),
    ],
)
def test_safe_upload_filename(raw, safe):
    assert fp.safe_upload_filename(raw) == safe
