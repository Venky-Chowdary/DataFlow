"""LGPL corresponding source ships inside the API image (owner decision 2, revised).

The owner accepted paramiko, psycopg2-binary and pymssql "with attribution",
without the ongoing obligation of a written offer. LGPL binary distribution is
then satisfied by shipping the corresponding source with the binaries:
``Dockerfile.api`` runs ``scripts/fetch_lgpl_sources.py``, which downloads the
source of exactly the version installed in the image and records SHA-256
checksums under /usr/share/doc/datawrap/third-party-source/.

These tests keep the Dockerfile step, the approved policy entries and the
requirements consistent (offline), and pin the fetch script's behaviour with a
fake index.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
SOURCE_DIR = "/usr/share/doc/datawrap/third-party-source"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


fetch = _load("fetch_lgpl_sources", REPO / "scripts" / "fetch_lgpl_sources.py")
scan = _load("licence_scan_for_lgpl", REPO / "scripts" / "licence_scan.py")


def _approved_lgpl() -> set[str]:
    policy = scan.load_policy(REPO / "licence-policy.json")
    return {
        key.split(":", 1)[1]
        for key, entry in policy["exceptions"].items()
        if key.startswith("pypi:")
        and "GPL" in entry["licence"]
        and (entry.get("decision") or {}).get("status") == "approved"
    }


def test_dockerfile_step_lists_exactly_the_approved_lgpl_packages() -> None:
    specs = fetch.dockerfile_specs((REPO / "Dockerfile.api").read_text(encoding="utf-8"))
    assert {s.dist for s in specs} == _approved_lgpl() == {"paramiko", "psycopg2-binary", "pymssql"}
    by_dist = {s.dist: s for s in specs}
    # psycopg2-binary's corresponding source is the psycopg2 sdist of the same version.
    assert by_dist["psycopg2-binary"].sdist == "psycopg2"
    # pymssql's manylinux wheel links FreeTDS (LGPL) statically: ship its source too.
    assert by_dist["pymssql"].extras == ("freetds",)


def test_every_shipped_lgpl_package_is_a_runtime_requirement() -> None:
    declared = set()
    for raw in (API / "requirements.txt").read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            declared.add(line.split(">")[0].split("=")[0].split("<")[0].split("[")[0].strip().lower())
    specs = fetch.dockerfile_specs((REPO / "Dockerfile.api").read_text(encoding="utf-8"))
    assert {s.dist for s in specs} <= declared


def test_dockerfile_step_runs_after_the_runtime_install_into_the_documented_dir() -> None:
    text = (REPO / "Dockerfile.api").read_text(encoding="utf-8")
    install = text.index("pip install -r /app/requirements.txt")
    step = text.index("fetch_lgpl_sources.py --dest")
    assert install < step, "the versions are read from the installed packages"
    assert f"--dest {SOURCE_DIR}" in text


def test_policy_notices_point_at_the_shipped_source() -> None:
    policy = scan.load_policy(REPO / "licence-policy.json")
    for dist in _approved_lgpl():
        assert f"source shipped in image at {SOURCE_DIR}" in policy["exceptions"][f"pypi:{dist}"]["notice"]


# ── the fetch script, against a fake index ─────────────────────────────────


def test_parse_spec() -> None:
    assert fetch.parse_spec("paramiko") == fetch.Spec("paramiko", "paramiko", ())
    assert fetch.parse_spec("psycopg2-binary=psycopg2") == fetch.Spec("psycopg2-binary", "psycopg2", ())
    assert fetch.parse_spec("pymssql+freetds") == fetch.Spec("pymssql", "pymssql", ("freetds",))
    with pytest.raises(ValueError):
        fetch.parse_spec("pymssql+gpl-thing")


def test_freetds_version_is_read_from_the_linked_binary(tmp_path: Path) -> None:
    so = tmp_path / "_mssql.so"
    so.write_bytes(b"\x00\x01 junk freetds v1.5.19\x00 more /root/freetds/freetds-1.5.19/src")
    assert fetch.freetds_version([so]) == "1.5.19"
    plain = tmp_path / "other.so"
    plain.write_bytes(b"no version here")
    with pytest.raises(fetch.SourceError, match="FreeTDS"):
        fetch.freetds_version([plain])


def _index(name: str, version: str, payload: bytes, *, bad_digest: bool = False) -> dict:
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "urls": [
            {"packagetype": "bdist_wheel", "filename": f"{name}-{version}-py3-none-any.whl",
             "url": "https://files.example/wheel", "digests": {"sha256": "0" * 64}},
            {"packagetype": "sdist", "filename": f"{name}-{version}.tar.gz",
             "url": f"https://files.example/{name}-{version}.tar.gz",
             "digests": {"sha256": ("f" * 64) if bad_digest else digest}},
        ]
    }


class FakeNet:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.seen: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.seen.append(url)
        if url not in self.files:
            raise fetch.SourceError(f"404 {url}")
        return self.files[url]


def test_fetch_ships_the_installed_versions_sdist_with_checksums(tmp_path: Path) -> None:
    payload = b"paramiko source"
    net = FakeNet({
        "https://pypi.org/pypi/paramiko/5.1.0/json": json.dumps(_index("paramiko", "5.1.0", payload)).encode(),
        "https://files.example/paramiko-5.1.0.tar.gz": payload,
    })
    rows = fetch.ship(
        [fetch.parse_spec("paramiko")], tmp_path, get=net, version_of=lambda dist: "5.1.0"
    )
    assert (tmp_path / "paramiko-5.1.0.tar.gz").read_bytes() == payload
    sums = (tmp_path / "SHA256SUMS").read_text(encoding="utf-8")
    assert f"{hashlib.sha256(payload).hexdigest()}  paramiko-5.1.0.tar.gz" in sums
    assert rows[0]["verified_against"] == "PyPI sha256 digest"
    manifest = json.loads((tmp_path / "MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["components"][0]["for"] == "paramiko 5.1.0"


def test_a_checksum_mismatch_fails_the_build(tmp_path: Path) -> None:
    net = FakeNet({
        "https://pypi.org/pypi/paramiko/5.1.0/json": json.dumps(
            _index("paramiko", "5.1.0", b"x", bad_digest=True)
        ).encode(),
        "https://files.example/paramiko-5.1.0.tar.gz": b"x",
    })
    with pytest.raises(fetch.SourceError, match="SHA-256"):
        fetch.ship([fetch.parse_spec("paramiko")], tmp_path, get=net, version_of=lambda d: "5.1.0")


def test_no_sdist_for_the_installed_version_fails_the_build(tmp_path: Path) -> None:
    index = _index("paramiko", "5.1.0", b"x")
    index["urls"] = [u for u in index["urls"] if u["packagetype"] != "sdist"]
    net = FakeNet({"https://pypi.org/pypi/paramiko/5.1.0/json": json.dumps(index).encode()})
    with pytest.raises(fetch.SourceError, match="no source distribution"):
        fetch.ship([fetch.parse_spec("paramiko")], tmp_path, get=net, version_of=lambda d: "5.1.0")


def test_freetds_source_follows_the_linked_version(tmp_path: Path, monkeypatch) -> None:
    payload = b"pymssql source"
    tds = b"freetds tarball"
    tds_sum = hashlib.sha256(tds).hexdigest()
    net = FakeNet({
        "https://pypi.org/pypi/pymssql/2.4.6/json": json.dumps(_index("pymssql", "2.4.6", payload)).encode(),
        "https://files.example/pymssql-2.4.6.tar.gz": payload,
        "https://www.freetds.org/files/stable/freetds-1.5.19.tar.gz": tds,
        "https://www.freetds.org/files/stable/freetds-1.5.19.tar.gz.sha256": f"{tds_sum}  freetds-1.5.19.tar.gz\n".encode(),
    })
    monkeypatch.setattr(fetch, "linked_binaries", lambda dist: [])
    monkeypatch.setattr(fetch, "freetds_version", lambda binaries: "1.5.19")
    rows = fetch.ship([fetch.parse_spec("pymssql+freetds")], tmp_path, get=net, version_of=lambda d: "2.4.6")
    assert (tmp_path / "freetds-1.5.19.tar.gz").read_bytes() == tds
    assert {r["file"] for r in rows} == {"pymssql-2.4.6.tar.gz", "freetds-1.5.19.tar.gz"}
    freetds_row = next(r for r in rows if r["file"].startswith("freetds"))
    assert freetds_row["for"] == "FreeTDS 1.5.19 (statically linked into pymssql 2.4.6)"
    assert freetds_row["verified_against"] == "freetds.org published SHA-256"


def test_freetds_without_a_published_checksum_is_labelled_not_claimed(tmp_path: Path, monkeypatch) -> None:
    net = FakeNet({
        "https://pypi.org/pypi/pymssql/2.4.6/json": json.dumps(_index("pymssql", "2.4.6", b"p")).encode(),
        "https://files.example/pymssql-2.4.6.tar.gz": b"p",
        "https://www.freetds.org/files/stable/freetds-1.5.19.tar.gz": b"t",
    })
    monkeypatch.setattr(fetch, "linked_binaries", lambda dist: [])
    monkeypatch.setattr(fetch, "freetds_version", lambda binaries: "1.5.19")
    rows = fetch.ship([fetch.parse_spec("pymssql+freetds")], tmp_path, get=net, version_of=lambda d: "2.4.6")
    label = next(r for r in rows if r["file"].startswith("freetds"))["verified_against"]
    assert "publishes no checksum" in label
