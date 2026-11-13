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


# ── libraries bundled inside the wheels (auditwheel ``<pkg>.libs/``) ────────


def _fake_dist(tmp_path: Path, name: str, libs: dict[str, bytes]) -> tuple[str, str, list[Path]]:
    libdir = tmp_path / f"{name}.libs"
    libdir.mkdir(parents=True, exist_ok=True)
    paths = []
    for filename, content in libs.items():
        path = libdir / filename
        path.write_bytes(content)
        paths.append(path)
    return paths


def test_library_basename_strips_the_auditwheel_hash() -> None:
    assert fetch.lib_basename("libkeyutils-dfe70bd6.so.1.5") == "libkeyutils"
    assert fetch.lib_basename("libpcre2-8-516f4c9d.so.0.7.1") == "libpcre2-8"
    assert fetch.lib_basename("libgfortran-83c28eba-468e71e5.so.5.0.0") == "libgfortran"
    assert fetch.lib_basename("libscipy_openblas64_-f48b354e.so") == "libscipy_openblas64_"


def test_inventory_classifies_every_bundled_library(tmp_path: Path) -> None:
    libs = _fake_dist(tmp_path, "pymssql", {
        "libkeyutils-2777d33d.so.1.6": b"\x00keyutils-1.5.10\x00",
        "libssl-7f59e8e2.so.1.1.1k": b"openssl",
        "libkrb5-d4b313c5.so.3.3": b"krb5",
    })
    rows = fetch.inventory([("pymssql", "2.4.6", libs)])
    by_lib = {r["library"]: r for r in rows}
    assert by_lib["libkeyutils"]["copyleft"] is True
    assert by_lib["libkeyutils"]["source"] == ["keyutils", "1.5.10"]
    assert by_lib["libssl"]["copyleft"] is False and by_lib["libssl"]["source"] is None
    assert "OpenSSL" in by_lib["libssl"]["component"]


def test_an_unknown_bundled_library_fails_the_build(tmp_path: Path) -> None:
    libs = _fake_dist(tmp_path, "newwheel", {"libmystery-0123abcd.so.1": b"?"})
    with pytest.raises(fetch.SourceError, match="libmystery"):
        fetch.inventory([("newwheel", "1.0", libs)])


def test_a_copyleft_library_with_an_unidentifiable_version_fails_the_build(tmp_path: Path) -> None:
    # libcrypt carries no version string. The review is keyed by the bundled
    # file's auditwheel name (its hash suffix changes with its content), not by
    # the wheel's version: the reviewed file maps to libxcrypt 4.5.2 ...
    libs = _fake_dist(tmp_path, "psycopg2_binary", {"libcrypt-bc2db45e.so.1": b"_crypt_gensalt_sha1crypt_rn"})
    rows = fetch.inventory([("psycopg2-binary", "2.9.13", libs)])
    assert rows[0]["source"] == ["libxcrypt", "4.5.2"]
    # ... and a rebuilt one (new hash) fails closed with a clear message.
    new = _fake_dist(tmp_path / "next", "psycopg2_binary", {"libcrypt-0badc0de.so.1": b"_crypt_gensalt_sha1crypt_rn"})
    with pytest.raises(fetch.SourceError, match=r"review bundled libraries.*libcrypt-0badc0de\.so\.1"):
        fetch.inventory([("psycopg2-binary", "2.9.13", new)])


# ── review 3: the bundled-library review is hermetic ───────────────────────

bundled = sys.modules["bundled_libraries"]
CONSTRAINTS = API / "constraints-image.txt"


def _pins() -> dict[str, str]:
    pins = {}
    for raw in CONSTRAINTS.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            name, sep, version = line.partition("==")
            assert sep and version and ";" not in version, f"not an exact pin: {raw!r}"
            pins[bundled._canon(name)] = version.strip()
    return pins


def test_reviewed_library_versions_are_keyed_by_the_bundled_file_name() -> None:
    assert bundled.WHEEL_LIB_VERSIONS, "the libcrypt review must be recorded"
    for filename in bundled.WHEEL_LIB_VERSIONS:
        assert bundled._BASENAME.match(filename), f"{filename} is not an auditwheel name (<lib>-<hash>.so...)"


def test_a_wheel_version_that_was_not_reviewed_fails_with_review_bundled_libraries(tmp_path: Path) -> None:
    libs = _fake_dist(tmp_path, "numpy", {"libscipy_openblas64_-f48b354e.so": b"openblas"})
    assert fetch.inventory([("numpy", bundled.REVIEWED_WHEELS["numpy"], libs)])
    with pytest.raises(fetch.SourceError, match=r"review bundled libraries: numpy 99\.0 .*reviewed at 2\."):
        fetch.inventory([("numpy", "99.0", libs)])
    with pytest.raises(fetch.SourceError, match=r"review bundled libraries: brandnew 1\.0 .*never reviewed"):
        fetch.inventory([("brandnew", "1.0", _fake_dist(tmp_path, "brandnew", {"libssl-7f59e8e2.so.3": b"ssl"}))])


def test_a_new_copyleft_library_version_says_review_bundled_libraries(tmp_path: Path) -> None:
    libs = _fake_dist(tmp_path, "pymssql", {"libkeyutils-1234abcd.so.1.6": b"keyutils-1.6.3"})
    with pytest.raises(fetch.SourceError, match=r"review bundled libraries.*keyutils 1\.6\.3"):
        fetch.inventory([("pymssql", bundled.REVIEWED_WHEELS["pymssql"], libs)])


def test_the_image_install_is_locked_by_a_constraints_file() -> None:
    from packaging.requirements import Requirement

    pins = _pins()
    header = CONSTRAINTS.read_text(encoding="utf-8")
    assert "uv pip compile apps/api/requirements.txt" in header and "--python-version 3.12" in header
    for raw in (API / "requirements.txt").read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        req = Requirement(line)
        pin = pins.get(bundled._canon(req.name))
        assert pin, f"{req.name} is not pinned in {CONSTRAINTS.name}: regenerate it (command in its header)"
        assert req.specifier.contains(pin, prereleases=True), f"{req.name}=={pin} violates {line!r}"


def test_the_lock_pins_exactly_the_reviewed_wheels() -> None:
    pins = _pins()
    for dist, version in bundled.REVIEWED_WHEELS.items():
        assert pins.get(dist) == version, (
            f"review bundled libraries: {CONSTRAINTS.name} pins {dist}=={pins.get(dist)}, but its bundled "
            f"libraries were reviewed at {version} (scripts/bundled_libraries.py REVIEWED_WHEELS)"
        )


def test_every_pip_install_in_the_image_uses_the_lock() -> None:
    text = (REPO / "Dockerfile.api").read_text(encoding="utf-8").replace("\\\n", " ")
    assert "COPY apps/api/constraints-image.txt /app/constraints-image.txt" in text
    runs = "\n".join(line for line in text.splitlines() if line.startswith("RUN "))
    installs = [seg for seg in runs.split("pip install")[1:] if not seg.lstrip().startswith("--upgrade pip")]
    assert len(installs) >= 3  # runtime requirements, vendor drivers, preflight
    for seg in installs:
        head = seg.split("&&")[0].split(";")[0]
        assert "-c /app/constraints-image.txt" in head, f"pip install{head} is not constrained"


def test_the_ci_licence_gate_scans_the_locked_versions() -> None:
    import yaml

    jobs = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))["jobs"]
    runs = "\n".join(str(step.get("run", "")) for step in jobs["security"]["steps"])
    assert "-r apps/api/requirements-dev.txt -c apps/api/constraints-image.txt" in runs


def test_libcrypt_must_really_be_libxcrypt(tmp_path: Path) -> None:
    libs = _fake_dist(tmp_path, "psycopg2_binary", {"libcrypt-bc2db45e.so.1": b"glibc libcrypt"})
    with pytest.raises(fetch.SourceError, match="libxcrypt"):
        fetch.inventory([("psycopg2-binary", "2.9.13", libs)])


def test_gcc_runtime_libraries_map_to_the_gcc_source_of_their_build(tmp_path: Path) -> None:
    libs = _fake_dist(tmp_path, "numpy", {
        "libquadmath-2284e583-a9307bba.so.0.0.0": b"libquadmath.so.0.0.0-8.5.0-28.el8_10.alma.1.x86_64.debug",
        "libgfortran-83c28eba-468e71e5.so.5.0.0": b"libgfortran.so.5.0.0-8.5.0-28.el8_10.alma.1.x86_64.debug",
    })
    rows = fetch.inventory([("numpy", "2.5.4", libs)])
    assert {tuple(r["source"]) for r in rows} == {("gcc", "8.5.0-el8")}


def test_every_mapped_copyleft_source_has_a_pinned_checksum_or_says_why_not() -> None:
    for key, upstream in fetch.UPSTREAM.items():
        assert upstream.urls and all(u.startswith("https://") for u in upstream.urls), key
        assert upstream.sha256 or upstream.sha512 or upstream.note, key


def test_ship_fetches_bundled_copyleft_sources_once_with_checksums(tmp_path: Path, monkeypatch) -> None:
    src = tmp_path / "site"
    a = _fake_dist(src, "pymssql", {"libkeyutils-2777d33d.so.1.6": b"keyutils-1.5.10"})
    b = _fake_dist(src, "other", {"libkeyutils-1111aaaa.so.1.6": b"keyutils-1.5.10"})
    tarball = b"keyutils 1.5.10 source"
    upstream = fetch.Upstream(
        "keyutils-1.5.10.tar.bz2", ("https://mirror.example/keyutils-1.5.10.tar.bz2",),
        sha512=hashlib.sha512(tarball).hexdigest(),
    )
    monkeypatch.setitem(fetch.UPSTREAM, ("keyutils", "1.5.10"), upstream)
    monkeypatch.setitem(bundled.REVIEWED_WHEELS, "other", "1.0")
    net = FakeNet({"https://mirror.example/keyutils-1.5.10.tar.bz2": tarball})
    dest = tmp_path / "out"
    rows = fetch.ship([], dest, get=net, bundled=[("pymssql", "2.4.6", a), ("other", "1.0", b)])
    assert (dest / "keyutils-1.5.10.tar.bz2").read_bytes() == tarball
    assert len(rows) == 1 and rows[0]["verified_against"] == "pinned SHA-512"
    assert "pymssql 2.4.6" in rows[0]["for"] and "other 1.0" in rows[0]["for"]
    inventory = json.loads((dest / "BUNDLED_LIBRARIES.json").read_text(encoding="utf-8"))
    assert {r["dist"] for r in inventory["libraries"]} == {"pymssql", "other"}


def test_a_bundled_source_checksum_mismatch_fails_the_build(tmp_path: Path, monkeypatch) -> None:
    a = _fake_dist(tmp_path / "site", "pymssql", {"libkeyutils-2777d33d.so.1.6": b"keyutils-1.5.10"})
    monkeypatch.setitem(fetch.UPSTREAM, ("keyutils", "1.5.10"), fetch.Upstream(
        "k.tar.bz2", ("https://mirror.example/k.tar.bz2",), sha512="0" * 128))
    net = FakeNet({"https://mirror.example/k.tar.bz2": b"tampered"})
    with pytest.raises(fetch.SourceError, match="SHA-512"):
        fetch.ship([], tmp_path / "out", get=net, bundled=[("pymssql", "2.4.6", a)])


def test_dockerfile_asks_for_the_bundled_scan() -> None:
    text = (REPO / "Dockerfile.api").read_text(encoding="utf-8").replace("\\\n", " ")
    run = next(line for line in text.splitlines() if "fetch_lgpl_sources.py --dest" in line)
    assert "--bundled" in run


# ── libraries linked statically into extension modules (best effort) ───────


def _ext(tmp_path: Path, name: str, files: dict[str, bytes]) -> list[Path]:
    out = []
    for rel, content in files.items():
        path = tmp_path / name / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        out.append(path)
    return out


def test_static_scan_reports_well_known_libraries(tmp_path: Path) -> None:
    files = _ext(tmp_path, "pyarrow", {
        "libarrow.so.2600": b"x OpenSSL 3.6.2 1 Jan 2026\x00 inflate 1.3.2 Copyright 1995 libcurl/8.20.0 y",
        "sqlite.so": b"3.46.1 2024-08-13 09:16:08 c9c2ab54ba1f5f46360f1b4f35d849cd3f080e6fc2b6c60e91b16c63f69a1234",
        "plain.so": b"nothing to see",
    })
    rows = fetch.static_scan([("pyarrow", "26.0.0", files)])
    got = {(r["library"], r["version"]) for r in rows}
    assert {("OpenSSL", "3.6.2"), ("zlib", "1.3.2"), ("curl", "8.20.0"), ("SQLite", "3.46.1")} <= got
    assert all(not r["copyleft"] for r in rows)


def test_freetds_in_pymssql_is_mapped_elsewhere_it_fails(tmp_path: Path) -> None:
    ok = _ext(tmp_path, "pymssql", {"_mssql.so": b"freetds v1.5.19"})
    rows = fetch.static_scan([("pymssql", "2.4.6", ok)])
    assert rows[0]["copyleft"] and rows[0]["source"] == "fetch_lgpl_sources.py spec pymssql+freetds"
    other = _ext(tmp_path, "otherdriver", {"_ext.so": b"freetds v1.4.0"})
    with pytest.raises(fetch.SourceError, match="FreeTDS"):
        fetch.static_scan([("otherdriver", "1.0", other)])


@pytest.mark.parametrize(
    "blob,component",
    [
        (b"keyutils-1.6.3", "keyutils"),
        (b"GNU Readline library", "GNU Readline"),
        (b"Libgcrypt 1.10.3", "Libgcrypt"),
        (b"GnuTLS 3.8.4", "GnuTLS"),
    ],
)
def test_an_unmapped_copyleft_static_library_fails_the_build(tmp_path: Path, blob, component) -> None:
    files = _ext(tmp_path, "newwheel", {"_native.so": blob})
    with pytest.raises(fetch.SourceError, match=component):
        fetch.static_scan([("newwheel", "1.0", files)])


def test_extension_module_listing_skips_libs_dirs_and_operator_installed_wheels(tmp_path: Path) -> None:
    sys.path.insert(0, str(REPO / "scripts"))
    import bundled_libraries as bl

    class _File:
        def __init__(self, rel: str):
            self.parts = tuple(rel.split("/"))
            self.name = self.parts[-1]
            self._rel = rel

        def locate(self):
            return tmp_path / self._rel

    class _Dist:
        def __init__(self, name, files):
            self.metadata = {"Name": name}
            self.version = "1.0"
            self.files = [_File(f) for f in files]

    dists = [
        _Dist("pyarrow", ["pyarrow/libarrow.so.2600", "pyarrow.libs/libz-0123abcd.so.1", "pyarrow/x.py"]),
        _Dist("ibm-db", ["ibm_db.cpython-312-x86_64-linux-gnu.so"]),
    ]
    listed = bl.installed_extension_modules(dists)
    assert [(n, [p.name for p in paths]) for n, _v, paths in listed] == [("pyarrow", ["libarrow.so.2600"])]


def test_every_mapped_static_library_is_in_the_dockerfile_step() -> None:
    specs = {s.dist: s for s in fetch.dockerfile_specs((REPO / "Dockerfile.api").read_text(encoding="utf-8"))}
    sys.path.insert(0, str(REPO / "scripts"))
    import bundled_libraries as bl

    for (dist, signature), _how in bl.STATIC_MAPPED.items():
        assert signature in specs[dist].extras, (dist, signature)
