"""Debian packages in the API image: copyleft ones ship their source (review 11).

The image ships Debian packages: the base image's own, plus unixodbc /
libodbc2 (LGPL-2.1) and curl from Dockerfile.api. Binary distribution of a
GPL / LGPL package needs its corresponding source, so the image build runs
``scripts/fetch_debian_sources.py``. It:

* lists every installed package with dpkg-query (exact source versions);
* reads each package's licence from /usr/share/doc/<pkg>/copyright, and fails
  on a package whose licence it cannot tell;
* for every copyleft package, downloads the .dsc, orig and debian tarballs of
  the exact source version from deb.debian.org (checked against apt's signed
  Sources index) or snapshot.debian.org (content-addressed). It verifies every
  file against the .dsc checksums, into the same directory as the other
  shipped sources.

Dockerfile.api is multi-stage: build-essential lives in the build stage only.
These tests run offline with a fake archive.
"""

from __future__ import annotations

import gzip
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


deb = _load("fetch_debian_sources", REPO / "scripts" / "fetch_debian_sources.py")


# ── fake archive ───────────────────────────────────────────────────────────

UNIXODBC_VER = "2.3.11-2+deb12u1"
FILES = {
    "unixodbc_2.3.11.orig.tar.gz": b"unixodbc upstream source",
    "unixodbc_2.3.11-2+deb12u1.debian.tar.xz": b"unixodbc debian packaging",
}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _dsc(files: dict[str, bytes], *, source: str = "unixodbc", version: str = UNIXODBC_VER) -> bytes:
    lines = ["Format: 3.0 (quilt)", f"Source: {source}", f"Version: {version}", "Checksums-Sha256:"]
    lines += [f" {_sha256(data)} {len(data)} {name}" for name, data in files.items()]
    lines += ["Files:"] + [f" {hashlib.md5(d).hexdigest()} {len(d)} {n}" for n, d in files.items()]  # noqa: S324
    return ("\n".join(lines) + "\n").encode()


DSC_NAME = f"unixodbc_{UNIXODBC_VER}.dsc"
DSC = _dsc(FILES)
ARCHIVE = {DSC_NAME: DSC, **FILES}


def _sources_index(archive: dict[str, bytes], *, source="unixodbc", version=UNIXODBC_VER,
                   directory="pool/main/u/unixodbc") -> str:
    lines = [f"Package: {source}", f"Version: {version}", f"Directory: {directory}", "Checksums-Sha256:"]
    lines += [f" {_sha256(d)} {len(d)} {n}" for n, d in archive.items()]
    return "\n".join(lines) + "\n\n"


def _lists(tmp_path: Path, text: str, *, gz: bool = False) -> Path:
    lists = tmp_path / "lists"
    lists.mkdir(parents=True, exist_ok=True)
    name = "deb.debian.org_debian-security_dists_bookworm-security_main_source_Sources"
    if gz:
        (lists / f"{name}.gz").write_bytes(gzip.compress(text.encode()))
    else:
        (lists / name).write_text(text, encoding="utf-8")
    return lists


class FakeNet:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.seen: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.seen.append(url)
        if url not in self.files:
            raise deb.SourceError(f"404 {url}")
        return self.files[url]


POOL = "https://deb.debian.org/debian-security/pool/main/u/unixodbc/"
PKGS = [
    deb.DebPackage("unixodbc", "2.3.11-2+deb12u1", "unixodbc", UNIXODBC_VER),
    deb.DebPackage("libodbc2", "2.3.11-2+deb12u1", "unixodbc", UNIXODBC_VER),
    deb.DebPackage("zlib1g", "1:1.2.13.dfsg-1", "zlib", "1:1.2.13.dfsg-1"),
]


def _docs(tmp_path: Path, texts: dict[str, str]) -> Path:
    root = tmp_path / "doc"
    for pkg, text in texts.items():
        (root / pkg).mkdir(parents=True, exist_ok=True)
        (root / pkg / "copyright").write_text(text, encoding="utf-8")
    return root


LGPL_DEP5 = (
    "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/\n\n"
    "Files: *\nCopyright: 1998 Peter Harvey\nLicense: LGPL-2.1+\n"
)
ZLIB_DEP5 = "Files: *\nCopyright: 1995 Jean-loup Gailly\nLicense: Zlib\n"


# ── packages and licences ──────────────────────────────────────────────────


def test_installed_packages_come_from_dpkg_query_with_source_versions() -> None:
    out = (
        "unixodbc\t2.3.11-2+deb12u1\tunixodbc\t2.3.11-2+deb12u1\tinstall ok installed\n"
        "bash\t5.2.15-2+b13\tbash\t5.2.15-2\tinstall ok installed\n"
        "gone\t1.0\tgone\t1.0\tdeinstall ok config-files\n"
    )
    pkgs = deb.installed_packages(lambda argv: out)
    assert [(p.name, p.source, p.source_version) for p in pkgs] == [
        ("unixodbc", "unixodbc", "2.3.11-2+deb12u1"),
        ("bash", "bash", "5.2.15-2"),  # binNMU +b13 is not a source version
    ]


@pytest.mark.parametrize(
    ("text", "status"),
    [
        (LGPL_DEP5, "copyleft"),
        ("On Debian systems, see /usr/share/common-licenses/GPL-3.\n", "copyleft"),
        (ZLIB_DEP5, "permissive"),
        ("License: public-domain\n", "permissive"),
        ("Files: *\nLicense: Expat\n", "permissive"),
        ("Files: *\nLicense: MIT AND GPL-2+\n", "copyleft"),
        ("Copyright 2020 nobody. All rights reserved.\n", "unknown"),
    ],
)
def test_licence_is_read_from_the_debian_copyright_file(tmp_path: Path, text: str, status: str) -> None:
    root = _docs(tmp_path, {"pkg": text})
    assert deb.classify(deb.DebPackage("pkg", "1", "pkg", "1"), root) == status


def test_a_package_without_a_copyright_file_is_unknown(tmp_path: Path) -> None:
    assert deb.classify(deb.DebPackage("ghost", "1", "ghost", "1"), tmp_path) == "unknown"


def test_dsc_checksums_are_parsed() -> None:
    assert deb.parse_dsc(DSC.decode()) == {name: _sha256(data) for name, data in FILES.items()}


def test_the_signed_sources_index_is_read_plain_or_gzipped(tmp_path: Path) -> None:
    for gz in (False, True):
        lists = _lists(tmp_path / str(gz), _sources_index(ARCHIVE), gz=gz)
        index = deb.load_source_index(lists)
        base, files = index[("unixodbc", UNIXODBC_VER)]
        assert base == POOL
        assert files == {n: _sha256(d) for n, d in ARCHIVE.items()}


# ── shipping ───────────────────────────────────────────────────────────────


def test_copyleft_packages_ship_dsc_orig_and_debian_tarballs_once_per_source(tmp_path: Path) -> None:
    docs = _docs(tmp_path, {"unixodbc": LGPL_DEP5, "libodbc2": LGPL_DEP5, "zlib1g": ZLIB_DEP5})
    lists = _lists(tmp_path, _sources_index(ARCHIVE))
    net = FakeNet({POOL + n: d for n, d in ARCHIVE.items()})
    dest = tmp_path / "out"
    dest.mkdir()
    (dest / "SHA256SUMS").write_text(f"{'a' * 64}  paramiko-5.1.0.tar.gz\n", encoding="utf-8")
    rows = deb.ship(dest, PKGS, docs=docs, lists=lists, get=net)
    for name, data in ARCHIVE.items():
        assert (dest / name).read_bytes() == data
    assert sorted(net.seen) == sorted(POOL + n for n in ARCHIVE), "each source file fetched once"
    sums = (dest / "SHA256SUMS").read_text(encoding="utf-8")
    assert "paramiko-5.1.0.tar.gz" in sums, "the PyPI sources' sums are kept"
    for name, data in ARCHIVE.items():
        assert f"{_sha256(data)}  {name}" in sums
    manifest = json.loads((dest / "DEBIAN_SOURCES.json").read_text(encoding="utf-8"))
    by_source = {s["source"]: s for s in manifest["sources"]}
    assert by_source["unixodbc"]["binary_packages"] == ["libodbc2", "unixodbc"]
    assert by_source["unixodbc"]["verified_against"].startswith("apt's signed Sources index")
    assert "zlib" not in by_source
    assert {p["name"]: p["licence_class"] for p in manifest["packages"]}["zlib1g"] == "permissive"
    assert len(rows) == 1


def test_an_unknown_licence_fails_the_build(tmp_path: Path) -> None:
    docs = _docs(tmp_path, {"unixodbc": LGPL_DEP5, "libodbc2": LGPL_DEP5})
    with pytest.raises(deb.SourceError, match=r"cannot tell the licence of zlib1g"):
        deb.ship(tmp_path / "out", PKGS, docs=docs, lists=_lists(tmp_path, ""), get=FakeNet({}))


def test_a_file_that_does_not_match_the_signed_index_fails(tmp_path: Path) -> None:
    docs = _docs(tmp_path, {"unixodbc": LGPL_DEP5, "libodbc2": LGPL_DEP5, "zlib1g": ZLIB_DEP5})
    lists = _lists(tmp_path, _sources_index(ARCHIVE))
    tampered = {**{POOL + n: d for n, d in ARCHIVE.items()}, POOL + "unixodbc_2.3.11.orig.tar.gz": b"evil"}
    with pytest.raises(deb.SourceError, match="does not match"):
        deb.ship(tmp_path / "out", PKGS, docs=docs, lists=lists, get=FakeNet(tampered))


def test_a_file_that_does_not_match_the_dsc_fails(tmp_path: Path) -> None:
    # The index vouches for a .dsc that names other checksums than the tarballs.
    bad_dsc = _dsc({**FILES, "unixodbc_2.3.11.orig.tar.gz": b"something else"})
    archive = {**ARCHIVE, DSC_NAME: bad_dsc}
    docs = _docs(tmp_path, {"unixodbc": LGPL_DEP5, "libodbc2": LGPL_DEP5, "zlib1g": ZLIB_DEP5})
    lists = _lists(tmp_path, _sources_index(archive))
    with pytest.raises(deb.SourceError, match=r"\.dsc"):
        deb.ship(tmp_path / "out", PKGS, docs=docs, lists=lists, get=FakeNet({POOL + n: d for n, d in archive.items()}))


def test_a_version_gone_from_the_archive_comes_from_snapshot(tmp_path: Path) -> None:
    docs = _docs(tmp_path, {"unixodbc": LGPL_DEP5, "libodbc2": LGPL_DEP5, "zlib1g": ZLIB_DEP5})
    lists = _lists(tmp_path, "")  # the archive has moved on
    sha1 = {n: hashlib.sha1(d).hexdigest() for n, d in ARCHIVE.items()}  # noqa: S324
    listing = {
        "result": [{"hash": h} for h in sha1.values()],
        "fileinfo": {h: [{"name": n, "archive_name": "debian-security"}] for n, h in sha1.items()},
    }
    net = FakeNet({
        f"https://snapshot.debian.org/mr/package/unixodbc/{UNIXODBC_VER}/srcfiles?fileinfo=1":
            json.dumps(listing).encode(),
        **{f"https://snapshot.debian.org/file/{sha1[n]}": d for n, d in ARCHIVE.items()},
    })
    dest = tmp_path / "out"
    deb.ship(dest, PKGS, docs=docs, lists=lists, get=net)
    assert (dest / DSC_NAME).read_bytes() == DSC
    manifest = json.loads((dest / "DEBIAN_SOURCES.json").read_text(encoding="utf-8"))
    assert manifest["sources"][0]["verified_against"].startswith("snapshot.debian.org")
    # A snapshot file whose content is not its address fails.
    net.files[f"https://snapshot.debian.org/file/{sha1[DSC_NAME]}"] = b"forged"
    with pytest.raises(deb.SourceError, match="SHA-1"):
        deb.ship(tmp_path / "out2", PKGS, docs=docs, lists=lists, get=net)


def test_source_file_names_drop_the_epoch() -> None:
    assert deb.dsc_name("zlib", "1:1.2.13.dfsg-1") == "zlib_1.2.13.dfsg-1.dsc"


# ── Dockerfile.api: multi-stage; Debian sources go to the companion bundle ──


def _stages():
    return deb.dockerfile_stages((REPO / "Dockerfile.api").read_text(encoding="utf-8"))


def _by_name():
    return {s.name: s for s in _stages()}


def _runtime_text() -> str:
    return "\n".join(s.body for s in deb.runtime_chain(_stages()))


def test_the_image_is_multi_stage_with_compilers_only_in_the_build_stage() -> None:
    stages = _by_name()
    build = stages["build"]
    assert "build-essential" in deb.apt_installs(build.body)
    chain = deb.runtime_chain(_stages())
    assert [s.name for s in chain] == ["runtime-base", "runtime"], "the last stage is the runtime image"
    runtime_pkgs = set().union(*(deb.apt_installs(s.body) for s in chain))
    assert not {p for p in runtime_pkgs if p == "build-essential" or p.endswith("-dev") or p in {"gcc", "g++", "make"}}
    runtime = _runtime_text()
    assert "COPY --from=build /opt/venv /opt/venv" in runtime
    # Everything that may compile (sdists of the runtime requirements) is built
    # in the build stage; the runtime stages install wheels only.
    assert "pip install -r /app/requirements.txt -c /app/constraints-image.txt" in build.body
    for line in runtime.splitlines():
        if "pip install" in line and "--upgrade pip" not in line:
            assert "--only-binary :all:" in line or "/app/packages/preflight" in line, line


def test_the_runtime_installs_exactly_the_reviewed_apt_packages() -> None:
    policy = json.loads((REPO / "licence-policy.json").read_text(encoding="utf-8"))
    reviewed = policy["apt_packages"]
    assert deb.runtime_apt_packages((REPO / "Dockerfile.api").read_text(encoding="utf-8")) == set(reviewed)
    for name in ("unixodbc", "libodbc2"):
        assert reviewed[name]["licence"].startswith("LGPL-2.1")
        assert reviewed[name]["source"] == "companion bundle <tag>-sources"
    assert deb.verify_dockerfile(REPO / "Dockerfile.api", REPO / "licence-policy.json") == []


def test_the_drift_check_fails_on_an_unreviewed_apt_package(tmp_path: Path) -> None:
    text = (REPO / "Dockerfile.api").read_text(encoding="utf-8")
    start = text.index("AS runtime-base")
    head, tail = text[:start], text[start:]
    tail = tail.replace("apt-get install -y --no-install-recommends", "apt-get install -y --no-install-recommends libgnutls30", 1)
    bad = tmp_path / "Dockerfile"
    bad.write_text(head + tail, encoding="utf-8")
    problems = deb.verify_dockerfile(bad, REPO / "licence-policy.json")
    assert problems and "libgnutls30" in problems[0]


def test_the_debian_step_runs_in_the_companion_stage_not_the_runtime() -> None:
    stages = _by_name()
    collect = stages["sources-collect"]
    assert collect.base == "runtime-base", "same packages as the runtime image"
    run = next(line for line in collect.body.splitlines() if "fetch_debian_sources.py --dest" in line)
    assert "--dest /sources" in run
    assert "deb-src" in run and "apt-get" in run and "update" in run
    assert "rm -rf /var/lib/apt/lists/*" in run
    assert "fetch_debian_sources.py --dest" not in _runtime_text()
    assert stages["sources"].base == "scratch"


def test_network_errors_are_retried_then_reported_checksum_errors_are_not() -> None:
    calls: list[str] = []

    def flaky(url: str) -> bytes:
        calls.append(url)
        if len(calls) < 3:
            raise OSError("503 throttled")
        return b"ok"

    assert deb.get_with_retries("https://x/y", flaky, pause=0) == b"ok"
    assert len(calls) == 3

    def down(url: str) -> bytes:
        raise OSError("unreachable")

    with pytest.raises(deb.SourceError, match="after 2 attempts"):
        deb.get_with_retries("https://x/y", down, attempts=2, pause=0)
