"""Large copyleft source lives in a companion bundle, not the runtime image.

Owner decision (2026-11-13). The runtime API image keeps the small sources:
- the Python-wheel LGPL sources (paramiko, psycopg2, pymssql, FreeTDS);
- the wheel-bundled keyutils and libxcrypt;
- the bundled-library inventory.

The Debian system-package sources and the GCC 8.5 source (for numpy's
libquadmath / libgfortran) go into a companion OCI image,
``<image>:<tag>-sources``. release.yml publishes it with the runtime image,
in the same registry. The runtime image carries a pointer:
``/usr/share/doc/datawrap/SOURCES.md``, the ``io.datawrap.sources``
label, and the companion's SHA256SUMS. These tests run offline.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

API = Path(__file__).resolve().parents[1]
REPO = API.parents[1]
SOURCE_DIR = "/usr/share/doc/datawrap/third-party-source"
COMPANION = "companion bundle <tag>-sources"


def _load(name: str, path: Path):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


fetch = _load("fetch_lgpl_sources", REPO / "scripts" / "fetch_lgpl_sources.py")
deb = _load("fetch_debian_sources", REPO / "scripts" / "fetch_debian_sources.py")
bundle = _load("source_bundle", REPO / "scripts" / "source_bundle.py")
bundled = sys.modules["bundled_libraries"]
scan = _load("licence_scan_for_bundle", REPO / "scripts" / "licence_scan.py")


class FakeNet:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.seen: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.seen.append(url)
        if url not in self.files:
            raise fetch.SourceError(f"404 {url}")
        return self.files[url]


def _libs(root: Path, name: str, libs: dict[str, bytes]) -> list[Path]:
    libdir = root / f"{name}.libs"
    libdir.mkdir(parents=True, exist_ok=True)
    out = []
    for filename, content in libs.items():
        (libdir / filename).write_bytes(content)
        out.append(libdir / filename)
    return out


GCC = b"libquadmath.so.0.0.0-8.5.0-28.el8_10.alma.1.x86_64.debug"
KEYUTILS = b"keyutils-1.5.10"


def _bundles(tmp_path: Path, monkeypatch) -> tuple[list, FakeNet]:
    numpy = _libs(tmp_path, "numpy", {"libquadmath-2284e583-a9307bba.so.0.0.0": GCC})
    pymssql = _libs(tmp_path, "pymssql", {"libkeyutils-2777d33d.so.1.6": KEYUTILS})
    gcc_tar, key_tar = b"gcc source", b"keyutils source"
    monkeypatch.setitem(bundled.UPSTREAM, ("gcc", "8.5.0-el8"), bundled.Upstream(
        "gcc.tar.xz", ("https://mirror.example/gcc.tar.xz",),
        sha512=hashlib.sha512(gcc_tar).hexdigest(), bundle="companion"))
    monkeypatch.setitem(bundled.UPSTREAM, ("keyutils", "1.5.10"), bundled.Upstream(
        "keyutils.tar.bz2", ("https://mirror.example/keyutils.tar.bz2",),
        sha512=hashlib.sha512(key_tar).hexdigest()))
    net = FakeNet({"https://mirror.example/gcc.tar.xz": gcc_tar, "https://mirror.example/keyutils.tar.bz2": key_tar})
    return [("numpy", "2.5.4", numpy), ("pymssql", "2.4.6", pymssql)], net


# ── which source goes where ────────────────────────────────────────────────


def test_only_the_gcc_source_is_companion_located() -> None:
    located = {key: up.bundle for key, up in bundled.UPSTREAM.items()}
    assert {k for k, v in located.items() if v == "companion"} == {("gcc", "8.5.0-el8")}
    assert set(located.values()) == {"image", "companion"}


def test_the_runtime_ships_small_sources_and_points_at_the_companion_for_gcc(tmp_path, monkeypatch) -> None:
    dists, net = _bundles(tmp_path, monkeypatch)
    dest = tmp_path / "runtime"
    rows = fetch.ship([], dest, get=net, bundled=dists, static=[])
    assert [r["file"] for r in rows] == ["keyutils.tar.bz2"]
    assert "https://mirror.example/gcc.tar.xz" not in net.seen
    assert not (dest / "gcc.tar.xz").exists()
    inventory = json.loads((dest / "BUNDLED_LIBRARIES.json").read_text(encoding="utf-8"))["libraries"]
    by_lib = {r["library"]: r for r in inventory}
    assert by_lib["libquadmath"]["source_location"] == COMPANION
    assert by_lib["libkeyutils"]["source_location"] == f"image: {SOURCE_DIR}"


def test_the_companion_step_ships_exactly_the_listed_families(tmp_path, monkeypatch) -> None:
    dists, net = _bundles(tmp_path, monkeypatch)
    dest = tmp_path / "companion"
    rows = fetch.ship_companion(["gcc"], dest, get=net, bundled=dists)
    assert [r["file"] for r in rows] == ["gcc.tar.xz"]
    assert (dest / "SHA256SUMS").read_text(encoding="utf-8").endswith("  gcc.tar.xz\n")
    assert json.loads((dest / "MANIFEST.json").read_text(encoding="utf-8"))["components"][0]["file"] == "gcc.tar.xz"
    with pytest.raises(fetch.SourceError, match="companion step lists"):
        fetch.ship_companion([], tmp_path / "c2", get=net, bundled=dists)
    with pytest.raises(fetch.SourceError, match="companion step lists"):
        fetch.ship_companion(["gcc", "keyutils"], tmp_path / "c3", get=net, bundled=dists)


# ── the pointer and the exactness check ────────────────────────────────────


def _companion(tmp_path: Path) -> Path:
    comp = tmp_path / "companion"
    comp.mkdir()
    files = {"unixodbc_2.3.11-2+deb12u1.dsc": b"dsc", "unixodbc_2.3.11.orig.tar.gz": b"orig",
             "gcc-8.5.0-20210514.tar.xz": b"gcc"}
    for name, data in files.items():
        (comp / name).write_bytes(data)
    (comp / "SHA256SUMS").write_text(
        "".join(f"{hashlib.sha256(d).hexdigest()}  {n}\n" for n, d in sorted(files.items())), encoding="utf-8")
    (comp / "DEBIAN_SOURCES.json").write_text(json.dumps({"sources": [{
        "source": "unixodbc", "version": "2.3.11-2+deb12u1", "binary_packages": ["libodbc2", "unixodbc"],
        "files": ["unixodbc_2.3.11-2+deb12u1.dsc", "unixodbc_2.3.11.orig.tar.gz"],
        "verified_against": "apt's signed Sources index (SHA-256) and the .dsc checksums"}]}), encoding="utf-8")
    (comp / "MANIFEST.json").write_text(json.dumps({"components": [{
        "file": "gcc-8.5.0-20210514.tar.xz", "for": "gcc 8.5.0-el8 (bundled in numpy 2.5.4: libquadmath)",
        "verified_against": "pinned SHA-512", "sha256": hashlib.sha256(b"gcc").hexdigest()}]}), encoding="utf-8")
    return comp


def test_sources_md_says_where_the_companion_is_and_what_it_holds(tmp_path) -> None:
    comp = _companion(tmp_path)
    ref = "ghcr.io/acme/datawrap/api:v1.2.3-sources"
    text = bundle.pointer(comp, ref)
    assert ref in text and f"docker pull {ref}" in text
    assert "sha256sum -c SHA256SUMS" in text
    for line in (comp / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        assert line in text
    assert "| unixodbc | 2.3.11-2+deb12u1 | libodbc2, unixodbc |" in text
    assert "gcc-8.5.0-20210514.tar.xz" in text and "numpy 2.5.4" in text
    assert SOURCE_DIR in text, "it also says what stays in the image"
    assert "io.datawrap.sources" in text


def test_the_companion_must_hold_exactly_the_runtimes_copyleft_packages(tmp_path) -> None:
    comp = _companion(tmp_path)
    runtime_src = tmp_path / "runtime"
    runtime_src.mkdir()
    (runtime_src / "BUNDLED_LIBRARIES.json").write_text(json.dumps({"libraries": [
        {"library": "libquadmath", "copyleft": True, "source": ["gcc", "8.5.0-el8"], "source_location": COMPANION},
        {"library": "libkeyutils", "copyleft": True, "source": ["keyutils", "1.5.10"],
         "source_location": f"image: {SOURCE_DIR}"},
    ]}), encoding="utf-8")
    docs = tmp_path / "doc"
    for pkg, text in {"unixodbc": "License: LGPL-2.1+\n", "libodbc2": "License: LGPL-2.1+\n",
                      "zlib1g": "License: Zlib\n"}.items():
        (docs / pkg).mkdir(parents=True)
        (docs / pkg / "copyright").write_text(text, encoding="utf-8")
    pkgs = [deb.DebPackage("unixodbc", "1", "unixodbc", "2.3.11-2+deb12u1"),
            deb.DebPackage("libodbc2", "1", "unixodbc", "2.3.11-2+deb12u1"),
            deb.DebPackage("zlib1g", "1", "zlib", "1:1.2.13.dfsg-1")]
    assert bundle.check(comp, runtime_src, packages=pkgs, docs=docs) == []
    # A copyleft package the companion lacks, and one it has but the runtime does not.
    (docs / "bash").mkdir()
    (docs / "bash" / "copyright").write_text("License: GPL-3+\n", encoding="utf-8")
    problems = bundle.check(comp, runtime_src, packages=[*pkgs, deb.DebPackage("bash", "5", "bash", "5.2.15-2")],
                            docs=docs)
    assert any("bash 5.2.15-2" in p and "missing" in p for p in problems)
    problems = bundle.check(comp, runtime_src, packages=pkgs[2:], docs=docs)
    assert any("unixodbc 2.3.11-2+deb12u1" in p and "not in the runtime" in p for p in problems)
    # The GCC source must match the inventory's companion-located sources.
    (comp / "MANIFEST.json").write_text(json.dumps({"components": []}), encoding="utf-8")
    assert any("gcc" in p for p in bundle.check(comp, runtime_src, packages=pkgs, docs=docs))


# ── Dockerfile drift check ─────────────────────────────────────────────────


def _dockerfile() -> str:
    return (REPO / "Dockerfile.api").read_text(encoding="utf-8")


def test_the_dockerfile_builds_the_companion_and_the_pointer() -> None:
    assert bundle.verify_dockerfile(REPO / "Dockerfile.api") == []
    text = _dockerfile()
    assert "FROM scratch AS sources" in text
    assert "fetch_lgpl_sources.py --companion gcc --dest /sources" in text
    assert "--out /usr/share/doc/datawrap/SOURCES.md" in text


def test_the_default_target_never_depends_on_the_companion() -> None:
    """Final review M2: Railway builds the default target for api, worker and
    scheduler. It must not download the Debian / GCC sources (a download
    failure would block a deploy); only the release target carries the pointer."""
    stages = deb.dockerfile_stages(_dockerfile())
    by_name = {s.name: s for s in stages}
    assert stages[-1].name == "runtime", "the default target is the deployable runtime"
    chain = deb.runtime_chain(stages)
    assert [s.name for s in chain] == ["runtime-base", "app", "runtime"]
    runtime_text = "\n".join(
        line for s in chain for line in s.body.splitlines() if not line.lstrip().startswith("#")
    )
    for forbidden in ("--from=sources-collect", "source_bundle.py", "fetch_debian_sources.py --dest",
                      "--companion", "SOURCES_REF"):
        assert forbidden not in runtime_text, forbidden
    release = by_name["release"]
    assert release.base == "app"
    assert "COPY --from=sources-collect /sources/SHA256SUMS" in release.body
    assert "source_bundle.py --check" in release.body and "source_bundle.py --pointer" in release.body


@pytest.mark.parametrize(
    ("old", "new", "complaint"),
    [
        ("--companion gcc --dest", "--companion gcc keyutils --dest", "companion families"),
        ("--companion gcc --dest", "--companion --dest", "companion families"),
        ("FROM scratch AS sources", "FROM runtime-base AS sources", "FROM scratch"),
        ("FROM app AS release", "FROM app AS releasing", "release stage"),
        ("/sources/SHA256SUMS /sources/MANIFEST.json", "/sources/ /sources/MANIFEST.json", "only the manifests"),
        ("# The source-step scripts are not part of the running image.",
         "COPY --from=sources-collect /sources/SHA256SUMS /x/", "default target"),
    ],
)
def test_the_drift_check_catches_a_broken_companion_layout(tmp_path, old, new, complaint) -> None:
    text = _dockerfile()
    assert old in text
    bad = tmp_path / "Dockerfile"
    bad.write_text(text.replace(old, new, 1), encoding="utf-8")
    problems = bundle.verify_dockerfile(bad)
    assert any(complaint in p for p in problems), problems


def test_the_drift_check_catches_debian_sources_in_the_runtime(tmp_path) -> None:
    text = _dockerfile()
    bad = tmp_path / "Dockerfile"
    marker = "# The source-step scripts are not part of the running image."
    bad.write_text(text.replace(marker, "RUN python /tmp/lgpl/fetch_debian_sources.py --dest /x\n" + marker, 1),
                   encoding="utf-8")
    assert any("default target" in p and "Debian" in p for p in bundle.verify_dockerfile(bad))


# ── policy, NOTICE ─────────────────────────────────────────────────────────


def test_notice_points_the_gcc_and_debian_sources_at_the_companion() -> None:
    notice = (REPO / "NOTICE").read_text(encoding="utf-8")
    gcc_rows = [line for line in notice.splitlines() if line.startswith("numpy ") and "GCC" in line]
    assert gcc_rows and all("source in companion bundle <tag>-sources" in line for line in gcc_rows)
    keyutils = [line for line in notice.splitlines() if "keyutils, LGPL" in line]
    assert keyutils and all(f"source shipped in image at {SOURCE_DIR}/" in line for line in keyutils)
    assert "Debian packages in the API image" in notice
    assert "unixodbc" in notice and "<image>:<tag>-sources" in notice


def test_a_copyleft_apt_package_must_name_the_companion(tmp_path) -> None:
    policy = json.loads((REPO / "licence-policy.json").read_text(encoding="utf-8"))
    policy["apt_packages"]["unixodbc"]["source"] = "shipped"
    path = tmp_path / "p.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(ValueError, match="companion bundle <tag>-sources"):
        scan.load_policy(path)


# ── keyutils 1.5.8: MD5-only, so CI records the SHA-256 and holds it ───────


def test_keyutils_158_keeps_the_md5_pin_and_a_lookaside_mirror() -> None:
    up = bundled.UPSTREAM[("keyutils", "1.5.8")]
    assert up.md5 == "3c7f463039b83833c12a9414c2fcb389" and not up.sha256
    assert any("src.fedoraproject.org/lookaside" in u and up.md5 in u for u in up.urls)


# ── CI and release ─────────────────────────────────────────────────────────


def _workflow(name: str) -> dict:
    return yaml.safe_load((REPO / ".github" / "workflows" / name).read_text(encoding="utf-8"))


def test_ci_builds_both_images_and_checks_them() -> None:
    jobs = _workflow("ci.yml")["jobs"]
    steps = jobs["api-image"]["steps"]
    builds = [s for s in steps if str(s.get("uses", "")).startswith("docker/build-push-action")]
    assert {b["with"].get("target") for b in builds} == {"sources", "release"}
    release = next(b for b in builds if b["with"].get("target") == "release")
    assert "SOURCES_REF=datawrap-api:ci-sources" in release["with"]["build-args"]
    # The deployable default target is built on its own, first, and must not
    # run any companion stage.
    default = next(s for s in steps if "docker buildx build" in str(s.get("run", "")))
    assert "--target" not in default["run"] and "sources-collect" in default["run"]
    assert steps.index(default) < steps.index(builds[0])
    text = json.dumps(steps)
    for probe in ("*.dsc", "gcc-*", "SOURCES.md", "io.datawrap.sources",
                  "sha256sum --check", "docker export", "pip check", "build-essential"):
        assert probe in text, probe
    assert "api-image" in jobs["required-checks"]["needs"]
    security = json.dumps(jobs["security"])
    assert "source_bundle.py --verify-dockerfile Dockerfile.api" in security
    assert "fetch_debian_sources.py --verify-dockerfile Dockerfile.api" in security


def test_ci_records_the_keyutils_158_sha256_and_fails_if_it_changes() -> None:
    steps = _workflow("ci.yml")["jobs"]["api-image"]["steps"]
    cache = next(s for s in steps if str(s.get("uses", "")).startswith("actions/cache"))
    assert "keyutils-1.5.8" in cache["with"]["key"]
    check = next(s for s in steps if "keyutils-1.5.8.tar.bz2" in str(s.get("run", "")))
    assert "md5sum" in check["run"] and "3c7f463039b83833c12a9414c2fcb389" in check["run"]
    assert "exit 1" in check["run"] and "sha256" in check["run"]


def test_release_publishes_the_companion_with_the_runtime_image() -> None:
    job = _workflow("release.yml")["jobs"]["publish-images"]
    api = next(m for m in job["strategy"]["matrix"]["include"] if m["image"] == "api")
    assert api["companion"] is True
    steps = job["steps"]
    names = [s.get("name", "") for s in steps]
    companion = next(i for i, s in enumerate(steps) if s.get("with", {}).get("target") == "sources")
    runtime = names.index("Build and push release image")
    assert companion < runtime, "the companion is published first; if it fails, nothing is released"
    comp = steps[companion]
    assert comp["with"]["push"] is True
    assert "-sources" in comp["with"]["tags"] and "steps.image.outputs.name" in comp["with"]["tags"]
    assert comp["if"] == "${{ matrix.companion }}"
    run = steps[runtime]["with"]
    assert "release" in run["target"], "the api release image is the release target (pointer)"
    assert "SOURCES_REF=" in run["build-args"] and "-sources" in run["build-args"]
    # M1: the OCI source label stays the repository (GHCR linking, Renovate,
    # scanners); the companion has its own label, set by the release stage.
    assert "org.opencontainers.image.source=https://github.com/${{ github.repository }}" in run["labels"]
    assert "-sources" not in run["labels"]
    verify = next(s for s in steps if "imagetools inspect" in str(s.get("run", "")))
    assert verify["if"] == "${{ matrix.companion }}" and "-sources" in verify["run"]


def test_the_oci_source_label_stays_the_repository() -> None:
    """Final review M1: org.opencontainers.image.source is the GitHub repository
    (GHCR links the package to it; Renovate and scanners read it). The companion
    reference has its own label, io.datawrap.sources."""
    text = _dockerfile()
    assert 'LABEL io.datawrap.sources="${SOURCES_REF}"' in text
    assert not [ln for ln in text.splitlines()
                if ln.startswith("LABEL") and "org.opencontainers.image.source" in ln]
    bad = text.replace(
        'LABEL io.datawrap.sources="${SOURCES_REF}"',
        'LABEL io.datawrap.sources="${SOURCES_REF}" org.opencontainers.image.source="${SOURCES_REF}"',
    )
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "Dockerfile"
        path.write_text(bad, encoding="utf-8")
        assert any("org.opencontainers.image.source" in p for p in bundle.verify_dockerfile(path))
    steps = _workflow("release.yml")["jobs"]["publish-images"]["steps"]
    verify = next(s for s in steps if "imagetools inspect" in str(s.get("run", "")))
    assert "io.datawrap.sources" in verify["run"]
