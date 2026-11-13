#!/usr/bin/env python3
"""Ship the corresponding source of every copyleft Debian package in the image.

Dockerfile.api runs this in the final stage, after its last ``apt-get
install``. It writes into the same directory as fetch_lgpl_sources.py
(``/usr/share/doc/datawrap/third-party-source``):

1. every installed package and its exact source version (``dpkg-query``);
2. each package's licence class, read from ``/usr/share/doc/<pkg>/copyright``:
   ``copyleft`` (GPL / LGPL / MPL / ... anywhere in it), ``permissive``, or
   ``unknown``. An unknown licence fails the build: nothing ships unreviewed;
3. for each copyleft source package, its ``.dsc`` and the tarballs the
   ``.dsc`` names (orig + debian), for the exact installed source version:
   * from deb.debian.org, each file checked against apt's Sources index, whose
     signature apt verified on ``apt-get update`` (the Dockerfile adds deb-src
     for this step and removes the lists afterwards); or
   * when that version has left the archive, from snapshot.debian.org, each
     file checked against its content address (SHA-1);
   and every file checked again against the ``.dsc``'s Checksums-Sha256;
4. ``SHA256SUMS`` (merged with the PyPI sources' sums) and
   ``DEBIAN_SOURCES.json`` (package -> source -> files, how verified).

Any download or checksum failure fails the image build.

``--verify-dockerfile`` (CI, offline) is the drift check: the final stage
must install exactly the apt packages reviewed in licence-policy.json
(``apt_packages``), and no compiler or -dev package.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import subprocess  # nosec B404 - fixed dpkg-query argv, no shell
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_lgpl_sources import SourceError, _http_get

__all__ = ["SourceError"]

SNAPSHOT_MR = "https://snapshot.debian.org/mr/package/{source}/{version}/srcfiles?fileinfo=1"
SNAPSHOT_FILE = "https://snapshot.debian.org/file/{sha1}"
DOC_ROOT = Path("/usr/share/doc")
APT_LISTS = Path("/var/lib/apt/lists")
_DPKG_FORMAT = "${Package}\t${Version}\t${source:Package}\t${source:Version}\t${Status}\n"

_COPYLEFT = re.compile(
    r"\b(A?GPL|LGPL|GNU (?:Affero |Lesser |Library )?General Public|SSPL|EUPL|OSL|MPL"
    r"|Mozilla Public|CDDL|EPL|Eclipse Public|CC-BY-SA)\b",
    re.IGNORECASE,
)
_PERMISSIVE = re.compile(
    r"\b(MIT|Expat|BSD|ISC|Zlib|public[- ]domain|Apache|X11|PSF|Unicode|curl|OpenSSL|SSLeay"
    r"|0BSD|CC0|Unlicense|BSL-1\.0|Boost|Beerware|permission is hereby granted"
    r"|redistribution and use in source and binary forms)\b",
    re.IGNORECASE,
)
_COMPILERS = {"build-essential", "gcc", "g++", "make", "cpp"}


@dataclass(frozen=True)
class DebPackage:
    name: str
    version: str
    source: str
    source_version: str


def get_with_retries(
    url: str, get: Callable[[str], bytes] = _http_get, attempts: int = 4, pause: float = 5.0
) -> bytes:
    """A download, retried on network errors (snapshot.debian.org throttles).

    A checksum mismatch is never retried: it is raised by the caller, not here.
    """
    for attempt in range(1, attempts + 1):
        try:
            return get(url)
        except OSError as exc:  # urllib's URLError / HTTPError, timeouts
            if attempt == attempts:
                raise SourceError(f"{url}: {type(exc).__name__}: {exc} (after {attempts} attempts)") from exc
            time.sleep(pause * attempt)
    raise AssertionError("unreachable")


def _dpkg_query(argv: list[str]) -> str:
    return subprocess.run(argv, check=True, capture_output=True, text=True).stdout  # nosec B603


def installed_packages(run: Callable[[list[str]], str] = _dpkg_query) -> list[DebPackage]:
    out = []
    for line in run(["dpkg-query", "-W", f"-f={_DPKG_FORMAT}"]).splitlines():
        parts = line.split("\t")
        if len(parts) != 5 or parts[4] != "install ok installed":
            continue
        name, version, source, source_version, _ = parts
        out.append(DebPackage(name, version, source or name, source_version or version))
    return out


def classify(pkg: DebPackage, docs: Path = DOC_ROOT) -> str:
    """``copyleft`` | ``permissive`` | ``unknown``, from the package's copyright file."""
    path = docs / pkg.name.split(":", 1)[0] / "copyright"
    if not path.is_file():
        return "unknown"
    text = path.read_text(encoding="utf-8", errors="replace")
    if _COPYLEFT.search(text):
        return "copyleft"
    return "permissive" if _PERMISSIVE.search(text) else "unknown"


def dsc_name(source: str, version: str) -> str:
    return f"{source}_{version.split(':', 1)[-1]}.dsc"


def _checksums(block: str, field: str = "Checksums-Sha256") -> dict[str, str]:
    files: dict[str, str] = {}
    lines = block.splitlines()
    for i, line in enumerate(lines):
        if line.startswith(f"{field}:"):
            for entry in lines[i + 1:]:
                if not entry.startswith(" "):
                    break
                digest, _size, name = entry.split()
                files[name] = digest.lower()
    return files


def parse_dsc(text: str) -> dict[str, str]:
    """The files a .dsc names, with their SHA-256 (Checksums-Sha256)."""
    return _checksums(text)


def _stanza_field(stanza: str, field: str) -> str:
    match = re.search(rf"^{field}:\s*(\S+)", stanza, re.MULTILINE)
    return match.group(1) if match else ""


def load_source_index(lists: Path = APT_LISTS) -> dict[tuple[str, str], tuple[str, dict[str, str]]]:
    """(source, version) -> (pool URL, {file: sha256}) from apt's (verified) Sources lists."""
    index: dict[tuple[str, str], tuple[str, dict[str, str]]] = {}
    for path in sorted(lists.glob("*_Sources*")) if lists.is_dir() else []:
        if path.name.endswith("_Sources"):
            text = path.read_text(encoding="utf-8", errors="replace")
        elif path.name.endswith("_Sources.gz"):
            text = gzip.decompress(path.read_bytes()).decode("utf-8", errors="replace")
        else:
            raise SourceError(f"{path.name}: unsupported index compression; run apt-get update "
                              "with -o Acquire::GzipIndexes=false")
        root = "https://" + path.name.split("_dists_", 1)[0].replace("_", "/")
        for stanza in text.split("\n\n"):
            source, version = _stanza_field(stanza, "Package"), _stanza_field(stanza, "Version")
            directory = _stanza_field(stanza, "Directory")
            if source and version and directory:
                index.setdefault((source, version), (f"{root}/{directory}/", _checksums(stanza)))
    return index


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _check_dsc(dsc: str, payloads: dict[str, bytes]) -> None:
    named = parse_dsc(payloads[dsc].decode("utf-8", errors="replace"))
    if not named:
        raise SourceError(f"{dsc}: no Checksums-Sha256 in the .dsc")
    for name, digest in named.items():
        if name not in payloads:
            raise SourceError(f"{dsc} names {name}, which was not downloaded")
        if _sha256(payloads[name]) != digest:
            raise SourceError(f"{name}: SHA-256 does not match the .dsc ({dsc})")


def _from_index(source: str, version: str, pool: str, files: dict[str, str],
                get: Callable[[str], bytes]) -> dict[str, bytes]:
    dsc = dsc_name(source, version)
    if dsc not in files:
        raise SourceError(f"{source} {version}: the Sources index lists no {dsc}")
    payloads = {}
    for name, digest in files.items():
        data = get(pool + name)
        if _sha256(data) != digest:
            raise SourceError(f"{name}: SHA-256 does not match apt's signed Sources index ({pool + name})")
        payloads[name] = data
    _check_dsc(dsc, payloads)
    return payloads


def _from_snapshot(source: str, version: str, get: Callable[[str], bytes]) -> dict[str, bytes]:
    listing = json.loads(get(SNAPSHOT_MR.format(source=source, version=version)))
    by_name: dict[str, str] = {}
    for sha1, infos in (listing.get("fileinfo") or {}).items():
        for info in infos:
            by_name.setdefault(info["name"], sha1)
    dsc = dsc_name(source, version)

    def fetch(name: str) -> bytes:
        sha1 = by_name.get(name)
        if not sha1:
            raise SourceError(f"{source} {version}: snapshot.debian.org lists no {name}")
        data = get(SNAPSHOT_FILE.format(sha1=sha1))
        if hashlib.sha1(data).hexdigest() != sha1:  # nosec B324 - snapshot's content address
            raise SourceError(f"{name}: SHA-1 does not match its snapshot.debian.org address")
        return data

    payloads = {dsc: fetch(dsc)}
    for name in parse_dsc(payloads[dsc].decode("utf-8", errors="replace")):
        payloads[name] = fetch(name)
    _check_dsc(dsc, payloads)
    return payloads


def ship(
    dest: Path,
    packages: list[DebPackage],
    *,
    docs: Path = DOC_ROOT,
    lists: Path = APT_LISTS,
    get: Callable[[str], bytes] = get_with_retries,
) -> list[dict[str, Any]]:
    classes = {p.name: classify(p, docs) for p in packages}
    unknown = sorted(n for n, c in classes.items() if c == "unknown")
    if unknown:
        raise SourceError(
            f"cannot tell the licence of {', '.join(unknown)} from /usr/share/doc/<package>/copyright; "
            "review them before they ship in the image"
        )
    groups: dict[tuple[str, str], list[str]] = {}
    for pkg in packages:
        if classes[pkg.name] == "copyleft":
            groups.setdefault((pkg.source, pkg.source_version), []).append(pkg.name)
    index = load_source_index(lists) if groups else {}
    dest.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    sums: dict[str, str] = {}
    for (source, version), binaries in sorted(groups.items()):
        if (source, version) in index:
            pool, files = index[(source, version)]
            payloads = _from_index(source, version, pool, files, get)
            how = "apt's signed Sources index (SHA-256) and the .dsc checksums"
        else:
            payloads = _from_snapshot(source, version, get)
            how = "snapshot.debian.org (content-addressed SHA-1) and the .dsc checksums"
        for name, data in payloads.items():
            (dest / name).write_bytes(data)
            sums[name] = _sha256(data)
        rows.append({
            "source": source,
            "version": version,
            "binary_packages": sorted(binaries),
            "files": sorted(payloads),
            "verified_against": how,
        })
    _merge_sums(dest / "SHA256SUMS", sums)
    (dest / "DEBIAN_SOURCES.json").write_text(
        json.dumps({
            "purpose": "corresponding source of the copyleft Debian packages in this image",
            "packages": [{**asdict(p), "licence_class": classes[p.name]} for p in packages],
            "sources": rows,
        }, indent=2) + "\n",
        encoding="utf-8",
    )
    return rows


def _merge_sums(path: Path, sums: dict[str, str]) -> None:
    lines: dict[str, str] = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if "  " in line:
                digest, name = line.split("  ", 1)
                lines[name] = digest
    lines.update(sums)
    path.write_text("".join(f"{d}  {n}\n" for n, d in sorted(lines.items())), encoding="utf-8")


def final_stage_apt_packages(dockerfile: str) -> set[str]:
    joined = dockerfile.replace("\\\n", " ")
    final = re.split(r"^FROM\s+", joined, flags=re.MULTILINE)[-1]
    pkgs: set[str] = set()
    for match in re.finditer(r"apt-get install ([^&;\n]+)", final):
        pkgs |= {w for w in match.group(1).split() if not w.startswith("-")}
    return pkgs


def verify_dockerfile(dockerfile: Path, policy: Path) -> list[str]:
    """Drift check: the final stage installs exactly the reviewed apt packages."""
    problems = []
    installed = final_stage_apt_packages(dockerfile.read_text(encoding="utf-8"))
    reviewed = json.loads(policy.read_text(encoding="utf-8")).get("apt_packages") or {}
    if extra := sorted(installed - set(reviewed)):
        problems.append(
            f"{dockerfile.name}'s final stage installs {extra}, which licence-policy.json "
            "apt_packages has not reviewed"
        )
    if stale := sorted(set(reviewed) - installed):
        problems.append(f"licence-policy.json apt_packages lists {stale}, which the final stage does not install")
    if tools := sorted(p for p in installed if p in _COMPILERS or p.endswith("-dev")):
        problems.append(f"the final stage installs build tools {tools}; they belong in the build stage")
    for name, entry in reviewed.items():
        missing = [k for k in ("licence", "why", "source") if not str(entry.get(k) or "").strip()]
        if missing:
            problems.append(f"licence-policy.json apt_packages[{name!r}] lacks {missing}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dest", type=Path)
    parser.add_argument("--lists-dir", type=Path, default=APT_LISTS)
    parser.add_argument("--doc-root", type=Path, default=DOC_ROOT)
    parser.add_argument("--verify-dockerfile", type=Path)
    parser.add_argument("--policy", type=Path, default=Path(__file__).resolve().parents[1] / "licence-policy.json")
    args = parser.parse_args(argv)
    if args.verify_dockerfile:
        problems = verify_dockerfile(args.verify_dockerfile, args.policy)
        for problem in problems:
            print(f"- {problem}")
        print("Debian source step: " + ("drift" if problems else "consistent"))
        return 1 if problems else 0
    if not args.dest:
        parser.error("--dest or --verify-dockerfile is required")
    try:
        rows = ship(args.dest, installed_packages(), docs=args.doc_root, lists=args.lists_dir)
    except SourceError as exc:
        print(f"fetch_debian_sources: {exc}", file=sys.stderr)
        return 1
    for row in rows:
        print(f"shipped {row['source']} {row['version']} ({', '.join(row['binary_packages'])})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
