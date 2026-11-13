#!/usr/bin/env python3
"""Ship the corresponding source of the LGPL components in the API image.

The owner accepted the LGPL drivers (paramiko, psycopg2-binary, pymssql) with
attribution, and without the ongoing obligation of a written offer. LGPL
binary distribution is then met by shipping the corresponding source with the
binaries. ``Dockerfile.api`` runs this script after installing the runtime
requirements, so the versions are the ones actually in the image:

* each package's source distribution for the **installed** version, from PyPI,
  verified against PyPI's SHA-256 digest (``psycopg2-binary`` ships the
  ``psycopg2`` sdist of the same version, the same code);
* FreeTDS, which pymssql's manylinux wheel links statically: its version is
  read from the linked binary, the tarball comes from freetds.org and is
  verified against the SHA-256 file published next to it when there is one;
* with ``--bundled``, the LGPL / GPL libraries the installed wheels bundle in
  their ``<pkg>.libs/`` directories (keyutils, libxcrypt, GCC's libquadmath /
  libgfortran), mapped in ``scripts/bundled_libraries.py`` with pinned
  checksums. An unreviewed bundled library fails the build.

Everything lands in ``--dest`` with ``SHA256SUMS`` and ``MANIFEST.json``. Any
failure (no sdist for the installed version, checksum mismatch, FreeTDS version
not found) fails the build: an image must not claim source it does not carry.

Usage (inside the image build)::

    python fetch_lgpl_sources.py --dest /usr/share/doc/datawrap/third-party-source \\
        paramiko psycopg2-binary=psycopg2 pymssql+freetds

    python fetch_lgpl_sources.py --verify-dockerfile Dockerfile.api   # CI drift check

A spec is ``dist[=sdist_name][+extra]``; the only extra is ``freetds``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import sys
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bundled_libraries import (
    UPSTREAM,
    BundledLibraryError,
    Upstream,
    installed_bundles,
    installed_extension_modules,
    inventory,
    lib_basename,
    static_scan,
)

__all__ = [
    "UPSTREAM",
    "Upstream",
    "installed_bundles",
    "installed_extension_modules",
    "inventory",
    "lib_basename",
    "static_scan",
]

PYPI_JSON = "https://pypi.org/pypi/{name}/{version}/json"
KNOWN_EXTRAS = ("freetds",)
_FREETDS_RE = re.compile(rb"freetds v(\d+\.\d+(?:\.\d+)?)")


#: The image cannot carry the corresponding source it is required to.
SourceError = BundledLibraryError


@dataclass(frozen=True)
class Spec:
    dist: str
    sdist: str
    extras: tuple[str, ...]


def parse_spec(text: str) -> Spec:
    head, *extras = text.strip().split("+")
    dist, _, sdist = head.partition("=")
    unknown = [e for e in extras if e not in KNOWN_EXTRAS]
    if not dist or unknown:
        raise ValueError(f"bad spec {text!r} (extras allowed: {KNOWN_EXTRAS})")
    return Spec(dist.strip(), (sdist or dist).strip(), tuple(extras))


def dockerfile_specs(dockerfile: str) -> list[Spec]:
    """The specs the Dockerfile's fetch step passes (one RUN, continuations joined)."""
    joined = dockerfile.replace("\\\n", " ")
    for line in joined.splitlines():
        if "fetch_lgpl_sources.py" in line and "--dest" in line and line.lstrip().startswith("RUN"):
            words = shlex.split(line.split("fetch_lgpl_sources.py", 1)[1])
            specs, skip = [], False
            for word in words:
                if skip:
                    skip = False
                    continue
                if word == "--dest":
                    skip = True
                    continue
                if word in {"&&", ";"}:
                    break
                if word.startswith("--"):
                    continue  # flags such as --bundled
                specs.append(parse_spec(word))
            return specs
    raise SourceError("Dockerfile has no `RUN ... fetch_lgpl_sources.py --dest ...` step")


class _HttpsOnlyRedirects(urllib.request.HTTPRedirectHandler):
    """Follow a redirect only to another HTTPS URL (a mirror may redirect)."""

    def redirect_request(
        self, req: urllib.request.Request, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> urllib.request.Request | None:
        if not str(newurl).startswith("https://"):
            raise SourceError(f"refusing a redirect from {req.full_url} to non-HTTPS {newurl}")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(_HttpsOnlyRedirects())


def _http_get(url: str) -> bytes:
    if not url.startswith("https://"):
        raise SourceError(f"refusing non-HTTPS source URL {url}")
    # HTTPS only (checked above and on every redirect), so bandit's file:/
    # scheme concern does not apply.
    with _OPENER.open(url, timeout=120) as resp:  # nosec B310
        final = resp.geturl()
        if not final.startswith("https://"):
            raise SourceError(f"refusing {url}: it ended at non-HTTPS {final}")
        return resp.read()


def linked_binaries(dist: str) -> list[Path]:
    files = metadata.distribution(dist).files or []
    return [Path(str(f.locate())) for f in files if f.name.endswith(".so")]


def freetds_version(binaries: list[Path]) -> str:
    for path in binaries:
        match = _FREETDS_RE.search(path.read_bytes())
        if match:
            return match.group(1).decode()
    raise SourceError(
        "FreeTDS version not found in pymssql's binaries; the wheel may no longer link it "
        "statically. Check the wheel and update the Dockerfile step."
    )


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sdist(spec: Spec, version: str, get: Callable[[str], bytes]) -> tuple[str, bytes, str]:
    index = json.loads(get(PYPI_JSON.format(name=spec.sdist, version=version)))
    sdists = [u for u in index.get("urls") or [] if u.get("packagetype") == "sdist"]
    if not sdists:
        raise SourceError(f"{spec.sdist} {version}: no source distribution on PyPI")
    entry = sdists[0]
    payload = get(entry["url"])
    if _sha256(payload) != entry["digests"]["sha256"]:
        raise SourceError(f"{entry['filename']}: SHA-256 does not match PyPI's digest")
    return entry["filename"], payload, "PyPI sha256 digest"


def _freetds(version: str, get: Callable[[str], bytes]) -> tuple[str, bytes, str]:
    up = UPSTREAM.get(("freetds", version))
    if up is None:
        raise SourceError(
            f"no pinned checksum for FreeTDS {version} (the version pymssql's wheel links): add "
            f"('freetds', '{version}') to UPSTREAM in scripts/bundled_libraries.py with a SHA-256 "
            "from a source independent of freetds.org (e.g. a distribution's package recipe)"
        )
    payload, how = _upstream(up, get)
    return up.filename, payload, how


def _upstream(up: Upstream, get: Callable[[str], bytes]) -> tuple[bytes, str]:
    if not (up.sha512 or up.sha256 or up.md5):
        raise SourceError(f"{up.filename}: no pinned checksum; refusing to ship an unverified download")
    errors = []
    for url in up.urls:
        try:
            payload = get(url)
        except (SourceError, OSError) as exc:  # try the next mirror, report all
            errors.append(f"{url}: {type(exc).__name__}")
            continue
        if up.sha512:
            if hashlib.sha512(payload).hexdigest() != up.sha512:
                raise SourceError(f"{up.filename}: SHA-512 does not match the pinned value ({url})")
            return payload, "pinned SHA-512"
        if up.sha256:
            if _sha256(payload) != up.sha256:
                raise SourceError(f"{up.filename}: SHA-256 does not match the pinned value ({url})")
            return payload, "pinned SHA-256"
        # MD5 only where it is the sole checksum ever published (a 2013
        # release); a second preimage of a published MD5 is not practical.
        if hashlib.md5(payload).hexdigest() != up.md5:  # nosec B324
            raise SourceError(f"{up.filename}: MD5 does not match the pinned value ({url})")
        return payload, "pinned MD5 (the only checksum published for this release)"
    raise SourceError(f"{up.filename}: no mirror answered ({'; '.join(errors)})")


def _bundled_rows(
    bundles: list[tuple[str, str, list[Path]]], get: Callable[[str], bytes]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(source rows to ship, the full bundled-library inventory)."""
    libraries = inventory(bundles)
    users: dict[tuple[str, str], list[str]] = {}
    for row in libraries:
        if row["source"]:
            users.setdefault(tuple(row["source"]), []).append(
                f"{row['dist']} {row['dist_version']}: {row['file']}"
            )
    rows = []
    for key in sorted(users):
        up = UPSTREAM[key]
        payload, how = _upstream(up, get)
        rows.append({
            "file": up.filename,
            "for": f"{key[0]} {key[1]} (bundled in " + "; ".join(sorted(users[key])) + ")",
            "data": payload,
            "verified_against": how,
        })
    return rows, libraries


def ship(
    specs: list[Spec],
    dest: Path,
    *,
    get: Callable[[str], bytes] = _http_get,
    version_of: Callable[[str], str] = metadata.version,
    bundled: list[tuple[str, str, list[Path]]] | None = None,
    static: list[tuple[str, str, list[Path]]] | None = None,
) -> list[dict[str, Any]]:
    """Ship sdists for ``specs`` and, with ``bundled``, the copyleft libraries' source."""
    dest.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for spec in specs:
        version = version_of(spec.dist)
        name, payload, how = _sdist(spec, version, get)
        rows.append({"file": name, "for": f"{spec.dist} {version}", "data": payload, "verified_against": how})
        if "freetds" in spec.extras:
            tds = freetds_version(linked_binaries(spec.dist))
            name, payload, how = _freetds(tds, get)
            rows.append({
                "file": name,
                "for": f"FreeTDS {tds} (statically linked into {spec.dist} {version})",
                "data": payload,
                "verified_against": how,
            })
    libraries: list[dict[str, Any]] = []
    if bundled is not None:
        extra, libraries = _bundled_rows(bundled, get)
        rows += extra
        # Best-effort: well-known libraries linked statically into extension
        # modules. A copyleft one without shipped source fails here.
        linked = static_scan(static) if static is not None else []
        (dest / "BUNDLED_LIBRARIES.json").write_text(
            json.dumps({"purpose": "native libraries bundled inside the Python wheels in this image",
                        "libraries": libraries,
                        "statically_linked_best_effort": linked}, indent=2) + "\n",
            encoding="utf-8",
        )
    for row in rows:
        (dest / row["file"]).write_bytes(row.pop("data"))
        row["sha256"] = _sha256((dest / row["file"]).read_bytes())
    (dest / "SHA256SUMS").write_text(
        "".join(f"{r['sha256']}  {r['file']}\n" for r in rows), encoding="utf-8"
    )
    (dest / "MANIFEST.json").write_text(
        json.dumps({"purpose": "LGPL corresponding source for the binaries in this image",
                    "components": rows}, indent=2) + "\n",
        encoding="utf-8",
    )
    return rows


def verify_dockerfile(dockerfile: Path, policy: Path, requirements: Path) -> list[str]:
    """Drift check: the Dockerfile step ships exactly the approved LGPL packages."""
    problems = []
    specs = dockerfile_specs(dockerfile.read_text(encoding="utf-8"))
    shipped = {s.dist for s in specs}
    entries = json.loads(policy.read_text(encoding="utf-8")).get("exceptions") or {}
    approved = {
        k.split(":", 1)[1]
        for k, e in entries.items()
        if k.startswith("pypi:") and "GPL" in str(e.get("licence"))
        and (e.get("decision") or {}).get("status") == "approved"
    }
    if shipped != approved:
        problems.append(
            f"Dockerfile ships source for {sorted(shipped)} but the approved (L)GPL packages are {sorted(approved)}"
        )
    declared = set()
    for raw in requirements.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line and not line.startswith("-"):
            declared.add(re.split(r"[<>=!~\[; ]", line, maxsplit=1)[0].lower())
    for dist in sorted(shipped - declared):
        problems.append(f"{dist} is not a runtime requirement; nothing to ship source for")
    # Versions: the step reads the installed version, so it cannot drift from
    # what pip resolved. Prove each installed version has a source distribution.
    for spec in specs:
        try:
            version = metadata.version(spec.dist)
        except metadata.PackageNotFoundError:
            problems.append(f"{spec.dist} is not installed here; install the runtime requirements first")
            continue
        try:
            index = json.loads(_http_get(PYPI_JSON.format(name=spec.sdist, version=version)))
        except (SourceError, OSError, ValueError) as exc:
            problems.append(f"{spec.sdist} {version}: PyPI lookup failed ({type(exc).__name__})")
            continue
        if not any(u.get("packagetype") == "sdist" for u in index.get("urls") or []):
            problems.append(f"{spec.sdist} {version}: no source distribution on PyPI")
        if "freetds" in spec.extras:
            try:
                freetds_version(linked_binaries(spec.dist))
            except SourceError as exc:
                problems.append(str(exc))
    if "--bundled" not in dockerfile.read_text(encoding="utf-8"):
        problems.append("the Dockerfile step does not pass --bundled (wheel-bundled libraries)")
    # Every library the installed wheels bundle is reviewed and, when copyleft,
    # resolves to a mapped source.
    try:
        inventory(installed_bundles())
    except BundledLibraryError as exc:
        problems.append(str(exc))
    try:
        static_scan(installed_extension_modules())
    except BundledLibraryError as exc:
        problems.append(str(exc))
    from bundled_libraries import STATIC_MAPPED

    by_dist = {s.dist: s for s in specs}
    for dist, signature in STATIC_MAPPED:
        if signature not in getattr(by_dist.get(dist), "extras", ()):
            problems.append(f"{dist}'s statically linked {signature} is mapped but the Dockerfile step lacks {dist}+{signature}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("specs", nargs="*")
    parser.add_argument("--dest")
    parser.add_argument(
        "--bundled",
        action="store_true",
        help="also ship the source of the LGPL/GPL libraries the installed wheels bundle",
    )
    parser.add_argument("--verify-dockerfile")
    parser.add_argument("--policy", default="licence-policy.json")
    parser.add_argument("--requirements", default="apps/api/requirements.txt")
    args = parser.parse_args(argv)
    if args.verify_dockerfile:
        problems = verify_dockerfile(Path(args.verify_dockerfile), Path(args.policy), Path(args.requirements))
        for problem in problems:
            print(f"- {problem}")
        print("LGPL source step: " + ("DRIFT" if problems else "consistent"))
        return 1 if problems else 0
    if not args.dest or not args.specs:
        parser.error("--dest and at least one spec are required")
    rows = ship(
        [parse_spec(s) for s in args.specs],
        Path(args.dest),
        bundled=installed_bundles() if args.bundled else None,
        static=installed_extension_modules() if args.bundled else None,
    )
    for row in rows:
        print(f"{row['sha256']}  {row['file']}  ({row['for']}; {row['verified_against']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
