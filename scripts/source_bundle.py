#!/usr/bin/env python3
"""The companion source bundle of the API image: pointer, exactness check, drift check.

Owner decision (2026-11-13): large copyleft source is not in the runtime image.
It is in a companion OCI image published with every release: the same
registry, and the runtime image's tag plus ``-sources``. That gives equivalent
access to copy the source from the same place. Dockerfile.api builds it as
the ``sources`` stage (FROM scratch). That stage holds:

* the Debian source of every copyleft system package in the runtime image
  (fetch_debian_sources.py) and the GCC 8.5 source for numpy's libquadmath /
  libgfortran (fetch_lgpl_sources.py --companion gcc);
* ``SHA256SUMS``, ``DEBIAN_SOURCES.json`` and ``MANIFEST.json``.

The runtime image keeps the small Python-wheel sources and the bundled-library
inventory in /usr/share/doc/datawrap/third-party-source. It also carries a
pointer:

* ``--pointer`` writes /usr/share/doc/datawrap/SOURCES.md: where the companion
  is for this tag, how to copy it, and every file in it with its SHA-256;
* ``--check`` (image build) fails unless the companion holds exactly the
  runtime image's copyleft Debian source packages, and exactly the
  companion-located wheel-library sources of its inventory;
* ``--verify-dockerfile`` (CI, offline) fails when the stages drift from
  this layout.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bundled_libraries import COMPANION_LOCATION, IMAGE_SOURCE_DIR
from fetch_debian_sources import (
    DOC_ROOT,
    DebPackage,
    copyleft_sources,
    dockerfile_stages,
    installed_packages,
    runtime_chain,
)
from fetch_lgpl_sources import companion_families

POINTER = "/usr/share/doc/datawrap/SOURCES.md"
COMPANION_COPY = "/usr/share/doc/datawrap/companion"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _sums(companion: Path) -> list[tuple[str, str]]:
    out = []
    for line in (companion / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        if "  " in line:
            digest, name = line.split("  ", 1)
            out.append((digest, name))
    return out


def pointer(companion: Path, ref: str) -> str:
    """SOURCES.md: where the companion bundle is, and what it holds."""
    sums = _sums(companion)
    debian = _json(companion / "DEBIAN_SOURCES.json").get("sources") or []
    wheel = _json(companion / "MANIFEST.json").get("components") or []
    lines = [
        "# Corresponding source of this image",
        "",
        "Datawrap ships the corresponding source of the copyleft components in this",
        "image in two places.",
        "",
        f"1. **In this image**, `{IMAGE_SOURCE_DIR}/`: the Python-wheel LGPL sources",
        "   (paramiko, psycopg2, pymssql, FreeTDS) and the sources of the small libraries",
        "   the wheels bundle (keyutils, libxcrypt), with `SHA256SUMS`, `MANIFEST.json` and",
        "   the bundled-library inventory `BUNDLED_LIBRARIES.json`.",
        "2. **In the companion source bundle** published with this image, in the same",
        "   registry, under the same tag plus `-sources`:",
        "",
        f"   `{ref}`",
        "",
        "   (also in this image's label `io.datawrap.sources`). It holds the Debian",
        "   source of every copyleft system package in this image",
        "   and the GCC source for numpy's libquadmath / libgfortran. To copy it:",
        "",
        "   ```",
        f"   docker pull {ref}",
        f"   cid=$(docker create {ref} none)",
        "   mkdir sources && docker export \"$cid\" | tar -x -C sources && docker rm \"$cid\"",
        "   cd sources && sha256sum -c SHA256SUMS",
        "   ```",
        "",
        f"A copy of the companion's `SHA256SUMS` and manifests is in `{COMPANION_COPY}/`.",
        "",
        f"## Contents of the companion bundle ({len(sums)} files)",
        "",
        f"### Debian source packages ({len(debian)})",
        "",
        "| Source | Version | Binary packages in this image |",
        "|---|---|---|",
    ]
    lines += [f"| {d['source']} | {d['version']} | {', '.join(d['binary_packages'])} |" for d in debian]
    lines += [
        "",
        "### Sources of libraries bundled in the Python wheels",
        "",
        "| File | For | Verified against |",
        "|---|---|---|",
    ]
    lines += [f"| {c['file']} | {c['for']} | {c['verified_against']} |" for c in wheel]
    lines += ["", "### Files and SHA-256", "", "```"]
    lines += [f"{digest}  {name}" for digest, name in sums]
    lines += ["```", ""]
    return "\n".join(lines)


def _wheel_keys(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    keys = set()
    for row in rows:
        family, version = str(row.get("for", "")).split(" ", 2)[:2]
        keys.add((family, version))
    return keys


def check(
    companion: Path,
    runtime_sources: Path,
    *,
    packages: list[DebPackage] | None = None,
    docs: Path = DOC_ROOT,
) -> list[str]:
    """The companion holds exactly what the runtime image needs it to hold."""
    problems = []
    want = copyleft_sources(installed_packages() if packages is None else packages, docs)
    debian = _json(companion / "DEBIAN_SOURCES.json").get("sources") or []
    have = {(d["source"], d["version"]) for d in debian}
    for key in sorted(set(want) - have):
        problems.append(
            f"{key[0]} {key[1]} ({', '.join(sorted(want[key]))}) is copyleft in the runtime image "
            "but missing from the companion bundle"
        )
    for key in sorted(have - set(want)):
        problems.append(f"{key[0]} {key[1]} is in the companion bundle but not in the runtime image")
    inventory = _json(runtime_sources / "BUNDLED_LIBRARIES.json").get("libraries") or []
    want_wheel = {tuple(r["source"]) for r in inventory if r.get("source_location") == COMPANION_LOCATION}
    have_wheel = _wheel_keys(_json(companion / "MANIFEST.json").get("components") or [])
    if want_wheel != have_wheel:
        problems.append(
            "the companion bundle's wheel-library sources "
            f"{sorted(' '.join(k) for k in have_wheel)} differ from the inventory's companion-located "
            f"{sorted(' '.join(k) for k in want_wheel)}"
        )
    listed = {name for _, name in _sums(companion)} if (companion / "SHA256SUMS").is_file() else set()
    named = {f for d in debian for f in d.get("files") or []}
    named |= {c["file"] for c in _json(companion / "MANIFEST.json").get("components") or []}
    if missing := sorted(named - listed):
        problems.append(f"files named in the companion's manifests but not in its SHA256SUMS: {missing}")
    return problems


_COMPANION_STEP = re.compile(r"fetch_lgpl_sources\.py --companion((?:\s+(?!--)\S+)*)\s+--dest\s+(\S+)")


def verify_dockerfile(dockerfile: Path) -> list[str]:
    """Drift check of the companion layout in Dockerfile.api."""
    problems = []
    stages = dockerfile_stages(dockerfile.read_text(encoding="utf-8"))
    by_name = {s.name: s for s in stages if s.name}
    chain = runtime_chain(stages)
    runtime_text = "\n".join(
        line for s in chain for line in s.body.splitlines() if not line.lstrip().startswith("#")
    )
    collect = by_name.get("sources-collect")
    if collect is None:
        return ["no `sources-collect` stage builds the companion bundle"]
    if collect.base not in {s.name for s in chain}:
        problems.append("sources-collect must be built FROM a runtime stage, so it sees the runtime's packages")
    match = _COMPANION_STEP.search(collect.body)
    families = set(match.group(1).split()) if match else set()
    if not match or match.group(2) != "/sources" or families != companion_families():
        problems.append(
            f"the companion step must be `fetch_lgpl_sources.py --companion "
            f"{' '.join(sorted(companion_families()))} --dest /sources`: its companion families "
            f"{sorted(families)} must equal UPSTREAM's {sorted(companion_families())}"
        )
    if "fetch_debian_sources.py --dest /sources" not in collect.body:
        problems.append("sources-collect does not run `fetch_debian_sources.py --dest /sources`")
    sources = by_name.get("sources")
    if sources is None or sources.base != "scratch":
        problems.append("the `sources` stage (the companion image) must be FROM scratch")
    elif [ln.strip() for ln in sources.body.splitlines() if ln.strip() and not ln.lstrip().startswith("#")] != [
        "COPY --from=sources-collect /sources /"
    ]:
        problems.append("the `sources` stage must hold only `COPY --from=sources-collect /sources /`")
    for forbidden, what in (
        ("--from=sources-collect", "copies from the companion stage"),
        ("fetch_debian_sources.py --dest", "runs the Debian source step"),
        ("--companion", "runs the companion source step"),
        ("source_bundle.py", "writes the companion pointer"),
    ):
        if forbidden in runtime_text:
            problems.append(
                f"the default target ({' -> '.join(s.name or s.base for s in chain)}) {what}; "
                "only the release stage may depend on sources-collect, so a deploy build "
                "never downloads the Debian / GCC sources"
            )
    release = by_name.get("release")
    if release is None or release.base not in {s.name for s in chain}:
        problems.append(
            "no release stage (FROM a default-target stage) carries the companion pointer"
        )
        return problems
    final = release.body
    if "ARG SOURCES_REF" not in final or 'LABEL io.datawrap.sources="${SOURCES_REF}"' not in final:
        problems.append(
            'the release stage must take ARG SOURCES_REF and set LABEL io.datawrap.sources="${SOURCES_REF}"'
        )
    if re.search(r"org\.opencontainers\.image\.source\s*=\s*\"?\$\{?SOURCES_REF", "\n".join(s.body for s in stages)):
        problems.append(
            "org.opencontainers.image.source must stay the repository URL (GHCR linking, Renovate, "
            "scanners); the companion reference belongs in io.datawrap.sources"
        )
    copies = list(re.finditer(r"COPY --from=sources-collect (.+)", final))
    if not copies:
        problems.append("the release stage copies no companion manifests")
    for copy in copies:
        srcs = copy.group(1).split()[:-1]
        if not srcs or any(not s.endswith((".json", "/SHA256SUMS")) for s in srcs):
            problems.append(f"the release stage may copy only the manifests from the companion, not {srcs}")
    if f"source_bundle.py --check {COMPANION_COPY}" not in final:
        problems.append("the release stage does not run `source_bundle.py --check` against the companion")
    if f"--out {POINTER}" not in final or "source_bundle.py --pointer" not in final:
        problems.append(f"the release stage does not write the pointer {POINTER}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pointer", type=Path, help="companion manifests directory")
    parser.add_argument("--ref", default="", help="where the companion is published (<image>:<tag>-sources)")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--check", type=Path, help="companion manifests directory")
    parser.add_argument("--runtime-sources", type=Path, default=Path(IMAGE_SOURCE_DIR))
    parser.add_argument("--verify-dockerfile", type=Path)
    args = parser.parse_args(argv)
    if args.verify_dockerfile:
        problems = verify_dockerfile(args.verify_dockerfile)
        for problem in problems:
            print(f"- {problem}")
        print("companion source bundle: " + ("DRIFT" if problems else "consistent"))
        return 1 if problems else 0
    if args.check:
        problems = check(args.check, args.runtime_sources)
        for problem in problems:
            print(f"- {problem}", file=sys.stderr)
        if problems:
            return 1
    if args.pointer:
        if not args.ref or not args.out:
            parser.error("--pointer needs --ref and --out")
        args.out.write_text(pointer(args.pointer, args.ref), encoding="utf-8")
    if not (args.check or args.pointer):
        parser.error("one of --pointer, --check, --verify-dockerfile is required")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
