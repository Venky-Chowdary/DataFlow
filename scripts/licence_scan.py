#!/usr/bin/env python3
"""Licence scan for the shipped dependencies, and the CI gate on new ones (F3).

Python: every distribution reachable from ``apps/api/requirements.txt``
(runtime) and ``apps/api/requirements-dev.txt`` (test tools), resolved
through the *installed* metadata (``importlib.metadata``) of the interpreter
running this script, extras and environment markers included.

npm: every package in ``package-lock.json`` (lockfile v3 records ``license``
and ``dev`` per package). When the lockfile has no licence the package's own
``package.json`` under ``--node-modules`` is read.

Each licence is classified against ``licence-policy.json``:

``allowed``      on the policy's allow list;
``copyleft``     GPL / AGPL / LGPL / SSPL / EUPL / OSL / MPL family (weak or strong);
``unknown``      no licence metadata, or text the scanner cannot map.

``--check`` fails (exit 1) when a package is copyleft or unknown and has no
entry in the policy's ``exceptions`` (each with the licence it was reviewed
under and why). A package whose licence changed from the reviewed one fails
too. Nothing is installed or downloaded.

Usage (repository root)::

    python scripts/licence_scan.py --json licence-report.json --markdown licence-report.md
    python scripts/licence_scan.py --check
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from importlib import metadata
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
POLICY = REPO / "licence-policy.json"
RUNTIME_REQS = REPO / "apps" / "api" / "requirements.txt"
DEV_REQS = REPO / "apps" / "api" / "requirements-dev.txt"
LOCKFILE = REPO / "package-lock.json"

_COPYLEFT = re.compile(
    r"\b(A?GPL|LGPL|GNU (?:Affero |Lesser |Library )?General Public|SSPL|Server Side Public"
    r"|EUPL|OSL|MPL|Mozilla Public|CDDL|EPL|Eclipse Public|CC-BY-SA)\b",
    re.IGNORECASE,
)

# Classifier / free-text spellings -> SPDX id. Anything else is kept verbatim
# and judged against the policy's allow list as is.
_ALIASES: list[tuple[str, str]] = [
    (r"^(the )?mit( license)?$|^mit/x11$|expat", "MIT"),
    (r"^apache( software)?( license)?[ ,-]*(version )?2(\.0)?$|^apache-2\.0$|^apache 2$|^asl 2$|^apache license, version 2\.0$|^apache software license$|^apache$", "Apache-2.0"),
    (r"^bsd$|^bsd license$|^bsd-like$", "BSD"),
    (r"^(new |modified |revised )?bsd[- ]?3([- ]clause)?( license)?$|^3-clause bsd( license)?$|^bsd 3-clause$|^(new|modified|revised) bsd( license)?$", "BSD-3-Clause"),
    (r"^(simplified |freebsd )?bsd[- ]?2([- ]clause)?( license)?$|^2-clause bsd$", "BSD-2-Clause"),
    (r"^isc( license)?( \(iscl\))?$", "ISC"),
    (r"^python software foundation( license)?$|^psf(-2\.0)?$|^psfl$", "PSF-2.0"),
    (r"^mozilla public license 2\.0 \(mpl 2\.0\)$|^mpl[- ]?2(\.0)?$", "MPL-2.0"),
    (r"^the unlicense \(unlicense\)$|^unlicense$", "Unlicense"),
    (r"^zlib$|^zlib/libpng$", "Zlib"),
    (r"^historical permission notice and disclaimer \(hpnd\)$", "HPND"),
]
_CLASSIFIER = {
    "MIT License": "MIT",
    "Apache Software License": "Apache-2.0",
    "BSD License": "BSD",
    "ISC License (ISCL)": "ISC",
    "Python Software Foundation License": "PSF-2.0",
    "Mozilla Public License 2.0 (MPL 2.0)": "MPL-2.0",
    "GNU Lesser General Public License v3 (LGPLv3)": "LGPL-3.0",
    "GNU Lesser General Public License v2 (LGPLv2)": "LGPL-2.0",
    "GNU Lesser General Public License v3 or later (LGPLv3+)": "LGPL-3.0-or-later",
    "GNU Library or Lesser General Public License (LGPL)": "LGPL",
    "GNU General Public License v2 (GPLv2)": "GPL-2.0",
    "GNU General Public License v3 (GPLv3)": "GPL-3.0",
    "GNU Affero General Public License v3": "AGPL-3.0",
    "The Unlicense (Unlicense)": "Unlicense",
    "zlib/libpng License": "Zlib",
    "Historical Permission Notice and Disclaimer (HPND)": "HPND",
    "Academic Free License (AFL)": "AFL",
    "Eclipse Public License 2.0 (EPL-2.0)": "EPL-2.0",
    "Public Domain": "Public-Domain",
}


@dataclass
class Package:
    ecosystem: str  # pypi | npm
    name: str
    version: str
    licence: str
    scope: str  # runtime | dev
    source: str  # where the licence text came from
    status: str = ""  # allowed | copyleft | unknown
    exception: str = ""


def _from_text(text: str) -> str:
    """SPDX id for a pasted licence text, or "" when it cannot be told."""
    if re.search(r"permission is hereby granted, free of charge", text, re.IGNORECASE):
        return "MIT"
    if re.search(r"apache license,? version 2\.0", text, re.IGNORECASE):
        return "Apache-2.0"
    if re.search(r"redistribution and use in source and binary forms", text, re.IGNORECASE):
        return "BSD"
    if re.search(r"GNU (?:LESSER|LIBRARY) GENERAL PUBLIC LICENSE", text, re.IGNORECASE):
        return "LGPL"
    if re.search(r"GNU GENERAL PUBLIC LICENSE", text, re.IGNORECASE):
        return "GPL"
    return ""


def normalise(raw: str) -> str:
    """Map a licence field to an SPDX-style id; "" when there is none."""
    text = " ".join((raw or "").split()).strip().strip(";").strip()
    if not text or text.upper() in {"UNKNOWN", "NONE"}:
        return ""
    if text.upper() == "UNLICENSED":
        return "UNLICENSED"
    if len(text) > 120:  # a whole licence text pasted into the field
        return _from_text(text)
    low = text.lower()
    for pattern, spdx in _ALIASES:
        if re.search(pattern, low):
            return spdx
    return text


def _python_licence(dist: metadata.Distribution) -> tuple[str, str]:
    meta = dist.metadata
    expression = (meta.get("License-Expression") or "").strip()
    if expression:
        return expression, "License-Expression"
    classifiers = [
        c.split(" :: ")[-1]
        for c in meta.get_all("Classifier") or []
        if c.startswith("License ::") and c != "License :: OSI Approved"
    ]
    mapped = sorted({_CLASSIFIER.get(c, c) for c in classifiers})
    if mapped:
        return " OR ".join(mapped), "classifier"
    field = normalise(meta.get("License") or "")
    if field:
        return field, "License"
    return "", "none"


def _req_name(line: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)(\[[^\]]*\])?", line)
    return match.group(0).strip() if match else ""


def _read_reqs(path: Path, seen: set[Path] | None = None) -> list[str]:
    seen = seen or set()
    if path in seen or not path.is_file():
        return []
    seen.add(path)
    out: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith(("-r ", "--requirement ")):
            out += _read_reqs((path.parent / line.split(None, 1)[1]).resolve(), seen)
            continue
        if line.startswith("-"):
            continue
        name = _req_name(line)
        if name:
            out.append(name)
    return out


def _canon(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def python_closure(roots: Iterable[str]) -> tuple[dict[str, metadata.Distribution], list[str]]:
    """Installed distributions reachable from ``roots`` (with extras), and missing names."""
    from packaging.requirements import Requirement

    installed = {_canon(d.metadata["Name"]): d for d in metadata.distributions() if d.metadata["Name"]}
    found: dict[str, metadata.Distribution] = {}
    missing: list[str] = []
    queue: list[tuple[str, frozenset[str]]] = []
    for root in roots:
        req = Requirement(root)
        queue.append((req.name, frozenset(req.extras)))
    done: set[tuple[str, frozenset[str]]] = set()
    while queue:
        name, extras = queue.pop()
        key = (_canon(name), extras)
        if key in done:
            continue
        done.add(key)
        dist = installed.get(_canon(name))
        if dist is None:
            missing.append(name)
            continue
        found[_canon(name)] = dist
        for spec in dist.requires or []:
            req = Requirement(spec)
            envs = [{"extra": e} for e in extras] or [{"extra": ""}]
            if req.marker is not None and not any(req.marker.evaluate(env) for env in envs):
                continue
            queue.append((req.name, frozenset(req.extras)))
    return found, sorted(set(missing))


def scan_python(runtime: Path = RUNTIME_REQS, dev: Path = DEV_REQS) -> tuple[list[Package], list[str]]:
    runtime_dists, missing_rt = python_closure(_read_reqs(runtime))
    all_dists, missing_all = python_closure(_read_reqs(dev) or _read_reqs(runtime))
    packages = []
    for key, dist in sorted(all_dists.items()):
        licence, source = _python_licence(dist)
        packages.append(
            Package(
                "pypi",
                dist.metadata["Name"],
                dist.version,
                licence,
                "runtime" if key in runtime_dists else "dev",
                source,
            )
        )
    return packages, sorted(set(missing_rt) | set(missing_all))


def scan_npm(lockfile: Path = LOCKFILE, node_modules: Path | None = None) -> list[Package]:
    lock = json.loads(lockfile.read_text(encoding="utf-8"))
    packages = []
    for path, entry in sorted((lock.get("packages") or {}).items()):
        if entry.get("link"):
            continue
        if "node_modules/" not in path:
            # The repository's own root package and workspaces.
            packages.append(
                Package(
                    "npm",
                    entry.get("name") or path or "(root)",
                    entry.get("version", ""),
                    normalise(str(entry.get("license") or "")),
                    "first-party",
                    "package-lock.json",
                )
            )
            continue
        name = entry.get("name") or path.rsplit("node_modules/", 1)[-1]
        licence = entry.get("license") or ""
        source = "package-lock.json"
        if isinstance(licence, dict):
            licence = licence.get("type", "")
        if not licence and node_modules is not None:
            pkg_json = node_modules / path.split("node_modules/", 1)[-1] / "package.json"
            if pkg_json.is_file():
                data = json.loads(pkg_json.read_text(encoding="utf-8"))
                lic = data.get("license") or data.get("licenses") or ""
                if isinstance(lic, list):
                    lic = " OR ".join(x.get("type", "") if isinstance(x, dict) else str(x) for x in lic)
                elif isinstance(lic, dict):
                    lic = lic.get("type", "")
                licence, source = str(lic), "node_modules package.json"
        packages.append(
            Package(
                "npm",
                name,
                entry.get("version", ""),
                normalise(str(licence)) or "",
                "dev" if entry.get("dev") or entry.get("devOptional") else "runtime",
                source if licence else "none",
            )
        )
    # One row per name@version (the lockfile nests duplicates).
    unique: dict[tuple[str, str], Package] = {}
    for pkg in packages:
        prior = unique.get((pkg.name, pkg.version))
        if prior is None or (prior.scope == "dev" and pkg.scope == "runtime"):
            unique[(pkg.name, pkg.version)] = pkg
    return list(unique.values())


def scan_first_party_python(repo: Path = REPO) -> list[Package]:
    """The repository's own Python packages (pyproject.toml), as first-party rows."""
    import tomllib

    rows = []
    for pyproject in sorted(repo.glob("packages/*/pyproject.toml")):
        project = tomllib.loads(pyproject.read_text(encoding="utf-8")).get("project") or {}
        licence = project.get("license")
        if isinstance(licence, dict):
            licence = licence.get("text") or licence.get("file") or ""
        rows.append(
            Package(
                "pypi",
                str(project.get("name") or pyproject.parent.name),
                str(project.get("version") or ""),
                str(licence or ""),
                "first-party",
                str(pyproject.relative_to(repo)),
            )
        )
    return rows


def _first_party(pkg: Package, policy: dict[str, Any]) -> bool:
    return pkg.name in set((policy.get("first_party") or {}).get(pkg.ecosystem) or [])


def _terms(expression: str) -> list[str]:
    cleaned = expression.replace("(", " ").replace(")", " ")
    return [t.strip() for t in re.split(r"\s+(?:OR|AND|WITH)\s+|/|,", cleaned) if t.strip()]


def classify(pkg: Package, policy: dict[str, Any]) -> Package:
    allowed = set(policy.get("allowed") or [])
    exceptions = policy.get("exceptions") or {}
    key = f"{pkg.ecosystem}:{pkg.name}"
    if _first_party(pkg, policy):
        # The project's own packages: proprietary (LICENSE), never a dependency.
        pkg.status = "first_party"
        return pkg
    if not pkg.licence or pkg.licence == "UNLICENSED":
        pkg.status = "unknown"
    else:
        terms = _terms(pkg.licence)
        is_or = " OR " in pkg.licence and " AND " not in pkg.licence
        if is_or and any(t in allowed for t in terms):
            # Dual-licensed: we may take the permissive option.
            pkg.status = "allowed"
        elif all(t in allowed for t in terms):
            pkg.status = "allowed"
        elif _COPYLEFT.search(pkg.licence):
            pkg.status = "copyleft"
        else:
            pkg.status = "unknown"
    entry = exceptions.get(key)
    if pkg.status != "allowed" and entry:
        pkg.exception = entry.get("why", "")
    return pkg


def violations(packages: Iterable[Package], policy: dict[str, Any]) -> list[str]:
    exceptions = policy.get("exceptions") or {}
    problems = []
    own_terms = set(policy.get("first_party_licences") or [])
    for pkg in packages:
        if pkg.status == "first_party":
            if pkg.licence not in own_terms:
                problems.append(
                    f"{pkg.ecosystem}:{pkg.name} is first-party but declares {pkg.licence or 'no licence'}; "
                    f"the project is proprietary (LICENSE): use one of {sorted(own_terms)}"
                )
            continue
        if pkg.status == "allowed":
            continue
        entry = exceptions.get(f"{pkg.ecosystem}:{pkg.name}")
        if not entry:
            problems.append(
                f"{pkg.ecosystem}:{pkg.name} {pkg.version} ({pkg.scope}) is {pkg.status}: "
                f"{pkg.licence or 'no licence metadata'}; review it and add an exception to "
                "licence-policy.json, or drop the dependency"
            )
        elif entry.get("licence") != pkg.licence:
            problems.append(
                f"{pkg.ecosystem}:{pkg.name} licence changed from the reviewed "
                f"{entry.get('licence')!r} to {pkg.licence!r}; review again"
            )
    return problems


def load_policy(path: Path = POLICY) -> dict[str, Any]:
    policy = json.loads(path.read_text(encoding="utf-8"))
    for key, entry in (policy.get("exceptions") or {}).items():
        if not key.startswith(("pypi:", "npm:")):
            raise ValueError(f"{path}: exception key {key!r} must start with pypi: or npm:")
        missing = [f for f in ("licence", "why") if not str(entry.get(f) or "").strip()]
        if missing:
            raise ValueError(f"{path}: exception {key!r} lacks {missing}")
        decision = entry.get("decision") or {}
        if decision and entry.get("owner_decision"):
            raise ValueError(f"{path}: exception {key!r} is both pending (owner_decision) and decided")
        if decision.get("status") == "approved" and re.search(r"GPL", str(entry["licence"])):
            # Shipping an (L)GPL library in the image is binary distribution:
            # the licence text and the source (with a written offer) must travel.
            text = str(entry.get("notice") or "")
            if "gnu.org/licenses" not in text or "Source:" not in text or "written offer" not in text:
                raise ValueError(
                    f"{path}: approved exception {key!r} must name the licence text "
                    "(gnu.org/licenses URL), a 'Source:' pointer and a written offer in its notice"
                )
    return policy


def markdown(packages: list[Package], missing: list[str]) -> str:
    flagged = [p for p in packages if p.status not in ("allowed", "first_party")]
    counts: dict[str, int] = {}
    for p in packages:
        counts[f"{p.ecosystem}/{p.scope}/{p.status}"] = counts.get(f"{p.ecosystem}/{p.scope}/{p.status}", 0) + 1
    lines = ["# Dependency licence report", "", "| Ecosystem / scope / status | Packages |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(counts.items())]
    lines += ["", "## Copyleft or unknown", "", "| Package | Version | Scope | Licence | Status | Exception |", "|---|---|---|---|---|---|"]
    for p in sorted(flagged, key=lambda p: (p.status, p.ecosystem, p.name.lower())):
        lines.append(
            f"| {p.ecosystem}:{p.name} | {p.version} | {p.scope} | {p.licence or '(none)'} | {p.status} | {p.exception or '-'} |"
        )
    if missing:
        lines += ["", "Not installed here, so not scanned: " + ", ".join(missing)]
    return "\n".join(lines) + "\n"


_NOTICE_NAME = re.compile(r"^NOTICE(\.(txt|md))?$", re.IGNORECASE)


def upstream_notices(packages: Iterable[Package]) -> list[tuple[str, str]]:
    """(package, text) for every shipped Python package that carries a NOTICE file.

    Apache-2.0 section 4(d): a redistribution must include the NOTICE text of
    the work it redistributes. The API image redistributes these packages.
    """
    out = []
    for pkg in packages:
        if pkg.ecosystem != "pypi" or pkg.scope != "runtime":
            continue
        try:
            dist = metadata.distribution(pkg.name)
        except metadata.PackageNotFoundError:
            continue
        texts = []
        for file in dist.files or []:
            if _NOTICE_NAME.match(file.name):
                path = Path(str(file.locate()))
                if path.is_file():
                    texts.append(path.read_text(encoding="utf-8", errors="replace").strip())
        for text in dict.fromkeys(texts):
            out.append((pkg.name, text))
    return out


def notice(packages: list[Package], policy: dict[str, Any], not_installed: Iterable[str] = ()) -> str:
    """The NOTICE file: attributions for what the API image and web bundle ship."""
    shipped = sorted(
        (p for p in packages if p.scope == "runtime" and p.status != "first_party"),
        key=lambda p: (p.ecosystem, p.name.lower()),
    )
    exceptions = policy.get("exceptions") or {}
    lines = [
        "Datawrap",
        "Copyright (c) 2026 Venky Chowdary. All rights reserved.",
        "Datawrap is proprietary software. See LICENSE.",
        "",
        "This product includes third-party software. This file lists it with its",
        "licence and reproduces the NOTICE text its authors require. Those",
        "components remain under their own licences; nothing here grants rights",
        "in Datawrap itself.",
        "",
        "Generated by scripts/licence_scan.py --notice NOTICE from the installed",
        "runtime packages (apps/api/requirements.txt) and the runtime npm",
        "packages in package-lock.json. Test tools (apps/api/requirements-dev.txt,",
        "npm devDependencies) are not distributed and are not listed.",
        "",
        "=" * 72,
        "Components under copyleft, proprietary or otherwise conditional terms",
        "=" * 72,
        "",
    ]
    for p in shipped:
        entry = exceptions.get(f"{p.ecosystem}:{p.name}")
        if p.status != "allowed" or (entry and entry.get("notice")):
            text = (entry or {}).get("notice") or (entry or {}).get("why") or "Not yet reviewed."
            lines += [f"{p.name} ({p.ecosystem}) - {p.licence or 'licence not declared'}", f"  {text}", ""]
    lines += ["=" * 72, "Third-party packages and their licences", "=" * 72, ""]
    for ecosystem, title in (("pypi", "Python (API image)"), ("npm", "JavaScript (web bundle)")):
        rows = [p for p in shipped if p.ecosystem == ecosystem]
        if not rows:
            continue
        lines += [title, "-" * len(title)]
        lines += [f"{p.name}: {p.licence or 'licence not declared'}" for p in rows]
        if ecosystem == "pypi":
            lines += [
                f"{name}: licence not scanned (not installed where this file was generated)"
                for name in sorted(not_installed)
            ]
        lines.append("")
    notices = upstream_notices(shipped)
    if notices:
        lines += ["=" * 72, "NOTICE files of redistributed packages", "=" * 72, ""]
        for name, text in notices:
            lines += [f"--- {name} ---", text, ""]
    return "\n".join(lines).rstrip() + "\n"


def direct_dependencies(runtime: Path = RUNTIME_REQS, lockfile: Path = LOCKFILE) -> set[str]:
    """``ecosystem:name`` of what the project itself declares for production.

    Transitive sets differ between platforms and Python versions; the direct
    ones do not, so NOTICE completeness is enforced on these.
    """
    names = {f"pypi:{_canon(_req_name(n).split('[')[0])}" for n in _read_reqs(runtime)}
    lock = json.loads(lockfile.read_text(encoding="utf-8")) if lockfile.is_file() else {}
    packages = lock.get("packages") or {}
    for path, entry in packages.items():
        if path and "node_modules/" not in path:
            names |= {
                f"npm:{dep}"
                for dep in entry.get("dependencies") or {}
                # A workspace (this repository's own package) is a link, not a dependency.
                if not (packages.get(f"node_modules/{dep}") or {}).get("link")
            }
    return names


def notice_gaps(direct: Iterable[str], text: str) -> list[str]:
    """Direct production dependencies the NOTICE file does not name."""
    named = {line.split(":", 1)[0].strip().lower() for line in text.splitlines() if ":" in line}
    named |= {_canon(n) for n in named}
    gaps = []
    for key in direct:
        _ecosystem, name = key.split(":", 1)
        if name.lower() not in named and _canon(name) not in named:
            gaps.append(key)
    return sorted(gaps)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--policy", default=str(POLICY))
    parser.add_argument("--runtime-reqs", default=str(RUNTIME_REQS))
    parser.add_argument("--dev-reqs", default=str(DEV_REQS))
    parser.add_argument("--lockfile", default=str(LOCKFILE))
    parser.add_argument("--node-modules", default=str(REPO / "node_modules"))
    parser.add_argument("--skip-python", action="store_true")
    parser.add_argument("--skip-npm", action="store_true")
    parser.add_argument("--json")
    parser.add_argument("--markdown")
    parser.add_argument("--check", action="store_true", help="exit 1 on unreviewed copyleft/unknown licences")
    parser.add_argument("--notice", help="write the NOTICE file here")
    parser.add_argument(
        "--check-notice",
        help="exit 1 when a shipped package is missing from this NOTICE file (with --check)",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="do not fail when a required Python distribution is not installed",
    )
    args = parser.parse_args(argv)

    policy = load_policy(Path(args.policy))
    packages: list[Package] = []
    missing: list[str] = []
    if not args.skip_python:
        py, missing = scan_python(Path(args.runtime_reqs), Path(args.dev_reqs))
        packages += py
    if not args.skip_npm:
        nm = Path(args.node_modules)
        packages += scan_npm(Path(args.lockfile), nm if nm.is_dir() else None)
    packages += scan_first_party_python()
    for pkg in packages:
        classify(pkg, policy)

    if args.json:
        Path(args.json).write_text(
            json.dumps({"packages": [asdict(p) for p in packages], "not_installed": missing}, indent=2) + "\n",
            encoding="utf-8",
        )
    if args.markdown:
        Path(args.markdown).write_text(markdown(packages, missing), encoding="utf-8")

    if args.notice:
        Path(args.notice).write_text(notice(packages, policy, missing), encoding="utf-8")

    problems = violations(packages, policy)
    if args.check_notice:
        gaps = notice_gaps(
            direct_dependencies(Path(args.runtime_reqs), Path(args.lockfile)),
            Path(args.check_notice).read_text(encoding="utf-8"),
        )
        if gaps:
            problems.append(
                "shipped packages missing from NOTICE (regenerate with --notice): " + ", ".join(gaps)
            )
    if missing and not args.allow_missing:
        problems.append("required distributions not installed, so unscanned: " + ", ".join(missing))
    flagged = sum(1 for p in packages if p.status not in ("allowed", "first_party"))
    print(f"scanned {len(packages)} packages; {flagged} copyleft/unknown; {len(problems)} unreviewed")
    for problem in problems:
        print(f"- {problem}")
    return 1 if args.check and problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
