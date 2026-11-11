"""Every documentation path the code cites exists (audit M-13).

A cleanup removed the docs tree while API responses, comments and CI config
still pointed at 28 of its files. This test scans the repository for
``docs/<path>.md`` references and fails on any that does not resolve, so a
pointer can no longer outlive the page it names. It also checks the links
between the documentation pages themselves.
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCS = REPO_ROOT / "docs"

DOC_REF = re.compile(r"(?<![A-Za-z0-9_./-])docs/[A-Za-z0-9_./-]+?\.md\b")
MD_LINK = re.compile(r"\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#{1,6}\s+(.*)$", re.MULTILINE)

TEXT_SUFFIXES = {
    ".py", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".json", ".yaml", ".yml", ".toml", ".sh", ".css",
    ".conf", ".template", ".tpl", ".tf", ".cfg", ".ini", ".md", ".txt", ".html", "",
}
SKIP_DIRS = {".git", "node_modules", "dist", "build", "__pycache__", ".venv", "venv", ".pytest_cache"}
SKIP_FILES = {"package-lock.json"}

# A path that is a script's *output*, not a pointer to a page a reader should
# open. Each entry names the only file allowed to mention it.
GENERATED_OUTPUTS = {
    "docs/CONNECTOR_READINESS_MATRIX.md": "apps/api/scripts/connector_readiness_matrix_doc.py",
}


def _tracked_files() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=REPO_ROOT, capture_output=True, check=True, timeout=60,
        ).stdout.decode("utf-8", "replace")
        files = [REPO_ROOT / rel for rel in out.split("\0") if rel]
    except (OSError, subprocess.SubprocessError):
        files = []
        for root, dirs, names in os.walk(REPO_ROOT):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            files.extend(Path(root) / n for n in names)
    keep = []
    for path in files:
        parts = set(path.relative_to(REPO_ROOT).parts)
        if parts & SKIP_DIRS or path.name in SKIP_FILES:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES or not path.is_file():
            continue
        if path.stat().st_size > 3_000_000:
            continue
        keep.append(path)
    return keep


def _references() -> dict[str, set[str]]:
    """``{cited docs path: {files citing it}}`` for everything outside docs/."""
    found: dict[str, set[str]] = {}
    for path in _tracked_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel.startswith("docs/") and rel.endswith(".md"):
            continue  # pages link to each other relatively; checked below
        if rel == "apps/api/tests/test_docs_references_exist.py":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if "docs/" not in text:
            continue
        for match in DOC_REF.finditer(text):
            found.setdefault(match.group(0), set()).add(rel)
    return found


def test_every_cited_docs_path_exists() -> None:
    references = _references()
    assert references, "the scan found no documentation references at all"
    dead = {
        ref: sorted(files)
        for ref, files in references.items()
        if not (REPO_ROOT / ref).is_file() and ref not in GENERATED_OUTPUTS
    }
    assert not dead, f"code cites documentation that does not exist: {dead}"


def test_generated_output_paths_are_only_named_by_their_script() -> None:
    references = _references()
    for ref, owner in GENERATED_OUTPUTS.items():
        assert references.get(ref, set()) <= {owner}, (ref, references.get(ref))


def test_the_scan_sees_known_references() -> None:
    """Guard the guard: these citations must be found, or the scan is blind."""
    references = _references()
    assert "apps/api/src/routers/workspace_router.py" in references["docs/WEBHOOK_SIGNING.md"]
    assert "apps/api/src/main.py" in references["docs/API_VERSIONING.md"]
    assert "scripts/start-api.sh" in references["docs/operate/deploy.md"]
    assert DOC_REF.search("see docs/NOT_A_REAL_PAGE.md for more")


def test_api_responses_point_at_existing_pages() -> None:
    from services.recovery_honesty import honesty_dict
    from src.main import api_info
    from src.routers.workspace_router import _signing_disclosure

    cited = {
        "GET /api/v1": asyncio.run(api_info())["policy_url"],
        "webhook signing disclosure": _signing_disclosure("whsec_example")["signing"]["docs"],
        "recovery honesty": honesty_dict()["operator_runbook"],
    }
    for where, path in cited.items():
        assert not path.startswith("/"), f"{where}: {path} reads as a URL the API does not serve"
        assert (REPO_ROOT / path).is_file(), f"{where} cites {path}, which does not exist"


def _slug(heading: str) -> str:
    text = re.sub(r"[`*_]", "", heading.strip().lower())
    text = re.sub(r"[^a-z0-9 \-]", "", text)
    return re.sub(r"\s", "-", text)


@pytest.mark.parametrize("page", sorted(DOCS.rglob("*.md")), ids=lambda p: p.relative_to(DOCS).as_posix())
def test_links_between_pages_resolve(page: Path) -> None:
    text = page.read_text(encoding="utf-8")
    for target in MD_LINK.findall(text):
        if re.match(r"^[a-z][a-z0-9+.-]*:", target):
            continue  # http:, https:, mailto:
        file_part, _, anchor = target.partition("#")
        linked = page if not file_part else (page.parent / file_part).resolve()
        assert linked.is_file(), f"{page.relative_to(REPO_ROOT)} links to missing {target}"
        if anchor and linked.suffix == ".md":
            slugs = {_slug(h) for h in HEADING.findall(linked.read_text(encoding="utf-8"))}
            assert anchor in slugs, f"{page.relative_to(REPO_ROOT)} links to missing anchor {target}"


@pytest.mark.parametrize("page", sorted(DOCS.rglob("*.md")), ids=lambda p: p.relative_to(DOCS).as_posix())
def test_each_page_says_what_it_was_verified_against(page: Path) -> None:
    if page.name in {"README.md", "CHANGELOG.md"}:
        return
    text = page.read_text(encoding="utf-8")
    assert "## Verified against" in text, page.name
    section = text.split("## Verified against", 1)[1]
    cited = re.findall(r"`((?:apps|packages|scripts|deploy|docs|\.github)/[A-Za-z0-9_./*-]+|[A-Za-z0-9_.-]+\.(?:ya?ml|toml|api|web|sh))`", section)
    assert cited, f"{page.name} names no files under Verified against"
    for rel in cited:
        if "*" in rel:
            assert list(REPO_ROOT.glob(rel)), f"{page.name}: nothing matches {rel}"
        else:
            assert (REPO_ROOT / rel.rstrip("/")).exists(), f"{page.name}: {rel} does not exist"
