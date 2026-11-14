#!/usr/bin/env python3
"""Generate the tool table in ``docs/use/mcp.md`` from the tool registry.

Reads ``src.ai.copilot.tools.TOOL_DEFINITIONS`` (the list MCP and Pilot share)
and ``src.ai.copilot.tool_permissions`` (which permission each tool needs, and
whether it reads, plans or changes something).

    python docs/tools/gen_mcp_tools.py            # rewrite the table
    python docs/tools/gen_mcp_tools.py --check    # exit 1 when stale
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
API_ROOT = REPO_ROOT / "apps" / "api"
TARGET = REPO_ROOT / "docs" / "use" / "mcp.md"

BEGIN = "<!-- BEGIN GENERATED: mcp-tools (docs/tools/gen_mcp_tools.py) -->"
END = "<!-- END GENERATED: mcp-tools -->"


def _bootstrap() -> None:
    for path in (API_ROOT, REPO_ROOT / "packages" / "preflight" / "src"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    if not (os.environ.get("DATAWRAP_DATA_DIR") or os.environ.get("DATAFLOW_DATA_DIR")):
        os.environ["DATAWRAP_DATA_DIR"] = tempfile.mkdtemp(prefix="datawrap-docs-")
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


def _summary(description: str) -> str:
    text = " ".join(str(description or "").split()).replace("``", "`").replace("|", "\\|")
    first = re.split(r"(?<=[.!?])\s", text, maxsplit=1)[0]
    return first if len(first) <= 150 else first[:147].rstrip() + "..."


def collect_rows() -> list[dict[str, str]]:
    """One row per registered tool: name, permission, effect, staging, summary."""
    _bootstrap()
    from src.ai.copilot import tool_permissions as tp
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    rows: list[dict[str, str]] = []
    for tool in TOOL_DEFINITIONS:
        name = str(tool["name"])
        requirement = tp.TOOL_PERMISSIONS.get(name)
        permission, effect = requirement if requirement else ("admin only (no entry)", "unknown")
        rows.append(
            {
                "name": name,
                "permission": str(permission),
                "effect": str(effect),
                "staged": "yes" if effect == tp.MUTATE else "no",
                "summary": _summary(tool.get("description", "")),
            }
        )
    return sorted(rows, key=lambda r: (r["effect"] != "read", r["effect"] != "plan", r["name"]))


def render() -> str:
    rows = collect_rows()
    counts = {e: sum(1 for r in rows if r["effect"] == e) for e in ("read", "plan", "mutate")}
    lines = [
        f"{len(rows)} tools: {counts['read']} read, {counts['plan']} plan, {counts['mutate']} change something.",
        "",
        "| Tool | Permission | Effect | Staged for Confirm | What it does |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| `{r['name']}` | `{r['permission']}` | {r['effect']} | {r['staged']} | {r['summary']} |"
        )
    return "\n".join(lines)


def table_rows(markdown: str) -> dict[str, tuple[str, str]]:
    """``{tool: (permission, effect)}`` parsed from the generated block."""
    block = markdown[markdown.index(BEGIN) : markdown.index(END)]
    found: dict[str, tuple[str, str]] = {}
    for match in re.finditer(r"^\| `([a-z0-9_]+)` \| `([^`]+)` \| ([a-z]+) \|", block, re.MULTILINE):
        found[match.group(1)] = (match.group(2), match.group(3))
    return found


def build_document(existing: str) -> str:
    if BEGIN not in existing or END not in existing:
        raise SystemExit(f"{TARGET} is missing the generated-block markers")
    head, _, rest = existing.partition(BEGIN)
    _, _, tail = rest.partition(END)
    return f"{head}{BEGIN}\n\n{render()}\n\n{END}{tail}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the table is stale")
    args = parser.parse_args(argv)
    existing = TARGET.read_text(encoding="utf-8")
    updated = build_document(existing)
    if args.check:
        if updated != existing:
            print("docs/use/mcp.md is stale; run docs/tools/gen_mcp_tools.py")
            return 1
        return 0
    TARGET.write_text(updated, encoding="utf-8")
    print(f"wrote {TARGET.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
