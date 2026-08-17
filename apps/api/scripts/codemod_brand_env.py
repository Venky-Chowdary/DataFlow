"""Codemod: os.getenv('DATAFLOW_*') -> getenv_brand('*') with dual-read."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {"brand_env.py"}
PAT = re.compile(r"""os\.getenv\(\s*(['"])DATAFLOW_([A-Z0-9_]+)\1""")


def main() -> None:
    files = 0
    hits = 0
    for path in ROOT.rglob("*.py"):
        if path.name in SKIP or "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "DATAFLOW_" not in text or "os.getenv" not in text:
            continue
        if not PAT.search(text):
            continue
        if "getenv_brand" not in text:
            if re.search(r"^import os\s*$", text, re.M):
                text = re.sub(
                    r"(^import os\s*$)",
                    r"\1\nfrom services.brand_env import getenv_brand",
                    text,
                    count=1,
                    flags=re.M,
                )
            else:
                text = "from services.brand_env import getenv_brand\n" + text

        def repl(m: re.Match[str]) -> str:
            return f'getenv_brand("{m.group(2)}"'

        new_text, n = PAT.subn(repl, text)
        # Dedupe import
        seen = False
        out_lines: list[str] = []
        for line in new_text.splitlines(True):
            if line.strip() == "from services.brand_env import getenv_brand":
                if seen:
                    continue
                seen = True
            out_lines.append(line)
        new_text = "".join(out_lines)
        if new_text != path.read_text(encoding="utf-8"):
            path.write_text(new_text, encoding="utf-8")
            files += 1
            hits += n
            print(f"  {path.relative_to(ROOT)} (+{n})")
    print(f"updated_files={files} replacements={hits}")


if __name__ == "__main__":
    main()
