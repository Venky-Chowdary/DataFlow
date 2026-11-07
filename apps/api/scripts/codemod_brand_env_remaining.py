"""Codemod remaining os.getenv / os.environ.get DATAFLOW_* reads."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {"brand_env.py", "codemod_brand_env.py", "codemod_brand_env_remaining.py"}
PATS = [
    re.compile(r"""os\.getenv\(\s*(['"])DATAFLOW_([A-Z0-9_]+)\1"""),
    re.compile(r"""os\.environ\.get\(\s*(['"])DATAFLOW_([A-Z0-9_]+)\1"""),
]


def ensure_import(text: str) -> str:
    if "getenv_brand" in text and "from services.brand_env import getenv_brand" in text:
        return text
    if re.search(r"^import os\s*$", text, re.M):
        return re.sub(
            r"(^import os\s*$)",
            r"\1\nfrom services.brand_env import getenv_brand",
            text,
            count=1,
            flags=re.M,
        )
    return "from services.brand_env import getenv_brand\n" + text


def dedupe_import(text: str) -> str:
    seen = False
    out: list[str] = []
    for line in text.splitlines(True):
        if line.strip() == "from services.brand_env import getenv_brand":
            if seen:
                continue
            seen = True
        out.append(line)
    return "".join(out)


def main() -> None:
    files = hits = 0
    for path in ROOT.rglob("*.py"):
        if path.name in SKIP or "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "DATAFLOW_" not in text:
            continue
        original = text
        n_total = 0
        for pat in PATS:
            if not pat.search(text):
                continue
            text = ensure_import(text)

            def repl(m: re.Match[str], _pat=pat) -> str:
                return f'getenv_brand("{m.group(2)}"'

            text, n = pat.subn(repl, text)
            n_total += n
        if n_total:
            text = dedupe_import(text)
            if text != original:
                path.write_text(text, encoding="utf-8")
                files += 1
                hits += n_total
                print(f"  {path.relative_to(ROOT)} (+{n_total})")
    print(f"updated_files={files} replacements={hits}")


if __name__ == "__main__":
    main()
