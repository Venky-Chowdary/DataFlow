"""Replace user-facing Datawrap / Datawrap Pilot / DataTransfer brand strings with Datawrap."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGETS = [
    ROOT / "apps" / "web" / "src",
    ROOT / "apps" / "web" / "index.html",
    ROOT / "README.md",
    ROOT / "design" / "BRAND.md",
    ROOT / "docs",
    ROOT / "apps" / "cli" / "README.md",
    ROOT / "packages" / "design-system" / "src",
]

# Order matters — longer phrases first.
REPLACEMENTS: list[tuple[str, str]] = [
    ("Datawrap", "Datawrap"),
    ("Datawrap", "Datawrap"),
    ("DataTransfer.Space", "Datawrap"),
    ("I'm **Datawrap Pilot**", "I'm **Datawrap Pilot**"),
    ("I'm Datawrap Pilot", "I'm Datawrap Pilot"),
    ("Datawrap Pilot Agent", "Datawrap Pilot Agent"),
    ("Datawrap Pilot", "Datawrap Pilot"),
    ("Datawrap", "Datawrap"),
    ("Dataflow", "Datawrap"),
    ("sales@dataflow.dev", "sales@datawrap.app"),
    ("noreply@dataflow.com", "noreply@datawrap.app"),
    ("admin@dataflow.app", "admin@datawrap.app"),
    ("https://dataflow.app", "https://datawrap.app"),
    ("http://dataflow.app", "https://datawrap.app"),
    ("api.dataflow.io", "api.datawrap.app"),
    ("your-company.dataflow.io", "your-company.datawrap.app"),
    ("dataflow.company.com", "datawrap.company.com"),
    ("dataflow@example.com", "datawrap@example.com"),
    ("dataflow@localhost", "datawrap@localhost"),
]

SKIP_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".ico", ".woff", ".woff2"}
# Keep technical CSS prefixes / package paths intact inside these patterns.
PROTECT = [
    (re.compile(r"@dataflow/"), "<<AT_DATAFLOW>>"),
    (re.compile(r"\bdf2-"), "<<DF2>>"),
    (re.compile(r"--df-"), "<<DFVAR>>"),
    (re.compile(r"\bdataflow_cli\b"), "<<DFCLI>>"),
    (re.compile(r"\bdataflow\.yaml\b"), "<<DFYAML>>"),
    (re.compile(r"\bdataflow_signal\b"), "<<DFSIGNAL>>"),
    (re.compile(r"ghcr\.io/[^\s\"']*dataflow"), "<<GHCR>>"),
]


def transform(text: str) -> str:
    protected: list[str] = []

    def stash(m: re.Match[str]) -> str:
        protected.append(m.group(0))
        return f"<<PROT{len(protected) - 1}>>"

    work = text
    for pat, _ in PROTECT:
        work = pat.sub(stash, work)
    for old, new in REPLACEMENTS:
        work = work.replace(old, new)
    # Also catch DATAFLOW in user-facing docs prose (not env vars — those dual-read).
    # Restore protected
    for i, val in enumerate(protected):
        work = work.replace(f"<<PROT{i}>>", val)
    return work


def main() -> None:
    files = hits = 0
    for target in TARGETS:
        paths = [target] if target.is_file() else list(target.rglob("*"))
        for path in paths:
            if not path.is_file() or path.suffix.lower() in SKIP_SUFFIXES:
                continue
            if path.suffix.lower() not in {
                ".ts", ".tsx", ".js", ".jsx", ".html", ".css", ".md", ".mdx", ".json",
            } and path.name != "index.html":
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except Exception:
                continue
            new = transform(text)
            if new != text:
                path.write_text(new, encoding="utf-8")
                files += 1
                print(f"  {path.relative_to(ROOT)}")
    print(f"updated_files={files}")


if __name__ == "__main__":
    main()
