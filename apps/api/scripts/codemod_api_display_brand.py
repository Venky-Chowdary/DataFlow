"""Rename user-facing API brand strings to Datawrap."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPLACEMENTS = [
    ("Datawrap", "Datawrap"),
    ("Datawrap", "Datawrap"),
    ("I'm **Datawrap Pilot**", "I'm **Datawrap Pilot**"),
    ("I'm Datawrap Pilot", "I'm Datawrap Pilot"),
    ("**Datawrap Pilot**", "**Datawrap Pilot**"),
    ("Datawrap Pilot Agent", "Datawrap Pilot Agent"),
    ("Datawrap Pilot", "Datawrap Pilot"),
    ("what is Datawrap", "what is Datawrap"),
    ("what makes Datawrap different", "what makes Datawrap different"),
    ("Datawrap", "Datawrap"),
]


def main() -> None:
    n_files = 0
    for path in ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        new = text
        for old, repl in REPLACEMENTS:
            new = new.replace(old, repl)
        new = new.replace("Datawrap Pilot", "Datawrap Pilot")
        new = new.replace("Datawrap", "Datawrap")
        if new != text:
            path.write_text(new, encoding="utf-8")
            n_files += 1
            print(f"  {path.relative_to(ROOT)}")
    print(f"updated_files={n_files}")


if __name__ == "__main__":
    main()
