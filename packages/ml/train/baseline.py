"""Build the optional automap baseline artifact.

Emits ``packages/ml/models/baseline.json`` — a plain target-name vocabulary.
Deliberately not a pickle: ``apps/api/services/ml_baseline.py`` scores against
this vocabulary with a self-contained char n-gram TF-IDF cosine, so the
transfer engine never has to deserialize executable objects to get an
automapping boost.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

SCHEMA_VERSION = 1

SEED_TARGETS = [
    "payment_amount",
    "payment_date",
    "customer_id",
    "account_number",
    "currency_code",
    "reference_number",
    "description",
    "status",
]


def normalize(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def collect_targets(data_path: Path) -> list[str]:
    targets: set[str] = set()
    if data_path.exists():
        with data_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                for mapping in record.get("output", {}).get("mappings", []):
                    target = normalize(mapping.get("target", ""))
                    if target:
                        targets.add(target)
    targets.update(normalize(t) for t in SEED_TARGETS)
    return sorted(targets)


def train_and_save() -> Path:
    root = Path(__file__).resolve().parents[1]
    data_path = root / "src" / "ml" / "data" / "synthetic_v1.jsonl"
    out_path = root / "models" / "baseline.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_dataset": data_path.name,
        "targets": collect_targets(data_path),
    }
    # Sorted keys + trailing newline keep the artifact diff-stable in git.
    out_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return out_path


if __name__ == "__main__":
    print(f"Baseline vocabulary saved to {train_and_save()}")
