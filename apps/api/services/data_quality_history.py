"""History-aware data quality profiling for recurring transfers.

Implements an Auto-Validate-by-History style gate: each transfer run produces
a per-column statistical profile; the next run compares its profile to the
historical baseline and flags deviations that exceed configurable thresholds.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir


@dataclass
class ColumnProfile:
    column: str
    dtype: str = "string"
    count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    min_value: str | None = None
    max_value: str | None = None
    mean: float | None = None
    std: float | None = None
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    top_values: list[tuple[str, int]] = field(default_factory=list)

    @property
    def null_rate(self) -> float:
        return self.null_count / self.count if self.count else 0.0

    @property
    def distinct_rate(self) -> float:
        return self.distinct_count / self.count if self.count else 0.0


def _to_sortable(value: Any) -> Any:
    """Best-effort convert a value to a sortable native type."""
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    return str(value)


def _coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(Decimal(str(value).replace(",", "")))
    except Exception:
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text.replace("Z", "+0000"), fmt)
        except ValueError:
            continue
    return None


def profile_column(values: list[Any], column: str, dtype: str = "string") -> ColumnProfile:
    """Compute a descriptive statistical profile for a single column."""
    profile = ColumnProfile(column=column, dtype=dtype, count=len(values))
    non_null = [v for v in values if v is not None and str(v).strip() not in {"", "null", "none"}]
    profile.null_count = profile.count - len(non_null)

    as_text = [str(v) for v in non_null]
    lengths = [len(t) for t in as_text]
    if lengths:
        profile.min_length = min(lengths)
        profile.max_length = max(lengths)
        profile.avg_length = sum(lengths) / len(lengths)

    profile.distinct_count = len(set(as_text))

    if non_null:
        if dtype in {"int", "integer", "float", "number", "decimal", "double"}:
            nums = [n for v in non_null if (n := _coerce_number(v)) is not None]
            if nums:
                if dtype in {"int", "integer"}:
                    min_num, max_num = int(min(nums)), int(max(nums))
                else:
                    min_num, max_num = min(nums), max(nums)
                profile.min_value = str(min_num)
                profile.max_value = str(max_num)
                profile.mean = sum(nums) / len(nums)
                if len(nums) > 1:
                    variance = sum((x - profile.mean) ** 2 for x in nums) / (len(nums) - 1)
                    profile.std = math.sqrt(variance)
        elif dtype in {"date", "datetime", "timestamp"}:
            dts = [d for v in non_null if (d := _coerce_datetime(v)) is not None]
            if dts:
                profile.min_value = min(dts).isoformat()
                profile.max_value = max(dts).isoformat()
        else:
            sorted_vals = sorted(as_text)
            profile.min_value = sorted_vals[0]
            profile.max_value = sorted_vals[-1]

    # Top-5 frequency; keep payload small.
    freq: dict[str, int] = {}
    for v in as_text:
        freq[v] = freq.get(v, 0) + 1
    profile.top_values = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:5]

    return profile


def profile_batch(
    rows: list[dict[str, Any]],
    schema: dict[str, str] | None = None,
) -> dict[str, ColumnProfile]:
    """Profile every column in a batch of rows."""
    schema = schema or {}
    if not rows:
        return {}
    columns = list(rows[0].keys())
    result: dict[str, ColumnProfile] = {}
    for col in columns:
        values = [row.get(col) for row in rows]
        result[col] = profile_column(values, col, schema.get(col, "string"))
    return result


def _profile_key(source: dict[str, Any], destination: dict[str, Any]) -> str:
    """Stable key for the historical profile of a source/destination pair."""
    parts = [
        source.get("kind", ""),
        source.get("format", ""),
        source.get("table") or source.get("collection") or source.get("schema", ""),
        destination.get("kind", ""),
        destination.get("format", ""),
        destination.get("table") or destination.get("collection") or destination.get("schema", ""),
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _profile_path(key: str) -> Path:
    base = data_dir() / "quality_profiles"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{key}.json"


def load_historical_profile(
    source: dict[str, Any],
    destination: dict[str, Any],
) -> dict[str, ColumnProfile] | None:
    """Load the most recent saved profile for a source/destination route."""
    path = _profile_path(_profile_key(source, destination))
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {name: ColumnProfile(**col) for name, col in data.get("columns", {}).items()}
    except Exception:
        return None


def save_profile(
    source: dict[str, Any],
    destination: dict[str, Any],
    profile: dict[str, ColumnProfile],
) -> None:
    """Persist a profile as the new baseline."""
    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "columns": {name: asdict(col) for name, col in profile.items()},
    }
    write_json_atomic(_profile_path(_profile_key(source, destination)), payload, default=str)


def detect_anomalies(
    current: dict[str, ColumnProfile],
    historical: dict[str, ColumnProfile] | None,
    *,
    fpr_target: float = 0.01,
) -> list[str]:
    """Return a list of anomaly messages for deviations from the historical baseline.

    ``fpr_target`` is kept as an API knob for future calibration; thresholds are
    currently set conservatively (z-score > 3, null-rate change > 0.1) which
    empirically keeps false positives low for stable recurring pipelines.
    """
    if not historical:
        return []

    issues: list[str] = []
    # Bound thresholds are fixed for simplicity; fpr_target can drive a learned
    # calibration layer later.
    _ = fpr_target
    z_threshold = 3.0
    null_rate_threshold = 0.10
    count_drop_threshold = 0.5

    for col, cur in current.items():
        hist = historical.get(col)
        if not hist:
            continue

        if cur.count > 0 and hist.count > 0:
            ratio = cur.count / hist.count
            if ratio < (1 - count_drop_threshold):
                issues.append(
                    f"Column '{col}' row count dropped by {(1 - ratio) * 100:.1f}% "
                    f"({cur.count} vs historical {hist.count})"
                )

        cur_null = cur.null_rate
        hist_null = hist.null_rate
        if abs(cur_null - hist_null) > null_rate_threshold:
            issues.append(
                f"Column '{col}' null-rate shifted from {hist_null:.2%} to {cur_null:.2%}"
            )

        if cur.mean is not None and hist.mean is not None and hist.std not in (None, 0.0):
            z = abs(cur.mean - hist.mean) / hist.std
            if z > z_threshold:
                issues.append(
                    f"Column '{col}' mean drifted by {z:.1f} standard deviations "
                    f"({cur.mean:.4f} vs historical {hist.mean:.4f})"
                )

        if cur.min_value != hist.min_value and (cur.min_value is None) != (hist.min_value is None):
            issues.append(f"Column '{col}' min-value disappeared or appeared")
        if cur.max_value != hist.max_value and (cur.max_value is None) != (hist.max_value is None):
            issues.append(f"Column '{col}' max-value disappeared or appeared")

    return issues


def validate_batch_against_history(
    rows: list[dict[str, Any]],
    source: dict[str, Any],
    destination: dict[str, Any],
    schema: dict[str, str] | None = None,
    *,
    save_baseline: bool = False,
    fpr_target: float = 0.01,
) -> tuple[bool, list[str], dict[str, ColumnProfile]]:
    """Profile a batch, compare it to the stored baseline, and optionally save it.

    Returns (passed, anomalies, current_profile).
    """
    current = profile_batch(rows, schema)
    historical = load_historical_profile(source, destination)
    anomalies = detect_anomalies(current, historical, fpr_target=fpr_target)
    if save_baseline:
        save_profile(source, destination, current)
    return len(anomalies) == 0, anomalies, current
