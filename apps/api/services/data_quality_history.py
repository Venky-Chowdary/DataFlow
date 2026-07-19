"""History-aware data quality for recurring transfers (last-N load intelligence).

Each successful (or sampled) run appends a column profile + quarantine summary to a
ring buffer for the source→destination route. The next Validate/Run compares the
current load to the rolling history with robust statistics (median + MAD), not a
single overwritten baseline.

Algorithms are O(columns × N) and intentionally allocation-light so they stay
safe on sample Validate and full-batch write paths.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.value_serializer import cell_to_string

# Keep enough history to answer "what changed across the last ~10 loads".
DEFAULT_HISTORY_LIMIT = max(5, min(50, int(os.getenv("DATAFLOW_QUALITY_HISTORY_LIMIT", "20"))))


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


@dataclass
class LoadRunRecord:
    """One persisted load observation for a route."""

    captured_at: str
    job_id: str | None = None
    row_count: int = 0
    columns: dict[str, Any] = field(default_factory=dict)
    quarantine_histogram: dict[str, int] = field(default_factory=dict)
    rejected_rows: int = 0


def _to_sortable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    return cell_to_string(value)


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
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip()
    text = text.replace("Z", "+0000")
    if text.endswith(("+00:00", "+0000")):
        text = text[:-6].replace("T", " ")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def profile_column(values: list[Any], column: str, dtype: str = "string") -> ColumnProfile:
    """Compute a descriptive statistical profile for a single column."""
    profile = ColumnProfile(column=column, dtype=dtype, count=len(values))
    non_null = [
        v
        for v in values
        if v is not None and cell_to_string(v).strip().lower() not in {"", "null", "none"}
    ]
    profile.null_count = profile.count - len(non_null)

    as_text = [cell_to_string(v) for v in non_null]
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

    freq: dict[str, int] = {}
    for v in as_text:
        freq[v] = freq.get(v, 0) + 1
    profile.top_values = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:5]

    return profile


def profile_batch(
    rows: list[dict[str, Any]],
    schema: dict[str, str] | None = None,
) -> dict[str, ColumnProfile]:
    schema = schema or {}
    if not rows:
        return {}
    columns = list(rows[0].keys())
    result: dict[str, ColumnProfile] = {}
    for col in columns:
        values = [row.get(col) for row in rows]
        result[col] = profile_column(values, col, schema.get(col, "string"))
    return result


def quarantine_histogram(rejected_details: list[dict[str, Any]] | None) -> dict[str, int]:
    """Bucket quarantine findings as ``column|reason_prefix`` for cross-run diffs."""
    hist: Counter[str] = Counter()
    for d in rejected_details or []:
        col = str(d.get("column") or "*")
        reason = str(d.get("reason") or "unknown").strip().lower()
        # Collapse noisy suffixes (row ids, unique values) to a stable prefix.
        reason = reason.split(":")[0].split("'")[0].strip()
        key = f"{col}|{reason[:80]}"
        hist[key] += 1
    return dict(hist.most_common(50))


def _profile_key(source: dict[str, Any], destination: dict[str, Any]) -> str:
    parts = [
        source.get("kind", ""),
        source.get("format", ""),
        source.get("table") or source.get("collection") or source.get("schema", ""),
        destination.get("kind", ""),
        destination.get("format", ""),
        destination.get("table") or destination.get("collection") or destination.get("schema", ""),
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _profile_collection():
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if mongo and getattr(mongo, "client", None) and type(mongo).__name__ != "MemoryMongoDBService":
            return mongo.get_database().get("quality_profiles")
    except Exception:
        pass
    return None


def _profile_path(key: str) -> Path:
    base = data_dir() / "quality_profiles"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{key}.json"


def _serialize_profile(profile: dict[str, ColumnProfile]) -> dict[str, Any]:
    return {name: asdict(col) for name, col in profile.items()}


def _deserialize_profile(data: dict[str, Any]) -> dict[str, ColumnProfile]:
    out: dict[str, ColumnProfile] = {}
    for name, col in (data or {}).items():
        if isinstance(col, ColumnProfile):
            out[name] = col
            continue
        if not isinstance(col, dict):
            continue
        # top_values may arrive as list-of-lists from JSON
        tv = col.get("top_values") or []
        fixed = []
        for item in tv:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                fixed.append((str(item[0]), int(item[1])))
        payload = {**col, "top_values": fixed, "column": col.get("column", name)}
        out[name] = ColumnProfile(**{
            k: payload[k]
            for k in ColumnProfile.__dataclass_fields__
            if k in payload
        })
    return out


def _normalize_store_doc(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Accept legacy single-baseline docs and ring-buffer ``runs`` docs."""
    if not raw:
        return {"runs": []}
    runs = list(raw.get("runs") or [])
    if not runs and raw.get("columns"):
        # Legacy: one overwritten baseline → synthesize a single run.
        runs = [{
            "captured_at": raw.get("captured_at") or datetime.now(timezone.utc).isoformat(),
            "job_id": raw.get("job_id"),
            "row_count": raw.get("row_count") or 0,
            "columns": raw.get("columns") or {},
            "quarantine_histogram": raw.get("quarantine_histogram") or {},
            "rejected_rows": raw.get("rejected_rows") or 0,
        }]
    return {"runs": runs}


def _load_store(source: dict[str, Any], destination: dict[str, Any]) -> dict[str, Any]:
    key = _profile_key(source, destination)
    coll = _profile_collection()
    if coll is not None:
        try:
            doc = coll.find_one({"_id": key})
            if doc:
                return _normalize_store_doc(doc)
        except Exception:
            pass
    path = _profile_path(key)
    if not path.exists():
        return {"runs": []}
    try:
        return _normalize_store_doc(json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return {"runs": []}


def _save_store(source: dict[str, Any], destination: dict[str, Any], store: dict[str, Any]) -> None:
    key = _profile_key(source, destination)
    runs = list(store.get("runs") or [])[-DEFAULT_HISTORY_LIMIT:]
    payload = {
        "_id": key,
        "captured_at": runs[-1]["captured_at"] if runs else datetime.now(timezone.utc).isoformat(),
        "runs": runs,
        # Keep latest columns at top-level for older readers.
        "columns": (runs[-1].get("columns") if runs else {}) or {},
        "row_count": (runs[-1].get("row_count") if runs else 0) or 0,
        "quarantine_histogram": (runs[-1].get("quarantine_histogram") if runs else {}) or {},
        "rejected_rows": (runs[-1].get("rejected_rows") if runs else 0) or 0,
        "job_id": (runs[-1].get("job_id") if runs else None),
    }
    coll = _profile_collection()
    if coll is not None:
        try:
            coll.replace_one({"_id": key}, payload, upsert=True)
            return
        except Exception:
            pass
    write_json_atomic(_profile_path(key), {k: v for k, v in payload.items() if k != "_id"})


def load_run_history(
    source: dict[str, Any],
    destination: dict[str, Any],
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Return oldest→newest run dicts (capped)."""
    runs = _load_store(source, destination).get("runs") or []
    lim = limit or DEFAULT_HISTORY_LIMIT
    return list(runs[-lim:])


def load_historical_profile(
    source: dict[str, Any],
    destination: dict[str, Any],
) -> dict[str, ColumnProfile] | None:
    """Most recent saved profile (backward-compatible API)."""
    runs = load_run_history(source, destination, limit=1)
    if not runs:
        return None
    return _deserialize_profile(runs[-1].get("columns") or {})


def save_profile(
    source: dict[str, Any],
    destination: dict[str, Any],
    profile: dict[str, ColumnProfile],
    *,
    job_id: str | None = None,
    rejected_details: list[dict[str, Any]] | None = None,
    rejected_rows: int = 0,
    row_count: int | None = None,
) -> None:
    """Append a profile to the route ring buffer (does not overwrite history)."""
    store = _load_store(source, destination)
    runs = list(store.get("runs") or [])
    columns = _serialize_profile(profile)
    rc = row_count if row_count is not None else max((c.count for c in profile.values()), default=0)
    runs.append({
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "job_id": job_id,
        "row_count": rc,
        "columns": columns,
        "quarantine_histogram": quarantine_histogram(rejected_details),
        "rejected_rows": int(rejected_rows or 0),
    })
    _save_store(source, destination, {"runs": runs})


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def _mad(values: list[float], med: float | None = None) -> float | None:
    """Median absolute deviation (robust scale)."""
    if not values:
        return None
    m = med if med is not None else _median(values)
    if m is None:
        return None
    return _median([abs(v - m) for v in values])


def _robust_z(value: float, series: list[float]) -> float | None:
    """Modified z-score using MAD; None when series too short / zero scale."""
    if len(series) < 2:
        return None
    med = _median(series)
    mad = _mad(series, med)
    if med is None or mad is None:
        return None
    if mad == 0.0:
        # All identical historically — any material delta is noteworthy.
        return abs(value - med) / max(abs(med) * 0.01, 1e-9) if value != med else 0.0
    # 0.6745 makes MAD comparable to std for normal data.
    return 0.6745 * (value - med) / mad


def detect_anomalies(
    current: dict[str, ColumnProfile],
    historical: dict[str, ColumnProfile] | None,
    *,
    fpr_target: float = 0.01,
    prior_runs: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Backward-compatible anomaly messages (single baseline or rolling)."""
    report = build_load_history_report(
        current_profile=current,
        prior_runs=prior_runs,
        fallback_baseline=historical,
        fpr_target=fpr_target,
    )
    return list(report.get("anomalies") or [])


def _histogram_delta(
    current: dict[str, int],
    prior_histograms: list[dict[str, int]],
) -> list[dict[str, Any]]:
    """Keys that appear now but were absent (or rare) in prior loads."""
    if not current:
        return []
    seen: Counter[str] = Counter()
    for h in prior_histograms:
        seen.update(h or {})
    novel: list[dict[str, Any]] = []
    for key, count in sorted(current.items(), key=lambda kv: -kv[1]):
        prev = seen.get(key, 0)
        if prev == 0:
            col, _, reason = key.partition("|")
            novel.append({
                "column": col,
                "reason": reason,
                "count": count,
                "prior_count": 0,
                "kind": "new_quarantine_pattern",
            })
        elif count >= max(3, prev * 3):
            col, _, reason = key.partition("|")
            novel.append({
                "column": col,
                "reason": reason,
                "count": count,
                "prior_count": prev,
                "kind": "quarantine_spike",
            })
    return novel[:20]


def build_load_history_report(
    *,
    current_profile: dict[str, ColumnProfile],
    prior_runs: list[dict[str, Any]] | None = None,
    fallback_baseline: dict[str, ColumnProfile] | None = None,
    current_quarantine: dict[str, int] | None = None,
    current_row_count: int | None = None,
    fpr_target: float = 0.01,
    compare_last_k: int = 10,
) -> dict[str, Any]:
    """Structured report for Validate/Run: drift vs last K loads + novel bad patterns."""
    _ = fpr_target  # reserved for calibrated thresholds
    runs = list(prior_runs or [])
    # Exclude a trailing run that is identical to "current" if caller included it.
    history = runs[-compare_last_k:] if runs else []
    anomalies: list[str] = []
    column_findings: list[dict[str, Any]] = []

    # Build per-column series from history.
    col_series: dict[str, dict[str, list[float]]] = {}
    for run in history:
        cols = _deserialize_profile(run.get("columns") or {})
        for name, prof in cols.items():
            bucket = col_series.setdefault(name, {"null_rate": [], "mean": [], "count": [], "distinct_rate": []})
            bucket["null_rate"].append(prof.null_rate)
            bucket["count"].append(float(prof.count))
            bucket["distinct_rate"].append(prof.distinct_rate)
            if prof.mean is not None:
                bucket["mean"].append(float(prof.mean))

    # If no ring history, fall back to single baseline dict.
    if not history and fallback_baseline:
        for name, prof in fallback_baseline.items():
            bucket = col_series.setdefault(name, {"null_rate": [], "mean": [], "count": [], "distinct_rate": []})
            bucket["null_rate"].append(prof.null_rate)
            bucket["count"].append(float(prof.count))
            bucket["distinct_rate"].append(prof.distinct_rate)
            if prof.mean is not None:
                bucket["mean"].append(float(prof.mean))

    z_cut = 3.5
    null_abs_cut = 0.10

    for col, cur in current_profile.items():
        series = col_series.get(col)
        if not series:
            continue

        finding: dict[str, Any] = {"column": col, "signals": []}

        if series["null_rate"]:
            hist_null = _median(series["null_rate"]) or 0.0
            if abs(cur.null_rate - hist_null) > null_abs_cut:
                msg = (
                    f"Column '{col}' null-rate shifted from {hist_null:.2%} "
                    f"(median of prior loads) to {cur.null_rate:.2%}"
                )
                anomalies.append(msg)
                finding["signals"].append({"kind": "null_rate", "message": msg})

        if series["count"] and cur.count > 0:
            med_count = _median(series["count"])
            if med_count and med_count > 0:
                ratio = cur.count / med_count
                if ratio < 0.5:
                    msg = (
                        f"Column '{col}' row count dropped by {(1 - ratio) * 100:.1f}% "
                        f"({cur.count} vs prior median {int(med_count)})"
                    )
                    anomalies.append(msg)
                    finding["signals"].append({"kind": "count_drop", "message": msg})

        if cur.mean is not None and series["mean"]:
            z = _robust_z(float(cur.mean), series["mean"])
            # Single prior observation: fall back to classic z vs that mean's std
            # when available on the fallback baseline profile.
            if z is None and len(series["mean"]) == 1 and fallback_baseline:
                hist_prof = fallback_baseline.get(col)
                if (
                    hist_prof
                    and hist_prof.mean is not None
                    and hist_prof.std not in (None, 0.0)
                ):
                    z = abs(float(cur.mean) - float(hist_prof.mean)) / float(hist_prof.std)
            if z is not None and abs(z) > z_cut:
                med = _median(series["mean"])
                msg = (
                    f"Column '{col}' mean drifted (robust z={z:.1f}) "
                    f"({cur.mean:.4f} vs prior median {med:.4f})"
                    if len(series["mean"]) >= 2
                    else (
                        f"Column '{col}' mean drifted by {z:.1f} standard deviations "
                        f"({cur.mean:.4f} vs historical {series['mean'][0]:.4f})"
                    )
                )
                anomalies.append(msg)
                finding["signals"].append({
                    "kind": "mean_drift",
                    "message": msg,
                    "robust_z": round(float(z), 2),
                })

        if finding["signals"]:
            column_findings.append(finding)

    prior_hists = [r.get("quarantine_histogram") or {} for r in history]
    novel_q = _histogram_delta(current_quarantine or {}, prior_hists)
    for item in novel_q:
        msg = (
            f"New quarantine pattern on '{item['column']}': {item['reason'] or 'error'} "
            f"(count={item['count']}; prior={item['prior_count']})"
        )
        anomalies.append(msg)

    row_counts = [float(r.get("row_count") or 0) for r in history if (r.get("row_count") or 0) > 0]
    cur_rows = current_row_count
    if cur_rows is None and current_profile:
        cur_rows = max((c.count for c in current_profile.values()), default=0)
    volume_note = None
    if cur_rows is not None and row_counts:
        med_rows = _median(row_counts)
        if med_rows and med_rows > 0 and cur_rows / med_rows < 0.5:
            volume_note = (
                f"Load volume {cur_rows:,} is below 50% of prior median {int(med_rows):,}"
            )
            anomalies.append(volume_note)

    return {
        "prior_load_count": len(history),
        "compare_last_k": compare_last_k,
        "anomalies": anomalies,
        "column_findings": column_findings,
        "novel_quarantine_patterns": novel_q,
        "volume_note": volume_note,
        "prior_runs_summary": [
            {
                "captured_at": r.get("captured_at"),
                "job_id": r.get("job_id"),
                "row_count": r.get("row_count") or 0,
                "rejected_rows": r.get("rejected_rows") or 0,
                "quarantine_keys": len(r.get("quarantine_histogram") or {}),
            }
            for r in history[-compare_last_k:]
        ],
        "passed": len(anomalies) == 0,
    }


def validate_batch_against_history(
    rows: list[dict[str, Any]],
    source: dict[str, Any],
    destination: dict[str, Any],
    schema: dict[str, str] | None = None,
    *,
    save_baseline: bool = False,
    fpr_target: float = 0.01,
    job_id: str | None = None,
    rejected_details: list[dict[str, Any]] | None = None,
    rejected_rows: int = 0,
) -> tuple[bool, list[str], dict[str, ColumnProfile]]:
    """Profile a batch, compare to last-N history, optionally append baseline."""
    current = profile_batch(rows, schema)
    history = load_run_history(source, destination)
    report = build_load_history_report(
        current_profile=current,
        prior_runs=history,
        current_quarantine=quarantine_histogram(rejected_details),
        current_row_count=len(rows),
        fpr_target=fpr_target,
    )
    anomalies = list(report.get("anomalies") or [])
    if save_baseline:
        save_profile(
            source,
            destination,
            current,
            job_id=job_id,
            rejected_details=rejected_details,
            rejected_rows=rejected_rows,
            row_count=len(rows),
        )
    return len(anomalies) == 0, anomalies, current


def compare_route_to_history(
    rows: list[dict[str, Any]],
    source: dict[str, Any],
    destination: dict[str, Any],
    schema: dict[str, str] | None = None,
    *,
    rejected_details: list[dict[str, Any]] | None = None,
    compare_last_k: int = 10,
    current_row_count: int | None = None,
) -> dict[str, Any]:
    """Full structured report for API/UI (does not mutate history).

    ``current_row_count`` lets streaming paths compare volume using the full
    table size while profiling only a bounded sample for column stats.
    """
    current = profile_batch(rows, schema)
    history = load_run_history(source, destination)
    return build_load_history_report(
        current_profile=current,
        prior_runs=history,
        current_quarantine=quarantine_histogram(rejected_details),
        current_row_count=int(current_row_count) if current_row_count is not None else len(rows),
        compare_last_k=compare_last_k,
    )
