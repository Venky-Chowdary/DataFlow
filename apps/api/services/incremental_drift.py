"""D188 — an incremental run verifies its cursor window; the rest is drift.

An incremental run selects source rows by their cursor: ``(seed, max]`` (or
``[seed, max]`` when the run re-reads the seed's tie group). Gate-8 proves that
exactly those rows landed. A source row *outside* the window that disagrees
with the destination — edited without moving its cursor, back-filled with an
old cursor value, hard-deleted, or a destination row changed by hand — is not
something this run read or wrote. Failing the run on it turns every later run
of a schedule red after one manual fix, while the data each run wrote is right.

This module owns the one window predicate every Gate-8 population uses and the
out-of-window drift measurement. Drift is reported on the run (count, kinds,
sample keys), never hidden: a loss *inside* the window still fails.

``DATAFLOW_INCREMENTAL_DRIFT_FAILS=1`` (off by default) restores fail-closed on
out-of-window drift; while it is off every drift report records the audit
event ``gate8.would_fail_out_of_window_drift``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DRIFT_FLAG = "DATAFLOW_INCREMENTAL_DRIFT_FAILS"
DRIFT_KEY = "out_of_window_drift"
_SAMPLE = 5

IN_WINDOW = "in"
BEFORE_WINDOW = "before"
AFTER_WINDOW = "after"


@dataclass(frozen=True)
class RunWindow:
    """The cursor window one incremental run selected, as the stream stamped it."""

    cursor: str
    pk: str
    seed: str | None
    max: str | None
    seed_inclusive: bool

    @classmethod
    def from_summary(cls, dest_summary: dict[str, Any] | None) -> RunWindow | None:
        summary = dest_summary or {}
        cursor = str(summary.get("incremental_window_cursor") or "").strip()
        if not cursor:
            return None
        seed = summary.get("incremental_window_seed")
        top = summary.get("incremental_window_max")
        return cls(
            cursor=cursor,
            pk=str(summary.get("incremental_window_pk") or "").strip(),
            seed=None if seed in (None, "") else str(seed),
            max=None if top in (None, "") else str(top),
            seed_inclusive=bool(summary.get("incremental_window_seed_inclusive")),
        )

    def renamed(self, to_target: dict[str, str]) -> RunWindow:
        """The same window addressed by destination column names."""
        return RunWindow(
            cursor=to_target.get(self.cursor, self.cursor),
            pk=to_target.get(self.pk, self.pk) if self.pk else "",
            seed=self.seed,
            max=self.max,
            seed_inclusive=self.seed_inclusive,
        )

    def position(self, row: Any) -> str:
        """``in`` / ``before`` / ``after`` the window for one row.

        A row whose cursor is unreadable (NULL) is never selected by a seeded
        cursor read, so it sits outside the window ("before": no later run
        picks it up either). A first run with no seed read the whole table.
        """
        from services.sync_cursor import (
            compare_cursor_values,
            row_after_watermark,
            row_in_run_window,
        )

        if not isinstance(row, dict):
            return BEFORE_WINDOW
        if self.seed is not None:
            after_seed = (
                row_in_run_window(row, self.cursor, self.seed)
                if self.seed_inclusive
                else row_after_watermark(row, self.cursor, self.seed, primary_key=self.pk)
            )
            if after_seed is not True:
                return BEFORE_WINDOW
        if self.max is not None:
            raw = row.get(self.cursor)
            if raw is None:
                # Only a first run (no seed) reads the whole table, NULL
                # cursors included; a seeded run's ``cursor > seed`` never does
                # (it returned above).
                return IN_WINDOW
            candidate = str(raw)
            if self.pk and self.pk != self.cursor:
                from services.keyset_pagination import encode_keyset_bookmark

                candidate = encode_keyset_bookmark([candidate, str(row.get(self.pk) or "")])
            if compare_cursor_values(candidate, self.max) > 0:
                return AFTER_WINDOW
        return IN_WINDOW


def stamp_run_window(
    dest_summary: dict[str, Any],
    *,
    cursor: str,
    pk: str,
    seed: Any,
    seed_inclusive: bool,
    high: Any,
) -> None:
    """Publish the window a run selected; every write path calls this one."""
    if not cursor:
        return
    dest_summary["incremental_window_cursor"] = cursor
    dest_summary["incremental_window_pk"] = pk or ""
    if seed not in (None, ""):
        dest_summary["incremental_window_seed"] = str(seed)
        dest_summary["incremental_window_seed_inclusive"] = bool(seed_inclusive)
    if high not in (None, ""):
        dest_summary["incremental_window_max"] = str(high)


def drift_fails_run() -> bool:
    from services.brand_env import getenv_brand_bool

    return getenv_brand_bool(DRIFT_FLAG, False)


def measure_drift(
    *,
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    columns: list[str],
    key_cols: list[str],
    dest_db_type: str,
    dest_types: dict[str, str] | None = None,
    skip_keys: set[str] | None = None,
) -> dict[str, Any]:
    """Compare out-of-window source rows with the destination, by key.

    ``source_rows`` are the source rows the run did not select; ``skip_keys``
    are keys the window owns (or that are newer than it) and must not be judged
    here. Returns ``{}`` when the populations agree.
    """
    from services.reconciliation import fingerprint_for_reconcile
    from services.verification_ladder import _index_by_pk

    if not key_cols:
        return {}
    skip = skip_keys or set()
    src = {k: v for k, v in _index_by_pk(source_rows, key_cols).items() if k not in skip}
    tgt = {k: v for k, v in _index_by_pk(target_rows, key_cols).items() if k not in skip}
    types = dest_types or {}
    cols = [c for c in columns if c]

    def _fp(row: dict[str, Any], col: str) -> str:
        return fingerprint_for_reconcile(
            row.get(col), ddl_type=str(types.get(col) or ""), engine=dest_db_type
        )

    changed: list[dict[str, Any]] = []
    changed_n = 0
    missing_n = 0
    missing: list[str] = []
    for key, srow in src.items():
        trow = tgt.get(key)
        if trow is None:
            missing_n += 1
            if len(missing) < _SAMPLE:
                missing.append(key)
            continue
        diff = next((c for c in cols if _fp(srow, c) != _fp(trow, c)), None)
        if diff is None:
            continue
        changed_n += 1
        if len(changed) < _SAMPLE:
            changed.append(
                {
                    "key": key,
                    "column": diff,
                    "source_value": srow.get(diff),
                    "target_value": trow.get(diff),
                }
            )
    dest_only = [k for k in tgt if k not in src]
    total = changed_n + missing_n + len(dest_only)
    if not total:
        return {}
    sample_keys = [c["key"] for c in changed] + missing + dest_only[:_SAMPLE]
    return {
        "rows": total,
        "changed": changed_n,
        "missing_in_destination": missing_n,
        "destination_only": len(dest_only),
        "sample_keys": sample_keys[:_SAMPLE],
        "changed_sample": [
            {**c, "source_value": _show(c["source_value"]), "target_value": _show(c["target_value"])}
            for c in changed
        ],
        "key_columns": list(key_cols),
    }


def _show(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    text = str(value)
    return text if len(text) <= 120 else text[:117] + "..."


def drift_sentence(drift: dict[str, Any]) -> str:
    """One operator sentence; empty when there is no drift."""
    n = int(drift.get("rows") or 0)
    if n <= 0:
        return ""
    parts = []
    if drift.get("changed"):
        parts.append(f"{int(drift['changed']):,} differ")
    if drift.get("missing_in_destination"):
        parts.append(f"{int(drift['missing_in_destination']):,} missing from the destination")
    if drift.get("destination_only"):
        parts.append(f"{int(drift['destination_only']):,} only in the destination")
    keys = ", ".join(str(k) for k in drift.get("sample_keys") or [])
    return (
        f"Out-of-window drift: {n:,} row(s) outside this run's cursor window "
        f"disagree between source and destination ({'; '.join(parts)}"
        + (f"; e.g. key {keys}" if keys else "")
        + "). The cursor did not select them, so this run neither wrote nor "
        "judged them — a change made without moving the cursor column is picked "
        "up by a full refresh or by bumping the cursor."
    )


def attach_drift(
    report: dict[str, Any], drift: dict[str, Any], *, resource: str = ""
) -> dict[str, Any]:
    """Record drift on a Gate-8 report; fail it only under the strict flag."""
    if not drift:
        return report
    out = dict(report)
    out[DRIFT_KEY] = drift
    sentence = drift_sentence(drift)
    base = str(out.get("message") or "").rstrip()
    if sentence and sentence not in base:
        out["message"] = f"{base} {sentence}".strip() if base else sentence
    warnings = list(out.get("warnings") or [])
    if sentence:
        warnings.append(sentence)
    out["warnings"] = warnings
    if drift_fails_run():
        out["passed"] = False
        out["out_of_window_drift_enforced"] = True
        return out
    from services.would_apply_audit import audit_would_apply

    audit_would_apply(
        "gate8.would_fail_out_of_window_drift",
        resource=resource or "transfer",
        dedupe_key=(resource, int(drift.get("rows") or 0)),
        details={
            "rows": drift.get("rows"),
            "sample_keys": drift.get("sample_keys"),
        },
        flag=DRIFT_FLAG,
    )
    return out
