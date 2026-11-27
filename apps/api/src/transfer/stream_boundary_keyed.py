"""Keyed-append boundary dedupe by key, not by the cursor's text (D335).

The inclusive run seed (``cursor >= watermark``) re-reads the tie group at the
stored watermark; a keyed append drops the peers the destination already
holds. The lookup asked the destination for ``cursor IN (:watermark_text)``
with the watermark as *text*. Whether that text matches is the destination's
own business:

* Oracle converts it with the session ``NLS_TIMESTAMP_FORMAT``
  (``YYYY-MM-DD HH24:MI:SS.FF9``); the ISO ``T`` of a stored watermark is
  ORA-01861, the lookup raised, the dedupe failed open and the watermark row
  was appended again on every run (QA PG -> Oracle r64-r75: key 30 on run 2,
  key 31 on run 3, with or without a schema change);
* SQL Server converts it to the column type: six fractional digits into a
  DATETIME, or a stored value the carrier rounded (DATETIME's 1/300 s, a
  DATETIME2(n) below the source precision), is no match - only the rows an
  earlier *incremental* run wrote (key 31, run 3) carry such a value;
* a TEXT cursor matches only in the exact spelling it was stored in.

A keyed append's identity is (cursor, key). The destination is now asked for
the boundary rows by *key* - the integer/text bind every engine converts the
same way - and the cursor is compared here: equal as the destination's own
fingerprint, or, for a temporal cursor, the same instant at the precision the
destination column can hold. Two source versions inside one carrier tick are
one value at the destination; nothing finer can be told apart there.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from connectors.writer_common import row_fingerprints

_LOOKUP_CHUNK = 500

#: dest_summary key: the keyed-append tie group this run re-read at the watermark.
BOUNDARY_TIE_KEY = "incremental_boundary_tie"


def carrier_tick(ddl: str, dialect: str) -> Decimal:
    """Smallest step (seconds) a temporal destination column stores; 0 = exact."""
    text = str(ddl or "").strip().upper()
    dial = str(dialect or "").lower()
    m = re.search(r"\(\s*(\d+)\s*\)", text)
    if text.startswith("SMALLDATETIME"):
        return Decimal(60)
    if text.startswith("DATETIME2") or text.startswith("DATETIMEOFFSET"):
        return Decimal(1).scaleb(-int(m.group(1))) if m else Decimal("1e-7")
    if text.startswith("DATETIME"):
        if dial in {"sqlserver", "mssql", "azure_sql"} and not m:
            return Decimal(1) / Decimal(300)
        return Decimal(1).scaleb(-int(m.group(1))) if m else Decimal(1)
    if text.startswith("DATE") and dial.startswith("oracle"):
        return Decimal(1)
    if text.startswith("TIMESTAMP") and m:
        return Decimal(1).scaleb(-int(m.group(1)))
    return Decimal(0)


def _instant(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    from connectors.cursor_bind import parse_cursor_datetime
    from services.value_serializer import present_cell_text

    text = present_cell_text(value)
    return parse_cursor_datetime(str(text)) if text not in (None, "") else None


def same_cursor(src: Any, dest: Any, *, ddl: str, dialect: str, dest_types: dict) -> bool:
    """True when ``dest`` is the stored form of the source cursor value ``src``."""
    fps = [
        fp
        for _k, fp in row_fingerprints(
            [(src,), (dest,)], ["c"], dest_db_type=dialect, dest_types={"c": ddl}
        )
    ]
    if len(fps) == 2 and fps[0] == fps[1]:
        return True
    a, b = _instant(src), _instant(dest)
    if a is None or b is None:
        return False
    if (a.tzinfo is None) != (b.tzinfo is None):
        # An NTZ carrier keeps the civil digits of an offset-bearing source.
        a, b = a.replace(tzinfo=None), b.replace(tzinfo=None)
    elif a.tzinfo is not None:
        a, b = a.astimezone(timezone.utc), b.astimezone(timezone.utc)
    if a == b:
        return True
    tick = carrier_tick(ddl, dialect)
    if not tick:
        return False
    delta = abs(Decimal(str((a - b).total_seconds())))
    return delta < tick


def _key_fp(values: tuple, cols: list[str], cfg: Any) -> str:
    return next(
        fp
        for _k, fp in row_fingerprints(
            [values], list(cols),
            dest_db_type=cfg.dest_bind_dialect, dest_types=cfg.digest_dest_types,
        )
    )


def source_key_texts(cfg: Any, headers: list[str], rows: list[list[Any]]) -> list[dict[str, str]] | None:
    """``{target key column: source text}`` per row, or None when a key is not on the page."""
    from services.keyset_pagination import header_column_index
    from services.value_serializer import present_cell_text

    to_source = {
        str(m.get("target") or ""): str(m.get("source") or "")
        for m in cfg.mappings or []
        if isinstance(m, dict) and m.get("target") and m.get("source")
    }
    pos = {}
    for col in cfg.pk_target_cols or []:
        idx = header_column_index(headers or [], to_source.get(col, col))
        if idx is None:
            return None
        pos[col] = idx
    return [
        {c: present_cell_text(row[i]) if i < len(row) else None for c, i in pos.items()}
        for row in rows
    ]


def read_dest_rows_by_key(cfg: Any, keys: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
    """Destination rows whose first key column is one of ``keys`` (exact key filter after)."""
    from services.target_sample import read_target_sample

    first = list(cfg.pk_target_cols)[0]
    wanted = list(dict.fromkeys(str(k.get(first)) for k in keys if k.get(first) is not None))
    out: list[dict[str, Any]] = []
    for start in range(0, len(wanted), _LOOKUP_CHUNK):
        chunk = wanted[start : start + _LOOKUP_CHUNK]
        rows = read_target_sample(
            cfg.dest_bind_dialect or cfg.dest_type,
            cfg.dest_cfg,
            schema=str(cfg.dest_cfg.get("schema") or "public"),
            table_name=cfg.dest_table,
            columns=list(columns),
            limit=max(10_000, 20 * len(chunk)),
            sort_key=first,
            key_values=chunk,
        )
        out.extend({str(k).lower(): v for k, v in r.items()} for r in rows or [])
    return out


def keyed_boundary_rows_landed(
    run: Any, headers: list[str], rows: list[list[Any]], *, cursor_dest_col: str
) -> set[int]:
    """Indices of ``rows`` (tie peers at the watermark) the destination already holds."""
    from .stream_boundary import remap_boundary_rows

    cfg, state = run.cfg, run.cur._boundary_fps_state
    key_cols = list(cfg.pk_target_cols)
    keys = source_key_texts(cfg, headers, rows)
    if keys is None:
        return set()
    seen: dict[str, list[Any]] = state.setdefault("keyed_dest", {})
    looked: set = state.setdefault("keyed_looked", set())
    todo = [k for k in keys if tuple(sorted(k.items())) not in looked]
    if todo:
        found = read_dest_rows_by_key(cfg, todo, list(dict.fromkeys([cursor_dest_col, *key_cols])))
        for k in todo:
            looked.add(tuple(sorted(k.items())))
        for r in found:
            kfp = _key_fp(tuple(r.get(c.lower()) for c in key_cols), key_cols, cfg)
            seen.setdefault(kfp, []).append(r.get(cursor_dest_col.lower()))
    mapped = remap_boundary_rows(cfg, headers, rows)
    targets = list(cfg.target_cols or [])
    try:
        cpos = targets.index(cursor_dest_col)
        kpos = [targets.index(c) for c in key_cols]
    except ValueError:
        return set()
    ddl = str((cfg.digest_dest_types or {}).get(cursor_dest_col) or "")
    summary = getattr(run.tally, "dest_summary", None)
    tie = summary.get(BOUNDARY_TIE_KEY) if isinstance(summary, dict) else None
    landed: set[int] = set()
    for i, row in enumerate(mapped):
        if row is None:
            continue
        kfp = _key_fp(tuple(row[p] for p in kpos), key_cols, cfg)
        copies = sum(
            1
            for d in seen.get(kfp, ())
            if same_cursor(row[cpos], d, ddl=ddl, dialect=cfg.dest_bind_dialect,
                           dest_types=cfg.digest_dest_types)
        )
        if copies:
            landed.add(i)
        if isinstance(tie, dict):
            # Copies the destination held before this run wrote (post-write check).
            tie.setdefault("pre_copies", {})[_key_token(keys[i])] = copies
    return landed


def _key_token(key: dict[str, Any]) -> str:
    import json

    return json.dumps({str(k): (None if v is None else str(v)) for k, v in key.items()}, sort_keys=True)


def record_boundary_tie(run: Any, headers: list[str], rows: list[list[Any]], *, cursor_dest_col: str, watermark: str) -> None:
    """Name every re-read tie peer for the post-write duplicate check."""
    summary = getattr(run.tally, "dest_summary", None)
    if not isinstance(summary, dict) or not run.cfg.pk_target_cols:
        return
    keys = source_key_texts(run.cfg, headers, rows)
    if not keys:
        return
    tie = summary.setdefault(
        BOUNDARY_TIE_KEY,
        {"cursor_column": cursor_dest_col, "watermark": str(watermark),
         "key_columns": list(run.cfg.pk_target_cols), "keys": []},
    )
    for k in keys:
        if k not in tie["keys"]:
            tie["keys"].append(k)


def boundary_duplicates(
    *, dest_summary: Any, endpoint: Any, mappings: list[dict] | None
) -> list[dict[str, Any]]:
    """Boundary keys the destination holds more than once at the watermark.

    A keyed append's source holds each key once at a cursor value, so two
    destination rows with that key at the same (carrier-precision) cursor are
    the watermark row appended twice - whatever the boundary dedupe decided.
    The destination is read by key, the same way on every engine.
    """
    from types import SimpleNamespace

    from .adapters import resolve_connector_config
    from .connector_capabilities import resolve_driver_type
    from .reconcile_step import _dest_types_from_mappings

    tie = (dest_summary or {}).get(BOUNDARY_TIE_KEY) if isinstance(dest_summary, dict) else None
    if not isinstance(tie, dict) or not tie.get("keys") or not tie.get("key_columns"):
        return []
    dialect = resolve_driver_type(getattr(endpoint, "format", "") or "")
    dest_cfg = resolve_connector_config(endpoint)
    types = _dest_types_from_mappings(list(mappings or []))
    cfg = SimpleNamespace(
        pk_target_cols=list(tie["key_columns"]),
        dest_bind_dialect=dialect,
        dest_type=dialect,
        dest_cfg=dest_cfg,
        dest_table=str(getattr(endpoint, "table", "") or dest_summary.get("table") or ""),
        digest_dest_types=types,
    )
    cursor_col = str(tie.get("cursor_column") or "")
    key_cols = list(tie["key_columns"])
    rows = read_dest_rows_by_key(cfg, list(tie["keys"]), list(dict.fromkeys([cursor_col, *key_cols])))
    ddl = str(types.get(cursor_col) or "")
    counts: dict[str, int] = {}
    wanted = {_key_fp(tuple(k.get(c) for c in key_cols), key_cols, cfg): k for k in tie["keys"]}
    for r in rows:
        kfp = _key_fp(tuple(r.get(c.lower()) for c in key_cols), key_cols, cfg)
        if kfp in wanted and same_cursor(
            tie.get("watermark"), r.get(cursor_col.lower()), ddl=ddl, dialect=dialect, dest_types=types
        ):
            counts[kfp] = counts.get(kfp, 0) + 1
    pre = tie.get("pre_copies") or {}
    return [
        {
            "key": wanted[k],
            "copies": n,
            "copies_before_run": pre.get(_key_token(wanted[k])),
            "cursor": tie.get("watermark"),
        }
        for k, n in counts.items()
        if n > 1
    ]


def apply_boundary_duplicate_gate(recon: Any, **kwargs: Any) -> Any:
    """Fail Gate-8 - loudly - when the watermark row was appended again (D335)."""
    if not isinstance(recon, dict):
        return recon
    try:
        dups = boundary_duplicates(**kwargs)
    except Exception as exc:  # noqa: BLE001 - an unmeasured check is said, not passed
        import logging

        logging.getLogger(__name__).warning("boundary duplicate check did not run: %s", exc)
        tie = (kwargs.get("dest_summary") or {}).get(BOUNDARY_TIE_KEY)
        if tie and recon.get("passed"):
            note = (
                "The watermark row's duplicate check could not read the destination "
                "by key, so exactly-once at the boundary is not proven."
            )
            recon["migration_proven"] = False
            recon["message"] = f"{str(recon.get('message') or '').strip()} {note}".strip()
        return recon
    recon["boundary_duplicates"] = dups
    if not dups:
        return _no_green_count_sentence(recon)
    named = "; ".join(
        ", ".join(f"{c}={v}" for c, v in (d.get("key") or {}).items())
        + f" ({d.get('copies')} copies at {d.get('cursor')}, source holds 1"
        + (
            f"; {d.get('copies_before_run')} already before this run"
            if (d.get("copies_before_run") or 0) > 1
            else ""
        )
        + ")"
        for d in dups[:8]
    )
    earlier = all((d.get("copies_before_run") or 0) >= int(d.get("copies") or 0) for d in dups)
    sentence = (
        f"Append delta NOT verified: {len(dups)} boundary key(s) at the stored "
        "watermark are at the destination more than once - the watermark row was "
        + ("appended again by an earlier run" if earlier else "appended again")
        + f" ({named}). The destination is not exactly-once."
    )
    recon["passed"] = False
    recon["migration_proven"] = False
    recon["verification_verdict"] = "failed"
    recon["error"] = sentence
    recon["message"] = f"{sentence} {str(recon.get('message') or '').strip()}".strip()
    return _no_green_count_sentence(recon)


def _no_green_count_sentence(recon: dict[str, Any]) -> dict[str, Any]:
    """A failed append never also reads "Append delta verified" (one verdict)."""
    message = str(recon.get("message") or "")
    if recon.get("passed") is False and "Append delta verified" in message:
        recon["message"] = message.replace(
            "Append delta verified", "Append row count matched (not a verdict)"
        )
    return recon
