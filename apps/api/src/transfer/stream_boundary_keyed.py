"""Append boundary dedupe: typed lookups, carrier-precision compare, fail closed (D335).

The inclusive run seed (``cursor >= watermark``) re-reads the tie group at the
stored watermark; an append must drop the peers the destination already holds
and append the rest. The old lookup asked the destination for
``cursor IN (:watermark_text)``, so whether a peer was found depended on the
destination's text conversion: Oracle's ``NLS_TIMESTAMP_FORMAT`` rejects the
ISO ``T`` (ORA-01861), SQL Server's DATETIME rounds to 1/300 s, a TEXT cursor
matches one spelling. A miss or an error failed *open*: the row was appended
again and the count-only verdict said "Append delta verified".

Now:

* the destination is read through SQLAlchemy with *typed* binds
  (``cursor_bind.sa_cursor_bind``, the readers' own watermark bind): a keyed
  append filters on **every** key column (OR of ANDs, chunked) inside a cursor
  window that is a superset of the tie group; a keyless append reads that
  window. A text cursor is windowed by its minute prefix in both ISO spellings;
* the cursor is compared here as an exact instant, after quantizing the source
  value to what the destination column stores (``carrier_instant``: PG/MySQL/
  SQL Server round, DATETIME to 1/300 s, Oracle DATE rounded or cut);
* keyed: a peer is landed when a stored row has its key and cursor; keyless:
  when a stored row equals it on every column the table holds (multiset);
* anything that cannot be measured - an error, a hit on the row cap, a cursor
  that cannot be bound - fails **closed**: the peers are not appended, the
  summary says why, and Gate-8 fails the run in strict mode and never calls it
  verified otherwise. The post-write check reads the same way.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any

from connectors.writer_common import row_fingerprints

#: dest_summary key: the tie group this run re-read at the watermark.
BOUNDARY_TIE_KEY = "incremental_boundary_tie"
_KEY_CHUNK = 200
_ROW_CAP = 50_000
_SQL_FAMILIES = frozenset({"postgresql", "mysql", "sqlserver", "oracle", "sqlite"})
_EPOCH = datetime(1970, 1, 1)
_TEXT_TS = re.compile(
    r"^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?\s*(Z|z|[+-]\d{2}(?::?\d{2})?)?$"
)
_OFFSET = re.compile(r"\s*(Z|z|[+-]\d{2}(?::?\d{2})?)$")


class BoundaryUnmeasured(RuntimeError):
    """The destination could not tell whether the tie peers are already there."""


# --- cursor values as exact instants ------------------------------------------


def _instant(value: Any) -> tuple[Decimal, bool] | None:
    """``(seconds since 1970 as an exact Decimal - UTC when zoned, zone-bearing)``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        aware = value.tzinfo is not None
        base = value.astimezone(timezone.utc).replace(tzinfo=None) if aware else value
        whole = base.replace(microsecond=0)
        seconds = Decimal((whole - _EPOCH) // timedelta(seconds=1))
        return seconds + Decimal(value.microsecond).scaleb(-6), aware
    from services.value_serializer import present_cell_text

    m = _TEXT_TS.match(str(present_cell_text(value) or "").strip())
    if not m:
        return None
    day, hh, mi, ss, frac, off = m.groups()
    base = datetime.fromisoformat(f"{day}T{hh or '00'}:{mi or '00'}:{ss or '00'}")
    seconds = Decimal((base - _EPOCH) // timedelta(seconds=1))
    if frac:
        seconds += Decimal("0." + frac)
    if off and off not in ("Z", "z"):
        sign = -1 if off[0] == "-" else 1
        digits = off[1:].replace(":", "")
        seconds -= sign * (int(digits[:2]) * 3600 + int(digits[2:4] or 0) * 60)
    return seconds, bool(off)


def _civil(value: Any) -> Any:
    """The wall-clock digits of a zoned value (an NTZ carrier keeps those)."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    from services.value_serializer import present_cell_text

    return _OFFSET.sub("", str(present_cell_text(value) or ""))


def _quantize(seconds: Decimal, ddl: str, dialect: str) -> list[Decimal]:
    """The value(s) the destination column stores for ``seconds``."""
    from connectors.sql_temporal import sql_base_type
    from services.carrier_instant import carrier_instant_digits

    base = sql_base_type(ddl or "") if ddl else ""
    fam = str(dialect or "").lower()
    if base == "SMALLDATETIME":
        return [(seconds / 60).quantize(Decimal(1), ROUND_HALF_UP) * 60]
    if base == "DATETIME" and fam in {"sqlserver", "mssql", "azure_sql", "azuresql"}:
        # 1/300 s ticks, read back as the engine's three-digit rounding.
        ticks = (seconds * 300).quantize(Decimal(1), ROUND_HALF_UP)
        return [(ticks / 300).quantize(Decimal("0.001"), ROUND_HALF_UP)]
    if base == "DATE" and fam.startswith("oracle"):
        # Oracle DATE holds whole seconds; a bound datetime is rounded or cut.
        return [
            seconds.quantize(Decimal(1), ROUND_HALF_UP),
            seconds.quantize(Decimal(1), ROUND_FLOOR),
        ]
    digits = carrier_instant_digits(ddl, engine=fam) if ddl else None
    if digits is None:
        return [seconds]
    return [seconds.quantize(Decimal(1).scaleb(-int(digits)), ROUND_HALF_UP)]


def same_cursor(src: Any, dest: Any, *, ddl: str, dialect: str) -> bool:
    """True when ``dest`` is the stored form of the source cursor value ``src``.

    Instants compare exactly after the carrier's own rounding; any other value
    (an integer or a text cursor) compares on the Gate-8 fingerprint.
    """
    a, b = _instant(src), _instant(dest)
    if a is None or b is None:
        fps = [
            fp
            for _k, fp in row_fingerprints(
                [(src,), (dest,)], ["c"], dest_db_type=dialect, dest_types={"c": ddl}
            )
        ]
        return len(fps) == 2 and fps[0] == fps[1]
    if a[1] != b[1]:
        a = _instant(_civil(src)) if a[1] else a
        b = _instant(_civil(dest)) if b[1] else b
        if a is None or b is None:
            return False
    return b[0] == a[0] or b[0] in _quantize(a[0], ddl, dialect)


# --- destination reads ---------------------------------------------------------


def _fmt(seconds: Decimal) -> str:
    whole = int(seconds.to_integral_value(ROUND_FLOOR))
    micro = int(((seconds - whole) * 1_000_000).to_integral_value(ROUND_FLOOR))
    return (_EPOCH + timedelta(seconds=whole, microseconds=micro)).isoformat(sep=" ")


def _cursor_window(col: Any, watermark: str, dialect: str) -> Any:
    """A predicate true for every stored form of the watermark (a superset)."""
    import sqlalchemy as sa

    from connectors.cursor_bind import cursor_kind, sa_cursor_bind

    kind = cursor_kind(getattr(col, "type", None))
    inst = _instant(watermark)
    if kind in ("datetime", "datetime_tz", "date") and inst is not None:
        pad = Decimal(2 * 86400) if kind == "date" else Decimal(120)
        if kind == "datetime_tz":
            lo, hi = _fmt(inst[0] - pad) + "+00:00", _fmt(inst[0] + pad) + "+00:00"
        else:
            civil = _instant(_civil(watermark)) if inst[1] else inst
            lo, hi = _fmt(civil[0] - pad), _fmt(civil[0] + pad)
        return sa.and_(
            col >= sa_cursor_bind(col, lo, dialect=dialect),
            col <= sa_cursor_bind(col, hi, dialect=dialect),
        )
    if kind in ("integer", "decimal", "float"):
        return col == sa_cursor_bind(col, watermark, dialect=dialect)
    if kind == "text" or isinstance(getattr(col, "type", None), sa.String):
        m = _TEXT_TS.match(str(watermark).strip())
        if not m:
            return col == str(watermark)
        if m.group(2) is None:
            return col.like(f"{m.group(1)}%")
        day, hh, mi = m.group(1), m.group(2), m.group(3)
        return sa.or_(*[col.like(f"{day}{sep}{hh}:{mi}%") for sep in ("T", " ")])
    raise BoundaryUnmeasured(f"the cursor column type {col.type!r} cannot be windowed")


def _key_match(table_cols: dict[str, Any], key_cols: list[str], keys: list[dict], dialect: str) -> Any:
    import sqlalchemy as sa

    from connectors.cursor_bind import sa_cursor_bind

    terms = []
    for key in keys:
        parts = []
        for c in key_cols:
            col = table_cols[c.lower()]
            v = key.get(c)
            parts.append(col.is_(None) if v is None else col == sa_cursor_bind(col, str(v), dialect=dialect))
        terms.append(sa.and_(*parts))
    return sa.or_(*terms)


def read_boundary_candidates(
    *, dialect: str, dest_cfg: dict, table: str, cursor_col: str, watermark: str,
    key_cols: list[str], keys: list[dict] | None, columns: list[str],
) -> list[dict[str, Any]]:
    """Destination rows that may be stored copies of the tie group (lower-cased keys).

    Raises :class:`BoundaryUnmeasured` when the answer is not complete.
    """
    from .engine_family import family_or_kind

    reader = _sql_candidates if family_or_kind(dialect) in _SQL_FAMILIES else _sample_candidates
    return reader(
        dialect=dialect, dest_cfg=dest_cfg, table=table, cursor_col=cursor_col,
        watermark=watermark, key_cols=key_cols, keys=keys, columns=columns,
    )


def _sql_candidates(*, dialect, dest_cfg, table, cursor_col, watermark, key_cols, keys, columns):
    import sqlalchemy as sa

    from connectors.generic_sql import _engine, _reflect_table, _schema_name
    from services.engine_pool import release_engine

    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect)
    engine = _engine(cfg)
    try:
        try:
            table_obj = _reflect_table(engine, table, _schema_name(cfg))
        except sa.exc.NoSuchTableError:
            return []  # nothing stored yet: nothing landed
        by_name = {str(c.name).casefold(): c for c in table_obj.c}
        need = [cursor_col, *(key_cols if keys is not None else [])]
        missing = [c for c in need if c.casefold() not in by_name]
        if missing:
            raise BoundaryUnmeasured("the destination has no column " + ", ".join(missing))
        names = list(dict.fromkeys([c for c in columns if c.casefold() in by_name] + need))
        table_cols = {c.lower(): by_name[c.casefold()] for c in names}
        window = _cursor_window(table_cols[cursor_col.lower()], str(watermark), dialect)
        chunks = [None] if keys is None else [
            keys[i : i + _KEY_CHUNK] for i in range(0, len(keys), _KEY_CHUNK)
        ]
        out: list[dict[str, Any]] = []
        with engine.connect() as conn:
            for chunk in chunks:
                where = window if chunk is None else sa.and_(
                    window, _key_match(table_cols, key_cols, chunk, dialect)
                )
                stmt = sa.select(*[table_cols[c.lower()] for c in names]).where(where)
                rows = conn.execute(stmt.limit(_ROW_CAP + 1)).fetchall()
                if len(rows) > _ROW_CAP:
                    raise BoundaryUnmeasured(
                        f"more than {_ROW_CAP:,} stored rows sit in the watermark's window"
                    )
                out.extend({c.lower(): v for c, v in zip(names, row)} for row in rows)
        return out
    finally:
        release_engine(engine)


def _sample_candidates(*, dialect, dest_cfg, table, cursor_col, watermark, key_cols, keys, columns):
    """Document and file destinations: the sample reader, never truncated silently."""
    from services.target_sample import read_target_sample

    if keys is None:
        sort_key, values = cursor_col, [str(watermark)]
    else:
        sort_key = key_cols[0]
        values = list(dict.fromkeys(str(k.get(sort_key)) for k in keys if k.get(sort_key) is not None))
    rows = read_target_sample(
        dialect, dest_cfg, schema=str(dest_cfg.get("schema") or "public"), table_name=table,
        columns=list(dict.fromkeys([*columns, cursor_col, *key_cols])), limit=_ROW_CAP + 1,
        sort_key=sort_key, key_values=values,
    )
    if len(rows or []) > _ROW_CAP:
        raise BoundaryUnmeasured(f"more than {_ROW_CAP:,} stored rows share the lookup value")
    return [{str(k).lower(): v for k, v in r.items()} for r in rows or []]


# --- the write-time dedupe -------------------------------------------------------


def _fp(values: tuple, cols: list[str], dialect: str, types: dict) -> str:
    if not cols:
        return ""
    return next(
        fp for _k, fp in row_fingerprints([values], list(cols), dest_db_type=dialect, dest_types=types)
    )


def _key_token(key: dict[str, Any]) -> str:
    return json.dumps({str(k): (None if v is None else str(v)) for k, v in key.items()}, sort_keys=True)


def source_key_texts(cfg: Any, headers: list[str], rows: list[list[Any]]) -> list[dict[str, Any]] | None:
    """``{target key column: source text}`` per row, or None when a key is not on the page."""
    from services.keyset_pagination import header_column_index
    from services.value_serializer import SQL_NULL_SENTINEL, present_cell_text

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
    out = []
    for row in rows:
        key = {}
        for c, i in pos.items():
            text = present_cell_text(row[i]) if i < len(row) else None
            key[c] = None if text in (None, SQL_NULL_SENTINEL) else text
        out.append(key)
    return out


def _summary(run: Any) -> dict[str, Any] | None:
    summary = getattr(run.tally, "dest_summary", None)
    return summary if isinstance(summary, dict) else None


def append_boundary_rows_landed(
    run: Any, headers: list[str], rows: list[list[Any]], *, cursor_dest_col: str,
    watermark: str, keyed: bool, verify: bool = False,
) -> set[int]:
    """Indices of the tie peers ``rows`` not to append (landed, or unmeasured)."""
    state = run.cur._boundary_fps_state
    try:
        landed = _landed(run, headers, rows, cursor_dest_col=cursor_dest_col,
                         watermark=watermark, keyed=keyed, record=not verify)
    except Exception as exc:  # noqa: BLE001 - fail CLOSED: never append unproven peers
        reason = str(exc) if isinstance(exc, BoundaryUnmeasured) else (
            f"the destination read failed ({type(exc).__name__})"
        )
        summary = _summary(run)
        if summary is not None:
            tie = summary.setdefault(BOUNDARY_TIE_KEY, {})
            tie["unmeasured"] = reason
            if not verify:
                tie["withheld_rows"] = int(tie.get("withheld_rows") or 0) + len(rows)
        import logging

        logging.getLogger(__name__).warning("Boundary dedupe unmeasured, rows withheld: %s", reason)
        landed = set(range(len(rows)))
    if state.get("fps") is None:
        state["fps"] = set()  # the re-read digest asks the same question of its page
    return landed


def _landed(run, headers, rows, *, cursor_dest_col, watermark, keyed, record):
    from .stream_boundary import remap_boundary_rows

    cfg, state = run.cfg, run.cur._boundary_fps_state
    dialect = cfg.dest_bind_dialect or cfg.dest_type
    types = dict(cfg.digest_dest_types or {})
    targets = list(cfg.target_cols or [])
    ddl = str(types.get(cursor_dest_col) or "")
    mapped = remap_boundary_rows(cfg, headers, rows)
    cpos = targets.index(cursor_dest_col)
    summary = _summary(run)
    tie = None
    if summary is not None and record:
        tie = summary.setdefault(BOUNDARY_TIE_KEY, {})
        tie.update(cursor_column=cursor_dest_col, watermark=str(watermark), types=types,
                   mode="keyed" if keyed else "keyless")
    if keyed:
        return _keyed_landed(run, headers, rows, mapped, tie, cpos=cpos, ddl=ddl,
                             dialect=dialect, types=types, cursor_dest_col=cursor_dest_col,
                             watermark=watermark)
    # Keyless: a row is its whole content, on the columns the table holds.
    if "keyless_dest" not in state:
        state["keyless_dest"] = read_boundary_candidates(
            dialect=dialect, dest_cfg=cfg.dest_cfg, table=cfg.dest_table,
            cursor_col=cursor_dest_col, watermark=str(watermark), key_cols=[], keys=None,
            columns=targets,
        )
    stored = state["keyless_dest"]
    live = set().union(*[r.keys() for r in stored]) if stored else set()
    cols = [c for c in targets if c.lower() in live and c != cursor_dest_col]
    pos = [targets.index(c) for c in cols]
    pool: dict[str, list] = {}
    for r in stored:
        pool.setdefault(_fp(tuple(r.get(c.lower()) for c in cols), cols, dialect, types), []).append(
            r.get(cursor_dest_col.lower())
        )
    landed: set[int] = set()
    src_counts: dict[str, int] = {}
    for i, row in enumerate(mapped):
        if row is None:
            continue
        rfp = _fp(tuple(row[p] for p in pos), cols, dialect, types)
        src_counts[rfp] = src_counts.get(rfp, 0) + 1
        bucket = pool.get(rfp, [])
        hit = next(
            (j for j, d in enumerate(bucket) if same_cursor(row[cpos], d, ddl=ddl, dialect=dialect)),
            None,
        )
        if hit is not None:
            bucket.pop(hit)  # one stored copy absorbs one re-read peer
            landed.add(i)
    if tie is not None:
        tie["match_columns"] = cols
        counts = tie.setdefault("row_counts", {})
        for k, n in src_counts.items():
            counts[k] = counts.get(k, 0) + n
    return landed


def _keyed_landed(run, headers, rows, mapped, tie, *, cpos, ddl, dialect, types, cursor_dest_col, watermark):
    cfg, state = run.cfg, run.cur._boundary_fps_state
    key_cols = list(cfg.pk_target_cols)
    keys = source_key_texts(cfg, headers, rows)
    if keys is None:
        raise BoundaryUnmeasured("a key column is not on the read page")
    cache: dict[str, list] = state.setdefault("keyed_dest", {})
    todo = [k for k in keys if _key_token(k) not in cache]
    if todo:
        found = read_boundary_candidates(
            dialect=dialect, dest_cfg=cfg.dest_cfg, table=cfg.dest_table,
            cursor_col=cursor_dest_col, watermark=str(watermark), key_cols=key_cols,
            keys=todo, columns=[cursor_dest_col, *key_cols],
        )
        by_fp: dict[str, list] = {}
        for r in found:
            kfp = _fp(tuple(r.get(c.lower()) for c in key_cols), key_cols, dialect, types)
            by_fp.setdefault(kfp, []).append(r.get(cursor_dest_col.lower()))
        for k in todo:
            cache[_key_token(k)] = by_fp.get(
                _fp(tuple(k.get(c) for c in key_cols), key_cols, dialect, types), []
            )
    landed: set[int] = set()
    for i, row in enumerate(mapped):
        if row is None:
            continue
        copies = sum(
            1 for d in cache[_key_token(keys[i])] if same_cursor(row[cpos], d, ddl=ddl, dialect=dialect)
        )
        if copies:
            landed.add(i)
        if tie is not None:
            tie["key_columns"] = key_cols
            if keys[i] not in tie.setdefault("keys", []):
                tie["keys"].append(keys[i])
            tie.setdefault("pre_copies", {})[_key_token(keys[i])] = copies
    return landed


# --- the post-write check (Gate-8) ---------------------------------------------


def boundary_duplicates(*, dest_summary: Any, endpoint: Any, mappings: list[dict] | None = None) -> list[dict[str, Any]]:
    """Tie-group rows the destination holds more often than the source does.

    Raises :class:`BoundaryUnmeasured` (or the read's own error) when that
    cannot be answered.
    """
    from .adapters import resolve_connector_config
    from .connector_capabilities import resolve_driver_type

    tie = dest_summary.get(BOUNDARY_TIE_KEY) if isinstance(dest_summary, dict) else None
    if not isinstance(tie, dict) or not tie.get("mode"):
        return []
    dialect = resolve_driver_type(getattr(endpoint, "format", "") or "")
    dest_cfg = resolve_connector_config(endpoint)
    table = str(getattr(endpoint, "table", "") or dest_summary.get("table") or "")
    types = dict(tie.get("types") or {})
    cursor_col = str(tie.get("cursor_column") or "")
    ddl = str(types.get(cursor_col) or "")
    watermark = str(tie.get("watermark"))
    if tie["mode"] == "keyed":
        key_cols = list(tie.get("key_columns") or [])
        keys = list(tie.get("keys") or [])
        if not keys:
            return []
        rows = read_boundary_candidates(
            dialect=dialect, dest_cfg=dest_cfg, table=table, cursor_col=cursor_col,
            watermark=watermark, key_cols=key_cols, keys=keys, columns=[cursor_col, *key_cols],
        )
        wanted = {_fp(tuple(k.get(c) for c in key_cols), key_cols, dialect, types): k for k in keys}
        counts: dict[str, int] = {}
        for r in rows:
            kfp = _fp(tuple(r.get(c.lower()) for c in key_cols), key_cols, dialect, types)
            if kfp in wanted and same_cursor(watermark, r.get(cursor_col.lower()), ddl=ddl, dialect=dialect):
                counts[kfp] = counts.get(kfp, 0) + 1
        pre = tie.get("pre_copies") or {}
        return [
            {"key": wanted[k], "copies": n, "expected": 1,
             "copies_before_run": pre.get(_key_token(wanted[k])), "cursor": watermark}
            for k, n in counts.items()
            if n > 1
        ]
    cols = list(tie.get("match_columns") or [])
    rows = read_boundary_candidates(
        dialect=dialect, dest_cfg=dest_cfg, table=table, cursor_col=cursor_col,
        watermark=watermark, key_cols=[], keys=None, columns=[cursor_col, *cols],
    )
    counts = {}
    for r in rows:
        if same_cursor(watermark, r.get(cursor_col.lower()), ddl=ddl, dialect=dialect):
            rfp = _fp(tuple(r.get(c.lower()) for c in cols), cols, dialect, types)
            counts[rfp] = counts.get(rfp, 0) + 1
    want = tie.get("row_counts") or {}
    return [
        {"key": {"row": k[:12] or "-"}, "copies": n, "expected": int(want[k]), "cursor": watermark}
        for k, n in counts.items()
        if k in want and n > int(want[k])
    ]


def apply_boundary_duplicate_gate(recon: Any, *, validation_mode: str = "strict", **kwargs: Any) -> Any:
    """Fail Gate-8 - loudly - when a watermark row is at the destination twice (D335).

    Every exit path runs :func:`_no_green_count_sentence`.
    """
    if not isinstance(recon, dict):
        return recon
    tie = (kwargs.get("dest_summary") or {}).get(BOUNDARY_TIE_KEY)
    try:
        unmeasured = str((tie or {}).get("unmeasured") or "")
        if unmeasured:
            raise BoundaryUnmeasured(unmeasured)
        dups = boundary_duplicates(**kwargs)
    except Exception as exc:  # noqa: BLE001 - unmeasured is said, and never green
        reason = str(exc) if isinstance(exc, BoundaryUnmeasured) else (
            f"the destination read failed ({type(exc).__name__})"
        )
        _unmeasured(recon, reason, int((tie or {}).get("withheld_rows") or 0), validation_mode)
        return _no_green_count_sentence(recon)
    recon["boundary_duplicates"] = dups
    if dups:
        named = "; ".join(
            ", ".join(f"{c}={v}" for c, v in (d.get("key") or {}).items())
            + f" ({d.get('copies')} copies at {d.get('cursor')}, source holds {d.get('expected')}"
            + (f"; {d.get('copies_before_run')} already before this run"
               if (d.get("copies_before_run") or 0) > 1 else "")
            + ")"
            for d in dups[:8]
        )
        earlier = all((d.get("copies_before_run") or 0) >= int(d.get("copies") or 0) for d in dups)
        sentence = (
            f"Append delta NOT verified: {len(dups)} row(s) at the stored watermark are at the "
            "destination more often than the source holds them - the watermark row was "
            + ("appended again by an earlier run" if earlier else "appended again")
            + f" ({named}). The destination is not exactly-once."
        )
        recon["passed"] = False
        recon["migration_proven"] = False
        recon["verification_verdict"] = "failed"
        recon["error"] = sentence
        recon["message"] = f"{sentence} {str(recon.get('message') or '').strip()}".strip()
    return _no_green_count_sentence(recon)


def _unmeasured(recon: dict[str, Any], reason: str, withheld: int, validation_mode: str) -> None:
    from services.validation_mode_contract import normalize_validation_mode

    note = (
        "Exactly-once at the stored watermark is NOT proven: the destination could not be "
        f"read to tell whether the watermark's rows were already there ({reason})."
        + (f" {withheld} re-read row(s) at the watermark were withheld, not appended." if withheld else "")
    )
    recon["boundary_check"] = {"status": "unmeasured", "reason": reason, "withheld_rows": withheld}
    recon["migration_proven"] = False
    if normalize_validation_mode(validation_mode) in {"strict", "maximum"}:
        recon["passed"] = False
        recon["verification_verdict"] = "failed"
        recon["error"] = note
    elif recon.get("verification_verdict") in (None, "verified"):
        recon["verification_verdict"] = "partial"
    recon["message"] = f"{note} {str(recon.get('message') or '').strip()}".strip()


def _no_green_count_sentence(recon: dict[str, Any]) -> dict[str, Any]:
    """A failed or unproven append never also reads "Append delta verified"."""
    message = str(recon.get("message") or "")
    unproven = recon.get("passed") is False or recon.get("verification_verdict") in ("failed", "partial")
    if unproven and "Append delta verified" in message:
        recon["message"] = message.replace(
            "Append delta verified", "Append row count matched (not a verdict)"
        )
    return recon
