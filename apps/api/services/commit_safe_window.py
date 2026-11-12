"""Commit-safe incremental window (audit C1, roadmap R1).

An incremental read selects ``cursor > watermark``. That is only complete when
rows become visible in cursor order, and they do not: a sequence value and
``now()`` are assigned *before* COMMIT. A transaction that takes id 11, stays
open while another commits 12 and 13, and then commits, leaves row 11 below a
watermark of 13 — no later run reads it and every run reports success.

Declaring the cursor ``monotonic_sequence`` cannot make this true; only a bound
in *commit* order can. This module owns that bound, per source engine:

``pg_snapshot_xmin`` — PostgreSQL
    Before a run reads, it records the snapshot's xmin horizon: the oldest
    transaction id still in progress. Every row that is not yet visible to the
    run was written by a transaction (or a subtransaction of one) whose id is
    at or above that horizon. The next run asks the source for rows *behind*
    the watermark whose ``xmin`` is at or above the stored horizon and, when
    there are any, seeds its read at the lowest such cursor value. The horizon
    is compared against the row's ``xmin`` rather than ``pg_visible_in_snapshot``
    because a snapshot's in-progress list names top-level transactions only — a
    row inserted under a SAVEPOINT carries a subtransaction id the list never
    holds, and would be reported visible while it was not.
    Cost: one aggregate query per run, bounded to the look-back window on a
    large table; rows are re-read only when a late commit actually happened.

``overlap_reread`` — every other transactional SQL source
    No engine-side bound is used (MySQL/SQL Server/Oracle expose theirs only to
    privileged sessions), so each run re-reads a fixed window behind the
    watermark: ``cursor_overlap_seconds`` for a temporal cursor,
    ``cursor_overlap_values`` for an integer one. A transaction that commits
    within the window is captured; one that stays open longer is not, and the
    route says so.

``serialized_writer`` — SQLite
    One write transaction at a time: commit order is write order.

``unguarded``
    A re-read must collapse on the row's identity. An append with no key and no
    unique cursor cannot tell a re-read row from a new one, so no window is
    applied and the route is never called commit-safe: it states "cannot
    guarantee no skipped rows" and recommends the periodic key census
    (:mod:`services.incremental_census`), which reports rows behind the
    watermark as drift.

The window plugs into :class:`services.sync_cursor.IncrementalReadScope`
(``read_watermark`` returns the window's seed); the stored watermark itself
never moves backwards.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

logger = logging.getLogger(__name__)

STRATEGY_PG = "pg_snapshot_xmin"
STRATEGY_OVERLAP = "overlap_reread"
STRATEGY_SERIAL = "serialized_writer"
STRATEGY_NONE = "unguarded"

GUARANTEE_COMMIT_SAFE = "commit_safe"
GUARANTEE_WITHIN_WINDOW = "commit_safe_within_window"
GUARANTEE_FROM_NEXT_RUN = "commit_safe_from_next_run"
GUARANTEE_NONE = "not_guaranteed"

#: Fallback re-read window behind the watermark, per run.
DEFAULT_OVERLAP_SECONDS = 300
DEFAULT_OVERLAP_VALUES = 1000
#: How far behind the watermark the PostgreSQL late-commit probe looks on a
#: large table. The probe transfers no rows, so it can afford to look further.
PG_LOOKBACK_SECONDS = 86_400
PG_LOOKBACK_VALUES = 1_000_000
#: At or below this estimated row count the PostgreSQL probe has no floor.
PG_UNBOUNDED_PROBE_ROWS = 1_000_000
#: Up to this many reported rows are checked against the destination by key.
PG_CANDIDATE_ROWS = 5_000

OVERLAP_SECONDS_ENV = "DATAFLOW_CURSOR_OVERLAP_SECONDS"
OVERLAP_VALUES_ENV = "DATAFLOW_CURSOR_OVERLAP_VALUES"

EVIDENCE_KEY = "commit_safe_window"
HORIZON_KEY = "commit_horizon"

#: Sources whose snapshot horizon bounds a read in commit order.
_SNAPSHOT_HORIZON_SOURCES = frozenset({"postgresql", "postgres"})
#: Sources with one write transaction at a time.
_SERIALIZED_WRITER_SOURCES = frozenset({"sqlite"})
#: Transactional SQL sources whose cursor seek is pushed into the read, so a
#: lowered seed is honoured by every page.
_OVERLAP_SOURCES = frozenset(
    {
        "mysql",
        "mariadb",
        "sqlserver",
        "mssql",
        "oracle",
        "oracledb",
        "snowflake",
        "bigquery",
        "redshift",
        "generic_sql",
        "db2",
        "cockroachdb",
    }
)

_INTEGER_TYPES = (
    "INT",
    "SERIAL",
    "NUMBER",
    "LONG",
)
_TEMPORAL_TYPES = ("TIMESTAMP", "DATETIME", "SMALLDATETIME")

_UNGUARDED_REASON = (
    "cannot guarantee no skipped rows: a transaction that commits after a "
    "later cursor value was read leaves its row behind the watermark"
)


class ReplaySeed(str):
    """A read seed that re-reads behind the stored watermark.

    It is the seed's own text everywhere a seed is used. A path that
    de-duplicates the re-read — the row path's page filter, the engine-side
    COPY's staging apply — also reads ``replay_through``: the cursor value of
    the stored watermark, up to which the rows it reads are re-reads.
    """

    replay_through: str = ""

    def __new__(cls, seed: str, replay_through: str = "") -> ReplaySeed:
        obj = super().__new__(cls, seed)
        obj.replay_through = str(replay_through or "")
        return obj


@dataclass(frozen=True)
class LateRows:
    """What the source reports behind the watermark since the stored bound."""

    count: int = 0
    min_cursor: str | None = None
    #: Lower cursor bound the probe was limited to ("" = none).
    floor: str = ""
    #: ``(cursor value, identity value)`` of each reported row, lowest cursor
    #: first — complete only when it holds ``count`` entries.
    candidates: tuple[tuple[str, str], ...] = ()
    #: How many of them the destination does not hold yet (None = not checked).
    undelivered: int | None = None


@dataclass(frozen=True)
class CommitSafeWindow:
    """The read window of one incremental run, and what it may claim."""

    strategy: str
    stored_watermark: str | None = None
    #: Where this run's read starts. Equals ``stored_watermark`` unless the
    #: window re-reads behind it.
    seed: str | None = None
    #: Cursor value up to which rows of this run are re-reads ("" = none).
    replay_through: str = ""
    #: The commit-order bound this run reads under; stored with the watermark.
    bound: dict[str, Any] = field(default_factory=dict)
    previous_bound: dict[str, Any] = field(default_factory=dict)
    late_rows_detected: int | None = None
    #: Of those, the rows the destination did not hold (None = not checked).
    late_rows_undelivered: int | None = None
    overlap: dict[str, Any] = field(default_factory=dict)
    guarantee: str = GUARANTEE_NONE
    reason: str = ""
    census_recommended: bool = False

    @property
    def replays(self) -> bool:
        return bool(self.replay_through)

    def metadata(self) -> dict[str, Any]:
        """Watermark metadata to store once this run's rows are at rest."""
        return {HORIZON_KEY: dict(self.bound)} if self.bound else {}

    def evidence(
        self, *, rows_replayed: int | None = None, rows_already_landed: int | None = None
    ) -> dict[str, Any]:
        """The record Gate-8 / Parity show for this run's cursor window."""
        out: dict[str, Any] = {
            "strategy": self.strategy,
            "guarantee": self.guarantee,
            "reason": self.reason,
            "stored_watermark": self.stored_watermark,
            "replay_seed": self.seed if self.replays else None,
            "census_recommended": self.census_recommended,
        }
        if self.bound:
            out["bound"] = dict(self.bound)
        if self.previous_bound:
            out["previous_bound"] = dict(self.previous_bound)
        if self.overlap:
            out["overlap"] = dict(self.overlap)
        if self.late_rows_detected is not None:
            out["late_rows_detected"] = int(self.late_rows_detected)
        if self.late_rows_undelivered is not None:
            out["late_rows_undelivered"] = int(self.late_rows_undelivered)
        if rows_replayed is not None:
            out["rows_replayed"] = int(rows_replayed)
            landed = int(rows_already_landed or 0)
            out["rows_already_landed"] = landed
            out["rows_recovered"] = max(0, int(rows_replayed) - landed)
        return out


# --------------------------------------------------------------------------
# Window arithmetic
# --------------------------------------------------------------------------


def _type_base(declared: Any) -> str:
    text = " ".join(str(declared or "").upper().split())
    return text.split("(", 1)[0].strip()


def cursor_distance_unit(cursor_type: Any, value: Any = None) -> str:
    """``seconds`` / ``values`` when a distance exists on this cursor, else ``""``.

    The declared type decides. With no declared type the value's own shape
    does: digits are a sequence, an ISO date-time is a clock.
    """
    base = _type_base(cursor_type)
    if base:
        if any(base.startswith(t) or t in base for t in _TEMPORAL_TYPES):
            return "seconds"
        if base == "DATE":
            return ""
        if any(t in base for t in _INTEGER_TYPES) and "INTERVAL" not in base:
            return "values"
        return ""
    text = str(value or "").strip()
    if text.lstrip("-").isdigit():
        return "values"
    return "seconds" if _parse_temporal(text) is not None else ""


_TEMPORAL_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f",
)


def _parse_temporal(text: str) -> tuple[datetime, Callable[[datetime], str]] | None:
    for fmt in _TEMPORAL_FORMATS:
        try:
            return datetime.strptime(text, fmt), (lambda d, f=fmt: d.strftime(f))
        except ValueError:
            continue
    if len(text) < 19:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed, (lambda d: d.isoformat())


def cursor_part(watermark: Any) -> str:
    """The cursor value of a stored watermark, without its tie-break."""
    from services.keyset_pagination import present_cursor_bookmark, split_cursor_bookmark
    from services.sync_cursor import watermark_is_composite

    bookmark = present_cursor_bookmark(watermark)
    if bookmark is None:
        return ""
    head, _ = split_cursor_bookmark(bookmark, has_tiebreak=watermark_is_composite(bookmark))
    return str(head)


def overlap_seed(
    watermark: Any, cursor_type: Any, *, seconds: int, values: int
) -> str | None:
    """The read seed ``overlap`` behind ``watermark``, or None when none exists.

    Cursor-only: the run re-reads the whole tie group at its seed, so a
    tie-break part would only narrow a window that must stay inclusive.
    """
    head = cursor_part(watermark)
    if not head:
        return None
    unit = cursor_distance_unit(cursor_type, head)
    if unit == "values":
        if values <= 0:
            return None
        try:
            return str(int(head) - int(values))
        except ValueError:
            return None
    if unit == "seconds":
        if seconds <= 0:
            return None
        parsed = _parse_temporal(head)
        if parsed is None:
            return None
        moment, render = parsed
        return render(moment - timedelta(seconds=int(seconds)))
    return None


def _env_int(name: str, default: int) -> int:
    from services.brand_env import getenv_brand

    raw = getenv_brand(name, "")
    try:
        return max(0, int(str(raw).strip())) if str(raw).strip() else default
    except ValueError:
        return default


def declared_overlap(contract: Mapping[str, Any] | None) -> tuple[int, int]:
    """``(seconds, values)`` of the fallback re-read window for this stream."""
    data = contract if isinstance(contract, Mapping) else {}

    def _pick(key: str, default: int) -> int:
        raw = data.get(key)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return max(0, int(raw))
        except (TypeError, ValueError):
            return default

    return (
        _pick("cursor_overlap_seconds", _env_int(OVERLAP_SECONDS_ENV, DEFAULT_OVERLAP_SECONDS)),
        _pick("cursor_overlap_values", _env_int(OVERLAP_VALUES_ENV, DEFAULT_OVERLAP_VALUES)),
    )


def commit_safety_strategy(source_type: str) -> str:
    """The strategy a source engine supports, before the route is considered."""
    src = (source_type or "").strip().lower()
    if src in _SNAPSHOT_HORIZON_SOURCES:
        return STRATEGY_PG
    if src in _SERIALIZED_WRITER_SOURCES:
        return STRATEGY_SERIAL
    if src in _OVERLAP_SOURCES:
        return STRATEGY_OVERLAP
    return STRATEGY_NONE


# --------------------------------------------------------------------------
# Planning
# --------------------------------------------------------------------------


def plan_commit_safe_window(
    *,
    source_type: str,
    sync_mode: str,
    cursor_column: str,
    cursor_type: Any,
    stored_watermark: str | None,
    metadata: Mapping[str, Any] | None,
    can_replay: bool,
    contract: Mapping[str, Any] | None,
    capture_bound: Callable[[], dict[str, Any]] | None = None,
    probe_late: Callable[..., LateRows] | None = None,
) -> CommitSafeWindow | None:
    """Decide one run's read window. Pure: engine access is injected.

    ``capture_bound`` returns the source's current commit-order bound and must
    be called *before* the run reads. ``probe_late`` reports rows behind
    ``below`` written by transactions at or above ``since_xmin``.
    """
    from services.sync_cursor import normalize_sync_mode, requires_incremental

    if not requires_incremental(normalize_sync_mode(sync_mode)) or not (cursor_column or "").strip():
        return None
    strategy = commit_safety_strategy(source_type)
    stored = None if stored_watermark in (None, "") else str(stored_watermark)
    seconds, values = declared_overlap(contract)

    if strategy == STRATEGY_SERIAL:
        return CommitSafeWindow(
            strategy=strategy,
            stored_watermark=stored,
            seed=stored,
            guarantee=GUARANTEE_COMMIT_SAFE,
            reason="the source runs one write transaction at a time: commit order is write order",
        )

    if strategy == STRATEGY_PG and capture_bound is not None:
        # Captured before anything is read: a row this run does not see belongs
        # to a transaction at or above this horizon.
        bound = dict(capture_bound() or {})
        if bound.get("xmin"):
            return _plan_pg(
                stored=stored,
                metadata=metadata,
                can_replay=can_replay,
                cursor_type=cursor_type,
                bound=bound,
                probe_late=probe_late,
            )
        # A PostgreSQL-compatible source without snapshot functions falls back
        # to the re-read window below.

    if strategy in (STRATEGY_OVERLAP, STRATEGY_PG):
        # PostgreSQL lands here only when its bound could not be captured.
        if not can_replay:
            return _unguarded(
                stored,
                "this route appends without a key, so a re-read window cannot be "
                "de-duplicated",
            )
        unit = cursor_distance_unit(cursor_type, cursor_part(stored) if stored else None)
        size = seconds if unit == "seconds" else values if unit == "values" else 0
        if not unit or size <= 0:
            return _unguarded(
                stored,
                "no re-read window is declared for this cursor"
                if unit
                else "this cursor type has no distance a re-read window can be measured in",
            )
        seed = overlap_seed(stored, cursor_type, seconds=seconds, values=values) if stored else None
        if stored and seed is None:
            return _unguarded(stored, "the stored watermark cannot be shifted by the re-read window")
        return CommitSafeWindow(
            strategy=STRATEGY_OVERLAP,
            stored_watermark=stored,
            seed=ReplaySeed(seed, cursor_part(stored)) if stored else None,
            replay_through=cursor_part(stored) if stored else "",
            overlap={"unit": unit, "size": int(size)},
            guarantee=GUARANTEE_WITHIN_WINDOW,
            reason=(
                f"each run re-reads the last {size:,} {unit} behind the watermark and "
                "collapses re-reads on the row identity; a transaction that stays open "
                "longer than that is not re-read"
            ),
            census_recommended=True,
        )

    return _unguarded(stored, "this source exposes no commit-order bound")


def _unguarded(stored: str | None, why: str) -> CommitSafeWindow:
    return CommitSafeWindow(
        strategy=STRATEGY_NONE,
        stored_watermark=stored,
        seed=stored,
        guarantee=GUARANTEE_NONE,
        reason=f"{_UNGUARDED_REASON} — {why}. Run the periodic key census to detect them.",
        census_recommended=True,
    )


def _plan_pg(
    *,
    stored: str | None,
    metadata: Mapping[str, Any] | None,
    can_replay: bool,
    cursor_type: Any,
    bound: dict[str, Any],
    probe_late: Callable[..., LateRows] | None,
) -> CommitSafeWindow:
    previous = dict((metadata or {}).get(HORIZON_KEY) or {})
    base = dict(
        strategy=STRATEGY_PG,
        stored_watermark=stored,
        seed=stored,
        bound=bound,
        previous_bound=previous,
    )
    if stored is None:
        return CommitSafeWindow(
            **base,
            guarantee=GUARANTEE_COMMIT_SAFE,
            reason="first read; the snapshot horizon is stored with the watermark",
        )
    if not previous.get("xmin") or probe_late is None:
        return CommitSafeWindow(
            **base,
            guarantee=GUARANTEE_FROM_NEXT_RUN,
            reason=(
                "the stored watermark predates the snapshot horizon: this run records "
                "one, and late commits are re-read from the next run on"
            ),
        )
    try:
        late = probe_late(
            since_xmin=str(previous["xmin"]), below=cursor_part(stored), cursor_type=cursor_type
        )
    except Exception as exc:  # noqa: BLE001 — unprobed is said, and probed again next run
        logger.warning("commit-safe window: late-commit probe failed: %s", exc)
        return CommitSafeWindow(
            **{**base, "bound": previous},
            guarantee=GUARANTEE_NONE,
            reason=(
                f"{_UNGUARDED_REASON} — the source could not be asked for rows committed "
                f"behind the watermark this run ({type(exc).__name__}); the stored bound is "
                "kept and the next run asks again"
            ),
            census_recommended=True,
        )
    floor_note = (
        f" (probe looks back to cursor {late.floor}; a row committed with an older "
        "cursor value than that is only found by the key census)"
        if late.floor
        else ""
    )
    if late.count and late.undelivered == 0:
        # Changed behind the watermark, and every one of those row versions is
        # already at the destination: rows edited in place without moving the
        # cursor. That is out-of-window drift (D188), not a late commit —
        # nothing to re-read, and the bound moves on.
        return CommitSafeWindow(
            **base,
            late_rows_detected=int(late.count),
            late_rows_undelivered=0,
            guarantee=GUARANTEE_WITHIN_WINDOW if late.floor else GUARANTEE_COMMIT_SAFE,
            reason=(
                f"{int(late.count):,} row(s) behind the watermark changed since the last "
                "read and the destination already holds each of them at that cursor "
                "value: edited in place, not committed late — nothing to re-read"
                + floor_note
            ),
            census_recommended=bool(late.floor),
        )
    if not late.count:
        return CommitSafeWindow(
            **base,
            late_rows_detected=0,
            guarantee=GUARANTEE_WITHIN_WINDOW if late.floor else GUARANTEE_COMMIT_SAFE,
            reason="no row was committed behind the watermark since the last read" + floor_note,
            census_recommended=bool(late.floor),
        )
    if not can_replay or late.min_cursor in (None, ""):
        return CommitSafeWindow(
            **{**base, "bound": previous},
            late_rows_detected=int(late.count),
            guarantee=GUARANTEE_NONE,
            reason=(
                f"{int(late.count):,} row(s) may have been committed behind the watermark "
                f"since the last read, and {_UNGUARDED_REASON.split(':', 1)[0]}: this route "
                "appends without a key, so they cannot be re-read without duplicating "
                "rows already delivered. Run the key census, or give the stream a key."
            ),
            census_recommended=True,
        )
    return CommitSafeWindow(
        **{**base, "seed": ReplaySeed(str(late.min_cursor), cursor_part(stored))},
        replay_through=cursor_part(stored),
        late_rows_detected=int(late.count),
        late_rows_undelivered=late.undelivered,
        guarantee=GUARANTEE_WITHIN_WINDOW if late.floor else GUARANTEE_COMMIT_SAFE,
        reason=(
            f"{int(late.count):,} row(s) behind the watermark were written by transactions "
            "not finished at the last read; this run re-reads from the lowest of them "
            "and collapses re-reads on the row identity" + floor_note
        ),
        census_recommended=bool(late.floor),
    )


# --------------------------------------------------------------------------
# PostgreSQL engine access
# --------------------------------------------------------------------------


def _pg_connect(src_cfg: Mapping[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host=str(src_cfg.get("host") or ""),
        port=int(src_cfg.get("port") or 5432),
        database=str(src_cfg.get("database") or ""),
        username=str(src_cfg.get("username") or ""),
        password=str(src_cfg.get("password") or ""),
        connection_string=str(src_cfg.get("connection_string") or ""),
        ssl=bool(src_cfg.get("ssl", False)),
    )


_PG_SNAPSHOT_SQL = (
    "SELECT txid_snapshot_xmin(txid_current_snapshot())::text, "
    "txid_snapshot_xmax(txid_current_snapshot())::text"
)


def capture_pg_bound(src_cfg: Mapping[str, Any]) -> dict[str, Any]:
    """The snapshot horizon of the source right now (``{}`` when unreadable).

    ``txid_current_snapshot`` rather than ``pg_current_snapshot``: the same
    64-bit values, and it exists on every supported server version.
    """
    try:
        conn = _pg_connect(src_cfg)
    except Exception as exc:  # noqa: BLE001 — the route then states it is unguarded
        logger.warning("commit-safe window: source connect failed: %s", exc)
        return {}
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(_PG_SNAPSHOT_SQL)
            row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        logger.warning("commit-safe window: snapshot horizon unreadable: %s", exc)
        return {}
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("commit-safe window: close skipped", exc_info=True)
    if not row or not row[0]:
        return {}
    return {
        "strategy": STRATEGY_PG,
        "xmin": str(row[0]),
        "xmax": str(row[1] or ""),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def probe_pg_late_rows(
    src_cfg: Mapping[str, Any],
    *,
    schema: str,
    table: str,
    cursor_column: str,
    since_xmin: str,
    below: str,
    cursor_type: Any = None,
    identity_column: str = "",
) -> LateRows:
    """Rows behind ``below`` written at or above the stored snapshot horizon.

    A row's ``xmin`` is a 32-bit id; it is widened against the snapshot's own
    64-bit ``xmax`` (every stored row was written before it). A frozen row whose
    old id aliases into the range is a false positive only: it is re-read and
    collapses on the row identity.

    On a large table the probe is floored at the PostgreSQL look-back window so
    it stays an index range scan; a probe that cannot be floored on such a table
    still runs (it is the only bound there is) and says so in the log.
    """
    from psycopg2 import sql

    from connectors.postgresql_reader import _cell

    conn = _pg_connect(src_cfg)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.reltuples::bigint FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s",
                (schema or "public", table),
            )
            est = cur.fetchone()
            est_rows = int(est[0]) if est and est[0] is not None else -1
            floor = ""
            if est_rows > PG_UNBOUNDED_PROBE_ROWS:
                floor = (
                    overlap_seed(
                        below, cursor_type, seconds=PG_LOOKBACK_SECONDS, values=PG_LOOKBACK_VALUES
                    )
                    or ""
                )
                if not floor:
                    logger.info(
                        "commit-safe window: unfloored late-commit probe on %s.%s (~%d rows)",
                        schema, table, est_rows,
                    )
            ident = sql.Identifier(cursor_column)
            conds = [sql.SQL("t.{c} < %s").format(c=ident)]
            params: list[Any] = [below]
            if floor:
                conds.append(sql.SQL("t.{c} >= %s").format(c=ident))
                params.append(floor)
            conds.append(
                sql.SQL("(snap.m - ((snap.m - t.xmin::text::bigint) & 4294967295)) >= %s")
            )
            query = sql.SQL(
                "WITH snap AS (SELECT txid_snapshot_xmax(txid_current_snapshot()) AS m) "
                "SELECT min(t.{c}), count(*) FROM {s}.{t} t, snap WHERE "
            ).format(
                c=ident, s=sql.Identifier(schema or "public"), t=sql.Identifier(table)
            ) + sql.SQL(" AND ").join(conds)
            cur.execute(query, [*params, int(since_xmin)])
            row = cur.fetchone()
            candidates: list[tuple[str, str]] = []
            if identity_column and row and 0 < int(row[1] or 0) <= PG_CANDIDATE_ROWS:
                # Few enough to name: the caller checks which of them the
                # destination lacks, so a row edited in place is not re-read.
                listing = sql.SQL(
                    "WITH snap AS (SELECT txid_snapshot_xmax(txid_current_snapshot()) AS m) "
                    "SELECT t.{c}, t.{k} FROM {s}.{t} t, snap WHERE "
                ).format(
                    c=ident, k=sql.Identifier(identity_column),
                    s=sql.Identifier(schema or "public"), t=sql.Identifier(table),
                ) + sql.SQL(" AND ").join(conds) + sql.SQL(" ORDER BY 1 LIMIT %s")
                cur.execute(listing, [*params, int(since_xmin), PG_CANDIDATE_ROWS + 1])
                candidates = [(_cell(c), _cell(k)) for c, k in cur.fetchall()]
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("commit-safe window: close skipped", exc_info=True)
    count = int(row[1] or 0) if row else 0
    low = _cell(row[0]) if row and row[0] is not None else None
    return LateRows(count=count, min_cursor=low, floor=floor, candidates=tuple(candidates))


def static_commit_safety(
    *,
    source_type: str,
    sync_mode: str,
    cursor_column: str,
    cursor_type: Any = None,
    has_identity: bool,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """What a route may claim about late commits, judged without touching the source.

    Preflight and the cursor-semantics verdict use this; a run executes the
    same decision with the engine's bound filled in (:func:`plan_commit_safe_window`).
    """
    from services.sync_cursor import normalize_sync_mode, requires_incremental

    if not requires_incremental(normalize_sync_mode(sync_mode)) or not (cursor_column or "").strip():
        return {}
    strategy = commit_safety_strategy(source_type)
    seconds, values = declared_overlap(contract)
    overlap: dict[str, Any] = {}
    census = "Run the periodic key census (key_census_every) to detect skipped rows."
    keyless = (
        "this route appends without a key, so rows committed behind the watermark "
        "cannot be re-read without duplicating rows already delivered. Give the "
        f"stream a key. {census}"
    )
    if strategy == STRATEGY_SERIAL:
        guarantee = GUARANTEE_COMMIT_SAFE
        statement = "The source commits one write transaction at a time: nothing commits late."
    elif strategy == STRATEGY_PG and has_identity:
        guarantee = GUARANTEE_COMMIT_SAFE
        statement = (
            "Late commits are covered: each run records the PostgreSQL snapshot horizon "
            "and the next run re-reads rows committed behind the watermark since then."
        )
    elif strategy == STRATEGY_PG:
        guarantee = GUARANTEE_NONE
        statement = f"Cannot guarantee no skipped rows: {keyless}"
    elif strategy == STRATEGY_OVERLAP and not has_identity:
        strategy, guarantee = STRATEGY_NONE, GUARANTEE_NONE
        statement = f"Cannot guarantee no skipped rows: {keyless}"
    elif strategy == STRATEGY_OVERLAP:
        unit = cursor_distance_unit(cursor_type) if _type_base(cursor_type) else "?"
        size = seconds if unit == "seconds" else values if unit == "values" else max(seconds, values)
        if not unit or size <= 0:
            strategy, guarantee = STRATEGY_NONE, GUARANTEE_NONE
            statement = (
                "Cannot guarantee no skipped rows: "
                + (
                    "the re-read window is declared off"
                    if unit
                    else "this cursor type has no distance a re-read window can be measured in"
                )
                + f". {census}"
            )
        else:
            guarantee = GUARANTEE_WITHIN_WINDOW
            span = (
                f"{size:,} {unit}"
                if unit != "?"
                else f"{seconds:,} seconds (temporal cursor) or {values:,} values (integer cursor)"
            )
            if unit != "?":
                overlap = {"unit": unit, "size": int(size)}
            statement = (
                f"Late commits are covered within a re-read window of {span} behind the "
                "watermark, de-duplicated on the key; a transaction open longer than that "
                f"is skipped. {census}"
            )
    else:
        strategy, guarantee = STRATEGY_NONE, GUARANTEE_NONE
        statement = (
            "Cannot guarantee no skipped rows: this source exposes no commit-order "
            f"bound. {census}"
        )
    return {
        "strategy": strategy,
        "guarantee": guarantee,
        "statement": statement,
        "census_recommended": guarantee in (GUARANTEE_NONE, GUARANTEE_WITHIN_WINDOW),
        "overlap": overlap,
    }
