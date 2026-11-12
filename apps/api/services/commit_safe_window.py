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
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

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
#: The same look-back for a DATE cursor, in its own unit (review M-3).
PG_LOOKBACK_DAYS = max(1, PG_LOOKBACK_SECONDS // 86_400)
#: At or below this estimated row count the PostgreSQL probe has no floor. A
#: table whose estimate is unknown (``reltuples = -1``) is floored too.
PG_UNBOUNDED_PROBE_ROWS = 1_000_000
#: Rows behind the watermark are checked against the destination in pages of
#: this many, in cursor order, until the first one it lacks (no overall cap).
PG_CHECK_PAGE_ROWS = 1_000
#: The snapshot's in-progress list is stored with the bound up to this many
#: transaction ids; a longer list is not stored and the horizon rule applies.
PG_XIP_MAX = 512
#: Every probe connection runs under this statement timeout (milliseconds).
PROBE_TIMEOUT_ENV = "DATAFLOW_COMMIT_SAFE_PROBE_TIMEOUT_MS"
DEFAULT_PROBE_TIMEOUT_MS = 5_000

OVERLAP_SECONDS_ENV = "DATAFLOW_CURSOR_OVERLAP_SECONDS"
OVERLAP_VALUES_ENV = "DATAFLOW_CURSOR_OVERLAP_VALUES"

EVIDENCE_KEY = "commit_safe_window"
HORIZON_KEY = "commit_horizon"

#: Sources whose snapshot horizon bounds a read in commit order.
_SNAPSHOT_HORIZON_SOURCES = frozenset({"postgresql", "postgres"})
#: PostgreSQL relation kinds whose rows carry the writing transaction's id
#: (``xmin``): an ordinary and a partitioned table. A view, materialized view or
#: foreign table has no such column to bound, so it takes the re-read window.
_TABLE_RELKINDS = frozenset({"r", "p"})
_RELKIND_NAMES = {
    "v": "a view",
    "m": "a materialized view",
    "f": "a foreign table",
}
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

#: Integer type names, matched as the declared type's whole first word. A
#: substring match read LONGTEXT and LONG VARCHAR (LONG) and POINT (INT) as
#: integers and shifted a zero-padded text cursor by integer arithmetic.
_INTEGER_TYPES = frozenset(
    {
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT",
        "SERIAL", "BIGSERIAL", "SMALLSERIAL",
        "INT2", "INT4", "INT8", "INT64",
        # DuckDB
        "HUGEINT", "UHUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
        # ClickHouse
        "INT16", "INT32", "INT128", "INT256",
        "UINT8", "UINT16", "UINT32", "UINT64", "UINT128", "UINT256",
    }
)
#: Exact numerics that are integers only at scale 0: ``NUMBER(10)``, ``NUMBER(10,0)``.
_SCALED_NUMERIC_TYPES = frozenset({"NUMBER", "NUMERIC", "DECIMAL"})
_TEMPORAL_TYPES = frozenset(
    {
        "TIMESTAMP", "TIMESTAMPTZ", "DATETIME", "DATETIME2", "DATETIMEOFFSET", "SMALLDATETIME",
        # Snowflake
        "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ",
        # DuckDB
        "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
        # ClickHouse
        "DATETIME64",
    }
)
#: Wrappers that do not change a type's order (ClickHouse).
_TYPE_WRAPPERS = ("NULLABLE", "LOWCARDINALITY")
_INT_SCALE_RE = re.compile(r"^\(\s*\d+\s*(?:,\s*0+\s*)?\)")
#: Calendar dates: their natural unit is a day.
_DATE_TYPES = frozenset({"DATE", "DATE32"})
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_SERIAL_APP_ASSIGNED = (
    "within one writer's commit order: the source runs one write transaction at a "
    "time, but this cursor can be set by the application before its row is written "
    "(it is not an AUTOINCREMENT key), so a row written later with an earlier value "
    "lands behind the watermark. Run the periodic key census (key_census_every) to "
    "detect skipped rows"
)

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
    #: ``(cursor value, whole-key values)`` of each reported row, lowest
    #: cursor first — complete only when it holds ``count`` entries.
    candidates: tuple[tuple[str, tuple[str, ...]], ...] = ()
    #: How many of them the destination does not hold yet (None = not checked).
    #: Checking stops at the first one it lacks, so this counts that page.
    undelivered: int | None = None
    #: How many were checked against the destination (None = not checked).
    checked: int | None = None
    #: ``in_progress`` (judged on the stored in-progress list) or ``horizon``.
    rule: str = ""
    #: Why the probe did not run ("" = it ran): on a large or never-analysed
    #: table a cursor with no unit to floor it by would scan the whole table.
    unprobed: str = ""


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
    #: How many were checked against the destination before the first gap.
    late_rows_checked: int | None = None
    #: How rows behind the watermark were judged (:func:`late_row_rule`).
    late_row_rule: str = ""
    overlap: dict[str, Any] = field(default_factory=dict)
    guarantee: str = GUARANTEE_NONE
    reason: str = ""
    census_recommended: bool = False
    #: A source probe this run could not complete ("" = none failed). Never
    #: silent: Gate-8 carries it as a warning.
    probe_error: str = ""

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
        if self.late_rows_checked is not None:
            out["late_rows_checked"] = int(self.late_rows_checked)
        if self.late_row_rule:
            out["late_row_rule"] = self.late_row_rule
            if self.late_row_rule == "horizon" and self.previous_bound.get("horizon_reason"):
                out["horizon_reason"] = str(self.previous_bound["horizon_reason"])
        if self.probe_error:
            out["probe_error"] = self.probe_error
        if rows_replayed is not None:
            out["rows_replayed"] = int(rows_replayed)
            landed = int(rows_already_landed or 0)
            out["rows_already_landed"] = landed
            out["rows_recovered"] = max(0, int(rows_replayed) - landed)
        return out


# --------------------------------------------------------------------------
# Window arithmetic
# --------------------------------------------------------------------------


def _declared_text(declared: Any) -> str:
    """The declared type, upper-cased, unqualified (``pg_catalog.int8`` → ``INT8``)
    and unwrapped (``Nullable(DateTime64(3))`` → ``DATETIME64(3)``)."""
    text = " ".join(str(declared or "").upper().split()).replace('"', "")
    for wrapper in _TYPE_WRAPPERS:
        if text.startswith(wrapper + "(") and text.endswith(")"):
            text = text[len(wrapper) + 1 : -1].strip()
    head, paren, rest = text.partition("(")
    return head.rsplit(".", 1)[-1].strip() + paren + rest


def _type_base(declared: Any) -> str:
    return _declared_text(declared).split("(", 1)[0].strip()


def _type_class(declared: Any) -> str:
    """``seconds`` / ``values`` / ``""`` for a declared type, by whole words.

    Text first: a text column has no distance, whatever its name contains.
    """
    from services.cursor_order import is_text_declared

    text = _declared_text(declared)
    if not text or is_text_declared(text):
        return ""
    word = re.split(r"[\s(]", text, maxsplit=1)[0]
    if word in _TEMPORAL_TYPES:
        return "seconds"
    if word in _INTEGER_TYPES:
        return "values"
    if word in _SCALED_NUMERIC_TYPES and _INT_SCALE_RE.match(text[len(word):].lstrip()):
        return "values"
    return ""


def probe_floor_unit(cursor_type: Any, value: Any = None) -> str:
    """The unit the late-commit probe can be floored in: ``seconds`` /
    ``values`` / ``days``, or ``""`` (text, uuid, ...: no distance).

    A DATE has a natural unit, a day (review M-3); with no declared type an
    ISO date value is one.
    """
    if _type_base(cursor_type):
        return "days" if _type_base(cursor_type) in _DATE_TYPES else _type_class(cursor_type)
    if _ISO_DATE_RE.match(str(value or "").strip()):
        return "days"
    return cursor_distance_unit(cursor_type, value)


def probe_floor(below: Any, cursor_type: Any) -> str:
    """The lowest cursor value the late-commit probe reads on a large table, or ``""``."""
    head = str(below or "").strip()
    if probe_floor_unit(cursor_type, head) == "days":
        from datetime import date

        try:
            day = date.fromisoformat(head[:10])
        except ValueError:
            return ""
        try:
            return (day - timedelta(days=PG_LOOKBACK_DAYS)).isoformat()
        except OverflowError:
            return date.min.isoformat()
    return overlap_seed(below, cursor_type, seconds=PG_LOOKBACK_SECONDS, values=PG_LOOKBACK_VALUES) or ""


def _canonical_integer(text: str) -> bool:
    """True when ``text`` is an integer written the one way ``str(int(...))`` writes it.

    A seed is rendered by ``str(int)``. Any other spelling (``005000``,
    ``+5``) would come back different, and on a text column a different
    spelling sorts somewhere else.
    """
    try:
        return str(int(text)) == text
    except ValueError:
        return False


def cursor_distance_unit(cursor_type: Any, value: Any = None) -> str:
    """``seconds`` / ``values`` when a distance exists on this cursor, else ``""``.

    The declared type decides. With no declared type the value's own shape
    does: an integer in canonical form is a sequence, an ISO date-time is a
    clock.
    """
    if _type_base(cursor_type):
        return _type_class(cursor_type)
    text = str(value or "").strip()
    if _canonical_integer(text):
        return "values"
    return "seconds" if _parse_temporal(text) is not None else ""


_TEMPORAL_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f",
)


def _strftime(moment: datetime, fmt: str) -> str:
    """``strftime`` with a four-digit year everywhere (glibc writes year 1 as '1')."""
    return moment.strftime(fmt.replace("%Y", f"{moment.year:04d}"))


class ClampedSeed(str):
    """A seed the re-read window could not reach: the earliest representable time."""


def _parse_temporal(text: str) -> tuple[datetime, Callable[[datetime], str]] | None:
    for fmt in _TEMPORAL_FORMATS:
        try:
            return datetime.strptime(text, fmt), (lambda d, f=fmt: _strftime(d, f))
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
        if values <= 0 or not _canonical_integer(head):
            # Refused rather than re-spelled: a zero-padded value shifted by
            # integer arithmetic loses its padding and sorts elsewhere.
            return None
        return str(int(head) - int(values))
    if unit == "seconds":
        if seconds <= 0:
            return None
        parsed = _parse_temporal(head)
        if parsed is None:
            return None
        moment, render = parsed
        try:
            return render(moment - timedelta(seconds=int(seconds)))
        except OverflowError:
            # A year-1 watermark, or a window wider than the calendar: the
            # re-read starts at the earliest representable time, and the run
            # says so — never a run with no window and no statement.
            return ClampedSeed(render(datetime.min.replace(tzinfo=moment.tzinfo)))
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


#: PostgreSQL-compatible products the run reads with the PostgreSQL driver
#: whose row ``xmin`` is not proven to be PostgreSQL's transaction id in commit
#: order: they take the re-read window, never the snapshot horizon.
XMIN_UNPROVEN_PRODUCTS = {
    "cockroachdb": "CockroachDB",
    "cockroach": "CockroachDB",
    "yugabytedb": "YugabyteDB",
    "yugabyte": "YugabyteDB",
}


def _read_driver(source: str) -> str:
    """The driver a run of this product reads with (the product itself when unknown)."""
    try:
        from src.transfer.connector_capabilities import resolve_driver_type, source_read_driver

        return str(source_read_driver(resolve_driver_type(source)) or source).strip().lower()
    except Exception:  # noqa: BLE001 — an unmapped id is judged as itself
        return source


def product_strategy_note(source_type: str) -> str:
    """What runs for a product read with another product's driver, or ``""``."""
    src = (source_type or "").strip().lower()
    if src in XMIN_UNPROVEN_PRODUCTS:
        return (
            f"{XMIN_UNPROVEN_PRODUCTS[src]} is read with the PostgreSQL driver, but its rows' "
            "xmin is not proven to be PostgreSQL's transaction id in commit order, so the "
            "route takes the re-read window instead of the snapshot horizon"
        )
    if src and src not in _OVERLAP_SOURCES and _read_driver(src) == "generic_sql":
        return f"'{src}' is read with the generic SQL driver, which applies the re-read window"
    return ""


def commit_safety_strategy(source_type: str) -> str:
    """The strategy a source supports, judged by the driver its run reads with.

    Preflight passes the product id and the run its read driver; both land on
    the same strategy (review Low): PostgreSQL's snapshot horizon only where the
    PostgreSQL driver reads and ``xmin`` is PostgreSQL's.
    """
    src = (source_type or "").strip().lower()
    if src in XMIN_UNPROVEN_PRODUCTS:
        return STRATEGY_OVERLAP
    if not (src in _SNAPSHOT_HORIZON_SOURCES or src in _SERIALIZED_WRITER_SOURCES or src in _OVERLAP_SOURCES):
        src = _read_driver(src)
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
    relation_kind: str | None = None,
    cursor_database_assigned: bool | None = None,
) -> CommitSafeWindow | None:
    """Decide one run's read window. Pure: engine access is injected.

    ``capture_bound`` returns the source's current commit-order bound and must
    be called *before* the run reads. ``probe_late`` reports rows behind
    ``below`` written by transactions at or above ``since_xmin``.
    ``relation_kind`` is the PostgreSQL ``pg_class.relkind`` of the source
    (``""`` when it could not be read; None when the caller did not ask): only
    a table carries the ``xmin`` the horizon bounds.
    ``cursor_database_assigned`` (SQLite): whether the source assigns the
    cursor itself (an AUTOINCREMENT key); None when the caller did not ask.
    """
    from services.sync_cursor import normalize_sync_mode, requires_incremental

    if not requires_incremental(normalize_sync_mode(sync_mode)) or not (cursor_column or "").strip():
        return None
    strategy = commit_safety_strategy(source_type)
    stored = None if stored_watermark in (None, "") else str(stored_watermark)
    seconds, values = declared_overlap(contract)

    if strategy == STRATEGY_SERIAL:
        if cursor_database_assigned is False:
            return CommitSafeWindow(
                strategy=strategy,
                stored_watermark=stored,
                seed=stored,
                guarantee=GUARANTEE_WITHIN_WINDOW,
                reason=_SERIAL_APP_ASSIGNED,
                census_recommended=True,
            )
        return CommitSafeWindow(
            strategy=strategy,
            stored_watermark=stored,
            seed=stored,
            guarantee=GUARANTEE_COMMIT_SAFE,
            reason="the source runs one write transaction at a time and assigns this cursor "
            "itself: commit order is write order",
        )

    fallback = product_strategy_note(source_type) if strategy == STRATEGY_OVERLAP else ""
    probe_error = ""
    if strategy == STRATEGY_PG and relation_kind is not None and relation_kind not in _TABLE_RELKINDS:
        fallback = not_a_table_note(relation_kind)
        if not relation_kind:
            probe_error = "the source relation's kind could not be read"
    elif strategy == STRATEGY_PG and capture_bound is not None:
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
        # to the re-read window below; a horizon that could not be read is said.
        fallback = "the source's snapshot horizon could not be read"
        probe_error = str(bound.get("error") or "")
        if probe_error:
            probe_error = f"snapshot horizon unreadable: {probe_error}"

    if strategy in (STRATEGY_OVERLAP, STRATEGY_PG):
        # PostgreSQL lands here only when its bound could not be captured, or
        # when the source is not a table.
        lead = f"{fallback}; " if fallback else ""
        if not can_replay:
            return _unguarded(
                stored,
                lead + "this route appends without a key, so a re-read window cannot be "
                "de-duplicated",
                probe_error=probe_error,
            )
        unit = cursor_distance_unit(cursor_type, cursor_part(stored) if stored else None)
        size = seconds if unit == "seconds" else values if unit == "values" else 0
        if not unit or size <= 0:
            return _unguarded(
                stored,
                lead
                + (
                    "no re-read window is declared for this cursor"
                    if unit
                    else "this cursor type has no distance a re-read window can be measured in"
                ),
                probe_error=probe_error,
            )
        seed = overlap_seed(stored, cursor_type, seconds=seconds, values=values) if stored else None
        if stored and seed is None:
            return _unguarded(
                stored,
                lead + "the stored watermark cannot be shifted by the re-read window",
                probe_error=probe_error,
            )
        if isinstance(seed, ClampedSeed):
            lead += (
                "the re-read window reaches past the earliest representable time, so it "
                "is clamped there; "
            )
        return CommitSafeWindow(
            strategy=STRATEGY_OVERLAP,
            stored_watermark=stored,
            seed=ReplaySeed(seed, cursor_part(stored)) if stored else None,
            replay_through=cursor_part(stored) if stored else "",
            overlap={"unit": unit, "size": int(size)},
            guarantee=GUARANTEE_WITHIN_WINDOW,
            reason=(
                lead
                + f"each run re-reads the last {size:,} {unit} behind the watermark and "
                "collapses re-reads on the row identity; a transaction that stays open "
                "longer than that is not re-read"
            ),
            census_recommended=True,
            probe_error=probe_error,
        )

    return _unguarded(stored, "this source exposes no commit-order bound")


def not_a_table_note(relation_kind: str) -> str:
    """Why a PostgreSQL source of this ``relkind`` gets no snapshot horizon."""
    name = _RELKIND_NAMES.get(relation_kind or "")
    if name:
        return (
            f"the source is {name}, whose rows carry no transaction id the "
            "snapshot horizon can bound"
        )
    return (
        "the source could not be confirmed to be a table, and only a table's rows "
        "carry the transaction id the snapshot horizon bounds"
    )


def unguarded_reason(why: str) -> str:
    """The "not guaranteed" sentence every unguarded route states, with its cause."""
    return f"{_UNGUARDED_REASON} — {why}. Run the periodic key census to detect them."


def _unguarded(stored: str | None, why: str, *, probe_error: str = "") -> CommitSafeWindow:
    return CommitSafeWindow(
        strategy=STRATEGY_NONE,
        stored_watermark=stored,
        seed=stored,
        guarantee=GUARANTEE_NONE,
        reason=unguarded_reason(why),
        census_recommended=True,
        probe_error=probe_error,
    )


def unplanned_window(
    stored: str | None, previous_bound: Mapping[str, Any] | None, exc: BaseException
) -> CommitSafeWindow:
    """The window of a run whose plan itself failed: it reads from the watermark,
    keeps the stored bound for the next run, and says it is not guaranteed."""
    previous = dict(previous_bound or {})
    return CommitSafeWindow(
        strategy=STRATEGY_NONE,
        stored_watermark=stored,
        seed=stored,
        bound=previous,
        previous_bound=previous,
        guarantee=GUARANTEE_NONE,
        reason=(
            f"{_UNGUARDED_REASON} — the commit-safe window could not be planned this run "
            f"({type(exc).__name__}); the run reads from the watermark and the next run "
            "plans again. Run the periodic key census to detect them."
        ),
        census_recommended=True,
        probe_error=f"{type(exc).__name__}: {exc}"[:300],
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
            since_xmin=str(previous["xmin"]), below=cursor_part(stored), cursor_type=cursor_type,
            bound=previous,
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
            probe_error=f"{type(exc).__name__}: {exc}"[:300],
        )
    if late.unprobed:
        # The bound moves on: the next run could not check the range either.
        return CommitSafeWindow(
            **base,
            late_row_rule=late.rule,
            guarantee=GUARANTEE_NONE,
            reason=unguarded_reason(late.unprobed),
            census_recommended=True,
        )
    floor_note = (
        f" (probe looks back to cursor {late.floor}; a row committed with an older "
        "cursor value than that is only found by the key census)"
        if late.floor
        else ""
    )
    if previous.get("xip_truncated"):
        floor_note += (
            " (the snapshot's in-progress list was too long to store, so rows are "
            "judged against its horizon)"
        )
    elif late.rule == "horizon" and previous.get("horizon_reason"):
        floor_note += (
            f" (rows are judged against the snapshot's horizon: "
            f"{previous['horizon_reason']})"
        )
    judged = dict(
        late_rows_detected=int(late.count),
        late_rows_checked=late.checked,
        late_row_rule=late.rule,
    )
    changed = (
        f"{int(late.count):,} row(s) changed or committed behind the watermark since "
        "the last read"
    )
    if late.count and late.undelivered == 0 and late.checked == late.count:
        # Every one of those row versions is already at the destination: rows
        # edited in place without moving the cursor, or delivered by the read
        # that saw them. Out-of-window drift (D188), not a gap — nothing to
        # re-read, and the bound moves on.
        return CommitSafeWindow(
            **base,
            **judged,
            late_rows_undelivered=0,
            guarantee=GUARANTEE_WITHIN_WINDOW if late.floor else GUARANTEE_COMMIT_SAFE,
            reason=(
                f"{changed}, and the destination already holds each of them at that "
                "cursor value (edited in place, or already delivered) — nothing to "
                "re-read" + floor_note
            ),
            census_recommended=bool(late.floor),
        )
    if not late.count:
        return CommitSafeWindow(
            **base,
            **judged,
            guarantee=GUARANTEE_WITHIN_WINDOW if late.floor else GUARANTEE_COMMIT_SAFE,
            reason="no row changed or was committed behind the watermark since the last "
            "read" + floor_note,
            census_recommended=bool(late.floor),
        )
    if not can_replay:
        # Counted and reported (evidence and a Gate-8 warning) this run; the
        # bound then moves on, so the probe's range does not grow run after run.
        return CommitSafeWindow(
            **base,
            **judged,
            guarantee=GUARANTEE_NONE,
            reason=(
                f"{changed}, and {_UNGUARDED_REASON.split(':', 1)[0]}: this route "
                "appends without a key, so they cannot be re-read without duplicating "
                "rows already delivered. Run the key census, or give the stream a key."
            ),
            census_recommended=True,
        )
    if late.min_cursor in (None, ""):
        return CommitSafeWindow(
            **{**base, "bound": previous},
            **judged,
            guarantee=GUARANTEE_NONE,
            reason=unguarded_reason(
                f"{changed}, but the lowest cursor among them could not be read; the stored "
                "bound is kept and the next run asks again"
            ),
            census_recommended=True,
        )
    if late.undelivered:
        where = (
            f"the destination lacks the one at cursor {late.min_cursor}, the first gap in "
            "cursor order"
        )
    elif late.undelivered == 0 and int(late.checked or 0) < int(late.count):
        # A check that stopped short proves nothing about the rest (review M-4).
        where = (
            f"only {int(late.checked or 0):,} of them could be checked against the "
            "destination"
        )
    elif late.undelivered == 0:
        where = "not all of them could be checked against the destination"
    else:
        where = "they could not be checked against the destination"
    return CommitSafeWindow(
        **{**base, "seed": ReplaySeed(str(late.min_cursor), cursor_part(stored))},
        **judged,
        replay_through=cursor_part(stored),
        late_rows_undelivered=late.undelivered,
        guarantee=GUARANTEE_WITHIN_WINDOW if late.floor else GUARANTEE_COMMIT_SAFE,
        reason=(
            f"{changed}; {where}, so this run re-reads from there and collapses "
            "re-reads on the row identity" + floor_note
        ),
        census_recommended=bool(late.floor),
    )


# --------------------------------------------------------------------------
# PostgreSQL engine access
# --------------------------------------------------------------------------


def probe_timeout_ms() -> int:
    """Statement timeout of every commit-safe probe connection (``0`` = none)."""
    return _env_int(PROBE_TIMEOUT_ENV, DEFAULT_PROBE_TIMEOUT_MS)


def _pg_connect(src_cfg: Mapping[str, Any]) -> Any:
    """An autocommit probe connection whose statements are bounded in time.

    A probe that runs out of time raises; the caller records it as a probe
    failure, which the run states ("not guaranteed this run") and Gate-8
    carries as a warning — never a silent pass.
    """
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=str(src_cfg.get("host") or ""),
        port=int(src_cfg.get("port") or 5432),
        database=str(src_cfg.get("database") or ""),
        username=str(src_cfg.get("username") or src_cfg.get("user") or ""),
        password=str(src_cfg.get("password") or ""),
        connection_string=str(src_cfg.get("connection_string") or ""),
        ssl=bool(src_cfg.get("ssl", False)),
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (str(probe_timeout_ms()),))
    except Exception:
        conn.close()
        raise
    return conn


_PG_SNAPSHOT_SQL = (
    "SELECT txid_snapshot_xmin(s)::text, txid_snapshot_xmax(s)::text, "
    "ARRAY(SELECT x::text FROM txid_snapshot_xip(s) AS x LIMIT %s) "
    "FROM (SELECT txid_current_snapshot() AS s) AS q"
)
#: Subtransactions open under the snapshot's in-progress transactions, read
#: right after it. A subtransaction's xid is never in the in-progress list.
#: Matched on the backend's 32-bit xid; the matched count shows whether every
#: listed transaction is still running in a backend (a prepared or finished
#: one is not, and the count could miss its subtransactions).
_PG_SUBXACT_SQL = (
    "SELECT count(DISTINCT a.backend_xid::text), coalesce(sum(st.subxact_count), 0)::bigint, "
    "coalesce(bool_or(st.subxact_overflowed), false) "
    "FROM pg_stat_get_backend_idset() AS b "
    "CROSS JOIN LATERAL pg_stat_get_backend_subxact(b) AS st "
    "JOIN pg_catalog.pg_stat_activity AS a ON a.pid = pg_stat_get_backend_pid(b) "
    "WHERE a.backend_xid::text::bigint = ANY(%s::bigint[])"
)
#: ``(relkind, estimated rows)`` of a relation sized by its whole inheritance
#: tree: a parent whose rows live in children (a Timescale hypertable, a legacy
#: partitioned table) has ``reltuples`` 0 itself (review Low). Members with
#: storage are summed; one never analysed (``-1``), or a tree with no storage,
#: makes the size unknown (``-1``), which the probe treats as large.
_PG_RELATION_SIZE_SQL = (
    "WITH RECURSIVE tree(oid) AS ("
    " SELECT c.oid FROM pg_catalog.pg_class c"
    " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
    " WHERE n.nspname = %s AND c.relname = %s"
    " UNION SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN tree t ON i.inhparent = t.oid)"
    " SELECT (SELECT c.relkind::text FROM pg_catalog.pg_class c"
    "         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
    "         WHERE n.nspname = %s AND c.relname = %s),"
    " CASE WHEN count(*) FILTER (WHERE c.relkind <> 'p') = 0"
    "        OR bool_or(c.relkind <> 'p' AND c.reltuples < 0) THEN -1"
    "      ELSE sum(c.reltuples) FILTER (WHERE c.relkind <> 'p') END::bigint"
    " FROM tree t JOIN pg_catalog.pg_class c ON c.oid = t.oid"
)


def late_row_rule(bound: Mapping[str, Any]) -> tuple[str, int, tuple[int, ...]]:
    """How the next run judges rows behind the watermark against ``bound``.

    ``("in_progress", xmax, xip)``: a row is new since the bound when its writer
    was in progress at capture (in ``xip``) or started after it (at or above
    ``xmax``). Every other id below ``xmax`` had finished, so its rows were
    visible to the read. Used only when the stored list is complete and no
    subtransaction was open at capture — a subtransaction's id is never listed.

    ``("horizon", xmin, ())``: every row written at or above the oldest
    in-progress id (the safe superset).
    """
    xmin = int(str(bound.get("xmin") or 0))
    xmax = str(bound.get("xmax") or "")
    if (
        bound.get("xip_complete") is True
        and bound.get("open_subxacts") == 0
        and xmax.isdigit()
    ):
        return "in_progress", int(xmax), tuple(int(x) for x in bound.get("xip") or ())
    return "horizon", xmin, ()


def capture_pg_bound(src_cfg: Mapping[str, Any]) -> dict[str, Any]:
    """The snapshot horizon of the source right now (``{}`` when unreadable).

    ``txid_current_snapshot`` rather than ``pg_current_snapshot``: the same
    64-bit values, and it exists on every supported server version.
    """
    try:
        conn = _pg_connect(src_cfg)
    except Exception as exc:  # noqa: BLE001 — the route then states it is unguarded
        logger.warning("commit-safe window: source connect failed: %s", exc)
        return {"error": f"{type(exc).__name__}: {exc}"[:300]}
    open_subxacts: int | None = None
    notes: list[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(_PG_SNAPSHOT_SQL, (PG_XIP_MAX + 1,))
            row = cur.fetchone()
            xip = [str(x) for x in (row[2] if row else None) or []]
            if row and row[0] and len(xip) <= PG_XIP_MAX:
                open_subxacts = _open_subxacts(cur, xip, notes)
    except Exception as exc:  # noqa: BLE001
        logger.warning("commit-safe window: snapshot horizon unreadable: %s", exc)
        return {"error": f"{type(exc).__name__}: {exc}"[:300]}
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("commit-safe window: close skipped", exc_info=True)
    if not row or not row[0]:
        return {}
    bound: dict[str, Any] = {
        "strategy": STRATEGY_PG,
        "xmin": str(row[0]),
        "xmax": str(row[1] or ""),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    if len(xip) <= PG_XIP_MAX:
        bound.update(xip=xip, xip_complete=True, open_subxacts=open_subxacts)
        if notes:
            # Why the next run judges rows on the horizon, in its evidence.
            bound["horizon_reason"] = notes[0]
    else:
        # Too long to store: the next run judges rows against the horizon,
        # and its evidence says so.
        bound.update(xip_complete=False, xip_truncated=True)
    return bound


#: Evidence wording when the server cannot report open subtransactions.
SUBXACT_FUNCTION_MISSING = (
    "this server cannot report open subtransactions (pg_stat_get_backend_subxact "
    "needs PostgreSQL 16 or later)"
)
SUBXACT_UNMATCHED = (
    "a transaction in progress at the capture was not found running in a backend "
    "(prepared, finished, or not visible to this role), so its open subtransactions "
    "could not be counted"
)


def _open_subxacts(cur: Any, xip: list[str], notes: list[str] | None = None) -> int | None:
    """Subtransactions open under the snapshot's in-progress transactions, or None.

    Read after the snapshot. A subtransaction is counted while its top-level
    transaction runs, so the count covers the snapshot's only when every listed
    transaction is still found running in a backend; otherwise (finished,
    prepared, or not visible to this role) the answer is None and the horizon
    rule applies. Needs PostgreSQL 16 (``pg_stat_get_backend_subxact``); on an
    older server the answer is None as well. ``notes`` receives why the answer
    is None, for the run's evidence.
    """
    said = notes if notes is not None else []
    wanted = sorted({int(x) % 4294967296 for x in xip})
    if not wanted:
        return 0
    try:
        cur.execute(_PG_SUBXACT_SQL, (wanted,))
        matched, count, overflowed = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 — unknown, so the horizon rule applies
        logger.info("commit-safe window: open subtransactions unreadable: %s", exc)
        missing = str(getattr(exc, "pgcode", "") or "") == "42883" or (
            "pg_stat_get_backend_subxact" in str(exc) and "does not exist" in str(exc)
        )
        said.append(
            SUBXACT_FUNCTION_MISSING
            if missing
            else f"open subtransactions could not be read ({type(exc).__name__})"
        )
        return None
    if int(matched or 0) != len(wanted):
        said.append(SUBXACT_UNMATCHED)
        return None
    return int(count or 0) + (1 if overflowed else 0)


def pg_relation_kind(src_cfg: Mapping[str, Any], schema: str, table: str) -> str:
    """``pg_class.relkind`` of the source relation, or ``""`` when it cannot be read.

    Never raises: an unreadable kind is planned as "not a table" and said.
    """
    return pg_relation_facts(src_cfg, schema, table)[0]


def pg_relation_facts(
    src_cfg: Mapping[str, Any], schema: str, table: str
) -> tuple[str, int | None]:
    """``(relkind, reltuples)`` of the source relation; ``("", None)`` when unreadable."""
    from connectors.sql_identifiers import split_qualified_table

    qualified, bare = split_qualified_table(table, (schema or "").strip() or None)
    try:
        conn = _pg_connect(src_cfg)
    except Exception as exc:  # noqa: BLE001 — planned as "not a table", and said
        logger.warning("commit-safe window: relation kind unreadable: %s", exc)
        return "", None
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(_PG_RELATION_SIZE_SQL, (qualified or "public", bare) * 2)
            row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        logger.warning("commit-safe window: relation kind unreadable: %s", exc)
        return "", None
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("commit-safe window: close skipped", exc_info=True)
    if not row or not row[0]:
        return "", None
    return str(row[0]), (int(row[1]) if row[1] is not None else None)


def sqlite_cursor_database_assigned(src_cfg: Mapping[str, Any], table: str, column: str) -> bool:
    """True when SQLite assigns ``column`` itself: an ``INTEGER PRIMARY KEY AUTOINCREMENT``.

    Any other column — a plain rowid alias included, which takes an explicit
    value as readily — can be set by the application before its row is
    written. Unreadable is answered False (the weaker claim).
    """
    import re as _re

    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table

    _schema, bare = split_qualified_table(table, None)
    try:
        with _engine(dict(src_cfg)).connect() as conn:
            row = conn.execute(
                sa.text("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :t"),
                {"t": bare},
            ).first()
    except Exception as exc:  # noqa: BLE001 — unreadable: the weaker claim
        logger.info("commit-safe window: SQLite cursor definition unreadable: %s", exc)
        return False
    ddl = str(row[0] or "") if row else ""
    name = _re.escape(column)
    pattern = (
        rf"(?:^|[,(]\s*)[\"`\[]?{name}[\"`\]]?\s+INTEGER\s+PRIMARY\s+KEY"
        rf"(?:\s+(?:ASC|DESC))?\s+AUTOINCREMENT\b"
    )
    return bool(_re.search(pattern, ddl, flags=_re.IGNORECASE))


def probe_pg_late_rows(
    src_cfg: Mapping[str, Any],
    *,
    schema: str,
    table: str,
    cursor_column: str,
    since_xmin: str,
    below: str,
    cursor_type: Any = None,
    identity_columns: Sequence[str] = (),
    bound: Mapping[str, Any] | None = None,
    undelivered: Callable[[list[tuple[str, tuple[str, ...]]]], list[Any]] | None = None,
) -> LateRows:
    """Rows behind ``below`` that changed or were committed since the stored bound.

    ``bound`` is the stored snapshot (:func:`late_row_rule` decides how it is
    read; without one, ``since_xmin`` is the horizon). A row's ``xmin`` is a
    32-bit id; it is widened against the snapshot's own 64-bit ``xmax`` (every
    stored row was written before it). A frozen row whose old id aliases into
    the range is a false positive only: it is checked and, if delivered, kept.

    With ``identity_columns`` (a whole unique key) and ``undelivered`` (which of
    a page of ``(cursor, key)`` rows the destination lacks, in order), the rows
    are checked page by page in cursor order until the first one the
    destination lacks: its cursor is ``min_cursor``. The count and every page
    read one REPEATABLE READ snapshot; every page is bounded and
    runs under the probe connection's statement timeout.

    On a large table the probe is floored at the PostgreSQL look-back window so
    it stays an index range scan; a probe that cannot be floored on such a table
    still runs (it is the only bound there is) and says so in the log.
    """
    from psycopg2 import sql

    from connectors.postgresql_reader import _cell

    rule, edge, xip = late_row_rule(dict(bound or {}) or {"xmin": since_xmin})
    conn = _pg_connect(src_cfg)
    checked: int | None = None
    missing: int | None = None
    first_gap: str | None = None
    try:
        with conn.cursor() as cur:
            # One snapshot for the count and every page of the walk (review 3,
            # High): read separately, a concurrent in-place edit moves the
            # candidate set between them, the walk never matches the count,
            # and every run of a busy table re-read it from its oldest edit.
            cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute(_PG_RELATION_SIZE_SQL, (schema or "public", table) * 2)
            est = cur.fetchone()
            est_rows = int(est[1]) if est and est[1] is not None else -1
            floor = ""
            # -1: never analysed, or a partitioned parent — size unknown, so
            # treated as large (review M5): an unfloored probe could scan it all.
            if est_rows > PG_UNBOUNDED_PROBE_ROWS or est_rows < 0:
                floor = probe_floor(below, cursor_type)
                if not floor:
                    # Not run: it would scan the whole table under the probe
                    # timeout, every run (review M-3). Said, as preflight says it.
                    logger.info(
                        "commit-safe window: late-commit probe on %s.%s (~%d rows) not run: "
                        "the cursor has no unit to floor it by", schema, table, est_rows,
                    )
                    return LateRows(rule=rule, unprobed=whole_table_scan_note(est_rows))
            ident = sql.Identifier(cursor_column)
            rel = sql.SQL("{s}.{t}").format(
                s=sql.Identifier(schema or "public"), t=sql.Identifier(table)
            )
            conds = [sql.SQL("t.{c} < %s").format(c=ident)]
            params: list[Any] = [below]
            if floor:
                conds.append(sql.SQL("t.{c} >= %s").format(c=ident))
                params.append(floor)
            widened = sql.SQL("(snap.m - ((snap.m - t.xmin::text::bigint) & 4294967295))")
            if rule == "in_progress":
                # Written by a transaction that started after the capture, or
                # was still running at it.
                conds.append(
                    sql.SQL("(") + widened + sql.SQL(" >= %s OR ") + widened
                    + sql.SQL(" = ANY(%s::bigint[]))")
                )
                params.extend([edge, list(xip)])
            else:
                conds.append(widened + sql.SQL(" >= %s"))
                params.append(edge)
            snap = sql.SQL("WITH snap AS (SELECT txid_snapshot_xmax(txid_current_snapshot()) AS m) ")
            where = sql.SQL(" AND ").join(conds)
            cur.execute(
                snap
                + sql.SQL("SELECT min(t.{c}), count(*) FROM {r} t, snap WHERE ").format(c=ident, r=rel)
                + where,
                params,
            )
            row = cur.fetchone()
            key_cols = [str(k) for k in identity_columns or () if k]
            if key_cols and undelivered is not None and row and int(row[1] or 0) > 0:
                order = [ident, *(sql.Identifier(k) for k in key_cols)]
                cols = sql.SQL(", ").join(sql.SQL("t.{}").format(o) for o in order)
                after = sql.SQL("({}) > ({})").format(
                    cols, sql.SQL(", ").join(sql.Placeholder() for _ in order)
                )
                checked, last = 0, None
                try:
                    while True:
                        page_where = where if last is None else where + sql.SQL(" AND ") + after
                        cur.execute(
                            snap
                            + sql.SQL("SELECT {cols} FROM {r} t, snap WHERE ").format(cols=cols, r=rel)
                            + page_where
                            + sql.SQL(" ORDER BY {cols} LIMIT %s").format(cols=cols),
                            [*params, *(last or ()), PG_CHECK_PAGE_ROWS],
                        )
                        rows = cur.fetchall()
                        page = [(_cell(r[0]), tuple(_cell(v) for v in r[1:])) for r in rows]
                        if not page:
                            missing = 0
                            break
                        lacking = undelivered(page)
                        checked += len(page)
                        if lacking:
                            # The first gap in cursor order seeds the re-read.
                            missing, first_gap = len(lacking), str(lacking[0][0])
                            break
                        if len(page) < PG_CHECK_PAGE_ROWS:
                            missing = 0
                            break
                        last = rows[-1]
                except Exception as exc:  # noqa: BLE001 — unchecked rows all count
                    logger.warning("commit-safe window: destination check stopped: %s", exc)
                    missing, first_gap = None, None
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("commit-safe window: close skipped", exc_info=True)
    count = int(row[1] or 0) if row else 0
    low = _cell(row[0]) if row and row[0] is not None else None
    if missing == 0 and checked == count:
        low = None
    elif missing:
        low = first_gap
    return LateRows(
        count=count, min_cursor=low, floor=floor, undelivered=missing,
        checked=checked if missing is not None else None, rule=rule,
    )


def whole_table_scan_note(relation_rows: int | None) -> str:
    """Why a large table's late-commit check is not run for a cursor with no unit."""
    size = (
        "not yet analysed, so its size is unknown"
        if relation_rows is None or relation_rows < 0
        else f"large (about {int(relation_rows):,} rows)"
    )
    return (
        f"the late-commit check would scan the whole table: the table is {size}, and "
        "this cursor type has no unit (a number, a time or a date) the check can be "
        "limited by"
    )


def static_commit_safety(
    *,
    source_type: str,
    sync_mode: str,
    cursor_column: str,
    cursor_type: Any = None,
    has_identity: bool,
    contract: Mapping[str, Any] | None = None,
    relation_kind: str | None = None,
    relation_rows: int | None = None,
    cursor_database_assigned: bool | None = None,
) -> dict[str, Any]:
    """What a route may claim about late commits, judged without reading rows.

    Preflight and the cursor-semantics verdict use this; a run executes the
    same decision with the engine's bound filled in (:func:`plan_commit_safe_window`).
    ``relation_kind``: the PostgreSQL source's ``pg_class.relkind`` when
    preflight read it (``""`` = unreadable); only a table gets the horizon.
    ``relation_rows``: its row estimate (``-1`` = never analysed, None =
    unknown); beyond the probe's unbounded size the claim is the look-back.
    ``cursor_database_assigned`` (SQLite): only a cursor the source assigns
    itself is commit-safe there; unknown is judged as application-assigned.
    """
    from services.sync_cursor import normalize_sync_mode, requires_incremental

    if not requires_incremental(normalize_sync_mode(sync_mode)) or not (cursor_column or "").strip():
        return {}
    strategy = commit_safety_strategy(source_type)
    seconds, values = declared_overlap(contract)
    overlap: dict[str, Any] = {}
    note = product_strategy_note(source_type)
    lead = note[0].upper() + note[1:] + ". " if note else ""
    if strategy == STRATEGY_PG and relation_kind is not None and relation_kind not in _TABLE_RELKINDS:
        note = not_a_table_note(relation_kind)
        lead = note[0].upper() + note[1:] + ". "
        strategy = STRATEGY_OVERLAP
    census = "Run the periodic key census (key_census_every) to detect skipped rows."
    keyless = (
        "this route appends without a key, so rows committed behind the watermark "
        "cannot be re-read without duplicating rows already delivered. Give the "
        f"stream a key. {census}"
    )
    unit = probe_floor_unit(cursor_type) if _type_base(cursor_type) else "?"
    lookback = {
        "values": f"{PG_LOOKBACK_VALUES:,} values",
        "seconds": f"{PG_LOOKBACK_SECONDS // 3600} hours",
        "days": f"{PG_LOOKBACK_DAYS:,} day" + ("s" if PG_LOOKBACK_DAYS != 1 else ""),
    }.get(
        unit,
        f"{PG_LOOKBACK_VALUES:,} values, {PG_LOOKBACK_SECONDS // 3600} hours or "
        f"{PG_LOOKBACK_DAYS:,} day",
    ) + " behind the watermark"
    floored = unit != ""
    large = relation_rows is not None and (
        relation_rows < 0 or relation_rows > PG_UNBOUNDED_PROBE_ROWS
    )
    if strategy == STRATEGY_SERIAL and cursor_database_assigned is True:
        guarantee = GUARANTEE_COMMIT_SAFE
        statement = (
            "The source commits one write transaction at a time and assigns this cursor "
            "itself (AUTOINCREMENT): nothing commits behind the watermark."
        )
    elif strategy == STRATEGY_SERIAL:
        guarantee = GUARANTEE_WITHIN_WINDOW
        statement = "Late commits are covered " + _SERIAL_APP_ASSIGNED + "."
    elif strategy == STRATEGY_PG and has_identity and large and not floored:
        guarantee = GUARANTEE_NONE
        statement = (
            "Cannot guarantee no skipped rows: "
            + whole_table_scan_note(relation_rows)
            + f", so it is not run. {census}"
        )
    elif strategy == STRATEGY_PG and has_identity and floored and large:
        guarantee = GUARANTEE_WITHIN_WINDOW
        statement = (
            f"Late commits are covered within a look-back of {lookback}: the table is "
            + ("not yet analysed, so its size is unknown" if relation_rows < 0 else "large")
            + ", so each run's late-commit probe is limited to that range, and a row "
            f"committed further behind is found only by the key census. {census}"
        )
    elif strategy == STRATEGY_PG and has_identity:
        guarantee = GUARANTEE_COMMIT_SAFE
        statement = (
            "Late commits are covered: each run records the PostgreSQL snapshot horizon "
            "and the next run re-reads rows committed behind the watermark since then"
            + (
                f" (on a table above {PG_UNBOUNDED_PROBE_ROWS:,} rows, or one not yet "
                f"analysed, the probe looks back {lookback})."
                if relation_rows is None and floored
                else (
                    f" (on a table above {PG_UNBOUNDED_PROBE_ROWS:,} rows, or one not yet "
                    "analysed, the check would scan the whole table for this cursor type, "
                    "so it is not run and the route is not guaranteed)."
                )
                if relation_rows is None
                else "."
            )
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
    head = "Cannot guarantee no skipped rows: "
    if lead and statement.startswith(head):
        # The refusal leads; the reason the horizon does not apply follows it.
        statement = head + lead + statement[len(head):]
    elif lead:
        statement = lead + statement
    return {
        "strategy": strategy,
        "guarantee": guarantee,
        "statement": statement,
        "census_recommended": guarantee in (GUARANTEE_NONE, GUARANTEE_WITHIN_WINDOW),
        "overlap": overlap,
    }
