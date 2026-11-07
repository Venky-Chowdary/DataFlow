"""One transaction rule for every PostgreSQL step that may fail and continue.

PostgreSQL aborts the whole transaction on the first failed statement: every
later statement on that connection answers "current transaction is aborted,
commands ignored until end of transaction block" until a ROLLBACK. A writer
that catches a failure and carries on (a fidelity CREATE falling back to a
types-only CREATE, an optional COMMENT, a probe) therefore has to undo the
failed statement first, or it turns one real, fixable error into an opaque
"aborted" a few statements later — which is what QA saw on MariaDB -> PG,
PG -> pgvector and mirror soft (round 11).

* :func:`savepoint` — run statements inside ``SAVEPOINT`` / ``ROLLBACK TO``;
  the failure is re-raised with the transaction still usable.
* :func:`tolerated` — the same for a step whose failure must not stop the
  load: logged, recorded, and swallowed only after the rollback.
* :func:`note_first_error` / :func:`explain_aborted` — the first real error
  that left a connection's transaction aborted is kept, so a later "aborted"
  names its cause in the job record.

Redshift has no savepoints, and an autocommit connection has no transaction to
protect; both run the statements bare (each statement is its own unit there).
"""

from __future__ import annotations

import logging
import uuid
import weakref
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)

_ABORTED = "current transaction is aborted"
_REDSHIFT = frozenset({"redshift", "amazon_redshift", "redshift_serverless"})

#: First real error per connection (weak keys: a closed connection forgets it).
_FIRST_ERRORS: "weakref.WeakKeyDictionary[Any, str]" = weakref.WeakKeyDictionary()
_FIRST_ERRORS_BY_ID: dict[int, str] = {}


def _connection_of(cursor: Any) -> Any:
    return getattr(cursor, "connection", None)


def savepoints_apply(cursor: Any, engine: str = "") -> bool:
    """True when a savepoint can (and needs to) protect this cursor's transaction."""
    if (engine or "").strip().lower() in _REDSHIFT:
        return False
    conn = _connection_of(cursor)
    try:
        return not bool(getattr(conn, "autocommit", False))
    except Exception:  # noqa: BLE001 - a closed connection has no transaction
        return False


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip() or type(exc).__name__
    return text.splitlines()[0][:400]


def note_first_error(conn: Any, exc: BaseException, label: str = "") -> None:
    """Remember the first real (non-"aborted") error raised on ``conn``."""
    if conn is None or _ABORTED in str(exc):
        return
    text = f"{label}: {_first_line(exc)}" if label else _first_line(exc)
    try:
        _FIRST_ERRORS.setdefault(conn, text)
    except TypeError:  # not weak-referenceable: keep it by id for this process
        _FIRST_ERRORS_BY_ID.setdefault(id(conn), text)


def first_error(conn: Any) -> str:
    if conn is None:
        return ""
    try:
        found = _FIRST_ERRORS.get(conn)
    except TypeError:
        found = None
    return found or _FIRST_ERRORS_BY_ID.get(id(conn), "")


def forget_errors(conn: Any) -> None:
    try:
        _FIRST_ERRORS.pop(conn, None)
    except TypeError:
        pass
    _FIRST_ERRORS_BY_ID.pop(id(conn), None)


def explain_aborted(exc: BaseException, conn: Any) -> str:
    """The job's error text: an "aborted" names the first real error before it."""
    text = str(exc)
    if _ABORTED not in text:
        return text
    cause = first_error(conn)
    if not cause:
        return text
    return (
        f"{cause} (PostgreSQL then refused every later statement in the same "
        f"transaction: {text.strip().splitlines()[0]})"
    )


@contextmanager
def savepoint(cursor: Any, *, engine: str = "", label: str = "") -> Iterator[None]:
    """Run the block under a savepoint; on failure roll back to it and re-raise."""
    if not savepoints_apply(cursor, engine):
        try:
            yield
        except BaseException as exc:
            note_first_error(_connection_of(cursor), exc, label)
            raise
        return
    name = f"df_sp_{uuid.uuid4().hex[:10]}"
    cursor.execute(f"SAVEPOINT {name}")
    try:
        yield
    except BaseException as exc:
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")
            cursor.execute(f"RELEASE SAVEPOINT {name}")
        except Exception as rollback_exc:  # noqa: BLE001 - the original error wins
            # The transaction stays aborted: this error is what later
            # "aborted" statements must name.
            note_first_error(_connection_of(cursor), exc, label)
            logger.warning("rollback to savepoint %s failed: %s", name, rollback_exc)
        raise
    else:
        cursor.execute(f"RELEASE SAVEPOINT {name}")


@contextmanager
def tolerated(cursor: Any, label: str, *, engine: str = "", level: int = logging.DEBUG) -> Iterator[None]:
    """A step whose failure must not stop the load — and must not poison it.

    The failure is rolled back to a savepoint before it is swallowed, so the
    next statement on the connection still runs.
    """
    try:
        with savepoint(cursor, engine=engine, label=label):
            yield
    except Exception as exc:  # noqa: BLE001 - tolerated by contract, logged
        logger.log(level, "%s skipped: %s", label, exc, exc_info=level <= logging.DEBUG)
