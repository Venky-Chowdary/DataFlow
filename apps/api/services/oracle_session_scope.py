"""Whether this run writes Oracle instants on a confirmed UTC session.

The grade of an offset or instant source into an Oracle ``TIMESTAMP WITH
[LOCAL] TIME ZONE`` column depends on the destination session, not on the
type pair (accuracy review finding 8, :mod:`connectors.oracle_session`). The
run binds what the destination's session read back; the classifier reads it:

* pinned and read back as UTC: the writer binds UTC digits on a UTC session,
  the instant is kept (an offset label, where the source had one, is
  disclosed, D136), no Risk Contract;
* anything else (statement refused, read-back not UTC, not checked, no
  connection): the instant may move, the pair needs a Risk Contract, and the
  reason says why.

A same-instance Oracle to Oracle copy runs server side (``INSERT ... SELECT``)
and is not judged by the writer's session.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from typing import Any, Iterator, Mapping

_ORACLE = frozenset({"oracle", "oracledb", "oracle_db", "oracle_autonomous", "oracle_autonomous_warehouse",
                     "amazon_rds_oracle"})
_UNCHECKED = (False, "the Oracle destination session time zone was not checked")
_PIN: ContextVar[tuple[bool, str] | None] = ContextVar("df_oracle_utc_pin", default=None)
_SAME_INSTANCE: ContextVar[bool] = ContextVar("df_oracle_same_instance", default=False)


def is_oracle(db_type: Any) -> bool:
    return str(db_type or "").strip().lower() in _ORACLE


@contextmanager
def bind_oracle_utc_pin(confirmed: bool, reason: str, *, same_instance: bool = False) -> Iterator[None]:
    pin = _PIN.set((bool(confirmed), str(reason or "")))
    same = _SAME_INSTANCE.set(bool(same_instance))
    try:
        yield
    finally:
        _SAME_INSTANCE.reset(same)
        _PIN.reset(pin)


def oracle_utc_pin() -> tuple[bool, str]:
    """``(confirmed, reason)`` for this run's Oracle destination session."""
    return _PIN.get() or _UNCHECKED


def oracle_pin_bound() -> bool:
    return _PIN.get() is not None


def oracle_same_instance() -> bool:
    return _SAME_INSTANCE.get()


def probe_oracle_utc_pin(cfg: Mapping[str, Any]) -> tuple[bool, str]:
    """Open the destination's engine and read the session zone back. Never raises."""
    try:
        from connectors.generic_sql import _engine
        from connectors.oracle_session import confirm_utc_session

        with _engine(dict(cfg)).connect() as conn:
            return confirm_utc_session(conn)
    except Exception as exc:  # noqa: BLE001 - no session, no confirmed pin
        return False, f"the Oracle destination session could not be opened ({type(exc).__name__})"


def _identity(cfg: Mapping[str, Any] | None) -> tuple[str, str, str]:
    cfg = cfg or {}
    service = str(cfg.get("service_name") or cfg.get("database") or "").strip().upper()
    return (str(cfg.get("host") or "").strip().lower(), str(cfg.get("port") or "1521"), service)


def same_oracle_instance(source_db: Any, source_cfg: Mapping[str, Any] | None,
                         dest_db: Any, dest_cfg: Mapping[str, Any] | None) -> bool:
    """Both endpoints Oracle on one host, port and service (the server-side copy)."""
    if not (is_oracle(source_db) and is_oracle(dest_db) and source_cfg and dest_cfg):
        return False
    ident = _identity(source_cfg)
    return bool(ident[0]) and ident == _identity(dest_cfg)


def oracle_destination_scope(dest_db: Any, dest_cfg: Mapping[str, Any] | None, *,
                             source_db: Any = "", source_cfg: Mapping[str, Any] | None = None) -> Any:
    """Bind the pin for an Oracle destination (probe once), else a no-op scope.

    An already bound pin (Execute binds it for the whole run) is kept, so the
    gates inside a run judge the session the writer uses.
    """
    if not is_oracle(dest_db) or oracle_pin_bound():
        return nullcontext()
    if not dest_cfg:
        return bind_oracle_utc_pin(*_UNCHECKED)
    confirmed, reason = probe_oracle_utc_pin(dest_cfg)
    return bind_oracle_utc_pin(
        confirmed, reason,
        same_instance=same_oracle_instance(source_db, source_cfg, dest_db, dest_cfg),
    )


__all__ = [
    "bind_oracle_utc_pin",
    "is_oracle",
    "oracle_destination_scope",
    "oracle_pin_bound",
    "oracle_same_instance",
    "oracle_utc_pin",
    "probe_oracle_utc_pin",
    "same_oracle_instance",
]
