"""PostgreSQL logical decoding CDC reader.

Production path uses ``pg_logical_slot_peek_changes`` + ``ack()`` via
``pg_replication_slot_advance`` so WAL is never consumed before destination
apply (at-least-once). Default plugin is ``pgoutput`` (Debezium-class binary
via :mod:`connectors.pgoutput_decoder`); falls back to ``test_decoding``.
"""

from __future__ import annotations

import hashlib
import logging
import os
from services.brand_env import getenv_brand
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from connectors.postgresql_conn import get_connection
from services.cdc_engine import ChangeBatch
from services.cdc_schema_history import (
    connection_fingerprint,
    last_ddl_at,
    rebuild_schema,
    record_ddl,
)

# test_decoding value rendering uses type suffixes like [text]:'value' or [int4]:1.
# The first colon separates column info from value; the value may itself contain colons.
_VALUE_RE = re.compile(r"^\s*(\w+)\[(\w+)\]:(.+)$")

_logger = logging.getLogger(__name__)
_OLD_KEY_PREFIX = "old-key:"
_NEW_TUPLE_PREFIX = "new-tuple:"


def _publication_name(database: str, table: str | list[str], cursor_key: str) -> str:
    """Stable publication name for pgoutput (must match slot scoping)."""
    if isinstance(table, (list, tuple)):
        from services.cdc_multi_table import tables_digest

        tbl = f"mt_{tables_digest(list(table))}"
    else:
        tbl = str(table)
    digest = hashlib.sha1(
        f"{database}|{tbl}|{cursor_key}".encode(),
        usedforsecurity=False,
    ).hexdigest()[:10]
    raw = f"df_pub_{database}_{tbl}_{digest}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


def _slot_name(database: str, table: str | list[str], cursor_key: str) -> str:
    if isinstance(table, (list, tuple)):
        from services.cdc_multi_table import tables_digest

        tbl = f"mt_{tables_digest(list(table))}"
    else:
        tbl = str(table)
    token = hashlib.sha256(cursor_key.encode()).hexdigest()[:8]
    raw = f"df_{database}_{tbl}_{token}".lower()
    return re.sub(r"[^a-z0-9_]", "_", raw)[:63]


def encode_pg_resume_token(
    slot: str,
    *,
    lsn: str | None = None,
    phase: str = "streaming",
    last_pk: str = "",
    table: str = "",
) -> str:
    """Compact watermark: slot + optional consistent-point LSN + phase.

    Phase is ``snapshot`` while the initial table dump is in progress and
    ``streaming`` once the dump finishes and logical decoding owns the cursor.
    Mid-dump progress is ``table`` + URL-encoded ``last_pk`` (PK values may
    contain ``|`` / ``=``). Streaming tokens omit both so handoff equality
    stays slot+LSN+phase. Legacy bare slot names remain valid inputs via
    :func:`decode_pg_resume_token`.
    """
    from urllib.parse import quote

    parts = [f"slot={slot}", f"phase={phase}"]
    if lsn:
        parts.append(f"lsn={lsn}")
    if phase == "snapshot":
        if table:
            parts.append(f"table={quote(str(table), safe='')}")
        if last_pk:
            parts.append(f"last_pk={quote(str(last_pk), safe='')}")
    return "|".join(parts)


def decode_pg_resume_token(
    token: str | None,
    *,
    database: str,
    table: str | list[str],
    cursor_key: str,
) -> tuple[str, str | None, str]:
    """Return ``(slot_name, lsn_or_none, phase)`` from a watermark or legacy slot."""
    default_slot = _slot_name(database, table, cursor_key)
    if not token:
        return default_slot, None, "initial"
    raw = str(token).strip()
    if not raw:
        return default_slot, None, "initial"
    if "=" not in raw and "|" not in raw:
        # Legacy: watermark was the bare replication slot name.
        return raw[:63], None, "streaming"
    slot = default_slot
    lsn: str | None = None
    phase = "streaming"
    for part in raw.split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "slot" and value:
            slot = value[:63]
        elif key == "lsn" and value:
            lsn = value
        elif key == "phase" and value:
            phase = value
    return slot, lsn, phase


def decode_pg_snapshot_progress(token: str | None) -> tuple[str, str]:
    """Return ``(table, last_pk)`` from a PG watermark. Empty when streaming/legacy."""
    from urllib.parse import unquote

    if not token or "=" not in str(token):
        return "", ""
    table = ""
    last_pk = ""
    for part in str(token).split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "table" and value:
            table = unquote(value)
        elif key == "last_pk" and value:
            last_pk = unquote(value)
    return table, last_pk


def _lsn_at_or_before(candidate: str, watermark: str) -> bool:
    """Whether ``candidate`` is no newer than ``watermark``.

    Used as the DDD-3 low-watermark test during an incremental snapshot chunk:
    a WAL event at or before the position captured before the chunk SELECT is
    already reflected in the rows that were read, so it must not be replayed
    over them as a "stream wins" override.

    :func:`compare_lsn` returns ``0`` for both equal stamps *and* cross-family
    incomparable pairs. Cross-family must **not** be treated as stale — drop
    only when the families match and ``candidate <= watermark``.
    """
    if not candidate or not watermark:
        return False
    from connectors.writer_common import compare_lsn, lsn_family

    if lsn_family(candidate) != lsn_family(watermark):
        return False
    # Inclusive: an event *at* the low watermark is already reflected in the
    # chunk SELECT (the watermark was captured immediately before it), so it
    # must not replay over the snapshotted rows either.
    return compare_lsn(candidate, watermark) <= 0


def _parse_value(raw: str) -> str:
    """Strip PostgreSQL test_decoding quotes and null markers."""
    from services.value_serializer import SQL_NULL_SENTINEL

    if raw == "null" or raw == "None":
        return SQL_NULL_SENTINEL
    if len(raw) >= 2 and raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1].replace("''", "'")
    return raw


def _parse_columns(payload: str) -> dict[str, str]:
    """Parse a space-separated list of ``col[type]:value`` tokens."""
    result: dict[str, str] = {}
    if not payload:
        return result

    # test_decoding separates tokens by spaces, but string values may contain spaces.
    # It quotes values with spaces, e.g. data[text]:'hello world'. Split on the
    # pattern `` col[type]:`` while respecting quoted segments.
    tokens: list[str] = []
    current = ""
    in_quote = False
    for char in payload:
        if char == "'":
            in_quote = not in_quote
            current += char
        elif char == " " and not in_quote:
            if current.strip():
                tokens.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        tokens.append(current.strip())

    for token in tokens:
        match = _VALUE_RE.match(token)
        if not match:
            continue
        col, _dtype, raw = match.group(1), match.group(2), match.group(3)
        result[col] = _parse_value(raw)
    return result


def _parse_change_line(line: str, schema: str, table: str) -> tuple[str, dict[str, str] | None, dict[str, str] | None] | None:
    """Return (operation, old_key_dict_or_none, new_tuple_dict_or_none) for a line."""
    prefix = f"table {schema}.{table}:"
    if not line.startswith(prefix):
        return None
    rest = line[len(prefix):].strip()

    if rest.startswith("INSERT:"):
        payload = rest[len("INSERT:"):].strip()
        return "insert", None, _parse_columns(payload)

    if rest.startswith("UPDATE:"):
        payload = rest[len("UPDATE:"):].strip()
        old_key: dict[str, str] | None = None
        new_tuple: dict[str, str] | None = None
        if payload.startswith(_OLD_KEY_PREFIX):
            payload = payload[len(_OLD_KEY_PREFIX):].strip()
            split_at = payload.find(_NEW_TUPLE_PREFIX)
            if split_at >= 0:
                old_key = _parse_columns(payload[:split_at].strip())
                new_tuple = _parse_columns(payload[split_at + len(_NEW_TUPLE_PREFIX):].strip())
            else:
                old_key = _parse_columns(payload)
        else:
            new_tuple = _parse_columns(payload)
        return "update", old_key, new_tuple

    if rest.startswith("DELETE:"):
        payload = rest[len("DELETE:"):].strip()
        return "delete", _parse_columns(payload), None

    return None


class PostgreSqlChangeStreamCdc:
    """Log-based CDC for PostgreSQL using logical decoding.

    Defaults to ``pgoutput`` (Debezium 2.x/3.x industry default). Opt out with
    ``logical_decoding_plugin=test_decoding`` or ``DATAFLOW_PGOUTPUT_DECODER=0``.
    Falls back to ``test_decoding`` when pgoutput slot creation fails.
    Query CDC remains the outer fallback in ``cdc_transfer``.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        table: str | list[str],
        primary_key: str,
        cursor_key: str,
        schema: str = "public",
        columns: list[str] | None = None,
        resume_token: str | None = None,
        batch_size: int = 1000,
        output_plugin: str | None = None,
        primary_keys: dict[str, str] | None = None,
    ) -> None:
        from services.cdc_multi_table import normalize_table_list

        self.cfg = cfg
        self.schema = schema
        self.tables = normalize_table_list(table)
        if not self.tables:
            raise ValueError("PostgreSQL CDC requires at least one table")
        self.table = self.tables[0]
        from services.cdc_identity import require_cdc_primary_keys_map

        self.primary_keys = require_cdc_primary_keys_map(
            self.tables, primary_key=primary_key, primary_keys=primary_keys
        )
        self.primary_key = self.primary_keys[self.table]
        self.cursor_key = cursor_key
        self.columns = columns
        self.batch_size = batch_size
        self.database = cfg.get("database") or "postgres"
        slot_table: str | list[str] = self.tables if len(self.tables) > 1 else self.table
        slot, lsn, phase = decode_pg_resume_token(
            resume_token,
            database=self.database,
            table=slot_table,
            cursor_key=cursor_key,
        )
        self.slot_name = slot
        self.consistent_point_lsn = lsn
        self.snapshot_table, self.snapshot_last_pk = decode_pg_snapshot_progress(
            resume_token
        )
        self._resume_snapshot = phase == "snapshot"
        self.phase = phase if phase != "initial" else "snapshot"
        self.resume_token = resume_token
        # Streaming resume must never recreate a missing/lost slot (silent WAL skip).
        self._resume_expected = bool(lsn) or phase == "streaming"
        self.output_plugin = output_plugin or self._select_plugin()
        self.source_key = connection_fingerprint(
            {**cfg, "type": "postgresql"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        self.decode_schema: dict[str, Any] = {}
        self.last_ddl_at: str | None = None
        self._last_event_at: datetime | None = None
        # Source commit clock when decoder exposes it (pgoutput rarely does).
        self._last_event_commit_at: datetime | None = None
        self._last_heartbeat_at: datetime | None = None
        self._lag_observation: dict[str, Any] | None = None
        self._slot_catalog_cache: dict[str, Any] | None = None
        self._slot_catalog_cache_at: float = 0.0
        self._schema_ready = False
        self._pending_ack_lsn: str | None = None
        self._pgoutput_decoder = None
        self._streaming_transport = None
        self._streaming_attempted = False
        self.publication_name = _publication_name(self.database, slot_table, cursor_key)
        self._processed_signal_ids: set[str] = set()
        self.signal_table = str(cfg.get("signal_table") or "dataflow_signal")
        self._signal_table_ready = False
        self._last_signal_poll_at = 0.0
        self._signal_poll_interval_sec = float(
            getenv_brand("CDC_SIGNAL_POLL_SEC", cfg.get("signal_poll_interval_sec") or 15)
        )
        from services.cdc_lease import CdcLeaseGuard

        holder = str(
            cfg.get("lease_holder_id") or getenv_brand("CDC_LEASE_HOLDER") or ""
        )
        self._lease = CdcLeaseGuard(
            cursor_key=cursor_key,
            resource=f"pg_slot:{self.slot_name}",
            holder_id=holder,
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "plugin": self.output_plugin,
                "tables": list(self.tables),
                "engine": "postgresql",
                "shared_reader": len(self.tables) > 1,
            },
        )

    @property
    def lease_holder_id(self) -> str:
        return self._lease.holder_id

    @lease_holder_id.setter
    def lease_holder_id(self, value: str) -> None:
        self._lease.holder_id = value

    @property
    def _lease_acquired(self) -> bool:
        return self._lease.acquired

    def _acquire_cdc_lease(self) -> None:
        """Fail-fast if another worker already owns this slot / cursor_key."""
        self._lease.ensure()

    def close(self) -> None:
        """Release the CDC lease so another worker can attach."""
        self._lease.release()

    def _select_plugin(self) -> str:
        """Select logical decoding plugin.

        Default is ``pgoutput`` (binary, Debezium-class). Opt out via
        ``logical_decoding_plugin=test_decoding`` or env
        ``DATAFLOW_PGOUTPUT_DECODER=0|false|off|test_decoding``.
        """
        preferred = (self.cfg.get("logical_decoding_plugin") or "").strip().lower()
        env_flag = str(
            self.cfg.get("pgoutput_decoder") or getenv_brand("PGOUTPUT_DECODER", "")
        ).strip().lower()
        if preferred == "test_decoding" or env_flag in {"0", "false", "off", "test_decoding"}:
            return "test_decoding"
        if preferred in {"pgoutput", ""} or env_flag in {
            "",
            "1",
            "true",
            "on",
            "pgoutput",
            "experimental",
        }:
            return "pgoutput"
        return preferred or "pgoutput"

    def _slot_catalog_status(self, *, max_age_sec: float = 2.0) -> dict[str, Any]:
        """Live ``pg_replication_slots`` proof for Theater / Freshness.

        Returns ``active``, ``restart_lsn``, ``confirmed_flush_lsn``, ``wal_status``
        (PG13+), ``plugin``, ``slot_exists``. Cached briefly so poll loops do not
        open a connection per decoded row.
        """
        import time as _time

        now = _time.monotonic()
        if (
            self._slot_catalog_cache is not None
            and (now - float(self._slot_catalog_cache_at or 0.0)) < max(0.25, float(max_age_sec))
        ):
            return dict(self._slot_catalog_cache)

        out: dict[str, Any] = {
            "slot_exists": False,
            "active": None,
            "restart_lsn": None,
            "confirmed_flush_lsn": None,
            "wal_status": None,
            "plugin": self.output_plugin,
        }
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    # PG13+ exposes wal_status (reserved/extended/unreserved/lost).
                    row = None
                    has_wal_status = False
                    try:
                        cur.execute(
                            """
                            SELECT active,
                                   restart_lsn::text,
                                   confirmed_flush_lsn::text,
                                   plugin,
                                   wal_status
                            FROM pg_replication_slots
                            WHERE slot_name = %s
                            """,
                            (self.slot_name,),
                        )
                        row = cur.fetchone()
                        has_wal_status = True
                    except Exception as col_exc:
                        msg = str(col_exc).lower()
                        if "wal_status" not in msg and "undefined" not in msg:
                            raise
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        cur.execute(
                            """
                            SELECT active,
                                   restart_lsn::text,
                                   confirmed_flush_lsn::text,
                                   plugin
                            FROM pg_replication_slots
                            WHERE slot_name = %s
                            """,
                            (self.slot_name,),
                        )
                        row = cur.fetchone()
                    if row:
                        out["slot_exists"] = True
                        out["active"] = bool(row[0]) if row[0] is not None else None
                        out["restart_lsn"] = str(row[1]) if row[1] else None
                        out["confirmed_flush_lsn"] = str(row[2]) if row[2] else None
                        if row[3]:
                            out["plugin"] = str(row[3])
                            self.output_plugin = str(row[3])
                        if has_wal_status and len(row) > 4 and row[4]:
                            out["wal_status"] = str(row[4])
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception as exc:
            _logger.debug("slot catalog probe failed for %s: %s", self.slot_name, exc)
            out["probe_error"] = str(exc)[:200]

        self._slot_catalog_cache = dict(out)
        self._slot_catalog_cache_at = now
        return out

    def cdc_metadata(self) -> dict[str, Any]:
        """Operator-visible CDC status for Job Theater / Validate."""
        lag_sec = self.replication_lag_seconds()
        obs = dict(self._lag_observation or {})
        slot = self._slot_catalog_status()
        confirmed = (
            slot.get("confirmed_flush_lsn")
            or self.consistent_point_lsn
        )
        return {
            "plugin": slot.get("plugin") or self.output_plugin,
            "slot_name": self.slot_name,
            "publication_name": self.publication_name if self.output_plugin == "pgoutput" else None,
            "phase": self.phase,
            "consistent_point_lsn": self.consistent_point_lsn,
            "replication_lag_bytes": obs.get("replication_lag_bytes", self.replication_lag_bytes()),
            "replication_lag_seconds": lag_sec,
            "cdc_lag_basis": obs.get("cdc_lag_basis"),
            "cdc_heartbeat_age_sec": obs.get("cdc_heartbeat_age_sec"),
            "freshness_severity": obs.get("freshness_severity"),
            "active": slot.get("active"),
            "slot_exists": slot.get("slot_exists"),
            "restart_lsn": slot.get("restart_lsn"),
            "confirmed_flush_lsn": confirmed,
            "wal_status": slot.get("wal_status"),
            "delivery": "at-least-once",
            **self._lease.theater_fields(),
        }

    def _conn(self):
        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 5432,
            database=self.database,
            username=self.cfg.get("username") or "",
            password=self.cfg.get("password") or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
        )

    def is_available(self) -> bool:
        """Check logical replication is enabled and the user can create a slot."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW wal_level")
                    row = cur.fetchone()
                    if not row or row[0] != "logical":
                        return False
                    cur.execute(
                        "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                        (self.slot_name,),
                    )
                    exists = cur.fetchone() is not None
                    if exists:
                        return True
                    test_slot = f"{self.slot_name}_avail_test"[:63]
                    plugin = self.output_plugin or "pgoutput"
                    # Guard against logical-replication probes hanging when the
                    # server is not really ready (e.g. lock_timeout / statement
                    # timeout disabled by session guards).
                    cur.execute("SET LOCAL statement_timeout = '5000ms'")
                    try:
                        cur.execute(
                            "SELECT pg_create_logical_replication_slot(%s, %s)",
                            (test_slot, plugin),
                        )
                        cur.execute("SELECT pg_drop_replication_slot(%s)", (test_slot,))
                    except Exception:
                        if plugin == "pgoutput":
                            # Fall back to test_decoding for availability probe.
                            self.output_plugin = "test_decoding"
                            cur.execute(
                                "SELECT pg_create_logical_replication_slot(%s, %s)",
                                (test_slot, "test_decoding"),
                            )
                            cur.execute("SELECT pg_drop_replication_slot(%s)", (test_slot,))
                        else:
                            raise
                conn.commit()
            return True
        except Exception:
            return False

    def _resume_token(
        self,
        *,
        phase: str | None = None,
        last_pk: str = "",
        table: str = "",
    ) -> str:
        p = phase or self.phase
        return encode_pg_resume_token(
            self.slot_name,
            lsn=self.consistent_point_lsn,
            phase=p,
            last_pk=last_pk if p == "snapshot" else "",
            table=table if p == "snapshot" else "",
        )

    def _read_slot_lsn(self, cur) -> str | None:
        """Return confirmed_flush_lsn or restart_lsn for this slot."""
        cur.execute(
            """
            SELECT confirmed_flush_lsn::text, restart_lsn::text
            FROM pg_replication_slots
            WHERE slot_name = %s
            """,
            (self.slot_name,),
        )
        row = cur.fetchone()
        if not row:
            return None
        value = row[0] or row[1]
        return str(value) if value else None

    def _ensure_slot(self, *, allow_create: bool = True, recreate_if_lost: bool = False) -> str | None:
        """Create the logical slot if needed; return consistent-point LSN.

        Slot is created *before* the initial snapshot so WAL from the snapshot
        window is retained (Debezium / PG logical-decoding handoff pattern).

        Poll/resume must pass ``allow_create=False``. Creating a slot at current
        WAL while a watermark exists skips the lost window — silent CDC loss.
        Snapshot recovery may pass ``recreate_if_lost=True`` to drop an
        invalidated ``wal_status=lost`` slot and establish a new consistent point.
        Semantics remain at-least-once; destination upserts must be idempotent.
        """
        self._acquire_cdc_lease()
        catalog = self._slot_catalog_status(max_age_sec=0)
        wal = str(catalog.get("wal_status") or "").strip().lower()
        exists = bool(catalog.get("slot_exists"))
        if exists and wal == "lost":
            if not recreate_if_lost:
                from services.cdc_cursor_gap import CdcSlotGapError

                raise CdcSlotGapError(
                    (
                        f"PostgreSQL slot {self.slot_name} wal_status=lost. "
                        "Poll will not recreate it at current WAL (that skips the lost window). "
                        "when_needed snapshots current source keys then streams from the new tip. "
                        "Not continuous CDC, not migration_proven."
                    ),
                    slot_name=self.slot_name,
                    wal_status="lost",
                    restart_lsn=str(catalog.get("restart_lsn") or ""),
                    confirmed_flush_lsn=str(
                        catalog.get("confirmed_flush_lsn") or self.consistent_point_lsn or ""
                    ),
                    cursor_key=self.cursor_key,
                )
            self._drop_replication_slot()
            exists = False
        if not exists and not allow_create:
            from services.cdc_cursor_gap import CdcSlotGapError

            raise CdcSlotGapError(
                (
                    f"PostgreSQL slot {self.slot_name} is missing. "
                    "Creating it now would start at current WAL and skip the lost window. "
                    "when_needed snapshots current source keys then streams from the new tip. "
                    "Not continuous CDC, not migration_proven."
                ),
                slot_name=self.slot_name,
                wal_status="slot_missing",
                confirmed_flush_lsn=str(self.consistent_point_lsn or ""),
                cursor_key=self.cursor_key,
            )
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,),
                )
                row = cur.fetchone()
                created_new = False
                if row is None:
                    try:
                        cur.execute(
                            "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                            (self.slot_name, self.output_plugin),
                        )
                    except Exception:
                        if self.output_plugin == "pgoutput":
                            self.output_plugin = "test_decoding"
                            cur.execute(
                                "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                                (self.slot_name, "test_decoding"),
                            )
                        else:
                            raise
                    created = cur.fetchone()
                    lsn = str(created[0]) if created and created[0] else None
                    created_new = True
                else:
                    # Honor existing slot plugin (cannot change without drop).
                    self.output_plugin = row[0] or self.output_plugin
                    lsn = self._read_slot_lsn(cur)
            conn.commit()
        self._slot_catalog_cache = None
        if lsn and (created_new or not self.consistent_point_lsn):
            self.consistent_point_lsn = lsn
        return self.consistent_point_lsn

    def _drop_replication_slot(self) -> None:
        """Drop an invalidated slot so snapshot can create a new consistent point."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (self.slot_name,))
            conn.commit()
        self._slot_catalog_cache = None
        _logger.warning(
            "Dropped PostgreSQL slot %s (wal_status=lost) so snapshot can recreate "
            "a consistent point. Lost-window events are gone.",
            self.slot_name,
        )

    def _assert_slot_within_retention(self) -> None:
        """Fail-closed before peek when the slot is missing or wal_status=lost."""
        if self.phase == "snapshot":
            return
        catalog = self._slot_catalog_status(max_age_sec=0)
        from services.cdc_retention_probe import classify_pg_slot_retention

        probe = classify_pg_slot_retention(
            slot_exists=catalog.get("slot_exists"),
            wal_status=str(catalog.get("wal_status") or ""),
            restart_lsn=str(catalog.get("restart_lsn") or ""),
            confirmed_flush_lsn=str(
                catalog.get("confirmed_flush_lsn") or self.consistent_point_lsn or ""
            ),
            watermark=self.consistent_point_lsn,
            resume_expected=bool(getattr(self, "_resume_expected", False)),
            cursor_key=self.cursor_key,
            slot_name=self.slot_name,
        )
        try:
            self._cdc_retention = probe
        except Exception:
            pass
        if probe.status != "gap":
            return
        from services.cdc_cursor_gap import CdcSlotGapError

        raise CdcSlotGapError(
            probe.message,
            slot_name=self.slot_name,
            wal_status=str(catalog.get("wal_status") or probe.retained),
            restart_lsn=str(catalog.get("restart_lsn") or ""),
            confirmed_flush_lsn=str(catalog.get("confirmed_flush_lsn") or ""),
            cursor_key=self.cursor_key,
        )

    def replication_lag_bytes(self) -> int | None:
        """Return WAL lag for this slot, or None if unavailable."""
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                        FROM pg_replication_slots
                        WHERE slot_name = %s
                        """,
                        (self.slot_name,),
                    )
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        return int(row[0])
        except Exception:
            return None
        return None

    def replication_lag_seconds(self) -> float | None:
        """Proven CDC lag seconds — never heartbeat age (Debezium-class honesty).

        Returns ``0`` when ``pg_wal_lsn_diff`` proves catch-up; ``None`` when
        behind on WAL without a source commit timestamp (basis ``wal_bytes``).
        """
        from services.cdc_lag_honesty import observe_cdc_lag

        obs = observe_cdc_lag(
            last_event_commit_at=self._last_event_commit_at,
            last_heartbeat_at=self._last_heartbeat_at,
            replication_lag_bytes=self.replication_lag_bytes(),
        )
        self._lag_observation = obs
        return obs.get("cdc_lag_seconds")

    def heartbeat(self) -> None:
        """Keep the lease alive and release WAL an idle slot no longer needs.

        A replication slot pins every WAL segment from ``confirmed_flush_lsn``
        onward. When the captured tables are quiet but the rest of the database
        is busy, that position never moves on its own and retention grows until
        the primary runs out of disk — the classic way a CDC pipeline takes down
        the database it is reading.

        Emitting a WAL message here (the previous behaviour) made this *worse*:
        the message is itself decoded through the slot, so each heartbeat pushed
        more WAL behind a position that was never advanced. The fix is to advance
        the slot directly, which is what actually frees the segments.

        Advancing is only safe if nothing undecoded would be skipped, because
        ``pg_replication_slot_advance`` discards rather than decodes. The check
        that makes it safe:

        1. Capture ``target = pg_current_wal_lsn()`` *first*.
        2. Peek the slot for changes up to that target.
        3. Advance only if the peek came back empty, which proves there is
           nothing in ``(confirmed_flush_lsn, target]`` to lose. Anything
           committed after the target is beyond it and is untouched.

        Capturing the target before peeking is what removes the race: a
        concurrent commit lands past the target, so it cannot be skipped.
        """
        now = datetime.now(timezone.utc)
        interval = float(getenv_brand("CDC_HEARTBEAT_SEC", "10"))
        if self._last_heartbeat_at is not None:
            age = (now - self._last_heartbeat_at).total_seconds()
            if age < max(1.0, interval):
                return
        self._last_heartbeat_at = now
        if self._lease.acquired:
            try:
                self._lease.renew()
            except Exception as exc:
                _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        if self._pending_ack_lsn:
            # Applied-but-unacked work exists; ack() owns the slot position.
            return
        if getenv_brand("CDC_IDLE_SLOT_ADVANCE", "1").strip().lower() in {
            "0",
            "false",
            "off",
        }:
            return
        if getattr(self, "phase", None) == "snapshot":
            # The initial dump hands off to streaming at the LSN captured inside
            # its REPEATABLE READ transaction. The slot has to retain WAL from
            # that point, and an idle dump has nothing to release anyway.
            return
        if self._incremental_snapshot_open():
            # An open snapshot window needs its bracketing events retained so
            # stale-event filtering still works. Releasing WAL now would drop
            # the events the window is comparing against.
            return
        self._advance_idle_slot()

    def _incremental_snapshot_open(self) -> bool:
        """Whether an incremental snapshot is mid-flight for this source."""
        try:
            from services.cdc_incremental_snapshot import list_signals

            return any(
                str(getattr(sig, "status", "")) in {"pending", "running", "in_progress"}
                for sig in list_signals(self.source_key)
            )
        except Exception as exc:
            # Unable to tell — assume a snapshot may be open and keep the WAL.
            _logger.debug("incremental snapshot state unknown: %s", exc)
            return True

    def _advance_idle_slot(self) -> None:
        """Move ``confirmed_flush_lsn`` forward when the slot has nothing pending."""
        from connectors.writer_common import compare_lsn

        current = ""
        peek_fn = (
            "pg_logical_slot_peek_binary_changes"
            if self.output_plugin == "pgoutput"
            else "pg_logical_slot_peek_changes"
        )
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_current_wal_lsn()::text")
                    row = cur.fetchone()
                    target = str(row[0]) if row and row[0] else ""
                    if not target:
                        return

                    if self.output_plugin == "pgoutput":
                        cur.execute(
                            f"SELECT 1 FROM {peek_fn}("  # nosec: B608 — peek_fn is one of two literals above
                            "%s, %s::pg_lsn, 1, 'proto_version', '1', "
                            "'publication_names', %s) LIMIT 1",
                            (self.slot_name, target, self.publication_name),
                        )
                    else:
                        cur.execute(
                            f"SELECT 1 FROM {peek_fn}(%s, %s::pg_lsn, 1) LIMIT 1",  # nosec: B608 — peek_fn is one of two literals above
                            (self.slot_name, target),
                        )
                    if cur.fetchone() is not None:
                        # Real changes are waiting. The normal poll will decode
                        # and apply them; advancing here would discard them.
                        return

                    cur.execute(
                        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
                        "WHERE slot_name = %s",
                        (self.slot_name,),
                    )
                    row = cur.fetchone()
                    current = str(row[0]) if row and row[0] else ""
                    if current and compare_lsn(current, target) >= 0:
                        return

                    cur.execute(
                        "SELECT pg_replication_slot_advance(%s, %s::pg_lsn)",
                        (self.slot_name, target),
                    )
                conn.commit()
            self.consistent_point_lsn = target
            _logger.info(
                "Released WAL on idle slot %s: confirmed_flush_lsn %s -> %s",
                self.slot_name,
                current or "unknown",
                target,
            )
        except Exception as exc:
            # Losing a retention optimisation is acceptable; failing the poll is not.
            _logger.debug("Postgres CDC idle slot advance skipped: %s", exc)

    def _poll_signal_table(self) -> None:
        """Debezium-compatible signal table → incremental snapshot enqueue."""
        import time as _time

        now = _time.monotonic()
        if (
            self._signal_table_ready
            and (now - self._last_signal_poll_at) < max(1.0, self._signal_poll_interval_sec)
        ):
            return
        from services.cdc_signal_table import ensure_signal_table, poll_signal_table

        try:
            with self._conn() as conn:
                if not self._signal_table_ready:
                    ensure_signal_table(conn, table=self.signal_table, dialect="postgresql")
                    self._signal_table_ready = True
                _, self._processed_signal_ids = poll_signal_table(
                    conn,
                    source_key=self.source_key,
                    table=self.signal_table,
                    default_table=self.table,
                    primary_key=self.primary_key,
                    processed_ids=self._processed_signal_ids,
                    dialect="postgresql",
                )
            self._last_signal_poll_at = now
        except Exception as exc:
            _logger.debug("Postgres CDC signal table poll skipped: %s", exc)

    def _fetch_live_schema(self) -> dict[str, Any]:
        """Load column types / nullability / PK from information_schema."""
        columns: dict[str, str] = {}
        nullable: dict[str, bool] = {}
        primary_key: list[str] = []
        try:
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                    (self.schema, self.table),
                )
                for name, data_type, is_nullable in cur.fetchall():
                    columns[str(name)] = str(data_type or "text")
                    nullable[str(name)] = str(is_nullable or "").upper() == "YES"
                cur.execute(
                    """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                          AND tc.table_schema = %s AND tc.table_name = %s
                        ORDER BY kcu.ordinal_position
                        """,
                    (self.schema, self.table),
                )
                primary_key = [str(r[0]) for r in cur.fetchall()]
        except Exception:
            _logger.debug("PostgreSQL live schema fetch failed", exc_info=True)
        return {"columns": columns, "nullable": nullable, "primary_key": primary_key}

    def _schema_fingerprint(self, snapshot: dict[str, Any]) -> str:
        cols = snapshot.get("columns") or {}
        nulls = snapshot.get("nullable") or {}
        pk = snapshot.get("primary_key") or []
        parts = [f"{k}:{cols[k]}:{int(bool(nulls.get(k, True)))}" for k in sorted(cols)]
        parts.append("pk=" + ",".join(pk))
        return "|".join(parts)

    def _ensure_decode_schema(self, *, resume_offset: Any = None) -> dict[str, Any]:
        """Rebuild decode schema from history (or seed from live catalog)."""
        if self._schema_ready and self.decode_schema:
            return self.decode_schema

        rebuilt = rebuild_schema(self.source_key, self._qualified_table(), resume_offset)
        if rebuilt:
            self.decode_schema = rebuilt
        else:
            live = self._fetch_live_schema()
            if live.get("columns"):
                record_ddl(
                    self.source_key,
                    self._qualified_table(),
                    ddl="SNAPSHOT",
                    offset=resume_offset or self.slot_name,
                    schema_snapshot=live,
                )
                self.decode_schema = live
        self.last_ddl_at = last_ddl_at(self.source_key, self._qualified_table())
        self._schema_ready = True
        return self.decode_schema

    def _maybe_record_schema_change(self, *, offset: Any = None) -> None:
        """Compare live catalog to decode schema; persist DDL history on drift."""
        live = self._fetch_live_schema()
        if not live.get("columns"):
            return
        if self._schema_fingerprint(live) == self._schema_fingerprint(self.decode_schema):
            return
        entry = record_ddl(
            self.source_key,
            self._qualified_table(),
            ddl="ALTER TABLE (detected)",
            offset=offset or self.slot_name,
            schema_snapshot=live,
        )
        self.decode_schema = live
        self.last_ddl_at = str(entry.get("recorded_at") or "") or self.last_ddl_at
        try:
            from services.cdc_mapping_review import flag_mapping_review

            live_cols = live.get("columns") or {}
            if isinstance(live_cols, dict):
                cols = [str(k) for k in live_cols.keys() if k]
            else:
                cols = [
                    str(c.get("name") or c.get("column") or "")
                    for c in live_cols
                    if isinstance(c, dict)
                ]
            flag_mapping_review(
                source_key=self.source_key,
                table=self._qualified_table(),
                reason="cdc_schema_drift",
                schema_version=int(entry.get("version") or 0) or None,
                ddl=str(entry.get("ddl") or ""),
                column_names=[c for c in cols if c],
            )
        except Exception:
            _logger.exception("Failed to flag CDC mapping review after schema drift")
        _logger.info(
            "Recorded PostgreSQL CDC schema change for %s v%s",
            self._qualified_table(),
            entry.get("version"),
        )

    def snapshot(self) -> Iterator[ChangeBatch]:
        """Initial dump after slot create, then hand off to streaming at the LSN.

        Order matches industry CDC practice:
        1. Create publication (pgoutput) then logical slot (consistent point LSN).
        2. Read the table under ``REPEATABLE READ`` on one connection so the
           dump is a single MVCC snapshot (not N independent page reads).
        3. Persist ``phase=streaming`` + LSN so poll resumes without a gap window
           outside the slot (duplicates during the dump are possible; upserts OK).
        """
        from connectors.postgresql_reader import _cell, _order_by_clause
        from connectors.sql_identifiers import quote_column_list, quote_table_ref
        from connectors.sql_snapshot_scan import fetch_scan_page
        from services.cdc_snapshot_resume import (
            classify_snapshot_resume,
            last_pk_from_records,
            quoted_pk_columns,
            snapshot_keyset_sql,
        )

        # pgoutput requires the publication before the slot retains WAL for it.
        if self.output_plugin == "pgoutput":
            self._ensure_publication()
            self._ensure_replica_identity()
        self._ensure_slot(allow_create=True, recreate_if_lost=True)
        resume_table = self.snapshot_table if self._resume_snapshot else ""
        resume_last_pk = self.snapshot_last_pk if self._resume_snapshot else ""
        keep_lsn = bool(self._resume_snapshot and self.consistent_point_lsn)
        self.phase = "snapshot"
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self.heartbeat()

        tables = list(self.tables)
        if resume_table in tables:
            tables = tables[tables.index(resume_table) :]

        with self._conn() as conn:
            # One RR transaction spans all tables so the multi-table dump shares
            # a consistent MVCC snapshot (Debezium initial-sync pattern).
            prev_autocommit = getattr(conn, "autocommit", True)
            try:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    if not keep_lsn:
                        cur.execute("SELECT pg_current_wal_lsn()::text")
                        snap_lsn_row = cur.fetchone()
                        if snap_lsn_row and snap_lsn_row[0]:
                            self.consistent_point_lsn = str(snap_lsn_row[0])
                    for table_name in tables:
                        table_last_pk = resume_last_pk if table_name == resume_table else ""
                        mode = classify_snapshot_resume(last_pk=table_last_pk, offset=0)
                        pk_cols = self._pk_columns_for(table_name)
                        quoted = quoted_pk_columns(pk_cols, '"')
                        order_by = _order_by_clause(
                            cur, self.schema, table_name, self.columns
                        )
                        table_ref = quote_table_ref(
                            table_name,
                            self.schema,
                            dialect="postgresql",
                            preserve_case=True,
                        )
                        col_sql = quote_column_list(self.columns, quote_char='"')
                        headers: list[str] = list(self.columns or [])
                        if mode == "scan":
                            cur.execute(
                                f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                                f"ORDER BY {order_by}"
                            )
                        while True:
                            if mode == "keyset":
                                sql, params = snapshot_keyset_sql(
                                    table_ref=table_ref,
                                    quoted_pk_columns=quoted,
                                    last_pk=table_last_pk,
                                    limit=self.batch_size,
                                    dialect="postgresql",
                                    select_list=col_sql,
                                )
                                cur.execute(sql, params)
                                fetched = cur.fetchall() or []
                            else:
                                fetched = fetch_scan_page(cur, self.batch_size)
                            if not fetched:
                                break
                            if cur.description:
                                headers = [desc[0] for desc in cur.description]
                            records = [
                                {headers[i]: _cell(v) for i, v in enumerate(row)}
                                for row in fetched
                            ]
                            table_last_pk = (
                                last_pk_from_records(records, pk_cols) or table_last_pk
                            )
                            yield ChangeBatch(
                                inserts=records,
                                resume_token=self._resume_token(
                                    phase="snapshot",
                                    last_pk=table_last_pk,
                                    table=table_name,
                                ),
                                table=table_name,
                            )
                            if len(fetched) < self.batch_size:
                                break
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception as exc:
                    _logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                raise
            finally:
                try:
                    conn.autocommit = prev_autocommit
                except Exception as exc:
                    _logger.warning("Exception suppressed: %s", exc, exc_info=exc)

        self.phase = "streaming"
        self.snapshot_last_pk = ""
        self.snapshot_table = ""
        self._resume_snapshot = False
        self.resume_token = self._resume_token(phase="streaming")
        yield ChangeBatch(
            resume_token=self.resume_token,
            ack_barrier=True,
        )

    def _pk_columns_for(self, table: str | None = None) -> list[str]:
        from services.cdc_snapshot_window import _pk_columns

        pk = self.primary_keys.get(table or self.table, self.primary_key)
        return _pk_columns(pk)

    def _pk_value(self, record: dict[str, str], *, table: str | None = None) -> str:
        """Composite-aware PK string shared with the snapshot-window key space.

        Treating ``order_id,line_id`` as one literal column name made every
        delete evaluate empty and every upsert fall through to plain INSERT.
        """
        if not record:
            return ""
        from services.cdc_snapshot_window import _pk_value as composite_pk_value

        key = composite_pk_value(record, self._pk_columns_for(table))
        return "" if key is None else key

    def _qualified_table(self, table: str | None = None) -> str:
        from connectors.sql_identifiers import quote_table_ref

        return quote_table_ref(
            table or self.table, self.schema or "public", dialect="postgresql"
        )

    def _ensure_publication(self) -> None:
        """Create a FOR TABLE publication required by the pgoutput plugin.

        Multi-table shared reader: one publication listing every captured table
        (Debezium-class single slot / N tables).
        """
        if self.output_plugin != "pgoutput":
            return
        from connectors.sql_identifiers import require_safe_identifier

        pub = require_safe_identifier(self.publication_name, preserve_case=False)
        self.publication_name = pub
        qualified_list = [self._qualified_table(t) for t in self.tables]
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (pub,),
                )
                if cur.fetchone() is None:
                    tables_sql = ", ".join(qualified_list)
                    cur.execute(f"CREATE PUBLICATION {pub} FOR TABLE {tables_sql}")
                else:
                    for qualified in qualified_list:
                        try:
                            cur.execute(f"ALTER PUBLICATION {pub} ADD TABLE {qualified}")
                        except Exception:
                            try:
                                conn.rollback()
                            except Exception as exc:
                                _logger.debug("Cleanup exception suppressed: %s", exc, exc_info=exc)
            conn.commit()
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pub,))
                if cur.fetchone() is None:
                    raise RuntimeError(f"pgoutput publication {pub} was not created")
            conn.commit()

    def _ensure_replica_identity(self) -> None:
        """Require FULL replica identity so UPDATE/DELETE emit old keys + TOAST.

        Without FULL (or an identity index covering TOAST columns), pgoutput
        marks unchanged TOAST as ``'u'`` with no old tuple — sparse updates
        would wipe destination columns. Fail soft only when the table is
        missing; log loudly otherwise.
        """
        for table in self.tables:
            qualified = self._qualified_table(table)
            try:
                with self._conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"ALTER TABLE {qualified} REPLICA IDENTITY FULL")
                    conn.commit()
            except Exception as exc:
                _logger.warning(
                    "Could not set REPLICA IDENTITY FULL on %s (TOAST-safe updates "
                    "may fail closed): %s",
                    qualified,
                    exc,
                )

    def _ensure_streaming_transport(self) -> Any:
        """Phase F4 — optional START_REPLICATION transport (feature-flagged)."""
        if self._streaming_attempted:
            return self._streaming_transport
        self._streaming_attempted = True
        try:
            from connectors.postgresql_cdc_transport import (
                open_streaming_transport_or_none,
                selected_pg_cdc_transport,
            )

            if selected_pg_cdc_transport() != "streaming":
                return None
            dsn = {
                "host": self.cfg.get("host"),
                "port": int(self.cfg.get("port") or 5432),
                "dbname": self.cfg.get("database") or self.database,
                "user": self.cfg.get("username"),
                "password": self.cfg.get("password"),
            }
            if self.cfg.get("connection_string"):
                # Prefer DSN string when operators supply it.
                dsn = {"dsn": self.cfg["connection_string"]}
            self._streaming_transport = open_streaming_transport_or_none(
                dsn_kwargs=dsn,
                slot_name=self.slot_name,
                publication_name=self.publication_name,
                output_plugin=self.output_plugin,
            )
        except Exception as exc:
            _logger.warning("CDC streaming transport init failed: %s", exc)
            self._streaming_transport = None
        return self._streaming_transport

    def _peek_or_stream_rows(self, cur: Any) -> list[tuple[Any, Any]]:
        """Return ``(lsn, payload)`` rows from streaming transport or peek SQL."""
        transport = self._ensure_streaming_transport()
        if transport is not None:
            changes = transport.poll(limit=self.batch_size)
            if changes:
                return [(c.lsn, c.payload) for c in changes]
            # Empty poll — still valid; do not fall through to peek on the same
            # slot (would race with the replication connection).
            return []
        if self.output_plugin == "pgoutput":
            cur.execute(
                """
                SELECT lsn::text, data
                FROM pg_logical_slot_peek_binary_changes(
                    %s, NULL, %s,
                    'proto_version', '1',
                    'publication_names', %s
                )
                """,
                (self.slot_name, self.batch_size, self.publication_name),
            )
        else:
            cur.execute(
                """
                SELECT lsn::text, data
                FROM pg_logical_slot_peek_changes(
                    %s, NULL, %s, 'include-xids', '1'
                )
                """,
                (self.slot_name, self.batch_size),
            )
        return list(cur.fetchall() or [])

    def ack(self, resume_token: Any = None) -> None:
        """Advance the slot confirmed_flush_lsn after successful destination apply.

        Poll uses ``peek_changes`` so WAL is not consumed before apply. Without
        ``ack``, the slot retains WAL and re-delivers (at-least-once). Calling
        ``ack`` after watermark persist makes progress durable.
        """
        lsn = self._pending_ack_lsn
        if resume_token:
            _, token_lsn, _ = decode_pg_resume_token(
                str(resume_token),
                database=self.database,
                table=self.tables if len(self.tables) > 1 else self.table,
                cursor_key=self.cursor_key,
            )
            if token_lsn:
                lsn = token_lsn
        if not lsn:
            return
        # Phase F4 — streaming: confirmed flush via replication feedback only
        # (slot_advance on a second connection races the replication session).
        if self._streaming_transport is not None:
            try:
                self._streaming_transport.ack(lsn)
                self.consistent_point_lsn = lsn
                self._pending_ack_lsn = None
                return
            except Exception as exc:
                _logger.warning(
                    "CDC streaming ack feedback failed for slot %s at %s: %s",
                    self.slot_name,
                    lsn,
                    exc,
                )
                raise
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_replication_slot_advance(%s, %s::pg_lsn)",
                        (self.slot_name, lsn),
                    )
                conn.commit()
            self.consistent_point_lsn = lsn
            self._pending_ack_lsn = None
        except Exception as exc:
            _logger.warning(
                "Postgres CDC ack failed for slot %s at %s: %s",
                self.slot_name,
                lsn,
                exc,
            )
            raise

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk reader for Debezium-style incremental snapshots."""
        from connectors.sql_identifiers import (
            quote_sql_identifier,
            require_safe_identifier,
        )

        from services.cdc_incremental_snapshot import snapshot_records_from_rows
        from services.cdc_snapshot_window import (
            _pk_columns,
            _pk_value,
            keyset_successor_predicate,
        )

        pk_cols = _pk_columns(sig.primary_key or self.primary_key)
        pk_quoted = [
            quote_sql_identifier(require_safe_identifier(c, preserve_case=True))
            for c in pk_cols
        ]
        order_sql = ", ".join(pk_quoted)
        # Snapshot chunks must read the table the signal names, not whichever
        # table this reader happens to be bound to. In shared multi-table mode
        # `self.table` is pinned to tables[0], so honouring it here meant a
        # signal for table B read rows from table A and was then marked
        # complete — table B was never backfilled and no error was raised.
        qualified = self._qualified_table(getattr(sig, "table", "") or self.table)
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        lsn_low = ""
        lsn_high = ""
        with self._conn() as conn:
            with conn.cursor() as cur:
                # DDD-3 watermarks bracketing the chunk SELECT. Without these the
                # stream-wins step had no way to tell whether a peeked WAL event
                # predated the read, so an event *older* than the chunk could
                # overwrite the fresher snapshot value.
                lsn_low = self._current_wal_lsn(cur)
                if last_pk:
                    where, params = keyset_successor_predicate(pk_quoted, last_pk)
                    cur.execute(
                        f"SELECT * FROM {qualified} WHERE {where} "  # nosec B608
                        f"ORDER BY {order_sql} LIMIT %s",
                        (*params, limit),
                    )
                else:
                    cur.execute(
                        f"SELECT * FROM {qualified} ORDER BY {order_sql} LIMIT %s",  # nosec B608
                        (limit,),
                    )
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
                lsn_high = self._current_wal_lsn(cur)
            conn.commit()
        # Publish the watermarks on the signal so the shared runner can stamp
        # `_df_lsn` and the peek step can discard pre-chunk events.
        # Only persist non-empty captures — update_signal writes every provided
        # field, so an empty string from a transient pg_current_wal_lsn failure
        # would wipe a previously good low watermark (MySQL GTID/binlog parity).
        try:
            from services.cdc_incremental_snapshot import update_signal

            persist: dict[str, str] = {}
            if lsn_low:
                sig.lsn_low = lsn_low
                persist["lsn_low"] = lsn_low
            if lsn_high:
                sig.lsn_high = lsn_high
                persist["lsn_high"] = lsn_high
            if persist:
                update_signal(sig.id, **persist)
        except Exception as exc:
            # Watermarks are an optimisation for stale-event rejection; losing
            # them degrades to plain upsert rather than corrupting the chunk.
            _logger.warning("Could not persist snapshot WAL watermarks: %s", exc)
        records = snapshot_records_from_rows(cols, rows)
        new_last = _pk_value(records[-1], pk_cols) if records else last_pk
        done = len(records) < limit
        return records, new_last if new_last is not None else last_pk, done

    @staticmethod
    def _current_wal_lsn(cur: Any) -> str:
        """Current WAL insert position, or empty string if unavailable."""
        try:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else ""
        except Exception as exc:
            # Standbys raise "recovery is in progress" — degrade to empty LSN,
            # never NameError on an unbound ``logger`` alias.
            _logger.debug("pg_current_wal_lsn unavailable: %s", exc)
            return ""

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Peek WAL (no ack) for DDD-3 stream-wins during an incremental snapshot chunk."""
        from services.cdc_cursor_gap import CdcCursorGapError

        try:
            self._ensure_slot(allow_create=False)
            if self.output_plugin == "pgoutput":
                self._ensure_publication()
        except CdcCursorGapError:
            raise
        except Exception:
            return []
        events: list[dict[str, Any]] = []
        peek_limit = min(int(sig.chunk_size or self.batch_size), 500)
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    if self.output_plugin == "pgoutput":
                        cur.execute(
                            """
                            SELECT lsn::text, data
                            FROM pg_logical_slot_peek_binary_changes(
                                %s, NULL, %s,
                                'proto_version', '1',
                                'publication_names', %s
                            )
                            """,
                            (self.slot_name, peek_limit, self.publication_name),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT lsn::text, data
                            FROM pg_logical_slot_peek_changes(
                                %s, NULL, %s, 'include-xids', '1'
                            )
                            """,
                            (self.slot_name, peek_limit),
                        )
                    rows = cur.fetchall() or []
                conn.commit()
        except Exception:
            return []

        from services.cdc_snapshot_window import _pk_columns, _pk_row_dict

        pk_cols = _pk_columns(sig.primary_key or self.primary_keys.get(
            (getattr(sig, "table", "") or "").strip() or self.table, self.primary_key
        ))
        # DDD-3 low watermark: an event at or before the position captured just
        # before the chunk SELECT is already reflected in the rows we read, so
        # letting it win would replace a fresh value with a stale one.
        lsn_low = str(getattr(sig, "lsn_low", "") or "")
        # The signal names the table being snapshotted; in shared multi-table
        # mode `self.table` is pinned to tables[0] and would decode the wrong one.
        sig_table = (getattr(sig, "table", "") or "").strip() or self.table

        def _is_stale(location: Any) -> bool:
            return bool(lsn_low) and _lsn_at_or_before(str(location or ""), lsn_low)

        if self.output_plugin == "pgoutput":
            from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_table

            if self._pgoutput_decoder is None:
                self._pgoutput_decoder = PgOutputDecoder()
            decoder = self._pgoutput_decoder
            for location, payload in rows:
                # Always decode: pgoutput is a stateful stream and skipping a
                # payload would desynchronise the relation cache. Filter after.
                decoded = list(
                    changes_for_table(decoder, payload, schema=self.schema, table=sig_table)
                )
                if _is_stale(location):
                    continue
                for change in decoded:
                    if change.op == "insert" and change.new_tuple:
                        events.append({"op": "c", "row": dict(change.new_tuple)})
                    elif change.op == "update" and change.new_tuple:
                        events.append({"op": "u", "row": dict(change.new_tuple)})
                    elif change.op == "delete" and change.old_tuple:
                        # Must key off sig_table's PK columns. Defaulting to
                        # tables[0] made mid-chunk deletes miss the snapshot
                        # window and re-insert tombstoned rows.
                        pk = self._pk_value(change.old_tuple, table=sig_table)
                        if pk:
                            events.append(
                                {"op": "d", "pk": pk, "row": _pk_row_dict(pk_cols, pk)}
                            )
            return events

        for location, line in rows:
            if _is_stale(location):
                continue
            text = (
                line.decode("utf-8", errors="replace")
                if isinstance(line, (bytes, memoryview))
                else (line or "")
            )
            upper = text.upper().strip()
            if upper.startswith(("BEGIN", "COMMIT", "ROLLBACK", "ABORT")):
                continue
            parsed = _parse_change_line(text, self.schema, sig_table)
            if parsed is None:
                continue
            op, old_key, new_tuple = parsed
            if op == "insert" and new_tuple:
                events.append({"op": "c", "row": dict(new_tuple)})
            elif op == "update" and new_tuple:
                events.append({"op": "u", "row": dict(new_tuple)})
            elif op == "delete" and old_key:
                pk = self._pk_value(old_key, table=sig_table)
                if pk:
                    events.append(
                        {"op": "d", "pk": pk, "row": _pk_row_dict(pk_cols, pk)}
                    )
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        """Peek WAL with txn buffering (Debezium-class) + incremental snapshots.

        Multi-table mode demuxes one slot into per-table batches that share an
        LSN resume token; callers must ``ack`` only after ``ack_barrier`` batches.
        """
        from services.cdc_incremental_runner import interleave_incremental_snapshot
        from services.cdc_multi_table import (
            MultiTableTransactionBuffer,
            parse_test_decoding_table,
        )

        # Crash mid-dump must finish the snapshot. Forcing streaming here
        # decoded WAL while undumped PK ranges were never written (silent loss).
        if self.phase == "snapshot":
            yield from self.snapshot()
            return

        self._poll_signal_table()

        for table_name in self.tables:
            yield from interleave_incremental_snapshot(
                self.source_key,
                table=table_name,
                fetch_chunk=self._fetch_incremental_chunk,
                stream_events_during_chunk=self._peek_stream_events_during_chunk,
                max_chunks_per_poll=1,
                dest_resume=self.resume_token,
            )

        if self.output_plugin == "pgoutput":
            self._ensure_publication()
            self._ensure_replica_identity()
        self._assert_slot_within_retention()
        self._ensure_slot(allow_create=not bool(getattr(self, "_resume_expected", False)))
        self.phase = "streaming"
        self._ensure_decode_schema(resume_offset=self.slot_name)
        self._maybe_record_schema_change(offset=self.slot_name)
        self.heartbeat()

        with self._conn() as conn:
            with conn.cursor() as cur:
                rows = self._peek_or_stream_rows(cur)
            conn.commit()

        buf = MultiTableTransactionBuffer()
        emitted = False
        table_set = {t.lower() for t in self.tables}
        table_by_lower = {t.lower(): t for t in self.tables}

        def _token_at(lsn: str | None) -> str:
            if lsn:
                self.consistent_point_lsn = lsn
                self._pending_ack_lsn = lsn
            return self._resume_token(phase="streaming")

        def _emit_commit(lsn: str | None):
            nonlocal emitted
            for batch in buf.commit(
                lsn=lsn,
                resume_token=_token_at(lsn),
                table_order=self.tables,
            ):
                emitted = True
                yield batch

        if self.output_plugin == "pgoutput":
            from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_tables

            if self._pgoutput_decoder is None:
                self._pgoutput_decoder = PgOutputDecoder()
            decoder = self._pgoutput_decoder
            for location, payload in rows:
                lsn = str(location) if location else None
                for change in changes_for_tables(
                    decoder,
                    payload,
                    schema=self.schema,
                    tables=table_set,
                ):
                    self._last_event_at = datetime.now(timezone.utc)
                    if change.op == "begin":
                        buf.begin(change.xid, lsn=lsn)
                    elif change.op == "commit":
                        yield from _emit_commit(lsn)
                    elif change.op == "insert" and change.new_tuple:
                        tbl = table_by_lower.get(
                            (change.relation or "").lower(), change.relation or self.table
                        )
                        pk = self._pk_value(change.new_tuple, table=tbl)
                        buf.insert(tbl, change.new_tuple, pk=pk or None, lsn=lsn)
                    elif change.op == "update" and change.new_tuple:
                        if change.toast_incomplete:
                            from services.cdc_toast import CdcToastIncompleteError

                            raise CdcToastIncompleteError(
                                f"CDC UPDATE on {change.relation} has TOAST gaps "  # nosec: B608 — error message, not SQL
                                "without old-tuple merge; set REPLICA IDENTITY FULL",
                                table=change.relation or self.table,
                                columns=list(change.toast_unchanged_cols or []),
                            )
                        tbl = table_by_lower.get(
                            (change.relation or "").lower(), change.relation or self.table
                        )
                        pk = self._pk_value(change.new_tuple, table=tbl)
                        buf.update(tbl, change.new_tuple, pk=pk or None, lsn=lsn)
                    elif change.op == "delete" and change.old_tuple:
                        tbl = table_by_lower.get(
                            (change.relation or "").lower(), change.relation or self.table
                        )
                        pk = self._pk_value(change.old_tuple, table=tbl)
                        if pk:
                            buf.delete(tbl, pk, lsn=lsn)
        else:
            for location, line in rows:
                lsn = str(location) if location else None
                text = (
                    line.decode("utf-8", errors="replace")
                    if isinstance(line, (bytes, memoryview))
                    else (line or "")
                )
                upper = text.upper().strip()
                if upper.startswith("BEGIN"):
                    parts = text.split()
                    xid = parts[1] if len(parts) > 1 else None
                    buf.begin(xid, lsn=lsn)
                    continue
                if upper.startswith("COMMIT"):
                    yield from _emit_commit(lsn)
                    continue
                if upper.startswith("ROLLBACK") or upper.startswith("ABORT"):
                    buf.rollback()
                    continue
                if "ALTER TABLE" in upper or ": DDL:" in upper:
                    self._maybe_record_schema_change(offset=self.slot_name)
                    continue
                parsed_tbl = parse_test_decoding_table(text)
                if not parsed_tbl:
                    continue
                schema_name, relation = parsed_tbl
                if schema_name.lower() != (self.schema or "").lower():
                    continue
                if relation.lower() not in table_set:
                    continue
                tbl = table_by_lower[relation.lower()]
                parsed = _parse_change_line(text, self.schema, tbl)
                if parsed is None:
                    continue
                op, old_key, new_tuple = parsed
                self._last_event_at = datetime.now(timezone.utc)
                if op == "insert" and new_tuple:
                    pk = self._pk_value(new_tuple, table=tbl)
                    buf.insert(tbl, new_tuple, pk=pk or None, lsn=lsn)
                elif op == "update" and new_tuple:
                    pk = self._pk_value(new_tuple, table=tbl)
                    buf.update(tbl, new_tuple, pk=pk or None, lsn=lsn)
                elif op == "delete" and old_key:
                    pk = self._pk_value(old_key, table=tbl)
                    if pk:
                        buf.delete(tbl, pk, lsn=lsn)

        if buf.open_xid is not None:
            if not buf.explicit_txn:
                for batch in buf.commit(
                    resume_token=self._resume_token(phase="streaming"),
                    table_order=self.tables,
                ):
                    emitted = True
                    self.resume_token = batch.resume_token
                    yield batch
                return
            if not emitted:
                yield ChangeBatch(
                    resume_token={
                        "phase": "streaming",
                        "txn_held": True,
                        "open_xid": buf.open_xid,
                        "token": self._resume_token(phase="streaming"),
                    }
                )
            return

        if not emitted:
            yield ChangeBatch(
                resume_token=_token_at(self.consistent_point_lsn),
                ack_barrier=True,
            )
