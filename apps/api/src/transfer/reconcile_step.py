"""Gate 8 reconciliation for the universal transfer engine."""

from __future__ import annotations

import copy
import logging
import re
import os
import hashlib
import json
import sqlite3
import tempfile
import time
from typing import Any

from connectors.writer_common import (
    map_rows_for_fingerprint,
    resolve_target_columns,
    transform_error_policy_for_validation_mode,
)
from services.ops_events import emit_event
from services.dest_precount import (
    OVERWRITE_SOURCE_KEYS_KEY,
    PRECOUNT_KEY,
    destination_row_count,
    VECTOR_IDENTITY_ENGINES,
    records_to_key_tuples,
    stamp_artifact_census,
    stamp_keyset_census,
    stamp_scd2_census,
    stamp_vector_census,
)
from services.reconcile_coverage import (
    CDC_SOURCE_IMAGE_COUNT,
    CDC_SOURCE_IMAGE_VALUES,
    NO_OP_DEST_UNCHANGED,
    ROUTE_PARTITIONED,
    SOURCE_DIGEST_ENGINE_POPULATION,
    SOURCE_DIGEST_REMAPPED_ROWS,
    SOURCE_DIGEST_SOURCE_REREAD,
    SOURCE_DIGEST_WRITE_PASS,
    SOURCE_DIGEST_WRITER_ACK,
    WHOLE_TABLE_NOT_COMPARABLE,
    WRITTEN_BATCH_KEYS,
    is_cdc_source_image_count_report,
    is_no_op_report,
    qualify_multi_stream_reconciliation,
)
from services.destination_key_collision_probe import (
    destination_enforces_key,
    sync_mode_appends_without_key_resolution,
)
from services.row_conservation import CENSUS_KEY, live_records_for_digest
from services.verification_ladder import PopulationTooLarge
from services.reconciliation import (
    KEYED_READBACK_ENGINES,
    TargetSampleUnavailable,
    checksum_rows,
    fingerprint_for_reconcile,
    overlay_physical_dest_types,
    read_target_sample,
    reconcile,
    sample_compare_rows,
    stamp_post_write_phase,
    verify_target,
)

from .adapters import records_to_matrix, resolve_connector_config
from .adapters_introspect import _introspect_table_schema_rich
from .models import EndpointConfig

_logger = logging.getLogger(__name__)


class _SampleReadExhausted(TargetSampleUnavailable):
    """A Gate-8 destination read that still failed after bounded retries."""

    def __init__(self, cause: TargetSampleUnavailable, *, attempts: int, transient: bool) -> None:
        self.attempts = attempts
        self.transient = transient
        super().__init__(f"{cause} (after {attempts} attempt(s))")


def _read_target_sample_retrying(purpose: str, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    """``read_target_sample`` with backoff for transient faults (QA MX2-15).

    Post-write verification runs after rows are committed; one dropped
    connection must not decide the job. Deterministic failures (grants,
    missing objects) are not retried. Exhausted reads raise
    :class:`_SampleReadExhausted` so callers still fail closed.
    """
    from services.error_handling import RetryBudget, classify_error, with_retry

    budget = RetryBudget()

    def _on_transient(exc: Exception, delay: float) -> None:
        _logger.warning(
            "Gate-8 %s read failed transiently (attempt %d/%d); retrying in %.1fs: %s",
            purpose, budget.attempts_made, budget.max_attempts, delay, exc,
        )

    try:
        return with_retry(
            lambda: read_target_sample(*args, **kwargs),
            budget=budget,
            on_transient=_on_transient,
        )
    except TargetSampleUnavailable as exc:
        transient = bool(classify_error(exc).get("retriable"))
        attempts = max(1, budget.attempts_made)
        _logger.error(
            "Gate-8 %s read unavailable after %d attempt(s) (transient=%s, table=%s): %s",
            purpose, attempts, transient, kwargs.get("table_name"), exc,
        )
        raise _SampleReadExhausted(exc, attempts=attempts, transient=transient) from exc


def _finalize_reconcile(
    payload: dict[str, Any],
    *,
    dest_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Every post-write reconcile return path gets an explicit phase."""
    out = stamp_post_write_phase(payload)
    snap = None
    if isinstance(dest_summary, dict):
        if "identity_hash_aligned" in dest_summary:
            out["identity_hash_aligned"] = bool(dest_summary.get("identity_hash_aligned"))
        alignment = dest_summary.get("identity_alignment")
        if isinstance(alignment, dict):
            out["identity_alignment"] = dict(alignment)
        versions = dest_summary.get("connector_versions")
        if isinstance(versions, dict) and versions:
            out["connector_versions"] = dict(versions)
        if dest_summary.get("source_independently_reread") is True:
            out["source_independently_reread"] = True
        snap = dest_summary.get("source_snapshot")
        raw_before = dest_summary.get(PRECOUNT_KEY)
        if out.get(PRECOUNT_KEY) is None and isinstance(raw_before, int):
            out[PRECOUNT_KEY] = raw_before
    if isinstance(snap, dict) and snap:
        out["source_snapshot"] = dict(snap)
    from services.reconciliation import attach_dest_readback

    out = attach_dest_readback(out)
    job_id = ""
    if isinstance(dest_summary, dict):
        job_id = str(dest_summary.get("job_id") or dest_summary.get("_id") or "")
    if job_id:
        try:
            from services.lineage_telemetry import emit_reconciliation, persist_event_on_job

            event = emit_reconciliation(
                run_id=str(out.get("run_id") or job_id),
                job_id=job_id,
                source_count=int(out.get("source_rows") or 0),
                target_count=int(out.get("target_rows") or 0),
                checksum_ok=out.get("checksum_match") if out.get("checksum_match") is not None else None,
            )
            persist_event_on_job(job_id, event)
        except Exception:
            pass
    return out


_ADVISORY_KEY_TOKENS = ("not enforced", "advisory", "informational")


def _destination_enforces_single_key(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_column: str,
) -> bool:
    """True when the destination itself rejects a duplicate of ``key_column``.

    Only a constraint the destination enforces makes a key-scoped digest
    comparable to the source digest: it is what guarantees the read-back returns
    one row per written key. A key named by the stream contract, the merge
    request or Map's identity inference guarantees nothing on an append-only
    write — appending the same batch twice into a keyless table left two rows per
    key, so the keyed read-back covered 2x the batch and its digest could never
    equal the source's. That failed a correct append with two hex strings.

    Enforcement is read from the destination catalog, and a catalog that
    declares its keys advisory (Snowflake/BigQuery-class ``NOT ENFORCED``) does
    not enforce them. Anything unproven answers False, which keeps the append
    delta as the proof rather than inventing comparability.
    """
    if not key_column or not table_name:
        return False
    probe_cfg = dict(cfg or {})
    if schema:
        probe_cfg.setdefault("schema", schema)
    try:
        _types, _nulls, keys = _introspect_table_schema_rich(
            db_type, probe_cfg, table_name, [], strict_namespace=True
        )
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "destination key enforcement unproven for %s.%s: %s",
            table_name,
            key_column,
            exc,
            exc_info=exc,
        )
        return False
    warnings = " ".join(str(w).lower() for w in (keys.get("warnings") or []))
    if any(token in warnings for token in _ADVISORY_KEY_TOKENS):
        return False
    return destination_enforces_key(
        key_column,
        destination_pk_columns=list(keys.get("primary_key_columns") or []),
        destination_unique_keys=list(keys.get("unique_keys") or []),
    )


def _dest_types_from_mappings(mappings: list[dict]) -> dict[str, str]:
    return {
        str(m.get("target") or ""): str(
            m.get("target_type")
            or m.get("inferredType")
            or m.get("type")
            or ""
        )
        for m in mappings
        if m.get("target")
        and (m.get("target_type") or m.get("inferredType") or m.get("type"))
    }


# Steps that legitimately change the written population. The transform-applied
# digest only compares against the destination when every declared step is
# row-local (or an engine-level no-op like scd2): population-changing recipes
# are proven by the row ledger and the dedupe winner checks instead.
def _proof_source_batches(
    *,
    source_endpoint: EndpointConfig,
    src_type: str,
    src_cfg: dict[str, Any],
    read_cols: list[str],
):
    """Yield raw source batches for the transform-applied proof.

    Its own seam so tests (and future snapshot/spool sources) can inject
    the same population the write consumed without disturbing the engine's
    own source readers.
    """
    if src_type == "mongodb":
        yield from _proof_mongo_source_batches(
            source_endpoint=source_endpoint, src_cfg=src_cfg, read_cols=read_cols
        )
        return
    import sqlalchemy as sa

    from connectors.sql_identifiers import quote_sql_identifier
    from services.dest_precount import (
        warehouse_sql_engine,
        warehouse_sql_table_ref,
    )
    from services.reconciliation import iter_select_row_dicts

    table_ref = warehouse_sql_table_ref(
        src_type,
        src_cfg,
        str(getattr(source_endpoint, "schema", "") or src_cfg.get("schema") or ""),
        str(source_endpoint.table or ""),
    )
    q = '"'
    if src_type in {"mysql", "mariadb", "clickhouse"}:
        q = "`"
    elif src_type in {"sqlserver", "mssql", "azure_sql"}:
        q = "["
    qcols = ", ".join(quote_sql_identifier(c, q) for c in read_cols)
    with warehouse_sql_engine(src_type, src_cfg) as eng:
        with eng.connect() as conn:
            for batch in iter_select_row_dicts(
                conn,
                sa.text(f"SELECT {qcols} FROM {table_ref}"),  # nosec B608
                read_cols,
            ):
                yield batch


_PROOF_OBJECT_STORES = frozenset({"s3", "gcs", "adls", "azure_blob", "minio", "r2"})


def _proof_file_dest(endpoint: EndpointConfig, dest_db_type: str) -> bool:
    return endpoint.kind != "database" or dest_db_type in (
        _PROOF_OBJECT_STORES | {"sftp"}
    )


def _proof_file_dest_batches(
    endpoint: EndpointConfig,
    dest_summary: dict[str, Any],
    dst_cfg: dict[str, Any],
    page: int = 5000,
):
    """Rows of the final written artifact, through the file reader."""
    from services.dest_precount import _iter_artifact_records

    from .connector_capabilities import resolve_driver_type

    dest_type = resolve_driver_type(endpoint.format or "")
    if endpoint.kind != "database":
        path = str(dest_summary.get("path") or dest_summary.get("export_path") or "")
        if not path or not os.path.isfile(path):
            raise FileNotFoundError(f"written file {path or '(no path)'} is not on disk")
        opened: Any = (open(path, "rb"), None)
        name, fmt = os.path.basename(path), endpoint.format
    elif dest_type == "sftp":
        from connectors.sftp_common import sftp_dest_path
        from services.object_streaming import open_sftp_binary

        cfg = {**dst_cfg, "table": str(endpoint.table or dst_cfg.get("table") or "")}
        opened = open_sftp_binary(cfg)
        name = sftp_dest_path(cfg["table"], cfg.get("file_format"))
        fmt = cfg.get("file_format") or None
    else:
        raise RuntimeError(
            f"{dest_type} objects are not re-read by the transform proof yet"
        )
    if opened is False:
        raise FileNotFoundError(f"written file {name} is missing from the destination")
    if opened is None:
        raise RuntimeError(f"written file {name} could not be opened for read-back")
    stream, closer = opened
    try:
        batch: list[dict[str, Any]] = []
        for rec in _iter_artifact_records(stream, name=name, fmt=fmt):
            batch.append(rec)
            if len(batch) >= page:
                yield batch
                batch = []
        if batch:
            yield batch
    finally:
        try:
            (closer or stream.close)()
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).debug("artifact close failed: %s", exc)


def _proof_mongo_source_batches(
    *,
    source_endpoint: EndpointConfig,
    src_cfg: dict[str, Any],
    read_cols: list[str],
    page: int = 5000,
):
    """Raw Mongo source pages, projected the way the transfer reader does."""
    from connectors.mongodb_common import _mongo_client
    from connectors.mongodb_reader import _connection_string, _page_docs_to_batch

    # The stream reader's resolution (stream.py src_db / _source_table): a
    # database named on the run wins over the connector's, and a collection
    # named in ``collection`` is the same source as one named in ``table``.
    database = str(
        getattr(source_endpoint, "database", "") or src_cfg.get("database") or "test"
    )
    collection = str(
        source_endpoint.table or getattr(source_endpoint, "collection", "") or ""
    )
    coll = _mongo_client(_connection_string(src_cfg))[database][collection]
    docs: list[dict[str, Any]] = []
    for doc in coll.find({}).sort("_id", 1).batch_size(page):
        docs.append(doc)
        if len(docs) >= page:
            yield _mongo_page_dicts(docs, src_cfg, read_cols, _page_docs_to_batch)
            docs = []
    if docs:
        yield _mongo_page_dicts(docs, src_cfg, read_cols, _page_docs_to_batch)


def _proof_mongo_dest_batches(
    *,
    endpoint: EndpointConfig,
    dst_cfg: dict[str, Any],
    read_cols: list[str],
    page: int = 5000,
):
    """The written Mongo collection, projected the way the Mongo reader does.

    A missing field and an explicit null both read back as NULL, the same as
    a SQL destination's read-back.
    """
    from connectors.mongodb_common import _mongo_client
    from connectors.mongodb_reader import _connection_string, _page_docs_to_batch
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    database = str(
        getattr(endpoint, "database", "") or dst_cfg.get("database") or "test"
    )
    collection = str(endpoint.table or getattr(endpoint, "collection", "") or "")
    coll = _mongo_client(_connection_string(dst_cfg))[database][collection]
    absent = {SQL_NULL_SENTINEL, DF_MISSING_SENTINEL}

    def _page(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {c: (None if v in absent else v) for c, v in row.items()}
            for row in _mongo_page_dicts(docs, dst_cfg, read_cols, _page_docs_to_batch)
        ]

    docs: list[dict[str, Any]] = []
    for doc in coll.find({}).sort("_id", 1).batch_size(page):
        docs.append(doc)
        if len(docs) >= page:
            yield _page(docs)
            docs = []
    if docs:
        yield _page(docs)


def _mongo_page_dicts(docs, cfg, read_cols, to_batch) -> list[dict[str, Any]]:  # type: ignore[no-untyped-def]
    batch = to_batch(docs, cfg=cfg, columns=list(read_cols), offset=0, total=None)
    out = []
    for row in batch.rows:
        rec = dict(zip(batch.headers, row))
        out.append({c: rec.get(c) for c in read_cols})
    return out


_TRANSFORM_PROOF_POPULATION_OPS = frozenset(
    {
        "dedupe",
        "dedupe_rows",
        "filter",
        "filter_rows",
        "divert_rows",
        "drop",
        "drop_rows",
        "router",
        "aggregate",
        "group_by",
    }
)

# G-1b: population-changing ops whose expected output is deterministic
# given a stable source re-read (row-removing predicates only — no arrival-
# order dependence, no cross-destination routing, no grouping). Dedupe is
# proven by the winner checks; router fans out to several destinations;
# aggregate/group_by need grouped digest semantics — all stay skipped.
_TRANSFORM_PROOF_PROVABLE_POPULATION_OPS = frozenset(
    {"filter", "filter_rows", "divert_rows", "drop", "drop_rows"}
)


# --- run-scoped proof scratch (leak fix) ------------------------------------
# The proof stores are sqlite files that grow to hundreds of MB on large runs.
# Every store file lives in a directory something owns: the proof's scope, or
# the store's own private dir removed on close / collection / exit. See
# ``proof_scratch``: a store opened outside the wrapper used to land in the
# shared temp dir, where no cleanup ever looked.
from .proof_scratch import (  # noqa: E402
    _PROOF_SCRATCH,  # noqa: F401 - re-exported for callers that bind a scope
    in_proof_scratch,
    own_scratch,
    proof_scratch_scope,
)


class _PinStore:
    """Disk-backed per-key state for the transform proof (G-1g).

    The write-time ledger pins key -> raw-row hash; the proof then joins
    re-read raw hashes, expected fingerprints and destination
    fingerprints per key. Holding those maps in memory costs ~0.9 KB per
    key — ~187 MB at 200k keys. A temp SQLite db keeps the proof O(1) in
    memory instead: classification is SQL and only a small sample of
    culprit keys is ever materialized for column naming.
    """

    def __init__(self) -> None:
        self.path, holder = own_scratch(self, "df_pinmap_", ".db")
        self.db = holder[0] = sqlite3.connect(self.path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        for ddl in (
            "CREATE TABLE pin (k TEXT PRIMARY KEY, h TEXT)",
            "CREATE TABLE rawh (k TEXT PRIMARY KEY, h TEXT)",
            "CREATE TABLE exp (k TEXT PRIMARY KEY, fp TEXT, decl TEXT)",
            "CREATE TABLE destk (k TEXT PRIMARY KEY, fp TEXT, vals TEXT)",
            "CREATE TABLE fpm (side TEXT, h TEXT)",
            "CREATE INDEX fpm_i ON fpm(h, side)",
            "CREATE TABLE cv (col TEXT, side TEXT, vh TEXT)",
            "CREATE INDEX cv_i ON cv(col, vh, side)",
            "CREATE TABLE eo (col TEXT, rvh TEXT, ovh TEXT)",
            "CREATE INDEX eo_i ON eo(col, rvh)",
            "CREATE TABLE orc (col TEXT, rvh TEXT, ovh TEXT)",
            "CREATE INDEX orc_i ON orc(col, rvh)",
        ):
            self.db.execute(ddl)

    def put(self, table: str, rows: list[tuple]) -> None:
        if rows:
            qs = ",".join("?" * len(rows[0]))
            self.db.executemany(
                f"INSERT OR REPLACE INTO {table} VALUES ({qs})",  # nosec B608
                rows,
            )

    def flush(self) -> None:
        self.db.commit()

    def side_count(self, side: str) -> int:
        return int(
            self.db.execute(
                "SELECT COUNT(*) FROM fpm WHERE side=?", (side,)
            ).fetchone()[0]
        )

    def fp_extra(self):
        # (h, extra, raw_count) -- dest fingerprints exceeding expected.
        return self.db.execute(
            "SELECT d.h, d.n - COALESCE(e.n,0), COALESCE(r.n,0) FROM "
            "(SELECT h, COUNT(*) n FROM fpm WHERE side='d' GROUP BY h) d "
            "LEFT JOIN (SELECT h, COUNT(*) n FROM fpm WHERE side='e' "
            "GROUP BY h) e ON e.h = d.h "
            "LEFT JOIN (SELECT h, COUNT(*) n FROM fpm WHERE side='r' "
            "GROUP BY h) r ON r.h = d.h "
            "WHERE d.n > COALESCE(e.n,0)"
        )

    def fp_missing_total(self) -> int:
        return int(
            self.db.execute(
                "SELECT COALESCE(SUM(n_e - n_d),0) FROM ("
                "SELECT e.n n_e, COALESCE(d.n,0) n_d FROM "
                "(SELECT h, COUNT(*) n FROM fpm WHERE side='e' GROUP BY h) e "
                "LEFT JOIN (SELECT h, COUNT(*) n FROM fpm WHERE side='d' "
                "GROUP BY h) d ON d.h = e.h) WHERE n_e > n_d"
            ).fetchone()[0]
        )

    def col_extra(self, tcol: str, scol: str):
        # (vh, extra, raw_count) -- dest col values exceeding expected.
        return self.db.execute(
            "SELECT d.vh, d.n - COALESCE(e.n,0), COALESCE(r.n,0) FROM "
            "(SELECT vh, COUNT(*) n FROM cv WHERE col=? AND side='d' "
            "GROUP BY vh) d "
            "LEFT JOIN (SELECT vh, COUNT(*) n FROM cv WHERE col=? AND "
            "side='e' GROUP BY vh) e ON e.vh = d.vh "
            "LEFT JOIN (SELECT vh, COUNT(*) n FROM cv WHERE col=? AND "
            "side='r' GROUP BY vh) r ON r.vh = d.vh "
            "WHERE d.n > COALESCE(e.n,0)",
            (tcol, tcol, scol),
        )

    def oracle_mismatch_cols(self) -> list[str]:
        return [
            r[0]
            for r in self.db.execute(
                "SELECT DISTINCT o.col FROM orc o "
                "JOIN eo e ON e.col = o.col AND e.rvh = o.rvh "
                "WHERE e.ovh IS NOT o.ovh"
            )
        ]

    def culprit_keys_by_kind(self, limit: int = 32) -> dict[str, list[str]]:
        """Culprit keys split by kind, for tamper drill-down.

        ``changed``/``missing_in_dest`` only name keys proven stable in
        source during the run (write-time pin hash == re-read hash), so a
        drifted key is never blamed as a culprit. ``extra_in_dest`` names
        keys present only on the destination."""
        lim = limit + 1
        stable = (
            "FROM exp e "
            "JOIN pin p ON e.k = p.k "
            "JOIN rawh r ON e.k = r.k AND p.h = r.h "
            "LEFT JOIN destk d ON e.k = d.k "
        )
        changed = [
            r[0]
            for r in self.db.execute(
                f"SELECT e.k {stable} WHERE d.k IS NOT NULL "  # nosec B608
                "AND d.fp IS NOT e.fp ORDER BY e.k LIMIT ?",
                (lim,),
            )
        ]
        missing = [
            r[0]
            for r in self.db.execute(
                f"SELECT e.k {stable} WHERE d.k IS NULL ORDER BY e.k LIMIT ?",  # nosec B608
                (lim,),
            )
        ]
        extra = [
            r[0]
            for r in self.db.execute(
                "SELECT d.k FROM destk d LEFT JOIN pin p ON p.k = d.k "
                "WHERE p.k IS NULL ORDER BY d.k LIMIT ?",
                (lim,),
            )
        ]
        out: dict[str, list[str]] = {}
        if changed:
            out["changed"] = changed[:limit]
        if missing:
            out["missing_in_dest"] = missing[:limit]
        if extra:
            out["extra_in_dest"] = extra[:limit]
        return out

    def drift_stats(self) -> tuple[int, int, int]:
        """(exp keys unpinned, exp key count, dest-only key count)."""
        drift_n = int(
            self.db.execute(
                "SELECT count(*) FROM exp e "
                "LEFT JOIN pin p ON e.k = p.k "
                "LEFT JOIN rawh r ON e.k = r.k "
                "WHERE p.h IS NULL OR p.h IS NOT r.h"
            ).fetchone()[0]
        )
        n_keys = int(
            self.db.execute("SELECT count(*) FROM exp").fetchone()[0]
        )
        dest_only = int(
            self.db.execute(
                "SELECT count(*) FROM destk d "
                "LEFT JOIN exp e ON d.k = e.k WHERE e.k IS NULL"
            ).fetchone()[0]
        )
        return drift_n, n_keys, dest_only

    def culprit_keys(self, limit: int = 33) -> tuple[int, list[str]]:
        """(culprit key count, up to ``limit`` sample keys).

        A culprit is a pinned source key (write-time raw hash == re-read
        hash) whose destination row is missing or fingerprint-differs
        from the recipe-expected row — provable on a stable population.
        """
        where = (
            "FROM exp e "
            "JOIN pin p ON e.k = p.k "
            "JOIN rawh r ON e.k = r.k AND p.h = r.h "
            "LEFT JOIN destk d ON e.k = d.k "
            "WHERE d.k IS NULL OR d.fp IS NOT e.fp"
        )
        n = int(
            self.db.execute(f"SELECT count(*) {where}").fetchone()[0]  # nosec B608
        )
        keys = [
            r[0]
            for r in self.db.execute(
                f"SELECT e.k {where} ORDER BY e.k LIMIT ?",  # nosec B608
                (limit,),
            )
        ]
        return n, keys

    def samples(self, keys: list[str]) -> dict[str, tuple]:
        """expected-decl and dest vals for a bounded set of keys."""
        out: dict[str, tuple] = {}
        for i in range(0, len(keys), 200):
            chunk = keys[i : i + 200]
            qs = ",".join("?" * len(chunk))
            exp_rows = dict(
                self.db.execute(
                    f"SELECT k, decl FROM exp WHERE k IN ({qs})",  # nosec B608
                    chunk,
                )
            )
            dest_rows = dict(
                self.db.execute(
                    f"SELECT k, vals FROM destk WHERE k IN ({qs})",  # nosec B608
                    chunk,
                )
            )
            for k in chunk:
                ed = exp_rows.get(k)
                dv = dest_rows.get(k)
                out[k] = (
                    json.loads(ed) if ed else None,
                    json.loads(dv) if dv else None,
                )
        return out

    def close(self) -> None:
        # Closes the db, removes the file and any private dir — once.
        self._scratch_release()


class _DiskKeySet:
    """Disk-backed string set for run-scoped key filtering (G-1g).

    Run-key scoping for SCD2/append proofs held every run key in a set —
    O(keys) memory again at 1M rows. Temp SQLite keeps membership
    lookups indexed and bounded.
    """

    def __init__(self) -> None:
        self.path, holder = own_scratch(self, "df_scope_", ".db")
        self.db = holder[0] = sqlite3.connect(self.path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("CREATE TABLE scopek (k TEXT PRIMARY KEY)")
        self._buf: list[str] = []

    def add(self, k: str) -> None:
        self._buf.append(k)
        if len(self._buf) >= 5000:
            self.flush()

    def flush(self) -> None:
        if self._buf:
            self.db.executemany(
                "INSERT OR IGNORE INTO scopek VALUES (?)",
                [(k,) for k in self._buf],
            )
            self._buf.clear()

    def __contains__(self, k: str) -> bool:
        self.flush()
        return bool(
            self.db.execute(
                "SELECT EXISTS(SELECT 1 FROM scopek WHERE k = ?)", (k,)
            ).fetchone()[0]
        )

    def close(self) -> None:
        # Closes the db, removes the file and any private dir — once.
        self._scratch_release()


_RECIPE_ORACLE_DIALECTS = {
    "postgresql",
    "postgres",
    "mysql",
    "mariadb",
    # X-19/X-20: SQL Server was silently left out — every sqlserver route
    # reported "recipe re-applied" with no independent oracle.
    "sqlserver",
    "mssql",
    "azure_sql",
}
_TSQL_DIALECTS = {"sqlserver", "mssql", "azure_sql"}


#: Exactly the characters ``str.strip()`` removes — the engine's ``trim``.
_PY_WHITESPACE = "".join(chr(c) for c in range(0x3001) if chr(c).isspace())


def _recipe_step_sql(
    op: str,
    opts: dict[str, Any],
    col_expr: str,
    params: dict[str, Any],
    *,
    src_type: str,
    dest_ddl: str = "",
    nan_is_null: bool = False,
) -> str | None:
    """Faithful SQL equivalent of one recipe step, or None when the dialect
    has none. The G-1d independent oracle recomputes declared columns in the
    source database and compares multisets — an op without a faithful
    mapping stays "recipe re-applied" (honest), never fake SQL.

    The oracle only runs on database sources, so every fragment states the
    engine's *typed* value model (``ShapeRunner(typed_values=True)``): '' and
    whitespace are stored values, only SQL NULL (or a reader's NULL wire
    sentinel) is null. ``nan_is_null``: the column is a float carrier whose
    NaN the engine reads as null."""
    tsql = src_type in _TSQL_DIALECTS
    mysql = src_type in {"mysql", "mariadb"}

    def _lit(v: Any) -> str:
        key = f"ov{len(params)}"
        params[key] = v
        return f":{key}"

    def _text_cast(expr: str) -> str:
        if tsql:
            return f"CAST({expr} AS nvarchar(max))"
        if src_type in {"mysql", "mariadb"}:
            return f"CAST({expr} AS CHAR)"
        return f"{expr}::text"

    def _btrim(expr: str) -> str:
        if tsql:
            return f"LTRIM(RTRIM({expr}))"
        if mysql:
            return f"TRIM({expr})"
        return f"BTRIM({expr})"

    if op == "case":
        mode = str(opts.get("mode") or "").casefold()
        if mode == "upper":
            return f"UPPER({col_expr})"
        if mode == "lower":
            return f"LOWER({col_expr})"
        return None
    if op == "trim":
        # The engine's trim is str.strip(): every Unicode whitespace character,
        # not only the space SQL TRIM removes — a '\tbob' cell read as a broken
        # step and failed the run.
        if tsql:
            return f"TRIM({_lit(_PY_WHITESPACE)} FROM {col_expr})"
        if mysql:
            # Whitespace has no meaning inside an ICU class: no escaping.
            ws = _PY_WHITESPACE
            return f"REGEXP_REPLACE({col_expr}, {_lit(f'^[{ws}]+|[{ws}]+$')}, '')"
        return f"BTRIM({col_expr}, {_lit(_PY_WHITESPACE)})"
    if op == "collapse_whitespace":
        # _fn_collapse_whitespace = re.sub(r"\s+", " ", t).strip()
        if tsql:
            # T-SQL has no regex engine — honestly uncovered here.
            return None
        if src_type in {"postgresql", "postgres"}:
            ws = _lit(r"\s+")
            return (
                "BTRIM(regexp_replace("
                f"{col_expr}, {ws}, {_lit(' ')}, 'g'))"
            )
        ws = _lit("\\s+")
        return (
            "TRIM(REGEXP_REPLACE("
            f"{col_expr}, {ws}, {_lit(' ')}))"
        )
    if op == "replace":
        if opts.get("regex"):
            pat = str(opts.get("search") or "")
            # Only patterns that cannot match an empty string and use no
            # look-around/inline-flag syntax are exact under the source's
            # regex engine — anything else stays uncovered.
            if re.search(r"\*|\?|\(\?", pat):
                return None
            if tsql:
                return None  # no T-SQL regex — honestly uncovered
            if src_type in {"postgresql", "postgres"}:
                return (
                    f"regexp_replace({col_expr}, {_lit(pat)}, "
                    f"{_lit(opts.get('replacement', ''))}, 'g')"
                )
            return (
                f"REGEXP_REPLACE({col_expr}, {_lit(pat)}, "
                f"{_lit(opts.get('replacement', ''))})"
            )
        return (
            f"REPLACE({col_expr}, {_lit(opts.get('search'))}, "
            f"{_lit(opts.get('replacement', ''))})"
        )
    if op == "default_if_null":
        # Typed is_blank: NULL or a NULL wire sentinel. '' and '   ' are values
        # on a database source — defaulting them here failed every run whose
        # trim step produced '' (trim -> default_if_null -> upper).
        txt = _text_cast(col_expr)
        nan = f"OR {txt} = 'NaN' " if nan_is_null else ""
        return (
            f"CASE WHEN {col_expr} IS NULL "
            f"{nan}"
            f"OR {_btrim(txt)} IN ({_lit('__df_sql_null__')}, "
            f"{_lit('__df_missing__')}, {_lit('__df_ddb_null__')}) "
            f"THEN {_lit(opts.get('value'))} "
            f"ELSE {txt} END"
        )
    if op == "null_if":
        expr = col_expr
        for v in opts.get("values") or []:
            expr = f"NULLIF({expr}, {_lit(v)})"
        return expr
    if op == "cast_column":
        to_type = str(opts.get("to_type") or "").casefold()
        if to_type == "text":
            if tsql:
                return f"CAST({col_expr} AS nvarchar(max))"
            cast = "TEXT" if src_type in {"postgresql", "postgres"} else "CHAR"
            return f"CAST({col_expr} AS {cast})"
        if to_type in {"integer", "bigint", "smallint", "int"}:
            # Engine: to_number -> to_integral_value(ROUND_HALF_UP), refusing
            # non-whole inputs. Each fragment rounds half-away-from-zero the
            # same way; a source value the engine refused never lands in eo,
            # so a NULL/0 oracle value for it simply never joins.
            if tsql:
                return (
                    "CAST(ROUND(TRY_CAST("
                    f"{col_expr} AS decimal(38,10)),0) AS bigint)"
                )
            if src_type in {"postgresql", "postgres"}:
                # numeric -> bigint rounds half-away-from-zero.
                return f"CAST(CAST({col_expr} AS numeric) AS bigint)"
            # MySQL CAST(x AS SIGNED) rounds; verified against the engine in
            # test_oracle_cast_column_integer_matches_mysql_cast.
            return f"CAST({col_expr} AS SIGNED)"
        if to_type == "decimal":
            precision = opts.get("precision")
            scale = opts.get("scale")
            if precision in (None, ""):
                # X-3: a bare cast_column->decimal can still be proven when the
                # approved mapping / resolved destination column declares the
                # target width, e.g. DECIMAL(20,4). No declared width -> stay
                # honestly uncovered (never invent a precision).
                m_decl = re.search(
                    r"(?:DECIMAL|NUMERIC|NUMBER)\s*\(\s*(\d{1,2})\s*(?:,\s*(\d{1,2})\s*)?\)",
                    str(dest_ddl or ""),
                    re.I,
                )
                if not m_decl:
                    return None
                precision = int(m_decl.group(1))
                scale = int(m_decl.group(2) or 0)  # unbounded decimal — no faithful target width
            try:
                width = int(precision)
                sc = 0 if scale in (None, "") else int(scale)
            except (TypeError, ValueError):
                return None
            # Engine quantizes ROUND_HALF_UP then refuses overflow; every
            # dialect's CAST(.. AS DECIMAL(p,s)) applies the same rule —
            # PG errors on overflow, MySQL clamps (harmless: the engine
            # refused that row so no eo entry exists), T-SQL TRY_CAST NULLs.
            if tsql:
                return f"TRY_CAST({col_expr} AS decimal({width},{sc}))"
            return f"CAST({col_expr} AS DECIMAL({width},{sc}))"
    return None


@in_proof_scratch
def _transform_applied_proof_impl(
    *,
    shape_recipe: dict[str, Any] | None,
    dest_summary: dict[str, Any],
    source_endpoint: EndpointConfig | None,
    endpoint: EndpointConfig,
    columns: list[str],
    mappings: list[dict] | None,
    source_schema: dict[str, str] | None,
    recon: dict[str, Any],
) -> dict[str, Any] | None:
    """G-1: prove the approved recipe actually ran on a shaped run.

    Both digests a shaped run compares derive from the same stream — a recipe
    step dropped upstream of the write (or a runner that silently no-ops)
    leaves source and destination equally *unshaped*, so their agreement is
    not proof. This re-reads the source raw, applies the approved recipe
    again, and compares that expected-output digest to what the destination
    actually holds (current source-keyed rows for SCD2; full read-back for
    overwrite/empty-target runs). A dropped step becomes a real digest
    mismatch — failed, naming the step that owns the diverging column.
    """
    if not dest_summary.get("shape_recipe_hash"):
        return None
    if not isinstance(shape_recipe, dict) or not (shape_recipe.get("steps")):
        return {
            "status": "unverifiable",
            "reason": "approved recipe text was not available to the verifier",
        }
    steps = [
        s
        for s in (shape_recipe.get("steps") or [])
        if isinstance(s, dict) and s.get("enabled") is not False
    ]
    pop_ops = {
        str(s.get("op") or "")
        for s in steps
        if str(s.get("op") or "") in _TRANSFORM_PROOF_POPULATION_OPS
    }
    if pop_ops - _TRANSFORM_PROOF_PROVABLE_POPULATION_OPS:
        return {
            "status": "skipped",
            "covered_by": "row ledger + dedupe winner checks",
            "reason": (
                "recipe changes the written population — removals are proven "
                "by the row ledger and dedupe winner checks"
            ),
        }
    if source_endpoint is None or getattr(source_endpoint, "kind", "") != "database":
        return {
            "status": "unverifiable",
            "reason": "independent source re-read requires a database endpoint",
        }

    import sqlalchemy as sa

    from connectors.sql_identifiers import quote_sql_identifier
    from services.dest_precount import (
        warehouse_sql_engine,
        warehouse_sql_table_ref,
    )
    from services.reconciliation import (
        canonical_checksum_from_iter,
        iter_select_row_dicts,
        normalize_cell,
    )
    from services.scd2_engine import _stamp_set
    from services.shape_apply import ShapeRunner
    from services.shape_models import ShapeRecipe
    from services.sync_cursor import is_overwrite_sync, map_source_to_target

    from .connector_capabilities import resolve_driver_type

    _NULL_SENTINEL = "\x00NULL\x00"

    _TEXT_FAMILY_DDL_RE = re.compile(
        r"^(?:VAR)?CHAR(?:ACTER)?(?:\s+VARYING)?|BPCHAR|(?:N)?(?:VAR)?CHAR2|"
        r"TEXT|TINYTEXT|MEDIUMTEXT|LONGTEXT|NTEXT|CLOB|NCLOB|CHARACTER|"
        r"STRING|ENUM|SET\b",
        re.I,
    )

    def _key_norm(v: Any) -> str:
        # Join keys + source-side row hashes. Strings stay literal — a text
        # key '01' never collapses into '1' or 'A ' into 'A'. Non-str driver
        # values take the canonical path so write-time and re-read reprs of
        # the same value agree (datetime vs ISO text, Decimal scale).
        # SQL NULL stays distinct from ''.
        if v is None:
            return _NULL_SENTINEL
        if isinstance(v, str):
            return v
        try:
            return normalize_cell(v)
        except Exception:
            return str(v)

    def _val_norm(v: Any, ddl: str = "") -> str:
        # Type-driven canonicalization for VALUE compares (recipe output vs
        # destination read-back). Strings stay literal unless the column is
        # numeric-family — 'A ' vs 'A' and '01' vs '1' on a TEXT column are
        # real changes and must be detected. Non-str driver values take the
        # Gate-8 canonical path (Decimal scale, datetimes, bytes) so
        # Decimal('30.990') read-back equals recipe output '30.99' on a
        # DECIMAL column.
        if v is None:
            return _NULL_SENTINEL
        try:
            # Oracle VARCHAR2 stores '' AS NULL natively — '' vs NULL
            # can never be told apart on read-back there, so they are
            # the same value on an Oracle destination only.
            if v == "" and "oracle" in str(dest_db_type or "").lower():
                return _NULL_SENTINEL
            if (
                isinstance(v, str)
                and ddl
                and _TEXT_FAMILY_DDL_RE.match(str(ddl).strip())
            ):
                # Declared text column: 'A ' vs 'A' and '01' vs '1' are
                # real changes and must be detected.
                return v
            return normalize_cell(v, ddl_type=ddl, engine=dest_db_type)
        except Exception:
            return str(v)

    def _val_ddl(col: str) -> str:
        return str(dest_types.get(col) or dest_types.get(col.lower()) or "")

    def _src_norm(v: Any, ddl: str = "") -> str:
        # Source-side hash normalization — must match stream._pin_norm on
        # the write ledger: declared text compares literally, numeric/date
        # columns canonicalize so COPY-text and driver reprs hash alike.
        if v is None:
            return _NULL_SENTINEL
        try:
            if isinstance(v, str) and ddl and _TEXT_FAMILY_DDL_RE.match(
                str(ddl).strip()
            ):
                return v
            return normalize_cell(v, ddl_type=str(ddl or ""), engine=src_type)
        except Exception:
            return str(v)

    def _src_ddl(col: str) -> str:
        ss = source_schema or {}
        return str(ss.get(col) or ss.get(col.lower()) or "")

    def _step_cols(s: dict[str, Any]) -> list[str]:
        opts = s.get("options") or {}
        col = (
            s.get("column")
            or opts.get("column")
            or s.get("target")
            or opts.get("target")
        )
        if col:
            return [str(col)]
        # Predicate steps name their columns inside a [col] expression.
        expr = str(
            opts.get("condition")
            or s.get("condition")
            or opts.get("expression")
            or ""
        )
        return re.findall(r"\[([^\]]+)\]", expr)

    declared_src: list[str] = []
    declared_map: dict[int, list[str]] = {}
    for i, s in enumerate(steps):
        cols = _step_cols(s)
        if cols:
            declared_map[i] = cols
            declared_src.extend(cols)

    mappings = mappings or []
    dest_db_type = resolve_driver_type(endpoint.format or "")
    dest_types = _dest_types_from_mappings(mappings)
    scd2_opts = dest_summary.get("scd2_opts")
    sync_mode = str(
        dest_summary.get("sync_mode")
        or dest_summary.get("effective_sync_mode")
        or ""
    )
    scope_pk: list[str] = []
    if scd2_opts:
        target_cols, _ = resolve_target_columns(
            mappings, source_schema or {}, preserve_case=True
        )
        digest_cols = [c for c in target_cols if c not in _stamp_set(scd2_opts)]
    else:
        digest_cols = _mapped_targets(mappings, columns)
        if not is_overwrite_sync(sync_mode):
            # G-1c: an append/upsert can hold rows outside this run's
            # population — scope the read-back to this run's keys so the
            # proof still compares honestly.
            scope_pk = [
                c
                for c in (
                    dest_summary.get("primary_key_columns")
                    or dest_summary.get("conflict_columns")
                    or []
                )
                if c in digest_cols
            ]
            if not scope_pk:
                return {
                    "status": "skipped",
                    "reason": (
                        "non-overwrite run with no key columns — the "
                        "destination read-back cannot be scoped to this "
                        "run's population"
                    ),
                }

    declared_tcols = [map_source_to_target(c, mappings) for c in declared_src]
    col_pairs = list(zip(declared_src, declared_tcols))

    # --- G-1e: streaming proof -----------------------------------------
    # Nothing below materializes a full row list: populations compare as
    # row-fingerprint Counters and per-declared-column value Counters, so
    # memory is bounded by distinct values, not row count.
    raw_count = 0
    # G-1h: the multiset fingerprints, per-column value multisets and the
    # oracle raw->output pairing live in the sqlite proof store — the
    # in-memory Counters/dicts they replaced were O(distinct values),
    # which degenerates to O(rows) on unique-valued columns
    # (~1.6 KB/row at 1M rows, 1.7 GB RSS).
    raw_src_cols = list(dict.fromkeys(declared_src))
    exp_tcols = list(dict.fromkeys(declared_tcols))
    scope_keys: _DiskKeySet | None = _DiskKeySet() if scope_pk else None

    def _fp(row: dict[str, Any]) -> tuple:
        return tuple(_key_norm(row.get(c)) for c in digest_cols)

    def _fph(row: dict[str, Any]) -> str:
        return hashlib.sha256(
            "\x1f".join(
                _val_norm(row.get(c), _val_ddl(c)) for c in digest_cols
            ).encode()
        ).hexdigest()[:16]

    def _vh(v: Any) -> str:
        return hashlib.sha256(str(v).encode()).hexdigest()[:16]

    # G-1d part 1: write-time raw-pin ledger. key -> hash of the raw row
    # the write consumed. A key whose re-read hash matches is pinned and
    # judged strictly (dest != expected fails naming the step); a moved,
    # new or missing key is drift -> unverifiable with counts.
    pin_meta = dest_summary.get("shape_raw_ledger")
    pin_store: _PinStore | None = None
    pin_key_cols: list[str] = []
    pin_hash_cols: list[str] = []
    if isinstance(pin_meta, dict) and pin_meta.get("keys"):
        pin_key_cols = [
            map_source_to_target(c, mappings)
            for c in (pin_meta.get("key_columns") or [])
        ]
        pin_hash_cols = list(pin_meta.get("hashed_columns") or [])
        _pin_path = str(pin_meta.get("file") or "")
        try:
            if all(c in digest_cols for c in pin_key_cols):
                pin_store = _PinStore()
                if _pin_path:
                    _buf: list[tuple] = []
                    with open(_pin_path) as _fh:
                        for line in _fh:
                            k, _, h = line.rstrip("\n").partition("\x00")
                            if k:
                                _buf.append((k, h))
                            if len(_buf) >= 5000:
                                pin_store.put("pin", _buf)
                                _buf = []
                    if _buf:
                        pin_store.put("pin", _buf)
                else:
                    pin_store.put(
                        "pin",
                        [
                            (str(k), str(v))
                            for k, v in (pin_meta.get("entries") or {}).items()
                        ],
                    )
                pin_store.flush()
        except Exception:
            if pin_store is not None:
                pin_store.close()
            pin_store = None
        if _pin_path:
            try:
                os.unlink(_pin_path)
            except OSError:
                pass
    keyed = pin_store is not None
    if pin_store is None:
        # Keyless runs still need the store: the multiset fingerprint and
        # per-column value state lives on disk for them too (G-1h).
        pin_store = _PinStore()
    _rawh_buf: list[tuple] = []
    _exp_buf: list[tuple] = []
    _destk_buf: list[tuple] = []
    _fpm_buf: list[tuple] = []
    _cv_buf: list[tuple] = []
    _eo_buf: list[tuple] = []

    def _flush_ms() -> None:
        pin_store.put("fpm", _fpm_buf)
        pin_store.put("cv", _cv_buf)
        pin_store.put("eo", _eo_buf)
        _fpm_buf.clear()
        _cv_buf.clear()
        _eo_buf.clear()
        if keyed:
            pin_store.put("rawh", _rawh_buf)
            pin_store.put("exp", _exp_buf)
            pin_store.put("destk", _destk_buf)
            _rawh_buf.clear()
            _exp_buf.clear()
            _destk_buf.clear()
        pin_store.flush()

    # D188 — an incremental run's expected output is the recipe applied to the
    # rows its cursor window selected, not to the whole source table: a row
    # edited without moving its cursor was not read, so recomputing it here
    # reads as "the destination does not hold the recipe's output".
    from services.incremental_drift import IN_WINDOW, RunWindow

    _proof_window = (
        RunWindow.from_summary(dest_summary) if scope_pk and not scd2_opts else None
    )

    def _expected_iter():
        nonlocal raw_count
        _extra = (
            [
                c
                for c in dict.fromkeys((_proof_window.cursor, _proof_window.pk))
                if c and c not in read_cols
            ]
            if _proof_window is not None
            else []
        )
        for batch in _proof_source_batches(
            source_endpoint=source_endpoint,
            src_type=src_type,
            src_cfg=src_cfg,
            read_cols=read_cols + _extra,
        ):
            if _proof_window is not None:
                batch = [
                    {k: v for k, v in r.items() if k not in _extra}
                    for r in batch
                    if _proof_window.position(r) == IN_WINDOW
                ]
                if not batch:
                    continue
            raw_count += len(batch)
            outs = runner.records(batch)
            for row in batch:
                trow = {
                    map_source_to_target(k, mappings): v
                    for k, v in row.items()
                }
                _fpm_buf.append(("r", _fph(trow)))
                for sc in raw_src_cols:
                    _cv_buf.append(
                        (sc, "r", _vh(_key_norm(row.get(sc))))
                    )
                if keyed:
                    _rawh_buf.append(
                        (
                            "\x1f".join(
                                _key_norm(trow.get(c))
                                for c in pin_key_cols
                            ),
                            hashlib.sha256(
                                "\x1f".join(
                                    _src_norm(row.get(c), _src_ddl(c))
                                    for c in pin_hash_cols
                                ).encode()
                            ).hexdigest()[:16],
                        )
                    )
            for i, out in enumerate(outs):
                orow = {
                    map_source_to_target(k, mappings): v
                    for k, v in out.items()
                }
                _fpm_buf.append(("e", _fph(orow)))
                if keyed:
                    _exp_buf.append(
                        (
                            "\x1f".join(
                                _key_norm(orow.get(c))
                                for c in pin_key_cols
                            ),
                            _fph(orow),
                            json.dumps(
                                {
                                    c: _val_norm(orow.get(c), _val_ddl(c))
                                    for c in digest_cols
                                }
                            ),
                        )
                    )
                if scope_keys is not None:
                    scope_keys.add(
                        "\x1f".join(
                            _key_norm(orow.get(c)) for c in scope_pk
                        )
                    )
                for sc, tc in col_pairs:
                    _cv_buf.append(
                        (tc, "e", _vh(_val_norm(orow.get(tc), _val_ddl(tc))))
                    )
                    if i < len(batch):
                        _eo_buf.append(
                            (
                                tc,
                                _vh(_key_norm(batch[i].get(sc))),
                                _vh(_val_norm(orow.get(tc), _val_ddl(tc))),
                            )
                        )
                yield orow
            _flush_ms()
        for out in runner.drain() or []:
            orow = {
                map_source_to_target(k, mappings): v
                for k, v in out.items()
            }
            _fpm_buf.append(("e", _fph(orow)))
            if keyed:
                _exp_buf.append(
                    (
                        "\x1f".join(
                            _key_norm(orow.get(c)) for c in pin_key_cols
                        ),
                        _fph(orow),
                        json.dumps(
                            {
                                c: _val_norm(orow.get(c), _val_ddl(c))
                                for c in digest_cols
                            }
                        ),
                    )
                )
            if scope_keys is not None:
                scope_keys.add(
                    "\x1f".join(
                        _key_norm(orow.get(c)) for c in scope_pk
                    )
                )
            for _sc, tc in col_pairs:
                _cv_buf.append(
                    (tc, "e", _vh(_val_norm(orow.get(tc), _val_ddl(tc))))
                )
            yield orow
        _flush_ms()

    try:
        # The recipe reads the SOURCE columns; ``columns`` is the shaped image.
        # Parsing against the image refused every rename/derive recipe ("column
        # 'name' is not available") and the proof never ran. The runner that
        # shaped the write stamped the columns it read (shape_proof).
        _read_cols = list(
            (dest_summary.get("shape_proof") or {}).get("input_columns") or columns
        )
        parsed = ShapeRecipe.parse(shape_recipe, source_columns=_read_cols)
        src_type = resolve_driver_type(source_endpoint.format or "")
        from services.shape_apply import is_typed_value_source as _itvs

        runner = ShapeRunner(parsed, typed_values=_itvs(src_type))
        src_cfg = resolve_connector_config(source_endpoint)
        read_cols = list(runner.recipe.input_columns or columns)
        expected_checksum = canonical_checksum_from_iter(
            _expected_iter(),
            digest_cols,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
    except Exception as exc:  # the proof must not crash the verifier
        return {
            "status": "unverifiable",
            "reason": f"expected-output recompute failed: {exc!s}"[:300],
        }
    exp_n = pin_store.side_count("e")

    # --- G-1d: independent SQL oracle for row-local ops ----------------
    # The recipe is re-applied in Python; a bug in _transform_value then
    # lands identically on the write and the expected side and is
    # invisible. For ops with a faithful SQL equivalent we recompute the
    # declared columns in the SOURCE database on the same re-read —
    # drift-free — and compare per-column multisets. Any disagreement is
    # an op-implementation defect, not drift, so it fails and names the
    # step. Coverage is honestly partial: ops/dialects without a faithful
    # SQL mapping are reported as "recipe re-applied" only.
    _ORACLE_DIALECTS = {
        "postgresql",
        "postgres",
        "mysql",
        "mariadb",
        # X-19/X-20: SQL Server was silently left out — every sqlserver
        # route reported "recipe re-applied" with no independent oracle.
        "sqlserver",
        "mssql",
        "azure_sql",
    }

    if src_type in {"mysql", "mariadb", "clickhouse"}:
        qs = '`'
    elif src_type in {"sqlserver", "mssql", "azure_sql"}:
        qs = "["
    else:
        qs = '"'
    # A Mongo source has no SQL table: the SQL oracle and the tracked-equality
    # join below are SQL-source only and already skip on an empty ref.
    src_ref_sql = "" if src_type == "mongodb" else warehouse_sql_table_ref(
        src_type,
        src_cfg,
        str(
            getattr(source_endpoint, "schema", "")
            or src_cfg.get("schema")
            or ""
        ),
        str(source_endpoint.table or ""),
    )
    oracle_params: dict[str, Any] = {}
    oracle_exprs: dict[str, str] = {}
    oracle_ok = (
        src_type in _ORACLE_DIALECTS
        and bool(declared_src)
        and not any(
            str(st.get("op") or "") in _TRANSFORM_PROOF_POPULATION_OPS
            for st in steps
        )
    )
    # X-3: resolve each step's source column to the approved mapping's
    # declared destination type so bare casts can be proven against the
    # contract width rather than staying uncovered.
    src_to_dest_ddl: dict[str, str] = {}
    for _m in mappings or []:
        _msrc = str(_m.get("source") or "")
        if not _msrc:
            continue
        _mtgt = str(_m.get("target") or _msrc)
        _mdl = str(
            dest_types.get(_mtgt)
            or dest_types.get(_mtgt.lower())
            or _m.get("target_type")
            or _m.get("inferredType")
            or ""
        )
        if _mdl:
            src_to_dest_ddl[_msrc] = _mdl
            src_to_dest_ddl[_msrc.lower()] = _mdl
    _covered_step_idx: set[int] = set()
    if oracle_ok:
        for _si, st in enumerate(steps):
            if not oracle_ok:
                break
            op = str(st.get("op") or "")
            opts = st.get("options") or {}
            _step_ok = True
            for scol in _step_cols(st):
                expr = oracle_exprs.get(scol) or quote_sql_identifier(
                    scol, qs
                )
                _step_ddl = str(
                    src_to_dest_ddl.get(scol)
                    or src_to_dest_ddl.get(str(scol).lower())
                    or ""
                )
                expr = _recipe_step_sql(
                    op, opts, expr, oracle_params,
                    src_type=src_type, dest_ddl=_step_ddl,
                    # NaN is null to the engine only on a raw float cell.
                    nan_is_null=(
                        scol not in oracle_exprs
                        and bool(re.match(r"(?i)\s*(float|double|real)", _src_ddl(scol)))
                    ),
                )
                if expr is None:
                    oracle_ok = False
                    _step_ok = False
                    break
                oracle_exprs[scol] = expr
            if _step_ok:
                _covered_step_idx.add(_si)
    # Honest labeling (P3): name every step that got no independent SQL
    # check so a re-apply verdict never reads as an independent proof.
    oracle_uncovered: list[str] = [
        f"step {i + 1} ({st.get('op') or 'unknown'})"
        for i, st in enumerate(steps)
        if i not in _covered_step_idx
    ]
    oracle_bad: set[str] = set()
    if oracle_ok and oracle_exprs:
        # The oracle returns (raw -> oracle) pairs per declared column; a
        # source value that moved between the seam read and the oracle
        # read is excluded (that is drift, owned by the population
        # classifier) — only agreed-upon inputs may judge the op
        # implementation.
        try:
            sel = ", ".join(
                f"{quote_sql_identifier(c, qs)} AS "
                f'{qs}__raw_{c}{qs}, {e} AS {qs}__o_{c}{qs}'
                for c, e in oracle_exprs.items()
            )
            stmt = sa.text(
                f"SELECT {sel} FROM {src_ref_sql}"  # nosec B608
            ).bindparams(**oracle_params)
            sel_cols = [
                x for c in oracle_exprs for x in (f"__raw_{c}", f"__o_{c}")
            ]
            _orc_buf: list[tuple] = []
            with warehouse_sql_engine(src_type, src_cfg) as oeng:
                with oeng.connect() as oconn:
                    for batch in iter_select_row_dicts(
                        oconn, stmt, sel_cols
                    ):
                        for r in batch:
                            for c in oracle_exprs:
                                _orc_buf.append(
                                    (
                                        c,
                                        _vh(
                                            _key_norm(r.get(f"__raw_{c}"))
                                        ),
                                        _vh(
                                            _val_norm(r.get(f"__o_{c}"), _val_ddl(c))
                                        ),
                                    )
                                )
                        if len(_orc_buf) >= 5000:
                            pin_store.put("orc", _orc_buf)
                            _orc_buf.clear()
            pin_store.put("orc", _orc_buf)
            pin_store.flush()
            for scol in pin_store.oracle_mismatch_cols():
                oracle_bad.add(map_source_to_target(scol, mappings))
        except Exception:
            oracle_ok = False
    if oracle_bad:
        bad_steps = sorted(
            {
                # steps[i], not the loop variable left over from building
                # declared_map — that named every culprit after the last step.
                f"step {i + 1} ({steps[i].get('op')} on "
                f"'{map_source_to_target(c, mappings)}')"
                for i, cols in declared_map.items()
                for c in cols
                if map_source_to_target(c, mappings) in oracle_bad
            }
        )
        return {
            "status": "failed",
            "reason": (
                "recipe output disagrees with the independent SQL oracle "
                f"on {sorted(oracle_bad)} — the step implementation, not "
                "the data, is wrong"
            ),
            "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
            "culprit_steps": bad_steps,
            "culprit_columns": sorted(oracle_bad),
            "culprit_keys": (
                pin_store.culprit_keys_by_kind() if keyed else {}
            ),
            "provenance": "independent SQL oracle",
        }

    if scd2_opts:
        actual_checksum = str(
            dest_summary.get("source_scoped_checksum")
            or dest_summary.get("active_checksum")
            or ""
        )
        actual_rows = int(
            dest_summary.get("source_scoped_rows")
            or dest_summary.get("active_rows")
            or 0
        )
        actual_label = "destination current rows"
        comparable = bool(actual_checksum)
        dest_scanned = False
    elif scope_keys is None:
        actual_label = "destination read-back"
    else:
        actual_label = "destination rows for this run's keys"
    if not scd2_opts:
        # G-1i: never trust the recon/writer-acknowledged checksum for the
        # transform proof — the proof must read the destination itself.
        # Filtered to this run's keys only for non-overwrite sync modes.
        comparable = True
        dest_scanned = True
        actual_rows = 0
        try:
            dst_cfg0 = resolve_connector_config(endpoint)
            _mongo_dest = dest_db_type == "mongodb"
            dest_ref0 = "" if _mongo_dest else warehouse_sql_table_ref(
                dest_db_type,
                dst_cfg0,
                str(
                    getattr(endpoint, "schema", "")
                    or dst_cfg0.get("schema")
                    or ""
                ),
                str(endpoint.table or ""),
            )
            dq0 = (
                "`"
                if dest_db_type in {"mysql", "mariadb", "clickhouse"}
                else '"'
            )
            sel_cols0 = list(
                dict.fromkeys(list(digest_cols) + list(scope_pk))
            )
            qc0 = ", ".join(
                quote_sql_identifier(c, dq0) for c in sel_cols0
            )

            def _scoped_gen(batches):
                nonlocal actual_rows
                for batch in batches:
                    for r in batch:
                        if scope_keys is not None and (
                            "\x1f".join(
                                _key_norm(r.get(c)) for c in scope_pk
                            )
                            not in scope_keys
                        ):
                            continue
                        actual_rows += 1
                        row = {c: r.get(c) for c in digest_cols}
                        _fpm_buf.append(("d", _fph(row)))
                        for _sc, tc in col_pairs:
                            _cv_buf.append(
                                (tc, "d", _vh(_val_norm(row.get(tc), _val_ddl(tc))))
                            )
                        if keyed:
                            _destk_buf.append(
                                (
                                    "\x1f".join(
                                        _key_norm(row.get(c))
                                        for c in pin_key_cols
                                    ),
                                    _fph(row),
                                    json.dumps(
                                        {
                                            c: _val_norm(row.get(c), _val_ddl(c))
                                            for c in digest_cols
                                        }
                                    ),
                                )
                            )
                        yield row
                    _flush_ms()

            if _proof_file_dest(endpoint, dest_db_type):
                # Files have no SQL dialect: re-open the final artifact itself.
                actual_checksum = canonical_checksum_from_iter(
                    _scoped_gen(
                        _proof_file_dest_batches(endpoint, dest_summary, dst_cfg0)
                    ),
                    digest_cols,
                    dest_db_type=dest_db_type,
                    dest_types=dest_types,
                )
            elif _mongo_dest:
                # MongoDB has no SQL dialect: re-read the written collection
                # through the Mongo reader (QA r12 D8).
                actual_checksum = canonical_checksum_from_iter(
                    _scoped_gen(
                        _proof_mongo_dest_batches(
                            endpoint=endpoint, dst_cfg=dst_cfg0, read_cols=sel_cols0
                        )
                    ),
                    digest_cols,
                    dest_db_type=dest_db_type,
                    dest_types=dest_types,
                )
            elif dest_db_type == "postgresql":
                # G-1i: a pooled connection can carry an older REPEATABLE
                # READ snapshot that predates the committed write — the
                # tamper test proved a pooled conn counted 5000 of 5001.
                # A dedicated connection always sees committed state.
                from connectors.postgresql_conn import get_connection

                _pc = get_connection(
                    host=dst_cfg0.get("host", ""),
                    port=int(dst_cfg0.get("port") or 5432),
                    database=dst_cfg0.get("database", ""),
                    username=dst_cfg0.get("username", ""),
                    password=dst_cfg0.get("password", ""),
                    connection_string=dst_cfg0.get("connection_string", ""),
                    ssl=bool(dst_cfg0.get("ssl", False)),
                )
                try:
                    _cur = _pc.cursor(name="df_readback")  # server-side stream
                    _cur.execute(  # nosec B608
                        f"SELECT {qc0} FROM {dest_ref0}"
                    )
                    _cnames = list(sel_cols0)

                    def _pg_batches():
                        while True:
                            _rows = _cur.fetchmany(5000)
                            if not _rows:
                                break
                            yield [dict(zip(_cnames, r)) for r in _rows]

                    actual_checksum = canonical_checksum_from_iter(
                        _scoped_gen(_pg_batches()),
                        digest_cols,
                        dest_db_type=dest_db_type,
                        dest_types=dest_types,
                    )
                finally:
                    try:
                        _pc.close()
                    except Exception:
                        pass
            else:
                with warehouse_sql_engine(dest_db_type, dst_cfg0) as e0:
                    with e0.connect() as c0:
                        c0.rollback()
                        if dest_db_type != "sqlite":
                            # SQLite only accepts SERIALIZABLE/READ
                            # UNCOMMITTED/AUTOCOMMIT — setting READ COMMITTED
                            # raised and made every shaped DB→SQLite proof
                            # 'unverifiable' (X-26).
                            c0 = c0.execution_options(
                                isolation_level="READ COMMITTED"
                            )
                        actual_checksum = canonical_checksum_from_iter(
                            _scoped_gen(
                                iter_select_row_dicts(
                                    c0,
                                    sa.text(
                                        f"SELECT {qc0} FROM {dest_ref0}"  # nosec B608
                                    ),
                                    sel_cols0,
                                )
                            ),
                            digest_cols,
                            dest_db_type=dest_db_type,
                            dest_types=dest_types,
                        )
        except Exception as exc:
            return {
                "status": "unverifiable",
                "reason": f"key-scoped destination read failed: {exc!s}"[
                    :300
                ],
                "recipe_hash": str(
                    dest_summary.get("shape_recipe_hash") or ""
                ),
            }

    if not comparable:
        return {
            "status": "unverifiable",
            "reason": (
                "destination read-back unavailable for the transform "
                "proof"
            ),
        }

    _leftover_n = int(dest_summary.get("leftover_deleted") or 0)
    if _leftover_n > 0 and dest_summary.get("overwrite_clear_recorded"):
        # This run recorded a successful destination clear before its
        # first write ("emptied" an existing table or "absent"/create-new)
        # — no pre-run row can reach the heal, so a leftover can only have
        # been written between the write and the reconcile: a mid-gap
        # tamper. When no clear ran for this run (skipped, non-overwrite,
        # resume arming a bare txn) heal-removal of stale rows is the
        # overwrite's normal job and stays "removed by overwrite" on the
        # report rather than a failed proof.
        _removed = [
            str(k)
            for k in (dest_summary.get("leftover_deleted_keys") or [])
        ]
        return {
            "status": "failed",
            "reason": (
                f"destination held {_leftover_n} extra row(s) not in "
                "the recipe output — healed by overwrite merge"
            )[:300],
            "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
            "culprit_keys": (
                {"extra_in_dest": _removed} if _removed else None
            ),
            "key_note": (
                " Culprit key(s) — extra_in_dest: " + ", ".join(_removed) + "."
                if _removed
                else None
            ),
            "provenance": "destination key census (overwrite leftover merge)",
        }

    # --- G-2: independent SCD2 invariants --------------------------------
    # Structural SQL on the written history table: exactly one current row
    # per business key, interval chains without gaps or overlaps, and
    # (when no declared step transforms a tracked column) every current
    # row's tracked values equal its source row. A violation fails the
    # run naming the invariant; a pass upgrades provenance to
    # "independent SCD2 invariants".
    scd2_provenance = ""
    if scd2_opts and dest_db_type in _ORACLE_DIALECTS and isinstance(
        scd2_opts, dict
    ):
        try:
            if dest_db_type in {"mysql", "mariadb", "clickhouse"}:
                _q = "`"
            elif dest_db_type in {"sqlserver", "mssql", "azure_sql"}:
                _q = "["
            else:
                _q = '"'
            _tsql_dest = dest_db_type in {"sqlserver", "mssql", "azure_sql"}
            _limit1 = "LIMIT 1" if not _tsql_dest else ""
            _top1 = "TOP 1 " if _tsql_dest else ""
            _keys = [str(k) for k in scd2_opts.get("keys") or []]
            _cur = str(scd2_opts.get("is_current") or "is_current")
            _vf = str(scd2_opts.get("valid_from") or "valid_from")
            _vt = str(scd2_opts.get("valid_to") or "valid_to")
            _ver = str(scd2_opts.get("version") or "")
            _tracked = [str(t) for t in scd2_opts.get("tracked") or []]
            if _keys:
                _dst_cfg_i = resolve_connector_config(endpoint)
                _dest_ref_i = warehouse_sql_table_ref(
                    dest_db_type,
                    _dst_cfg_i,
                    str(
                        getattr(endpoint, "schema", "")
                        or _dst_cfg_i.get("schema")
                        or ""
                    ),
                    str(endpoint.table or ""),
                )
                from services.scd2_engine import (
                    scd2_is_current_predicate,
                )

                _cur_pred = scd2_is_current_predicate(
                    dest_db_type, quote_sql_identifier(_cur, _q)
                )
                _kcols = ", ".join(
                    quote_sql_identifier(k, _q) for k in _keys
                )
                _violations: list[str] = []
                with warehouse_sql_engine(
                    dest_db_type, _dst_cfg_i
                ) as _ieng:
                    with _ieng.connect() as _iconn:
                        _n = _iconn.execute(
                            sa.text(
                                f"SELECT {_top1}1 FROM {_dest_ref_i} "  # nosec B608
                                f"WHERE {_cur_pred} GROUP BY {_kcols} "
                                f"HAVING COUNT(*) <> 1 {_limit1}".rstrip()
                            )
                        ).first()
                        if _n:
                            _violations.append(
                                "not exactly one current row per "
                                "business key"
                            )
                        _qver = quote_sql_identifier(_ver, _q) if _ver else None
                        _qvf = quote_sql_identifier(_vf, _q)
                        _qvt = quote_sql_identifier(_vt, _q)
                        _order = f"{_qver}, {_qvf}" if _qver else _qvf
                        if _tsql_dest:
                            _gap_pred = (
                                "o.nxt IS NOT NULL AND ("
                                "o.vt IS NULL AND o.nxt IS NOT NULL "
                                "OR o.vt IS NOT NULL AND o.nxt IS NULL "
                                "OR o.vt <> o.nxt)"
                            )
                        else:
                            _gap_pred = (
                                "o.nxt IS NOT NULL "
                                "AND o.vt IS DISTINCT FROM o.nxt"
                            )
                        _adj = _iconn.execute(
                            sa.text(
                                f"SELECT {_top1}1 FROM (SELECT "
                                f"{_kcols}, {_qvt} AS vt, {_qvf} AS vf, "
                                f"LEAD({_qvf}) OVER (PARTITION BY {_kcols} "
                                f"ORDER BY {_order}) AS nxt "
                                f"FROM {_dest_ref_i}) o "  # nosec B608
                                f"WHERE {_gap_pred} {_limit1}".rstrip()
                            )
                        ).first()
                        if _adj:
                            _violations.append(
                                "gapped or overlapping validity "
                                "intervals for a key"
                            )
                        _flag = _iconn.execute(
                            sa.text(
                                f"SELECT {_top1}1 FROM {_dest_ref_i} "  # nosec B608
                                f"WHERE {_cur_pred} AND {_qvt} IS NOT NULL "
                                f"{_limit1}".rstrip()
                            )
                        ).first()
                        if _flag:
                            _violations.append(
                                "a current row carries an end date"
                            )
                        # Tracked-equality: only when no declared step
                        # transforms a tracked column, and only against a
                        # SQL source — comparing transformed output to raw
                        # source would be a false failure by construction.
                        _src_ref_i = src_ref_sql if src_ref_sql else ""
                        _tset = {str(t) for t in _tracked}
                        _transformed_t = {
                            map_source_to_target(c, mappings)
                            for c in declared_src
                        }
                        if (
                            _tracked
                            and _src_ref_i
                            and not (_tset & _transformed_t)
                            and src_type in _ORACLE_DIALECTS
                        ):
                            _inv_map = {
                                map_source_to_target(c, mappings): c
                                for c in read_cols
                            }
                            _join = " AND ".join(
                                f"s.{quote_sql_identifier(str(_inv_map.get(k) or k), qs)}"
                                f" = d.{quote_sql_identifier(k, _q)}"
                                for k in _keys
                            )
                            def _distinct_frag(t: str) -> str:
                                dq = f"d.{quote_sql_identifier(t, _q)}"
                                sq = (
                                    f"s.{quote_sql_identifier(str(_inv_map.get(t) or t), qs)}"
                                )
                                if _tsql_dest:
                                    # IS DISTINCT FROM is SQL Server 2022+;
                                    # emit the portable null-safe form.
                                    dt = f"CAST({dq} AS nvarchar(max))"
                                    st = f"CAST({sq} AS nvarchar(max))"
                                    return (
                                        f"({dt} <> {st} "
                                        f"OR ({dq} IS NULL AND {sq} IS NOT NULL) "
                                        f"OR ({dq} IS NOT NULL AND {sq} IS NULL))"
                                    )
                                return (
                                    f"{dq}::text IS DISTINCT FROM {sq}::text"
                                )

                            _diffs = " OR ".join(
                                _distinct_frag(t) for t in _tracked
                            )
                            _eq = _iconn.execute(
                                sa.text(
                                    f"SELECT {_top1}1 FROM {_dest_ref_i} d "  # nosec B608
                                    f"JOIN {_src_ref_i} s ON {_join} "
                                    f"WHERE {_cur_pred.replace(quote_sql_identifier(_cur, _q), 'd.' + quote_sql_identifier(_cur, _q))} "
                                    f"AND ({_diffs}) {_limit1}".rstrip()
                                )
                            ).first()
                            if _eq:
                                _violations.append(
                                    "a current row's tracked columns "
                                    "differ from its source row"
                                )
                if _violations:
                    return {
                        "status": "failed",
                        "reason": (
                            "SCD2 invariant violated: "
                            + "; ".join(_violations)
                        )[:300],
                        "recipe_hash": str(
                            dest_summary.get("shape_recipe_hash") or ""
                        ),
                        "culprit_steps": ["scd2 invariant"],
                        "provenance": "independent SCD2 invariants",
                    }
                scd2_provenance = "independent SCD2 invariants"
        except Exception:
            scd2_provenance = ""

    if expected_checksum == actual_checksum and actual_rows == exp_n:
        return {
            "status": "verified",
            "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
            "rows": exp_n,
            "scoped_to_run_keys": scope_keys is not None,
            "provenance": (
                "independent SQL oracle"
                + (" + SCD2 invariants" if scd2_provenance else "")
                if oracle_ok and oracle_exprs
                else (
                    scd2_provenance
                    or "recipe re-applied"
                )
            ),
            "oracle_uncovered_steps": oracle_uncovered,
            "oracle_coverage": {
                "covered": len(steps) - len(oracle_uncovered),
                "total": len(steps),
                "scd2_invariants": bool(scd2_provenance),
            },
        }

    # G-1a: the re-read runs AFTER the write — a busy source may have
    # drifted in between. Row-local steps never change the written row
    # count, so a count divergence is always drift (or a population issue
    # the ledger itself reports); anything ambiguous is unverifiable —
    # never a false failure that rolls back correct data.
    if actual_rows != exp_n and not keyed:
        # G-1b: for row-removing recipes a dropped step inflates the
        # written population toward the raw count — keep going so the
        # declared-column check can prove it. Anything else is drift.
        # Keyed runs fall through: the pinned-key classifier below judges
        # a missing/differing destination row on a stable key as a
        # provable defect (naming the key), not source drift.
        if not (pop_ops and actual_rows == raw_count):
            reason = (
                "source changed during run — re-read returned "
                f"{exp_n} recipe-applied rows but "
                f"{actual_label} holds {actual_rows}"
            )
            if pin_store is not None:
                pin_store.flush()
                _drift_n, _n_keys, _donly = pin_store.drift_stats()
                reason = (
                    "source changed during run — "
                    f"{_drift_n + _donly} of {_n_keys + _donly} keys "
                    f"changed in source during run; "
                    f"{_n_keys - _drift_n} proven"
                )
            return {
                "status": "unverifiable",
                "reason": reason,
                "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
                "expected_rows": exp_n,
                "actual_rows": actual_rows,
            }

    # Multiset classification over row fingerprints: a destination row
    # provably equal to the raw re-read is untransformed by construction
    # (a post-write source change cannot write raw values to the
    # destination) — it names the step and fails. Every other divergence
    # is ambiguous — touched-column drift and wrong values are
    # observationally identical here — and lands as unverifiable, never
    # blame. (The wrong-value case is owned by the G-1d oracle above.)
    culprits: list[str] = []
    drift = False
    drift_counts: tuple | None = None
    try:
        if not dest_scanned:
            dst_cfg = resolve_connector_config(endpoint)
            dest_ref = warehouse_sql_table_ref(
                dest_db_type,
                dst_cfg,
                str(
                    getattr(endpoint, "schema", "")
                    or dst_cfg.get("schema")
                    or ""
                ),
                str(endpoint.table or ""),
            )
            dq = (
                "`"
                if dest_db_type in {"mysql", "mariadb", "clickhouse"}
                else '"'
            )
            sel_cols = list(
                dict.fromkeys(list(digest_cols) + list(scope_pk))
            )
            qc = ", ".join(quote_sql_identifier(c, dq) for c in sel_cols)
            stmt = sa.text(f"SELECT {qc} FROM {dest_ref}")  # nosec B608
            if scd2_opts:
                from services.scd2_engine import scd2_is_current_predicate

                cur_pred = scd2_is_current_predicate(
                    dest_db_type,
                    quote_sql_identifier(str(scd2_opts["is_current"]), dq),
                )
                stmt = sa.text(  # nosec B608
                    f"SELECT {qc} FROM {dest_ref} WHERE {cur_pred}"
                )
            dest_scanned = True
            with warehouse_sql_engine(dest_db_type, dst_cfg) as deng:
                with deng.connect() as dconn:
                    for batch in iter_select_row_dicts(
                        dconn, stmt, sel_cols
                    ):
                        for r in batch:
                            if scope_keys is not None and (
                                "\x1f".join(
                                    _key_norm(r.get(c)) for c in scope_pk
                                )
                                not in scope_keys
                            ):
                                continue
                            row = {c: r.get(c) for c in digest_cols}
                            _fpm_buf.append(("d", _fph(row)))
                            for _sc, tc in col_pairs:
                                _cv_buf.append(
                                    (
                                        tc,
                                        "d",
                                        _vh(_val_norm(row.get(tc), _val_ddl(tc))),
                                    )
                                )
                            if keyed:
                                _destk_buf.append(
                                    (
                                        "\x1f".join(
                                            _key_norm(row.get(c))
                                            for c in pin_key_cols
                                        ),
                                        _fph(row),
                                        json.dumps(
                                            {
                                                c: _val_norm(row.get(c), _val_ddl(c))
                                                for c in digest_cols
                                            }
                                        ),
                                    )
                                )
                        _flush_ms()

        if keyed:
            # Pinned-key judging (disk-backed — G-1g): a key whose
            # re-read raw hash equals the write-time hash is stable, so
            # dest != expected on it is a provable defect (wrong value,
            # partial application, dropped step) — it fails and names the
            # step. Moved/new/missing keys are drift -> unverifiable,
            # with counts in the message. Only a bounded sample of
            # culprit keys is materialized for column naming.
            pin_store.flush()
            drift_n, n_keys, dest_only = pin_store.drift_stats()
            # A destination key that was never in the written population
            # is a foreign row (inserted/duplicated), not source drift —
            # a source row deleted mid-run stays in pin and counts as
            # drift, but a key absent from pin entirely is a defect.
            foreign_n = int(
                pin_store.db.execute(
                    "SELECT count(*) FROM destk d "
                    "LEFT JOIN pin p ON p.k = d.k WHERE p.k IS NULL"
                ).fetchone()[0]
            )
            drift_n += dest_only - foreign_n
            culpr_n, culprit_keys_p = pin_store.culprit_keys()
            culpr_n += foreign_n
            culprit_cols_p: set[str] = set()
            if culpr_n:
                _samples = pin_store.samples(culprit_keys_p)
                for _k in culprit_keys_p:
                    _evals, _dvals = _samples.get(_k, (None, None))
                    if _evals is None:
                        continue
                    if _dvals is None:
                        # Pinned source key produced no destination row —
                        # provable loss on a stable population.
                        culprit_cols_p.update(declared_tcols)
                        continue
                    # Compare every digest column — a tampered or wrongly
                    # written column that no recipe step touches must name
                    # itself, not blame the recipe's declared columns.
                    for _tc in digest_cols:
                        if _val_norm(_dvals.get(_tc), _val_ddl(_tc)) != _evals.get(_tc):
                            culprit_cols_p.add(_tc)
            culprits = (
                sorted(culprit_cols_p or declared_tcols)
                if culpr_n
                else []
            )
            drift = (drift_n > 0) and not culprits
            drift_counts = (drift_n, n_keys)
        else:
            # Multiset classification on the sqlite store (G-1h): a dest
            # fingerprint exceeding its expected count is a culprit row
            # iff it matches a raw re-read fingerprint — raw values on
            # the destination can only come from an un-transformed
            # write. Anything else is ambiguous (drift or wrong values).
            pin_store.flush()
            culprit_n = 0
            ambiguous_extra = 0
            for _h, xtra, n_raw in pin_store.fp_extra():
                culprit_n += min(xtra, n_raw)
                ambiguous_extra += xtra
            ambiguous = (
                ambiguous_extra - culprit_n + pin_store.fp_missing_total()
            )
            if culprit_n:
                culprit_cols: set[str] = set()
                for scol, tcol in col_pairs:
                    if tcol not in exp_tcols:
                        continue
                    hit = 0
                    for _v, xtra, n_raw in pin_store.col_extra(tcol, scol):
                        hit += min(xtra, n_raw)
                    if hit:
                        culprit_cols.add(tcol)
                culprits = sorted(culprit_cols or declared_tcols)
            else:
                culprits = []
            drift = bool(ambiguous) and not culprit_n
            drift_counts = None
    except Exception:
        # Localization is best-effort; never upgrade an ambiguous
        # divergence into a blamed failure.
        drift = True
        culprits = []

    if drift:
        drift_reason = (
            "source changed during run — the re-read population no "
            "longer matches what was written"
        )
        if drift_counts and drift_counts[1]:
            drift_reason = (
                f"source changed during run — {drift_counts[0]} of "
                f"{drift_counts[1]} keys changed in source during run; "
                f"{drift_counts[1] - drift_counts[0]} proven"
            )
        return {
            "status": "unverifiable",
            "reason": drift_reason,
            "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
            "expected_rows": exp_n,
            "actual_rows": actual_rows,
            "culprit_keys": (
                pin_store.culprit_keys_by_kind() if keyed else {}
            ),
        }
    if not culprits:
        return {
            "status": "unverifiable",
            "reason": (
                "expected/actual digest mismatch could not be attributed "
                "to a declared step or to source drift"
            ),
            "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
            "expected_rows": exp_n,
            "actual_rows": actual_rows,
            "culprit_keys": (
                pin_store.culprit_keys_by_kind() if keyed else {}
            ),
        }

    culprit_steps = []
    for i, s in enumerate(steps):
        cols = declared_map.get(i) or []
        if any(map_source_to_target(c, mappings) in culprits for c in cols):
            culprit_steps.append(f"step {i + 1} ({s.get('op')} on '{cols[0]}')")
    unexplained_cols = sorted(set(culprits) - set(declared_tcols))
    non_recipe_drift_cols: list[str] = []
    if not culprit_steps and unexplained_cols:
        # Drift sits on columns no recipe step produces — name them and say
        # so; blaming every step would misdirect the operator.
        non_recipe_drift_cols = unexplained_cols
        culprit_steps = [
            "drift on "
            + ", ".join(unexplained_cols[:8])
            + ("…" if len(unexplained_cols) > 8 else "")
            + " — not explained by a recipe step"
        ]
    elif not culprit_steps:
        culprit_steps = [
            f"step {i + 1} ({s.get('op')})"
            for i, s in enumerate(steps)
            if str(s.get("op") or "") != "scd2"
        ] or [f"recipe {dest_summary.get('shape_recipe_hash')}"]

    key_culprits = pin_store.culprit_keys_by_kind() if keyed else {}
    key_note = ""
    if key_culprits:
        _kc_parts = []
        for _lbl in ("changed", "missing_in_dest", "extra_in_dest"):
            if key_culprits.get(_lbl):
                _kc_parts.append(
                    f"{_lbl}: {', '.join(key_culprits[_lbl][:8])}"
                )
        key_note = " Culprit key(s) — " + "; ".join(_kc_parts) + "."

    return {
        "status": "failed",
        "recipe_hash": str(dest_summary.get("shape_recipe_hash") or ""),
        "expected_checksum": expected_checksum,
        "actual_checksum": actual_checksum,
        "expected_rows": exp_n,
        "actual_rows": actual_rows,
        "culprit_columns": culprits,
        "culprit_steps": culprit_steps,
        "non_recipe_drift_columns": non_recipe_drift_cols,
        "culprit_keys": key_culprits,
        "key_note": key_note,
        "actual_label": actual_label,
    }


def _transform_applied_proof(**kwargs: Any) -> Any:
    """Run the transform proof with its scratch stores bound to this call."""
    with proof_scratch_scope():
        return _transform_applied_proof_impl(**kwargs)


def _compute_source_checksum_from_spool(
    source_spool: Any,
    columns: list[str],
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    target_cols: list[str] | None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    validation_mode: str = "strict",
    destination_pk_columns: list[str] | None = None,
) -> tuple[str, str]:
    """Remap Gate-8 from the engine spool in bundles — never rebuild ``records``."""
    from connectors.engine_record_spill import iter_fingerprints_from_spool
    from services.fingerprint_accumulator import FingerprintAccumulator

    if target_cols is None:
        target_cols, _ = resolve_target_columns(
            mappings, source_schema or {}, preserve_case=True
        )
    acc = FingerprintAccumulator()
    policy = transform_error_policy_for_validation_mode(validation_mode)
    acc.add_many(
        iter_fingerprints_from_spool(
            source_spool,
            mappings,
            target_cols,
            headers=list(getattr(source_spool, "headers", None) or columns),
            column_types=source_schema or {},
            dest_db_type=dest_db_type or "",
            dest_types=dest_types or {},
            error_policy=policy,
            destination_pk_columns=destination_pk_columns,
        )
    )
    return acc.digest(), SOURCE_DIGEST_REMAPPED_ROWS


def _compute_source_checksum(
    records: list[dict],
    columns: list[str],
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    writer_checksum: str,
    target_cols: list[str] | None = None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    validation_mode: str = "strict",
    destination_pk_columns: list[str] | None = None,
    source_spool: Any = None,
) -> tuple[str, str]:
    """Return ``(digest, provenance)`` for the source side of Gate-8.

    When source ``records`` or an engine ``source_spool`` are available, always
    remap and fingerprint them. Preferring the writer checksum first made
    Gate-8 circular: dest digest was compared to the writer's own ack, not to
    the remapped source population.

    The writer checksum remains the fallback when no records or spool were
    supplied, and the provenance is returned with it so the report can say so.
    That fallback is not a corner case: a streaming pass hands over no rows,
    which is exactly how the large tables move, and labelling those runs
    ``full_checksum`` claimed two independent digests had agreed when only one
    digest existed.
    """
    if source_spool is not None and getattr(source_spool, "row_count", 0):
        return _compute_source_checksum_from_spool(
            source_spool,
            columns,
            mappings,
            source_schema,
            target_cols,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
            validation_mode=validation_mode,
            destination_pk_columns=destination_pk_columns,
        )
    if not records:
        return str(writer_checksum or ""), SOURCE_DIGEST_WRITER_ACK
    _, data_rows = records_to_matrix(records, columns)
    if target_cols is None:
        target_cols, _ = resolve_target_columns(
            mappings, source_schema or {}, preserve_case=True
        )
    mapped_rows, _rejected = map_rows_for_fingerprint(
        headers=columns,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=source_schema or {},
        error_policy=transform_error_policy_for_validation_mode(validation_mode),
        dest_types=dest_types or {},
        preserve_case=True,
        dest_kind=dest_db_type or "",
        destination_pk_columns=destination_pk_columns,
    )
    return (
        checksum_rows(
            mapped_rows,
            target_cols,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        ),
        SOURCE_DIGEST_REMAPPED_ROWS,
    )


def _mapped_sources(mappings: list[dict], target_cols: list[str]) -> list[str]:
    """Source column feeding each of ``target_cols`` (the target name when unmapped)."""
    from services.mapping_constraints import write_mappings

    by_target = {
        str(m.get("target") or m.get("source") or ""): str(m.get("source") or "")
        for m in write_mappings(mappings)
    }
    return list(dict.fromkeys(by_target.get(t) or t for t in target_cols))


def _source_rows_as_written(
    source_rows: list[dict],
    *,
    mappings: list[dict],
    target_cols: list[str],
    dest_types: dict[str, str],
    dest_type: str,
    validation_mode: str,
) -> list[dict]:
    """The re-read source pre-image, mapped exactly as the writer mapped it.

    D123/D132 — one owner for transform-aware read-back: the same mapping,
    transforms and quarantine policy the write pass ran. A row the writer held
    out to quarantine is held out here too, so it can never count as a
    missing or mismatched destination row.
    """
    headers = list(dict.fromkeys(str(k) for r in source_rows for k in r))
    mapped, _held_out = map_rows_for_fingerprint(
        headers=headers,
        data_rows=[[r.get(h) for h in headers] for r in source_rows],
        mappings=mappings,
        target_cols=target_cols,
        error_policy=transform_error_policy_for_validation_mode(validation_mode),
        dest_types=dest_types,
        dest_kind=dest_type,
    )
    return [dict(zip(target_cols, row)) for row in mapped]


def _mapped_targets(mappings: list[dict], columns: list[str]) -> list[str]:
    """Return the ordered list of target column names used for reconciliation.

    Declared omissions are excluded: they have no destination carrier, and
    falling back to their source name asked the destination for a column that
    was never created, which failed the read-back and reported Gate-8 as
    unavailable on a write that had actually landed.
    """
    from services.mapping_constraints import write_mappings

    targets = list(dict.fromkeys(
        str(m.get("target") or m.get("source") or "")
        for m in write_mappings(mappings)
        if m.get("target") or m.get("source")
    ))
    return targets or columns


def _sort_key_for_columns(targets: list[str], mappings: list[dict] | None = None) -> str | None:
    """Pick a stable key for sample alignment.

    Prefer real identity columns (id / *_id / code / uuid) over the first mapped
    column — airports-like tables have no id, and aligning on ``city`` is weak
    when the destination already holds prior append loads.
    """
    if not targets:
        return None
    lower_map = {c.lower(): c for c in targets}
    # Operator / contract primary key from mapping metadata.
    for m in mappings or []:
        for key in ("primary_key", "is_primary_key", "identity"):
            if m.get(key) in (True, "true", "1", 1):
                tgt = str(m.get("target") or "").strip()
                if tgt and tgt.lower() in lower_map:
                    return lower_map[tgt.lower()]
    preferred = (
        "id",
        "uuid",
        "guid",
        "code",
        "pk",
        "airport_code",
        "iata",
        "icao",
    )
    for name in preferred:
        if name in lower_map:
            return lower_map[name]
    for c in targets:
        cl = c.lower()
        if cl.endswith("_id") or cl.endswith("_uuid") or cl.endswith("_code"):
            return c
    return targets[0]


def _source_key_values(
    records: list[dict],
    *,
    sort_key: str | None,
    mappings: list[dict],
    limit: int = 50,
) -> list[Any]:
    """Extract distinct source-side key values for keyed destination sample reads."""
    if not sort_key or not records:
        return []
    source_sort_key = sort_key
    sk = sort_key.lower()
    for m in mappings:
        if str(m.get("target") or "").lower() == sk and m.get("source"):
            source_sort_key = str(m["source"])
            break
    from services.value_serializer import SQL_NULL_SENTINEL

    seen: set[str] = set()
    values: list[Any] = []
    # Records keyed by engine-returned headers can carry the folded case
    # (Snowflake/Oracle fold unquoted names to UPPER), so an exact lookup on
    # the contract-declared key misses every row and the keyed scope stays
    # empty — same fold handled by keyset_pagination.header_column_index.
    from services.keyset_pagination import header_column_index

    key_names = [source_sort_key]
    if source_sort_key != sort_key:
        key_names.append(sort_key)

    def _key_raw(rec: dict) -> Any:
        for name in key_names:
            raw = rec.get(name)
            if raw is not None:
                return raw
            idx = header_column_index(list(rec.keys()), name)
            if idx is not None:
                return rec[list(rec.keys())[idx]]
        return None

    for rec in records:
        if not isinstance(rec, dict):
            continue
        raw = _key_raw(rec)
        if raw is None or raw == "":
            continue
        # D86 — the NULL sentinel is metadata, never a bind value. Keep real
        # NULL keys out of the IN (...) list; read_target_sample's IS NULL
        # branch matches destination NULL keys instead.
        if str(raw).lower() == SQL_NULL_SENTINEL.lower():
            if None not in values:
                values.append(None)
            continue
        marker = str(raw)
        if marker in seen:
            continue
        seen.add(marker)
        values.append(raw)
        if len(values) >= limit:
            break
    return values


def _measured_count(measured: Any, fallback: int) -> int:
    """The measured count when one exists (including 0), else ``fallback``."""
    if isinstance(measured, bool):
        return int(fallback)
    if isinstance(measured, int) and measured >= 0:
        return measured
    return int(fallback)


def _as_count(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _ladder_declined(report: dict[str, Any], rows: int, budget: int) -> dict[str, Any]:
    """Record why L2/L4/L5 localization did not run, without weakening Gate-8.

    L1 and the L3 full-population checksum are streaming and still apply; only
    the in-memory localization layers are declined.
    """
    out = dict(report or {})
    out["verification_ladder"] = {
        "layers": {},
        "passed": bool(out.get("passed")),
        "assurance_level": str(out.get("assurance_level") or ""),
        "population_proof": False,
        "population_checksum_proof": bool(out.get("checksum_match")),
        "skipped": True,
        "reason": (
            f"Population of {rows} rows exceeds VERIFICATION_LADDER_MAX_ROWS={budget}; "
            "in-memory L2/L4/L5 localization declined before loading. "
            "Gate-8 L1 row balance and L3 full-population checksum still apply."
        ),
        "localization": {},
        "localization_summary": "",
    }
    return out


def _ladder_declined_for_shape(
    report: dict[str, Any], recipe_hash: str
) -> dict[str, Any]:
    """Decline source/destination cell equality for a shaped run, and say so.

    The rows left in the source are the pre-shape values; the destination holds
    the post-shape ones. Comparing them would report the operator's own declared
    change as corruption, so the ladder names the recipe instead of asserting an
    equality shaping made false. Gate-8 row balance and the destination re-read
    are unaffected.
    """
    out = dict(report or {})
    out["verification_ladder"] = {
        "layers": {},
        "passed": bool(out.get("passed")),
        "assurance_level": str(out.get("assurance_level") or ""),
        "population_proof": False,
        "population_checksum_proof": bool(out.get("checksum_match")),
        "skipped": True,
        "shape_recipe_hash": recipe_hash,
        "reason": (
            f"Transform recipe {recipe_hash} rewrote source-side values on the read, "
            "so the source table no longer holds what this run wrote; "
            "source→destination cell equality is declined rather than reported "
            "false. Gate-8 row balance and the destination re-read still apply."
        ),
        "localization": {},
        "localization_summary": "",
    }
    return out


def _maybe_engine_profile_ladder(
    report: dict[str, Any],
    *,
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    records: list[dict],
    columns: list[str],
    dest_summary: dict[str, Any],
    mappings: list[dict],
    dest_type: str,
) -> dict[str, Any] | None:
    """Engine-side L2 for oversized same-engine SQL routes, at any scale.

    The in-memory ladder refuses above ``VERIFICATION_LADDER_MAX_ROWS`` and only
    L1/L3 survive — so on exactly the large tables where a migration's stakes are
    highest, the column-level checks that catch a silently nulled or truncated
    field stop running. When both ends are the same SQL engine family, the engine
    can still compute per-column NULL/min/max/sum in SQL over any number of rows,
    so that signal is restored rather than declined. Returns ``None`` for small
    populations (the richer in-memory ladder runs) and for any route it cannot
    profile, leaving the existing decline in charge.
    """
    if source_endpoint is None or getattr(source_endpoint, "kind", "") != "database":
        return None
    if dest_summary.get("shape_recipe_hash"):
        # Column aggregates over the unshaped source describe values this run
        # deliberately changed, so a rounded or filtered column would be reported
        # as drift. The shaped decline in the caller states the reason instead.
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint):
        # CALL/SELECT is not a physical table — profiling get_orders would
        # read a colliding relation and publish false Gate-8 proof.
        return None
    from services.sync_cursor import is_overwrite_sync

    # Whole-table aggregates are not the batch that an append/upsert wrote.
    # Profiling them would mix historical rows into Gate-8 L1/L2.
    sync = str(dest_summary.get("sync_mode") or "")
    if sync and not is_overwrite_sync(sync):
        return None
    from services.column_profile import engine_profile_ladder, profile_supported
    from services.verification_ladder import (
        MAX_LADDER_ROWS,
        attach_ladder_to_reconcile_report,
    )

    known = max(
        _as_count(report.get("source_rows")),
        _as_count(report.get("target_rows")),
        len(records or []),
    )
    if known <= MAX_LADDER_ROWS:
        return None

    from .connector_capabilities import resolve_driver_type

    source_type = resolve_driver_type(source_endpoint.format or "")
    # Same- or cross-engine, as long as both ends are SQL engines the profiler
    # knows how to render. Cross-engine narrows to value-based statistics itself.
    if not (profile_supported(source_type) and profile_supported(dest_type)):
        return None

    from services.mapping_constraints import is_intentional_omit

    pairs: list[tuple[str, str]] = []
    types: dict[str, str] = {}
    for m in mappings or []:
        if is_intentional_omit(m):
            continue
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or src).strip()
        if not src or not tgt:
            continue
        pairs.append((src, tgt))
        tt = str(m.get("target_type") or m.get("inferredType") or "").strip()
        if tt:
            types[tgt] = tt
    if not pairs:
        pairs = [(c, c) for c in (columns or []) if c]
    if not pairs:
        return None
    physical = dest_summary.get("column_types") or dest_summary.get("target_types")
    types = overlay_physical_dest_types(types, physical)

    src_cfg = resolve_connector_config(source_endpoint)
    dst_cfg = resolve_connector_config(endpoint)
    try:
        ladder = engine_profile_ladder(
            source_engine=source_type,
            source_cfg=src_cfg,
            source_schema=str(source_endpoint.schema or src_cfg.get("schema") or ""),
            source_table=str(
                source_endpoint.table
                or source_endpoint.collection
                or src_cfg.get("table")
                or ""
            ),
            dest_engine=dest_type,
            dest_cfg=dst_cfg,
            dest_schema=str(dst_cfg.get("schema") or dest_summary.get("schema") or ""),
            dest_table=str(
                endpoint.table or dest_summary.get("table") or dst_cfg.get("table") or ""
            ),
            pairs=pairs,
            types=types,
            source_rows=_as_count(report.get("source_rows")),
            target_rows=_as_count(report.get("target_rows")),
            rejected_rows=_as_count(report.get("rejected_rows")),
            coerced_null_rows=_as_count(report.get("coerced_null_rows")),
            rows_skipped=_as_count(report.get("rows_skipped")),
        )
    except Exception as exc:  # noqa: BLE001 — any failure declines to the existing skip
        logging.getLogger(__name__).info("engine profile ladder skipped: %s", exc)
        return None
    if ladder is None:
        return None
    return attach_ladder_to_reconcile_report(report, ladder)






def _read_verify_rows(
    db_type: str,
    cfg: dict[str, Any],
    *,
    table: str,
    columns: list[str] | None,
    postgres_default: bool = False,
) -> list[dict[str, Any]]:
    """Single owner for the per-engine whole-population read the Gate-8
    ladder, its source re-read, and the keyed no-op re-verify share.
    """
    key = str(db_type or "").strip().lower()
    cols = columns if columns is not None else []
    # Resolve the readers through the module so tests patching
    # services.verification_ladder.read_*_rows keep intercepting (the
    # function-local imports this replaces did the same at call time).
    from services import verification_ladder as _vl

    if key == "sqlite":
        return _vl.read_sqlite_rows(
            database=str(cfg.get("database") or ""),
            table=table,
            columns=cols,
            connection_string=str(cfg.get("connection_string") or ""),
            host=str(cfg.get("host") or ""),
        )
    if key == "mysql":
        return _vl.read_mysql_rows(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 3306),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            table=table,
            columns=cols,
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
    if key == "snowflake":
        return _vl.read_snowflake_rows(
            host=str(cfg.get("host") or ""),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            schema=str(cfg.get("schema") or "PUBLIC"),
            table=table,
            columns=cols,
            connection_string=str(cfg.get("connection_string") or ""),
            warehouse=str(cfg.get("warehouse") or ""),
        )
    if key in {"sqlserver", "mssql", "azure_sql"}:
        return _vl.read_sqlserver_rows(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 1433),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            schema=str(cfg.get("schema") or "dbo"),
            table=table,
            columns=cols,
        )
    if key in {"postgresql", "redshift"} or postgres_default:
        return _vl.read_postgres_rows(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 5432),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            schema=str(cfg.get("schema") or "public"),
            table=table,
            columns=cols,
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
    return []


def _covered_keyed_noop_verify(
    *,
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None,
    sync_mode: str,
    rows_before: int | None,
    target_rows: int,
    pk_column: str,
    db_type: str,
) -> dict[str, Any] | None:
    """Prove a quiet keyed poll: the destination's covered content, not just
    its COUNT, still equals the source's.

    ``target_rows == rows_before`` only proves the destination did not gain or
    lose rows — an in-place UPDATE on a previously written row preserves COUNT,
    so a tampered prior-batch row passed Gate-8 green (M3 regression: the
    bounded window made the source re-read empty and the count-only no-op
    proof accepted it). For a keyed route on a populated destination we
    re-read both sides and compare cell-by-cell on the shared key space:
    a destination key absent from the source is a ghost row, and a cell diff
    on a shared key is a tamper. Source-only keys are tolerated — dedupe
    collapse and rolled-back batches leave covered-but-unwritten keys.
    """
    if not pk_column or target_rows <= 0 or source_endpoint is None:
        return None
    try:
        from services.row_conservation import KIND_KEYED, conservation_kind

        if (
            conservation_kind(sync_mode, dest_count_before=rows_before)
            != KIND_KEYED
        ):
            return None
    except Exception:
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint):
        return None
    from .connector_capabilities import resolve_driver_type

    src_type = resolve_driver_type(source_endpoint.format)
    src_cfg = resolve_connector_config(source_endpoint)
    dest_cfg = resolve_connector_config(endpoint)
    map_list = mappings or []
    src_pk = next(
        (
            str(m.get("source") or "")
            for m in map_list
            if str(m.get("target") or "") == pk_column
        ),
        pk_column,
    )
    columns = [str(m.get("target") or "") for m in map_list if m.get("target")]
    if pk_column not in columns:
        columns.append(pk_column)
    try:
        dest_rows = _read_verify_rows(
            db_type,
            dest_cfg,
            table=str(endpoint.table or dest_cfg.get("table") or ""),
            columns=columns,
            postgres_default=True,
        )
        src_rows = _read_verify_rows(
            src_type,
            src_cfg,
            table=str(source_endpoint.table or src_cfg.get("table") or ""),
            columns=[
                str(m.get("source") or "") for m in map_list if m.get("source")
            ] or None,
        )
    except PopulationTooLarge as exc:
        return {
            "passed": False,
            "unproven": True,
            "checksum_match": False,
            "assurance_level": "none",
            "message": (
                "Keyed no-op poll could not re-verify covered content: "
                f"population read exceeded budget ({exc.rows_read:,} rows). "
                "Not migration_proven."
            ),
        }
    except Exception as exc:
        # A covered destination we cannot re-read has no tamper proof — the
        # count-only green must not stamp verified (X-22 doctrine).
        return {
            "passed": False,
            "unproven": True,
            "checksum_match": False,
            "assurance_level": "none",
            "message": (
                "Keyed no-op poll could not re-verify covered content "
                f"({exc}). Count unchanged is not a value proof. "
                "Not migration_proven."
            ),
        }
    if src_rows is None or dest_rows is None:
        return None
    # D188 — a quiet poll's window is empty: every row it can see sits outside
    # what the cursor selected. A disagreement there (a source edit that did
    # not move the cursor, a hard delete, a hand-changed destination row) is
    # out-of-window drift: measured cell-by-cell on canonical fingerprints and
    # reported on the run, not this run's failure (fail closed again under
    # DATAFLOW_INCREMENTAL_DRIFT_FAILS=1).
    from services.incremental_drift import attach_drift, measure_drift

    pairs = [
        (str(m.get("source") or ""), str(m.get("target") or ""))
        for m in map_list
        if m.get("source") and m.get("target")
    ]
    if not any(t == pk_column for _s, t in pairs):
        pairs.append((src_pk, pk_column))
    as_target = [
        {t: r.get(s) for s, t in pairs} for r in src_rows if isinstance(r, dict)
    ]
    drift = measure_drift(
        source_rows=as_target,
        target_rows=[r for r in dest_rows if isinstance(r, dict)],
        columns=[t for _s, t in pairs],
        key_cols=[pk_column],
        dest_db_type=db_type,
    )
    if drift:
        return attach_drift(
            {
                "passed": True,
                "source_rows": 0,
                "target_rows": len(dest_rows),
                "assurance_level": NO_OP_DEST_UNCHANGED,
                "message": (
                    "No new source rows since the last watermark — nothing "
                    f"written, destination unchanged at {len(dest_rows):,} row(s)."
                ),
            },
            drift,
            resource=str(endpoint.table or ""),
        )
    return None


def _maybe_attach_verification_ladder(
    report: dict[str, Any],
    *,
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    records: list[dict],
    columns: list[str],
    dest_summary: dict[str, Any],
    mappings: list[dict],
    validation_mode: str,
) -> dict[str, Any]:
    """Property 5 — run L1–L5 when source+dest populations fit the row budget."""
    if is_no_op_report(report):
        # Nothing was read past the watermark, so there is no batch for the
        # ladder to verify: it would compare a zero-row write against a sink
        # that legitimately holds earlier rows, fail L1 conservation, and veto a
        # correct quiet poll. The destination count not moving is the proof.
        return report
    if is_cdc_source_image_count_report(report):
        # COUNT-only CDC: leftover dest keys are expected. L1–L5 would compare
        # the live source table to dest as snapshot identity, fail L4/L5 on
        # extras, and veto a correct catch-up. Dest COUNT vs source image is
        # the proof; leftover MERGE stays a no-op.
        return report
    from services.verification_ladder import (
        MAX_LADDER_ROWS,
        attach_ladder_to_reconcile_report,
        mappings_rewrite_written_cells,
        read_mysql_rows,
        read_postgres_rows,
        read_sqlite_rows,
        read_snowflake_rows,
        read_sqlserver_rows,
        run_five_layer_verification,
    )

    from .connector_capabilities import resolve_driver_type

    def _return(report_: dict[str, Any], reason: str) -> dict[str, Any]:
        """Early-exit guard: a route whose only proof is an engine count token
        may never keep a 'verified' stamp when the cell ladder did not run
        (X-22 live). Every skip names itself so the next run shows which exit
        fired instead of looking like value proof."""
        report_["ladder_declined_reason"] = reason
        if _report_is_count_proof_only(report_):
            report_["unproven"] = True
        return report_

    dest_type = resolve_driver_type(endpoint.format)

    # Oversized same-engine SQL routes: the in-memory ladder below declines, but
    # the engine can still profile every column at full scale. Purely additive —
    # only runs when the population exceeds the in-memory budget.
    engine_profile = _maybe_engine_profile_ladder(
        report,
        endpoint=endpoint,
        source_endpoint=source_endpoint,
        records=records,
        columns=columns,
        dest_summary=dest_summary,
        mappings=mappings,
        dest_type=dest_type,
    )
    if engine_profile is not None:
        return engine_profile

    known = max(
        _as_count(report.get("source_rows")),
        _as_count(report.get("target_rows")),
        len(records or []),
    )
    if known > MAX_LADDER_ROWS:
        # Every oversized dest, not just PG/SQLite: a MySQL/MariaDB route that
        # could not profile must still name the decline instead of returning a
        # bare Gate-8 report with no ladder at all.
        return _return(
            _ladder_declined(report, known, MAX_LADDER_ROWS),
            "population_exceeds_ladder_budget",
        )

    if dest_type not in {
        "sqlite",
        "postgresql",
        "redshift",
        "mysql",
        "snowflake",
        "sqlserver",
        "mssql",
        "azure_sql",
    }:
        # X-22: MySQL/SQL Server dests used to exit here — the row read below
        # would have thrown under the PG reader and the run stayed count-only.
        return _return(report, "destination_engine_not_in_ladder")
    target_cols = _mapped_targets(mappings, columns) if mappings else list(columns or [])
    if not target_cols:
        return _return(report, "no_mapped_target_columns")

    pk_cols: list[str] = []
    for key in ("primary_key_columns", "conflict_columns"):
        raw = dest_summary.get(key) or []
        if isinstance(raw, (list, tuple)):
            pk_cols = [str(x) for x in raw if x]
            if pk_cols:
                break
    pk_column = pk_cols[0] if pk_cols else ""
    # Identity maps often use `id` — accept when present on both sides.
    if not pk_column and "id" in {c.lower() for c in target_cols}:
        pk_column = next(c for c in target_cols if c.lower() == "id")

    dest_cfg = resolve_connector_config(endpoint)
    source_rows: list[dict] = [r for r in (records or []) if isinstance(r, dict)]
    target_rows: list[dict] = []

    # Prefer full SQL population over buffered records (streaming passes []).
    try:
        target_rows = _read_verify_rows(
            dest_type,
            dest_cfg,
            table=str(endpoint.table or dest_cfg.get("table") or ""),
            columns=target_cols,
            postgres_default=True,
        )
    except PopulationTooLarge as exc:
        return _return(
            _ladder_declined(report, exc.rows_read, exc.budget),
            "dest_population_exceeds_budget",
        )
    except Exception as exc:
        logging.getLogger(__name__).debug("ladder dest load failed: %s", exc)
        # X-22: a covered destination whose population could not be read has no
        # value proof — a count-only green must not stamp "verified". A report
        # already carrying real digests is proven either way: a measured
        # mismatch is a failure, not an unproven verdict.
        if not _report_has_value_digest(report):
            report["unproven"] = True
        return _return(report, "dest_population_read_failed")

    # A shaping recipe rewrote source-side values on the read, so the rows still
    # in the source table are no longer what this run wrote. Re-reading them here
    # would compare pre-shape values against post-shape values and report every
    # shaped cell as a mismatch, so the cell ladder declines and says why — the
    # proof for a shaped run is the pinned recipe hash plus the destination
    # re-read, not a source/destination cell equality that shaping made false.
    shaped_run = bool(dest_summary.get("shape_recipe_hash"))
    if source_endpoint is not None and not source_rows and shaped_run:
        return _return(
            _ladder_declined_for_shape(
                report, str(dest_summary["shape_recipe_hash"])
            ),
            "shaped_run_pinned_recipe",
        )
    source_reread = False
    # D123/D132 — a source re-read is the pre-image. When the mapping renames
    # or transforms a cell, the re-read must be read by *source* name and run
    # through the writer's own mapping (below) before any layer compares it.
    rewrites = mappings_rewrite_written_cells(mappings)
    if source_endpoint is not None and not source_rows:
        from services.procedure_source import is_callable_source

        if not is_callable_source(source_endpoint):
            src_type = resolve_driver_type(source_endpoint.format)
            src_cfg = resolve_connector_config(source_endpoint)
            try:
                source_reread = True
                source_rows = _read_verify_rows(
                    src_type,
                    src_cfg,
                    table=str(source_endpoint.table or src_cfg.get("table") or ""),
                    columns=(
                        _mapped_sources(mappings, target_cols) if rewrites else target_cols
                    ),
                )
            except PopulationTooLarge as exc:
                return _return(
                    _ladder_declined(report, exc.rows_read, exc.budget),
                    "source_population_exceeds_budget",
                )
            except Exception as exc:
                logging.getLogger(__name__).debug("ladder source load failed: %s", exc)

    if not source_rows or not target_rows:
        # X-22: a covered source whose re-read attempted but came back empty
        # (driver threw, or zero rows) while the destination holds rows has no
        # value proof — do not let the count-only green stamp "verified".
        if (
            source_reread
            and target_rows
            and not _report_has_value_digest(report)
        ):
            report["unproven"] = True
        return _return(report, "source_or_dest_population_empty")

    dest_types = {
        str(m.get("target") or ""): str(m.get("target_type") or m.get("inferredType") or "")
        for m in mappings
        if m.get("target") and (m.get("target_type") or m.get("inferredType"))
    }
    from services.sync_cursor import is_overwrite_sync

    sync_mode = str(
        dest_summary.get("sync_mode") or dest_summary.get("effective_sync_mode") or ""
    )
    allow_extra = (
        not is_overwrite_sync(sync_mode)
        and sync_mode.lower() not in {"full_refresh_mirror", "mirror", "scd2"}
    )
    raw_before = dest_summary.get(PRECOUNT_KEY)
    dest_before = int(raw_before) if isinstance(raw_before, int) else None
    from services.row_conservation import (
        KIND_KEYED,
        KeyCensus,
        conservation_kind,
    )

    kind = conservation_kind(sync_mode, dest_count_before=dest_before)
    census = KeyCensus.from_mapping(dest_summary.get(CENSUS_KEY))
    keyed = kind == KIND_KEYED
    # M3: a bounded source re-read stamps the exact [seed, max_seen] window
    # its digest covers. Every other population this ladder compares has to
    # share that window — without it the unbounded source re-read feeds L1–L5
    # whole-table rows while ``source_checksum`` describes only the window,
    # and ``key_rows`` below degenerates to every key in the table.
    from services.incremental_drift import (
        AFTER_WINDOW,
        IN_WINDOW,
        RunWindow,
    )

    window = RunWindow.from_summary(dest_summary)
    window_scoped_target = False
    # D188 — the source rows the cursor did NOT select, kept to report drift.
    before_window_rows: list[dict] = []
    after_window_rows: list[dict] = []
    full_target_rows = target_rows
    if window is not None and source_rows:
        _positions = [(r, window.position(r)) for r in source_rows]
        source_rows = [r for r, p in _positions if p == IN_WINDOW]
        if source_reread:
            after_window_rows = [r for r, p in _positions if p == AFTER_WINDOW]
            before_window_rows = [
                r for r, p in _positions if p not in (IN_WINDOW, AFTER_WINDOW)
            ]
        if not keyed and allow_extra and dest_before and target_rows:
            # D123 — an append into an occupied table: the rows this run wrote
            # are the destination rows inside the same cursor window (an append
            # table can repeat a key across runs, so a key lookup would match
            # history). Rows earlier runs appended stay outside the comparison.
            _to_target = {
                str(m.get("source") or ""): str(m.get("target") or "")
                for m in mappings
                if m.get("source") and m.get("target")
            }
            _t_window = window.renamed(_to_target)
            if _t_window.cursor in target_cols:
                target_rows = [
                    r for r in target_rows if _t_window.position(r) == IN_WINDOW
                ]
                window_scoped_target = True
    if source_reread and rewrites:
        def _as_written(rows: list[dict]) -> list[dict]:
            if not rows:
                return rows
            return _source_rows_as_written(
                rows,
                mappings=mappings,
                target_cols=target_cols,
                dest_types=dest_types,
                dest_type=dest_type,
                validation_mode=validation_mode,
            )

        source_rows = _as_written(source_rows)
        before_window_rows = _as_written(before_window_rows)
        after_window_rows = _as_written(after_window_rows)
    # X-OW1: a keyed run into an occupied destination is only comparable on
    # the written batch's keys — the destination legitimately holds rows this
    # run never wrote. Restrict the re-read population to the batch keys so
    # L2–L5 grade the batch, not pre-existing rows; cardinality still uses the
    # whole-table count via target_row_count / target_rows_before.
    batch_scoped = False
    if (
        census is not None
        and dest_before
        and pk_column
        and source_rows
    ):
        key_rows = [r for r in (records or []) if isinstance(r, dict)] or source_rows
        batch_keys = {
            str(r.get(pk_column))
            for r in key_rows
            if isinstance(r, dict) and r.get(pk_column) is not None
        }
        if batch_keys:
            target_rows = [
                r
                for r in target_rows
                if isinstance(r, dict) and str(r.get(pk_column)) in batch_keys
            ]
            # Same scope on the source side: an incremental re-read covers the
            # whole source table, and comparing it against the batch-keys-only
            # destination subset flags every legitimately-skipped source row.
            source_rows = [
                r
                for r in source_rows
                if isinstance(r, dict) and str(r.get(pk_column)) in batch_keys
            ]
            batch_scoped = True
    # D67/D95: rows the writer held out to quarantine are in the source
    # re-read but never entered the destination. Hashing them in the L3
    # source digest makes every correct quarantining run fail Gate-8 on its
    # own quarantine (the report already passed on the quarantine_excluded
    # basis — the ladder then vetoed it and the job undid the 39 good rows).
    # Exclude the quarantined keys from the compared source population when
    # the pk resolves; residual detail stays honest via rejected_rows.
    _rej_keys: set[str] | None = None
    if (
        pk_column
        and int(report.get("rejected_rows") or 0) > 0
        and source_rows
    ):
        _rej_keys = set()
        for _d in dest_summary.get("rejected_details") or []:
            if not isinstance(_d, dict):
                continue
            _pk = _d.get("source_pk") or _d.get("destination_pk")
            if _pk is not None:
                _rej_keys.add(str(_pk))
                continue
            _row = _d.get("row")
            if isinstance(_row, dict):
                _v = _row.get(pk_column)
            elif isinstance(_row, (str, int, float)):
                _v = _row  # scalar row payload = the row's pk value
            else:
                _v = None
            if _v is None:
                for _vals_key in ("source_values", "values"):
                    _vals = _d.get(_vals_key)
                    if isinstance(_vals, dict) and _vals.get(pk_column) is not None:
                        _v = _vals.get(pk_column)
                        break
            if _v is not None:
                _rej_keys.add(str(_v))
        if _rej_keys:
            source_rows = [
                r
                for r in source_rows
                if not isinstance(r, dict)
                or str(r.get(pk_column)) not in _rej_keys
            ]
    # D205 — an append into an occupied table: grade the written batch, not
    # the rows earlier runs wrote (see ladder_batch_scope).
    ladder_scope = str(report.get("checksum_scope") or "")
    if allow_extra and dest_before and not keyed and not (batch_scoped or window_scoped_target):
        from .ladder_batch_scope import scope_append_target_to_batch

        _scoped, ladder_scope = scope_append_target_to_batch(
            checksum_scope=ladder_scope, quarantined=bool(_rej_keys),
            target_rows=target_rows, source_rows=source_rows,
            pk_cols=pk_cols or [pk_column], dest_type=dest_type, dest_cfg=dest_cfg,
            schema=str(endpoint.schema or dest_cfg.get("schema") or ""),
            table_name=str(endpoint.table or dest_cfg.get("table") or ""),
            columns=target_cols, dest_types=dest_types,
        )
        if _scoped is not None:
            target_rows = _scoped
    ladder = run_five_layer_verification(
        source_rows=source_rows,
        target_rows=target_rows,
        columns=target_cols,
        pk_column=pk_column,
        pk_columns=pk_cols,
        # A measured 0 is a count, not a missing one. ``report["source_rows"] or
        # len(...)`` replaced a quiet incremental poll's reader count (0) with a
        # whole-table re-read, so L1 expected every existing row to be new and
        # failed the steady state of every incremental schedule (QA ACC-02).
        source_row_count=_measured_count(report.get("source_rows"), len(source_rows)),
        target_row_count=_measured_count(report.get("target_rows"), len(target_rows)),
        rejected_rows=int(report.get("rejected_rows") or 0),
        coerced_null_rows=int(report.get("coerced_null_rows") or 0),
        rows_skipped=int(report.get("rows_skipped") or 0),
        # Report-level digests cover the whole table and use the engine's own
        # scheme; once the row lists are re-scoped to the batch keys neither is
        # comparable, so L3 recomputes both sides on the scoped populations.
        # The same holds when quarantined keys were removed above: the source
        # digest still hashes the held-out rows, so it can never equal the
        # destination digest — recompute on the compared populations (D67/D95).
        source_checksum=(
            ""
            if (batch_scoped or window_scoped_target or _rej_keys)
            else str(report.get("source_checksum") or "")
        ),
        target_checksum=(
            ""
            if (batch_scoped or window_scoped_target or _rej_keys)
            else str(report.get("target_checksum") or "")
        ),
        dest_db_type=dest_type,
        dest_types=dest_types,
        allow_extra_rows=allow_extra,
        checksum_scope=ladder_scope,
        target_rows_before=dest_before,
        keyed_cardinality=keyed,
        keyed_expected_delta=census.expected_delta if keyed and census else None,
        # maximum: always run L4/L5. strict/balanced: localize only on L3 fail.
        always_localize=str(validation_mode or "").lower() == "maximum",
        # A table re-read is the pre-image. Crosswalks, masks, and renames
        # already match on the written-batch checksum; L2 must not veto them.
        l2_skip_reason=(
            "source_reread_is_pre_transform" if source_reread and rewrites else ""
        ),
    )
    out = attach_ladder_to_reconcile_report(report, ladder)
    if window_scoped_target:
        # Round 5 (D203 prov.) — the ladder allows the extra rows an append
        # table holds, so a window appended twice passed. Exactly-once is
        # this run's own verdict, inside the window (D188).
        from services.incremental_drift import (
            fail_on_window_duplicates,
            measure_window_duplicates,
        )

        _ident_keys = list(pk_cols) or ([_t_window.pk] if _t_window.pk else [])
        out = fail_on_window_duplicates(out, measure_window_duplicates(
            source_rows=source_rows, target_rows=target_rows,
            match_cols=(
                list(dict.fromkeys([_t_window.cursor, *_ident_keys]))
                if _ident_keys else list(target_cols)
            ),
            dest_db_type=dest_type, dest_types=dest_types,
        ))
    if window is not None and source_reread and keyed and (pk_cols or pk_column):
        # D188 — rows the cursor did not select are not this run's verdict,
        # but a disagreement there is reported, never silently dropped.
        from services.incremental_drift import attach_drift, measure_drift
        from services.verification_ladder import _index_by_pk

        _key_cols = pk_cols or [pk_column]
        _owned = set(_index_by_pk(source_rows, _key_cols)) | set(
            _index_by_pk(after_window_rows, _key_cols)
        )
        drift = measure_drift(
            source_rows=before_window_rows,
            target_rows=full_target_rows,
            columns=target_cols,
            key_cols=_key_cols,
            dest_db_type=dest_type,
            dest_types=dest_types,
            skip_keys=_owned | (_rej_keys or set()),
        )
        out = attach_drift(out, drift, resource=str(endpoint.table or ""))
    return out


def _identity_watermark_evidence(
    *,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table: str,
    pk_cols: list[str],
) -> dict[str, Any]:
    """Generator state for the destination keys after the write.

    Row checksums cannot see this: a migration that carries explicit key values
    leaves Postgres and Oracle generators at their pre-migration value, so the
    first application insert after cutover collides on the primary key. Repair
    is forward-only and opt-out via ``identity_watermark_repair``.
    """
    from services.identity_watermark import (
        identity_watermark_supported,
        verify_identity_watermark,
    )

    if not (table and pk_cols):
        return {}
    if not identity_watermark_supported(db_type):
        return {
            "supported": False,
            "verified": False,
            "reason": f"generator state is not readable on '{db_type}'",
        }
    repair = cfg.get("identity_watermark_repair")
    evidence = verify_identity_watermark(
        db_type,
        cfg,
        schema=schema or "",
        table=str(table),
        columns=pk_cols,
        repair=True if repair is None else bool(repair),
    )
    evidence["supported"] = True
    return evidence


def _schema_state_evidence(
    *,
    source_endpoint: EndpointConfig | None,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table: str,
) -> dict[str, Any]:
    """Constraints, indexes, nullability and defaults, compared catalog to catalog.

    A run can move every row and still leave the client a table with no primary
    key, no unique constraint and no index. Both sides are read here on this
    module's own connections, never from writer bookkeeping.
    """
    from .connector_capabilities import resolve_driver_type

    if source_endpoint is None or source_endpoint.kind != "database" or not table:
        return {}
    if str(db_type or "").lower() in _PROOF_OBJECT_STORES | {"sftp"}:
        return {
            "skipped": True,
            "reason": "file_destination",
            "note": "A file holds no constraints, indexes or defaults to compare.",
        }
    src_cfg = resolve_connector_config(source_endpoint)
    src_type = resolve_driver_type(
        str(src_cfg.get("type") or source_endpoint.format or "")
    ).lower()
    src_table = str(source_endpoint.table or src_cfg.get("table") or "")
    if not src_table:
        return {}
    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint) or is_callable_source(src_cfg):
        return {
            "skipped": True,
            "reason": "callable_source",
            "note": (
                "Physical catalog compare is not run against a CALL/SELECT stream name."
            ),
        }

    from services.dialect_profiles import schema_from_cfg
    from services.physical_state_diff import verify_physical_state

    return verify_physical_state(
        source_db_type=src_type,
        source_cfg=src_cfg,
        source_schema=str(schema_from_cfg(src_type, src_cfg) or ""),
        source_table=src_table,
        dest_db_type=db_type,
        dest_cfg=cfg,
        dest_schema=schema,
        dest_table=table,
    )


def _source_foreign_keys(
    schema_state: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Relationships the source guaranteed, and tokens that could not be read.

    Structured catalog facts are the relationship. The rendered ``child->parent``
    string is only a fallback. A token that does not parse is returned, not
    dropped: dropping it would make Gate-8 say the source declared no foreign key.
    """
    from services.physical_state_diff import foreign_keys_from_catalog_state

    source = schema_state.get("source") if isinstance(schema_state, dict) else None
    return foreign_keys_from_catalog_state(source if isinstance(source, dict) else {})


def _referential_integrity_evidence(
    *,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table: str,
    schema_state: dict[str, Any],
    source_schema: str = "",
) -> dict[str, Any]:
    """Orphan proof for every source relationship the destination does not enforce."""
    foreign_keys, unparsed = _source_foreign_keys(schema_state)
    if not foreign_keys and not unparsed:
        return {
            "verified": False,
            "asked": False,
            "reason": "source declares no foreign keys",
            "relations": [],
            "orphan_rows": 0,
        }
    if not foreign_keys:
        return {
            "verified": False,
            "asked": True,
            "reason": "source foreign key could not be read",
            "relations": [],
            "unavailable_relations": list(unparsed),
            "orphan_rows": 0,
        }

    from services.destination_ri_probe import verify_destination_referential_integrity

    evidence = verify_destination_referential_integrity(
        db_type,
        cfg,
        schema=schema,
        table=table,
        foreign_keys=foreign_keys,
        source_schema=source_schema,
    )
    evidence["asked"] = True
    if unparsed:
        evidence["verified"] = False
        unavailable = list(evidence.get("unavailable_relations") or [])
        unavailable.extend(unparsed)
        evidence["unavailable_relations"] = unavailable
        evidence["unparsed_foreign_keys"] = list(unparsed)
    return evidence


def _classify_ri_anomaly_origin(
    evidence: dict[str, Any],
    source_endpoint: EndpointConfig,
    schema_state: dict[str, Any],
) -> None:
    """Say whether destination orphans were already orphans in the source.

    ``source``: the source child rows reference parents the source lacks — the
    copy is faithful and the data is wrong. ``destination``: the source holds
    every parent, so the destination parent set is incomplete. A source scan
    that did not complete is ``undetermined``, never either answer.
    """
    evidence["anomaly_origin"] = "undetermined"
    try:
        from services.population_orphan_probe import probe_population_fk_orphans

        src_cfg = resolve_connector_config(source_endpoint)
        src_table = str(source_endpoint.table or src_cfg.get("table") or "")
        foreign_keys, _unparsed = _source_foreign_keys(schema_state)
        scan = probe_population_fk_orphans(
            child_table=src_table,
            mappings=[],
            foreign_keys=foreign_keys,
            source_config=src_cfg,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "source orphan classification failed: %s", exc, exc_info=exc
        )
        evidence["anomaly_origin_reason"] = f"source orphan scan failed: {exc}"
        return
    if not scan.get("ran") or not scan.get("complete"):
        evidence["anomaly_origin_reason"] = str(scan.get("note") or "source scan incomplete")
        return
    source_orphans = int(scan.get("orphan_count") or 0)
    evidence["source_orphan_rows"] = source_orphans
    evidence["anomaly_origin"] = "source" if source_orphans > 0 else "destination"


def _apply_n5_gate8_extensions(
    stamped: dict[str, Any],
    n5_ctx: dict[str, Any],
) -> dict[str, Any]:
    """Fail-closed control totals (G21) and dest RI (G22) on the Gate-8 report."""
    from services.control_totals import apply_control_totals_to_reconcile
    from services.destination_ri_probe import apply_dest_ri_to_reconcile

    mappings = list(n5_ctx.get("mappings") or [])
    out = apply_control_totals_to_reconcile(
        stamped,
        mappings=mappings,
        source_db_type=str(n5_ctx.get("source_db_type") or ""),
        source_cfg=n5_ctx.get("source_cfg") if isinstance(n5_ctx.get("source_cfg"), dict) else None,
        source_schema=str(n5_ctx.get("source_schema") or ""),
        source_table=str(n5_ctx.get("source_table") or ""),
        dest_db_type=str(n5_ctx.get("dest_db_type") or ""),
        dest_cfg=n5_ctx.get("dest_cfg") if isinstance(n5_ctx.get("dest_cfg"), dict) else None,
        dest_schema=str(n5_ctx.get("dest_schema") or ""),
        dest_table=str(n5_ctx.get("dest_table") or ""),
        rejected_rows=int(n5_ctx.get("rejected_rows") or 0),
    )
    phys = out.get("physical_state") if isinstance(out.get("physical_state"), dict) else {}
    ri = phys.get("referential_integrity") if isinstance(phys, dict) else None
    asked = False
    if isinstance(ri, dict):
        asked = bool(ri.get("asked")) or bool(ri.get("relations"))
    asked = asked or bool(n5_ctx.get("source_has_fks"))
    return apply_dest_ri_to_reconcile(
        out,
        evidence=ri if isinstance(ri, dict) else None,
        has_relationships=asked,
        quarantined_orphans=int(n5_ctx.get("orphan_fk_rows") or 0),
    )


def _writer_supplied_engine_digests(
    dest_summary: dict[str, Any] | None,
) -> tuple[str, str, int] | None:
    """Both digests from a writer that computed them in the engine.

    Returns ``(source_checksum, target_checksum, target_rows)``. Only the pair is
    usable: a run that produced one of them and left the other to be recomputed
    here would be comparing a numeric engine digest against a Python hex digest.
    """
    summary = dest_summary or {}
    source = str(summary.get("engine_source_checksum") or "").strip()
    target = str(summary.get("engine_target_checksum") or "").strip()
    if not source or not target:
        return None
    # A count token on both sides is a row count, not a digest pair: taking it
    # here would grade ``pk_join_count:37 == pk_join_count:37`` as value proof
    # and stamp the batch size as the destination population.
    if is_count_proof_token(source) or is_count_proof_token(target):
        return None
    rows = summary.get("rows_written")
    return source, target, int(rows or 0)


def _with_reread_dest_count(
    paired: tuple[str, str, int],
    *,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table_name: str,
    rows_before: int | None,
) -> tuple[str, str, int]:
    """Writer digest pair with the destination COUNT(*) re-read, not the batch.

    The pair's row count is the writer's ``rows_written`` — the batch. Into an
    occupied destination (incremental run 2+, append) that is not the
    population: L1 graded ``batch - target_rows_before == batch`` and failed
    every correct second run, and a quiet poll (batch 0) missed the no-op proof
    (QA RT-02). When the count cannot be re-read and the destination already
    held rows, the population is reported unmeasured (-1, the
    ``verify_target`` convention) so Gate-8 fails closed.
    """
    source, target, batch = paired
    measured = destination_row_count(
        db_type, cfg, schema=schema, table_name=table_name
    )
    if measured is not None:
        if rows_before and int(measured) != int(batch):
            _logger.info(
                "gate8: writer digest pair for %s — batch %d, destination COUNT(*) "
                "%d (held %d before)",
                table_name, int(batch), int(measured), int(rows_before),
            )
        return source, target, int(measured)
    if rows_before:
        _logger.warning(
            "gate8: destination COUNT(*) re-read unavailable for %s (%s) after a "
            "write into %d existing row(s); population left unmeasured",
            table_name, db_type, int(rows_before),
        )
        return source, target, -1
    return paired


_COUNT_PROOF_TOKEN = re.compile(r"^(?:dest_count|pk_join_count):(\d+)$")

#: Proof scopes whose token counts rows rather than digesting them: a full COPY
#: comparing destination COUNT(*) to the source snapshot, and a keyed upsert
#: comparing the destination key join to the staged population.
_COUNT_PROOF_SCOPES = (
    "dest_count_equals_source_snapshot",
    "dest_pk_join_equals_staging",
    # File incremental COPY: dest_after = dest_before + staging. Same token
    # as a full COPY — comparing it to a hex dest digest is hash-vs-count.
    "dest_count_equals_before_plus_staging",
)


def is_count_proof_token(checksum: Any) -> bool:
    """True for an engine copy's ``dest_count:<n>`` / ``pk_join_count:<n>`` token."""
    return bool(_COUNT_PROOF_TOKEN.match(str(checksum or "").strip()))


def _report_is_count_proof_only(report: dict[str, Any] | None) -> bool:
    """True when the reconcile report's only proof is an engine count token —
    dest COUNT(*) against the source snapshot, or the dest key join against
    staging. Such a run has no value proof until the cell ladder attaches
    (X-22 live): no early exit may leave it reading 'verified'."""
    r = report or {}
    if is_count_proof_token(r.get("checksum")) or is_count_proof_token(
        r.get("target_checksum")
    ):
        return True
    scope = str(r.get("proof_scope") or "")
    if scope and any(known in scope for known in _COUNT_PROOF_SCOPES):
        return True
    scope = str(r.get("checksum_scope") or "")
    return bool(scope) and any(known in scope for known in _COUNT_PROOF_SCOPES)


def _report_has_value_digest(report: dict[str, Any] | None) -> bool:
    """True when the report already carries a real (non-count-token) value
    digest — a measured checksum mismatch is a proven failure and a matching
    digest is proven equality, so the ladder may never downgrade either to
    'unproven' (regression vs test_gate8_verdict_real_shapes)."""
    r = report or {}
    return bool(
        (r.get("checksum") or r.get("target_checksum"))
        and not is_count_proof_token(r.get("checksum"))
        and not is_count_proof_token(r.get("target_checksum"))
    )


def _report_has_proven_value_match(report: dict[str, Any] | None) -> bool:
    """True only when the report compares real value digests and they matched.

    ``checksum_match is True`` plus a non-count-token digest on BOTH sides —
    a count-only engine copy's one-sided read-back digest, or a ``dest_count:n``
    token, never earns the green "verified" (X-41b).
    """
    r = report or {}
    return bool(
        r.get("checksum_match") is True
        and r.get("source_checksum")
        and r.get("target_checksum")
        and not is_count_proof_token(r.get("source_checksum"))
        and not is_count_proof_token(r.get("target_checksum"))
    )


_SOURCE_DIGEST_MODES = ("inline_write_pass", "source_reread")


def source_digest_from_summaries(
    *summaries: dict[str, Any] | None,
) -> tuple[str, str]:
    """``(checksum, checksum_mode)`` of the first summary holding a real source digest.

    A staged merge (SCD2, mirror) runs two inner passes: the source→staging
    write, whose write-pass fingerprint *is* the remapped source population,
    and a staging→target apply whose digest — when it is a digest at all and
    not an engine copy's count token — covers the same rows. Only a value
    digest may stand as the source side of Gate-8; a count token or a writer's
    own ack would make the comparison a hash-to-count or a tautology.
    """
    for summary in summaries:
        if not summary:
            continue
        checksum = str(summary.get("checksum") or "").strip()
        mode = str(summary.get("checksum_mode") or "").strip()
        if checksum and mode in _SOURCE_DIGEST_MODES and not is_count_proof_token(checksum):
            return checksum, mode
    return "", ""


def _engine_count_proof_only(dest_summary: dict[str, Any] | None) -> int | None:
    """Rows an engine copy proved by counting when a count is all the proof it has.

    A server-side copy never brings a row into this process, so a route whose
    proof scope is destination COUNT(*) against the source snapshot COUNT(*)
    reports ``dest_count:<n>`` where a value digest would go — and a keyed
    upsert, whose destination holds rows earlier runs wrote, reports
    ``pk_join_count:<n>`` for the same reason. Reading the destination back here
    produces a hex digest, and comparing that to either token compares a hash to
    a row count: every correct run of those routes failed with "checksum
    mismatch". The count is graded as the cardinality proof it is instead, and
    the report says plainly that value fidelity was not compared.
    """
    summary = dest_summary or {}
    if _writer_supplied_engine_digests(summary) is not None:
        return None
    scope = str(summary.get("proof_scope") or "")
    if not any(known in scope for known in _COUNT_PROOF_SCOPES):
        return None
    matched = _COUNT_PROOF_TOKEN.match(str(summary.get("checksum") or "").strip())
    return int(matched.group(1)) if matched else None


def _merge_cardinality_only_report(
    *,
    source_rows: int,
    active_rows: int,
    active_checksum: str,
    rejected_rows: int,
    coerced_null_rows: int,
    rows_skipped: int,
    note: str,
) -> dict[str, Any]:
    """SCD2/mirror verdict when the run holds no source value digest.

    The active-row population is compared to the source population and the
    report says plainly that value fidelity was not compared — never a hash
    against a count, never the destination digest on both sides.
    """
    from services.reconciliation import ReconciliationReport

    expected_rows = max(source_rows - rejected_rows - rows_skipped, 0)
    balanced = active_rows == expected_rows
    return ReconciliationReport(
        passed=balanced,
        source_rows=source_rows,
        target_rows=active_rows,
        source_checksum="",
        target_checksum=active_checksum,
        rejected_rows=rejected_rows,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
        checksum_scope=WHOLE_TABLE_NOT_COMPARABLE,
        message=(
            (
                f"Active row count verified by engine copy: {active_rows:,} live "
                f"row(s) on the destination for {source_rows:,} source row(s). "
                if balanced
                else (
                    f"Active row count mismatch by engine copy: expected "
                    f"{expected_rows:,} live row(s) on the destination, found "
                    f"{active_rows:,}. "
                )
            )
            + note
        ),
    ).to_dict()


def _keyed_join_proof(dest_summary: dict[str, Any] | None) -> bool:
    """True when the count proof came from a destination key join, not COUNT(*)."""
    summary = dest_summary or {}
    scope = str(summary.get("proof_scope") or "")
    if "dest_pk_join_equals_staging" not in scope:
        return False
    return bool(_COUNT_PROOF_TOKEN.match(str(summary.get("checksum") or "").strip()))


def _is_append_first_load(
    sync_mode: str,
    rows_before: int | None,
    target_rows: int,
    expected_rows: int,
    dest_summary: dict[str, Any] | None,
) -> bool:
    """True when an append run's destination equals the staged population.

    An append into a table this run created (or that was empty) has no
    pre-existing rows — ``dest == staged`` exactly, so the key census is a
    valid value compare. Only a later append run into an occupied table is
    genuinely incomparable (dest = before + delta). X-134.
    """
    return (
        sync_mode_appends_without_key_resolution(sync_mode)
        and rows_before == 0
        and target_rows == expected_rows
        and (dest_summary or {}).get("dest_table_existed_before") is not True
    )


_FULL_KEYED_COMPARE_MAX_ROWS = 1_000_000


def _read_full_source_records(ctx: dict[str, Any], total: int) -> list[dict[str, Any]] | None:
    source = ctx.get("source_endpoint")
    if source is None or str(getattr(source, "kind", "") or "") != "database":
        return None
    from .connector_capabilities import resolve_driver_type

    src_cfg = resolve_connector_config(source)
    src_type = resolve_driver_type(str(src_cfg.get("type") or getattr(source, "format", "") or "")).lower()
    table = str(getattr(source, "table", "") or "").strip()
    pairs = [
        (str(m.get("source") or ""), str(m.get("target") or m.get("source") or ""))
        for m in (ctx.get("mappings") or [])
        if isinstance(m, dict) and str(m.get("source") or "").strip()
    ]
    if not table or not pairs:
        return None
    sort_key = str(ctx.get("sort_key") or "")
    src_sort = next((s for s, t in pairs if t.lower() == sort_key.lower()), None)
    from services.dialect_profiles import schema_from_cfg

    rows = _read_target_sample_retrying(
        "full keyed compare (source)",
        src_type,
        src_cfg,
        schema=schema_from_cfg(src_type, src_cfg),
        table_name=table,
        columns=[s for s, _t in pairs],
        limit=total,
        sort_key=src_sort,
    )
    return [r for r in rows if isinstance(r, dict)] if len(rows) == total else None


def _name_row_on_checksum_only_failure(
    report: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    """Name the changed row when only the whole-population checksum failed.

    The keyed sample covers at most 50 rows. A file or object destination has
    no census driver, so a change outside the sample failed Gate-8 with no row
    named. On that failure path only, re-read every written key through the
    same reader and compare with the same comparator as the sample.
    """
    if report.get("passed") is not False or report.get("checksum_match") is not False:
        return report
    if not ctx:
        return report
    from services.dest_precount import _object_store_kind

    db_type = str(ctx.get("db_type") or "")
    if db_type != "sftp" and _object_store_kind(db_type) not in {"s3", "gcs", "adls"}:
        return report
    sample = report.get("sample_compare") or {}
    if not (isinstance(sample, dict) and sample.get("passed") and int(sample.get("compared") or 0) > 0):
        return report
    records = list(ctx.get("records") or [])
    total = max(len(records), int(report.get("source_rows") or 0))
    if total <= int(sample.get("rows_compared") or 0):
        return report
    if total > _FULL_KEYED_COMPARE_MAX_ROWS:
        return {
            **report,
            "full_keyed_compare": {
                "passed": None,
                "skipped": f"{total:,} rows exceeds the {_FULL_KEYED_COMPARE_MAX_ROWS:,}-row cap",
            },
        }
    try:
        if len(records) < total:
            # Streaming runs keep only a bounded sample in memory: re-read the
            # source population through the same reader.
            records = _read_full_source_records(ctx, total)
            if records is None:
                return report
        keys = _source_key_values(
            records, sort_key=ctx.get("sort_key"), mappings=ctx.get("mappings"), limit=total
        )
        rows = _read_target_sample_retrying(
            "full keyed compare",
            db_type,
            ctx.get("cfg") or {},
            schema=str(ctx.get("schema") or ""),
            table_name=str(ctx.get("table_name") or ""),
            columns=ctx.get("target_cols") or None,
            limit=total,
            sort_key=ctx.get("sort_key"),
            key_values=keys or None,
        )
        full = sample_compare_rows(
            records,
            rows,
            ctx.get("mappings") or [],
            target_columns=ctx.get("target_cols"),
            sample_size=total,
            sort_key=ctx.get("sort_key"),
            dest_db_type=db_type,
            dest_types=ctx.get("dest_types"),
            duplicate_policy=str(ctx.get("duplicate_policy") or ""),
            source_locale=str(ctx.get("source_locale") or ""),
        )
    except Exception as exc:  # noqa: BLE001 — naming never changes the verdict
        _logger.warning("Gate-8 full keyed compare unavailable: %s", exc)
        return report
    full["coverage"] = "full_keyed"
    out = {**report, "full_keyed_compare": full}
    if full.get("passed") is False and full.get("mismatches"):
        out["message"] = (
            f"{str(report.get('message') or '').rstrip()} A keyed compare of all "
            f"{total:,} written rows found the change: {_cell_mismatch_text(full)}."
        )
    return out


def _localize_checksum_mismatch(
    report: dict[str, Any],
    dest_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Name what the two hashes disagree about when no cell was found to differ.

    A strict mismatch that survives a clean key-aligned sample on a conserved
    population is not a corrupted cell — it is the two sides having been hashed
    on different bases or over different populations. Reporting only the pair of
    hashes left the operator with nothing to act on, so the classification and
    the facts behind it are stamped for the run panel:

    ``digest_basis`` — how each side was obtained (write-pass, independent
    re-read, writer ack) and, when known, the columns whose granularity the
    destination carrier decides.

    The verdict itself does not move: a mismatch still fails Gate-8.
    """
    if report.get("checksum_match") is not False:
        return report
    if report.get("passed") is True:
        # ``checksum_match=False`` also means "digest unproven" on runs where
        # no digest comparison could run (count-only scopes, a source with no
        # census driver). The run verified on its stated basis — appending a
        # "Gate-8 still fails" clause would contradict the verdict.
        return report
    if (report.get("full_keyed_compare") or {}).get("passed") is False:
        return report
    summary = dest_summary or {}
    sample = report.get("sample_compare") or {}
    compared = int((sample or {}).get("compared") or 0)
    sample_clean = bool(sample) and bool(sample.get("passed")) and compared > 0
    if not sample_clean:
        return report
    rows = int(sample.get("rows_compared") or 0)
    basis = {
        "source_digest": str(
            report.get("source_checksum_provenance")
            or summary.get("checksum_mode")
            or ""
        ),
        "source_scope": str(report.get("checksum_scope") or ""),
        "carrier_rounded_columns": list(report.get("carrier_rounded_columns") or []),
        "keyed_sample_rows_without_mismatch": rows or compared,
        "keyed_sample_cells_without_mismatch": compared,
    }
    scope = f"{rows:,} row(s) / {compared:,} cell(s)" if rows else f"{compared:,} cell(s)"
    report["mismatch_class"] = "comparison_basis_or_population_scope"
    report["digest_basis"] = basis
    report["message"] = (
        f"{str(report.get('message') or '').rstrip()} No differing cell was found "
        f"in {scope} of key-aligned data, so the two digests differ in how or "
        f"over what they were taken, not in a sampled value — source digest "
        f"{basis['source_digest'] or 'unknown'}"
        + (f", scope {basis['source_scope']}" if basis["source_scope"] else "")
        + ". Gate-8 still fails: an unexplained digest difference is not proof."
    )
    return report


_CULPRIT_KEYS_LIMIT = 32


def _name_gate8_culprits(
    report: dict[str, Any],
    *,
    source_endpoint: Any,
    endpoint: Any,
    dest_summary: dict[str, Any] | None,
    mappings: list[dict[str, Any]] | None = None,
) -> None:
    """Name culprit keys on a failed keyed run.

    Gate-8 fails on a digest difference over the whole population; without a
    named culprit the operator only has two hex strings. On keyed routes we
    can do better in bounded memory: stream ``(pk, row hash)`` from each side
    into a temp sqlite table, then join to name up to ``_CULPRIT_KEYS_LIMIT``
    keys that changed value, went missing, or exist only on the destination.
    Runs only on the failure path, never on verify.
    """
    summary = dest_summary or {}
    src_fmt = str(getattr(source_endpoint, "format", "") or "")
    dst_fmt = str(getattr(endpoint, "format", "") or "")
    if src_fmt == "postgresql" and dst_fmt == "postgresql":
        _name_gate8_culprits_pg(
            report,
            source_endpoint=source_endpoint,
            endpoint=endpoint,
            dest_summary=summary,
        )
        return
    _name_gate8_culprits_generic(
        report,
        source_endpoint=source_endpoint,
        endpoint=endpoint,
        dest_summary=summary,
        mappings=mappings,
    )
    return


def _name_gate8_culprits_pg(
    report: dict[str, Any],
    *,
    source_endpoint: Any,
    endpoint: Any,
    dest_summary: dict[str, Any] | None,
) -> None:
    """PG→PG census — both sides hash in-engine with the same algorithm."""
    summary = dest_summary or {}
    pk_cols = [
        str(c)
        for c in (summary.get("primary_key_columns") or [])
        if str(c or "").strip()
    ]
    if len(pk_cols) != 1:
        return
    pk = pk_cols[0]
    src_tbl = str(getattr(source_endpoint, "table", "") or "").strip()
    dst_tbl = str(getattr(endpoint, "table", "") or "").strip()
    if not src_tbl or not dst_tbl:
        return
    try:
        from services.dest_precount import warehouse_sql_table_ref
        from connectors.sql_identifiers import quote_sql_identifier as _qi
        from .connector_capabilities import resolve_driver_type
        from connectors.postgresql_conn import get_connection

        dest_type = resolve_driver_type(endpoint.format or "") or "postgresql"
        src_type = resolve_driver_type(source_endpoint.format or "") or "postgresql"
        src_cfg = resolve_connector_config(source_endpoint)
        dst_cfg = resolve_connector_config(endpoint)
        qpk = _qi(pk, '"')
        qpk_d = _qi(pk, '"')
        src_ref = warehouse_sql_table_ref(
            src_type, src_cfg, str(src_cfg.get("schema") or ""), src_tbl
        )
        dst_ref = warehouse_sql_table_ref(
            dest_type, dst_cfg, str(dst_cfg.get("schema") or ""), dst_tbl
        )
        stmt_src = (
            f"SELECT {qpk} AS pk, md5(row_to_json(t)::text) AS h "  # nosec B608
            f"FROM {src_ref} t"
        )
        stmt_dst = (
            f"SELECT {qpk_d} AS pk, md5(row_to_json(t)::text) AS h "  # nosec B608
            f"FROM {dst_ref} t"
        )

        fd, path = tempfile.mkstemp(prefix="df_culprits_", suffix=".sqlite3")
        os.close(fd)
        try:
            db = sqlite3.connect(path)
            db.execute(
                "CREATE TABLE k(side INTEGER NOT NULL, pk TEXT, h TEXT)"
            )
            db.execute("CREATE INDEX ki ON k(side, pk)")

            def _load(cfg, stmt, side):
                # Dedicated connection + server-side cursor: a pooled conn
                # can carry a stale REPEATABLE READ snapshot, and the
                # egress-guarded sqlalchemy pool refuses targets the
                # engine's own psycopg2 path just wrote to.
                pc = get_connection(
                    host=cfg.get("host", ""),
                    port=int(cfg.get("port") or 5432),
                    database=cfg.get("database", ""),
                    username=cfg.get("username", ""),
                    password=cfg.get("password", ""),
                    connection_string=cfg.get("connection_string", ""),
                    ssl=bool(cfg.get("ssl", False)),
                )
                try:
                    cur = pc.cursor(name="df_culprits")
                    cur.execute(stmt)
                    while True:
                        chunk = cur.fetchmany(5000)
                        if not chunk:
                            break
                        db.executemany(
                            "INSERT INTO k(side, pk, h) VALUES (?,?,?)",
                            ((side, str(r[0]), r[1]) for r in chunk),
                        )
                    db.commit()
                finally:
                    pc.close()

            _load(src_cfg, stmt_src, 0)
            _load(dst_cfg, stmt_dst, 1)

            changed, missing, extra = _census_join(db)
            db.close()
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    except Exception as exc:
        _logger.warning(
            "Gate-8 culprit-key naming skipped: %s", exc, exc_info=exc
        )
        return
    _report_culprit_keys(report, changed, missing, extra)


def _census_join(db: Any) -> tuple[list[Any], list[Any], list[Any]]:
    """Join a loaded (side, pk, h) census into changed/missing/extra keys."""
    lim = _CULPRIT_KEYS_LIMIT + 1
    changed = [
        r[0]
        for r in db.execute(
            "SELECT s.pk FROM k s JOIN k d ON s.pk = d.pk AND d.side = 1 "
            "WHERE s.side = 0 AND s.h <> d.h LIMIT ?",
            (lim,),
        )
    ]
    missing = [
        r[0]
        for r in db.execute(
            "SELECT s.pk FROM k s LEFT JOIN k d "
            "ON s.pk = d.pk AND d.side = 1 "
            "WHERE s.side = 0 AND d.pk IS NULL LIMIT ?",
            (lim,),
        )
    ]
    extra = [
        r[0]
        for r in db.execute(
            "SELECT d.pk FROM k d LEFT JOIN k s "
            "ON d.pk = s.pk AND s.side = 0 "
            "WHERE d.side = 1 AND s.pk IS NULL LIMIT ?",
            (lim,),
        )
    ]
    return changed, missing, extra


def _report_culprit_keys(
    report: dict[str, Any],
    changed: list[Any],
    missing: list[Any],
    extra: list[Any],
) -> None:
    culprits: dict[str, Any] = {}
    parts: list[str] = []
    for label, keys, desc in (
        ("changed", changed, "value changed"),
        ("missing_in_dest", missing, "missing from destination"),
        ("extra_in_dest", extra, "present only on destination"),
    ):
        if keys:
            culprits[label] = keys[:_CULPRIT_KEYS_LIMIT]
            suffix = "…" if len(keys) > _CULPRIT_KEYS_LIMIT else ""
            parts.append(
                f"{desc}: {', '.join(culprits[label][:8])}{suffix} "
                f"({len(culprits[label])}{'+' if len(keys) > _CULPRIT_KEYS_LIMIT else ''} key(s))"
            )
    if not culprits:
        return
    report["culprit_keys"] = culprits
    msg = str(report.get("message") or "")
    report["message"] = f"{msg} Culprit key(s) — {'; '.join(parts)}."


def _culprit_census_conn(fmt: str, cfg: dict[str, Any]) -> tuple[str, Any] | None:
    """(engine, connection) for the failure-path key census, or None when the
    engine cannot be read back for a census."""
    from .connector_capabilities import resolve_driver_type

    eng = resolve_driver_type(fmt or "") or str(fmt or "")
    if eng in {"", "generic_sql", "unknown"}:
        # DuckDB/SQLite resolve to the generic routing key — the census needs
        # the real engine from the endpoint's own format.
        eng = str(fmt or "").strip().lower() or eng
    if eng == "sqlserver":
        # resolve_driver_type names SQL Server "sqlserver"; the census helpers
        # below key it "mssql". Without the alias Gate-8 said SQL Server "has
        # no census driver" while listing mssql among them (D173).
        eng = "mssql"
    if eng == "postgresql":
        from connectors.postgresql_conn import get_connection

        return eng, get_connection(
            host=cfg.get("host", ""),
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl", False)),
        )
    if eng == "mysql":
        from services.copy_mysql_pg import _mysql_connect

        return eng, _mysql_connect(cfg)
    if eng == "duckdb":
        import duckdb

        from connectors.sqlite_common import duckdb_file_path

        return eng, duckdb.connect(
            database=duckdb_file_path(
                str(cfg.get("database") or cfg.get("path") or ""),
                str(cfg.get("connection_string") or ""),
            )
            or ":memory:"
        )
    if eng == "sqlite":
        from connectors.sqlite_common import sqlite_file_path

        return eng, sqlite3.connect(
            sqlite_file_path(
                str(cfg.get("database") or cfg.get("path") or ""),
                str(cfg.get("connection_string") or ""),
                "",
            )
        )
    if eng == "mssql":
        from services.copy_sqlserver_sqlserver import _ss_connect

        return eng, _ss_connect(cfg)
    if eng == "oracle" or eng.startswith("oracle"):
        # D212: an Oracle side used to have "no census driver", so an
        # Oracle -> SQL Server engine copy proved COUNT(*) only.
        from services.value_readback import oracle_census_connection

        return "oracle", oracle_census_connection(cfg)
    if eng == "snowflake":
        from connectors.snowflake_conn import get_connection, normalize_account
        from connectors.sql_identifiers import snowflake_fold_identifier

        return eng, get_connection(
            account=normalize_account(str(cfg.get("host") or "")),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            database=str(cfg.get("database") or ""),
            schema=snowflake_fold_identifier(str(cfg.get("schema") or "PUBLIC")),
            warehouse=str(cfg.get("warehouse") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
        )
    return None


def _culprit_table_ref(eng: str, cfg: dict[str, Any], table: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier as _qi

    if eng == "duckdb" or eng == "sqlite":
        return _qi(table, '"')
    from services.dest_precount import warehouse_sql_table_ref

    return warehouse_sql_table_ref(
        eng, cfg, str(cfg.get("schema") or ""), table
    )


def _generic_key_census(
    *,
    source_endpoint: Any,
    endpoint: Any,
    dest_summary: dict[str, Any] | None,
    mappings: list[dict[str, Any]] | None,
) -> tuple[list[Any], list[Any], list[Any], str | None]:
    """Cross-engine key census: (changed, missing, extra, unavailable_reason).

    Streams ``(pk, row hash)`` from each side into a temp sqlite table and
    joins — usable both to *detect* a value difference on count-only routes
    (X-68) and to *name* culprit keys on a failed run. ``reason`` is the
    human-readable gate hit when the census could not run; empty lists with
    ``reason=None`` mean a clean compare.
    """
    """Cross-engine census — each side's (pk, row hash) is computed in Python
    from streamed rows so engines with different in-engine hash functions
    (MySQL vs DuckDB, MySQL vs Postgres, …) still compare honestly.

    The hash runs ``normalize_cell`` — the same canonicalizer Gate-8 digests
    use — over the mapped destination column order, so a timedalta-vs-time
    or padded-CHAR pair is judged by the same rule the verdict used.
    """
    summary = dest_summary or {}
    pk_cols = [
        str(c)
        for c in (summary.get("primary_key_columns") or [])
        if str(c or "").strip()
    ]
    if len(pk_cols) != 1:
        pk_cols = [
            str(c)
            for c in (summary.get("conflict_columns") or [])
            if str(c or "").strip()
        ]
    pairs = [
        (
            str(m.get("source") or ""),
            str(m.get("target") or m.get("destination") or m.get("source") or ""),
        )
        for m in (mappings or [])
        if isinstance(m, dict) and str(m.get("source") or "").strip()
    ]
    if len(pk_cols) != 1 and pairs:
        # The census needs the key the writer joined on — engine-copy
        # summaries may not carry ``primary_key_columns`` even when the run's
        # mappings declare one (X-42: id was a single-column PK yet the
        # reason text claimed one was required). Recover it the same way the
        # verify ladder does before giving up.
        try:
            from services.primary_key import resolve_identity_key

            _src_pk, _tgt_pk = resolve_identity_key(
                mappings=list(mappings or []),
                source_columns=[s for s, _t in pairs],
                dest_kind=str(getattr(endpoint, "format", "") or ""),
                purpose="uniqueness",
            )
            if _tgt_pk:
                pk_cols = [str(_tgt_pk)]
        except Exception:  # noqa: BLE001 — naming is best-effort, never fatal
            pass
    if len(pk_cols) != 1:
        return [], [], [], (
            "the run recorded no single-column primary key to join the "
            "destination key census on"
        )
    pk = pk_cols[0]
    src_tbl = str(getattr(source_endpoint, "table", "") or "").strip()
    dst_tbl = str(getattr(endpoint, "table", "") or "").strip()
    if not src_tbl or not dst_tbl:
        return [], [], [], (
            "source or destination table name was not recorded for the census"
        )
    if not pairs:
        return [], [], [], "no column mappings were recorded for the census"
    src_pk = next((s for s, t in pairs if t.lower() == pk.lower()), pk)
    src_cols = [s for s, _t in pairs if s.lower() != src_pk.lower()]
    dst_cols = [t for s, t in pairs if s.lower() != src_pk.lower() and str(t or "").strip()]
    if len(src_cols) != len(dst_cols) or not dst_cols:
        return [], [], [], (
            "the recorded column mappings do not pair source and target "
            "columns for the census join"
        )
    try:
        from services.reconciliation import normalize_cell
        from .connector_capabilities import resolve_driver_type

        src_cfg = resolve_connector_config(source_endpoint)
        dst_cfg = resolve_connector_config(endpoint)
        src_eng = resolve_driver_type(
            getattr(source_endpoint, "format", "") or ""
        ) or "unknown"
        dst_eng = resolve_driver_type(endpoint.format or "") or "unknown"
        if src_eng in {"", "generic_sql", "unknown"}:
            src_eng = str(getattr(source_endpoint, "format", "") or "unknown").lower()
        if dst_eng in {"", "generic_sql", "unknown"}:
            dst_eng = str(getattr(endpoint, "format", "") or "unknown").lower()
        src_conn_info = _culprit_census_conn(
            getattr(source_endpoint, "format", "") or "", src_cfg
        )
        dst_conn_info = _culprit_census_conn(endpoint.format or "", dst_cfg)
        if src_conn_info is None or dst_conn_info is None:
            _missing = []
            if src_conn_info is None:
                _missing.append(f"source '{src_eng}'")
            if dst_conn_info is None:
                _missing.append(f"destination '{dst_eng}'")
            return [], [], [], (
                " and ".join(_missing)
                + " has no census driver (postgres/mysql/mssql/duckdb/sqlite/snowflake)"
            )
        changed: list[Any] = []
        missing: list[Any] = []
        extra: list[Any] = []
        fd, path = tempfile.mkstemp(prefix="df_culprits_", suffix=".sqlite3")
        os.close(fd)
        try:
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE k(side INTEGER NOT NULL, pk TEXT, h TEXT)")
            db.execute("CREATE INDEX ki ON k(side, pk)")

            def _load(conn_info, cfg, eng, ref_table, pk_col, cols, side):
                e2, conn = conn_info
                q = '"' if e2 != "mysql" else "`"
                from connectors.sql_identifiers import quote_sql_identifier as _qi

                sel = ", ".join(
                    [_qi(pk_col, q)] + [_qi(c, q) for c in cols]
                )
                stmt = f"SELECT {sel} FROM {ref_table}"  # nosec B608
                try:
                    cur = conn.cursor()
                    cur.execute(stmt)
                    while True:
                        chunk = cur.fetchmany(5000)
                        if not chunk:
                            break
                        db.executemany(
                            "INSERT INTO k(side, pk, h) VALUES (?,?,?)",
                            (
                                (
                                    side,
                                    str(r[0]),
                                    hashlib.md5(
                                        "\x1f".join(
                                            normalize_cell(v, engine=dst_eng)
                                            for v in r[1:]
                                        ).encode("utf-8", "replace"),
                                        usedforsecurity=False,
                                    ).hexdigest(),
                                )
                                for r in chunk
                            ),
                        )
                    db.commit()
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass

            _load(
                src_conn_info,
                src_cfg,
                src_eng,
                _culprit_table_ref(src_eng, src_cfg, src_tbl),
                src_pk,
                src_cols,
                0,
            )
            _load(
                dst_conn_info,
                dst_cfg,
                dst_eng,
                _culprit_table_ref(dst_eng, dst_cfg, dst_tbl),
                pk,
                dst_cols,
                1,
            )
            changed, missing, extra = _census_join(db)
            db.close()
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    except Exception as exc:
        _logger.warning(
            "Gate-8 key census skipped: %s", exc, exc_info=exc
        )
        return [], [], [], (
            "the destination key census failed while reading a side "
            f"({type(exc).__name__}: {exc})"
        )
    return changed, missing, extra, None


def _name_gate8_culprits_generic(
    report: dict[str, Any],
    *,
    source_endpoint: Any,
    endpoint: Any,
    dest_summary: dict[str, Any] | None,
    mappings: list[dict[str, Any]] | None,
) -> None:
    """Name culprit keys on a failed keyed run via :func:`_generic_key_census`."""
    changed, missing, extra, reason = _generic_key_census(
        source_endpoint=source_endpoint,
        endpoint=endpoint,
        dest_summary=dest_summary,
        mappings=mappings,
    )
    if reason is not None:
        report["culprit_keys_unavailable_reason"] = (
            "culprit keys could not be named — " + reason
        )
        return
    _report_culprit_keys(report, changed, missing, extra)


def _attach_match_summary(
    report: dict[str, Any],
    dest_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Say what was compared, how much of it agreed, and what to do about it.

    A reconcile verdict of two hex strings is not proof an operator can use: it
    names no population, no percentage anyone can reproduce, and no next move.
    This stamps the denominators the run actually holds — source population,
    destination population, the rows this pass moved, and the keyed sample's row
    and cell counts — so a match percentage always says what it is a percentage
    *of*, plus an ordered remediation list whose entries map to real controls.

    Nothing here softens a verdict: a failed report stays failed, and a
    percentage taken from a sample is labelled sample evidence, never population
    proof.
    """
    summary = dest_summary or {}
    sample = report.get("sample_compare") or {}
    rows = int(sample.get("rows_compared") or 0)
    cells = int(sample.get("cells_compared") or sample.get("compared") or 0)
    differing = len(list(sample.get("mismatches") or []))
    match: dict[str, Any] = {
        "source_rows": report.get("source_rows"),
        "dest_rows": report.get("target_rows"),
        "dest_rows_before": summary.get(PRECOUNT_KEY),
        "rows_moved_this_run": summary.get("rows_written"),
        "rejected_rows": report.get("rejected_rows"),
        "sample_rows_compared": rows,
        "sample_cells_compared": cells,
        "sample_cells_differing": differing,
        "scope": str(report.get("checksum_scope") or ""),
    }
    if cells:
        agreeing = max(0, cells - differing)
        match["sample_match_percent"] = round(100.0 * agreeing / cells, 4)
        match["denominator"] = (
            f"{cells} cell(s) across {rows} key-aligned row(s) of the read-back "
            "sample — sample evidence, not population proof"
        )
    else:
        match["sample_match_percent"] = None
        match["denominator"] = "no key-aligned cells were comparable in this run"
    report["match_summary"] = match

    if report.get("passed"):
        return report
    actions: list[dict[str, str]] = []
    mismatches = [m for m in (sample.get("mismatches") or []) if isinstance(m, dict)]
    if mismatches:
        first = mismatches[0]
        actions.append({
            "action": "open_map",
            "label": f"Fix the mapping for {first.get('source')} → {first.get('target')}",
            "why": (
                f"{len(mismatches)} sampled cell(s) differ, e.g. source "
                f"{first.get('source_value')!r} vs destination "
                f"{first.get('target_value')!r} — a value that changed on write is "
                "a transform or type decision, not a digest artefact."
            ),
        })
    if report.get("mismatch_class") == "comparison_basis_or_population_scope":
        actions.append({
            "action": "overwrite_or_keyed_resync",
            "label": "Re-run this table with overwrite, or upsert on its key",
            "why": (
                "No sampled cell differs, so the digests disagree on basis or "
                "population, not on data. A full refresh or a keyed merge gives "
                "both sides one comparable population."
            ),
        })
    if int(report.get("rejected_rows") or 0) > 0:
        actions.append({
            "action": "replay_quarantine",
            "label": f"Replay {int(report['rejected_rows']):,} quarantined row(s)",
            "why": "Those rows are absent from the destination until they are replayed.",
        })
    if summary.get("resumed_from") is not None:
        actions.append({
            "action": "resume",
            "label": "Resume from the checkpoint",
            "why": "The pass stopped part-way; resuming moves only the tail.",
        })
    if actions:
        report["remediation"] = actions
    return report


def _engine_digest_enabled() -> bool:
    """Operator gate — default **off** until two gaps are closed.

    The mechanism is proven on its own (see ``test_engine_checksum.py``, and it
    is what caught the truncated-microseconds bug), but reading both populations
    back is not yet safe as the default here:

    * **Snapshot.** A PostgreSQL full refresh reads under REPEATABLE READ, so
      rows inserted while it runs are correctly outside the transfer. Digesting
      the source afterwards on a fresh connection sees them and reports a
      mismatch on a transfer that was right. The digest has to run inside the
      reader's snapshot, not after it.
    * **Scope.** Whole-table digests are meaningless for an append or upsert
      into a destination that already held rows; that case re-scopes the target
      to the written keys further down, and a whole-population source digest
      cannot be compared against it.

    Until both are handled, this stays behind ``DATAFLOW_ENGINE_DIGEST=1`` so it
    can be measured and exercised without deciding any operator's Gate-8 verdict.
    """
    from services.brand_env import getenv_brand

    return (getenv_brand("ENGINE_DIGEST", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _cdc_source_image_gate(dest_summary: dict[str, Any] | None) -> bool:
    """True when Gate-8 must compare dest COUNT to the live source table.

    Changelog event count and last-batch writer checksum are not that
    population. Leftover MERGE stays a no-op on this path.
    """
    summary = dest_summary if isinstance(dest_summary, dict) else {}
    if str(summary.get("checksum_mode") or "") == "cdc_source_image":
        return True
    mode = str(
        summary.get("sync_mode") or summary.get("effective_sync_mode") or ""
    ).strip().lower()
    return mode == "cdc"


def _engine_population_digests(
    *,
    source_endpoint: EndpointConfig | None,
    dest_cfg: dict[str, Any],
    dest_db_type: str,
    dest_schema: str,
    dest_table: str,
    mappings: list[dict],
    source_schema: dict[str, str] | None,
    dest_types: dict[str, str] | None,
) -> tuple[str, str, int] | None:
    """Digest both populations in their own engines, or ``None`` to fall back.

    Gate-8 compares the two sides to each other, so when they render values
    identically the comparison does not need Python to see a single row. That
    is worth reaching for: per-cell fingerprinting is the largest cost in a
    transfer by a wide margin.

    Returns ``None`` — leaving today's path untouched — whenever anything is not
    provably comparable: a different engine, a transform, a declared omission, a
    type that is only nearly the same, or any error while querying. Falling back
    costs time; guessing costs correctness.
    """
    if source_endpoint is None or source_endpoint.kind != "database":
        return None
    from services.engine_checksum import (
        comparable_column_pairs,
        engines_comparable,
        postgresql_engine_checksum,
    )

    from .connector_capabilities import resolve_driver_type

    source_driver = resolve_driver_type(source_endpoint.format or "")
    if not engines_comparable(source_driver, dest_db_type):
        return None
    pairs = comparable_column_pairs(mappings, source_schema, dest_types)
    if not pairs:
        return None

    from services.procedure_source import is_callable_source

    if is_callable_source(source_endpoint):
        # CALL/SELECT extract is not a physical table — engine checksum would
        # probe a fake relation named after the procedure. Row accounting stays
        # on dest COUNT / committed_offset.
        return None

    source_table = source_endpoint.table or source_endpoint.collection or ""
    if not source_table or not dest_table:
        return None

    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import quote_table_ref

    src_cfg = resolve_connector_config(source_endpoint)
    try:
        source_ref = quote_table_ref(
            source_table, source_endpoint.schema or "public", dialect="postgresql"
        )
        dest_ref = quote_table_ref(
            dest_table, dest_schema or "public", dialect="postgresql"
        )
    except Exception:
        return None

    def _digest(cfg: dict[str, Any], table_ref: str, columns: list[str]):
        conn = get_connection(
            host=cfg.get("host", ""),
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                return postgresql_engine_checksum(cur, table_ref, columns)
        finally:
            conn.close()

    try:
        source_digest = _digest(src_cfg, source_ref, [p[0] for p in pairs])
        dest_digest = _digest(dest_cfg, dest_ref, [p[1] for p in pairs])
    except Exception as exc:
        logging.getLogger(__name__).info(
            "Engine population digest unavailable, using row fingerprints: %s", exc
        )
        return None
    if source_digest is None or dest_digest is None:
        return None
    return source_digest.checksum, dest_digest.checksum, dest_digest.row_count


def _reread_dest_engine_digest(
    paired: tuple[str, str, int],
    *,
    db_type: str,
    dest_summary: dict[str, Any] | None,
    cfg: dict[str, Any],
    schema: str,
    table_name: str,
    mappings: list[dict],
) -> tuple[str, str, int] | None:
    """Re-read the destination's engine digest at *verify* time (G-1i).

    The writer's ``engine_target_checksum`` is honest but stale: it was
    computed inside the copy transaction before commit, so post-write
    tampering is invisible to it. For a PostgreSQL destination we can ask the
    engine for the same additive digest over the same mapped columns, read
    now — the source side stays the write-time snapshot digest (that is the
    population that was copied). Returns the same ``(source, target, rows)``
    triple with ``target`` replaced by the verify-time read, or ``None`` when
    the destination cannot be re-read.
    """
    summary = dest_summary or {}
    if db_type != "postgresql":
        return None
    if is_count_proof_token(paired[0]) or is_count_proof_token(paired[1]):
        return None
    target_cols = [
        str(c)
        for c in (
            (summary.get("source_snapshot") or {}).get(
                "engine_digest_target_columns"
            )
            or []
        )
        if str(c or "").strip()
    ]
    if not target_cols:
        target_cols = [
            str(m.get("target") or "")
            for m in mappings
            if isinstance(m, dict) and str(m.get("target") or "").strip()
        ]
    if not target_cols or not table_name:
        return None
    try:
        from connectors.postgresql_conn import get_connection
        from connectors.sql_identifiers import quote_table_ref
        from services.engine_checksum import postgresql_engine_checksum

        dest_ref = quote_table_ref(
            table_name, schema or "public", dialect="postgresql"
        )
        conn = get_connection(
            host=cfg.get("host", ""),
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            with conn.cursor() as cur:
                digest = postgresql_engine_checksum(cur, dest_ref, target_cols)
        finally:
            conn.close()
    except Exception:
        return None
    if digest is None:
        return None
    return paired[0], digest.checksum, int(digest.row_count)


def _unique_check_dialect(db_type: str) -> str | None:
    """Identifier dialect for post-load rule checks, or ``None`` when the
    destination is not a SQL engine the shared warehouse helper serves
    (SQLite/Postgres/MySQL/MariaDB plus every ``warehouse_sql_quote_dialect``
    family: MSSQL, Oracle, Snowflake, BigQuery, DuckDB, Databricks, Redshift)."""
    from services.dest_precount import count_dialect
    from services.dialect_profiles import warehouse_sql_quote_dialect

    db = count_dialect(str(db_type or ""))
    if db == "mariadb":
        db = "mysql"
    if db in {"sqlite", "postgresql", "mysql", "clickhouse"}:
        return db
    return warehouse_sql_quote_dialect(db)


def _count_dest_duplicates(
    dialect: str,
    dest_cfg: dict[str, Any],
    *,
    endpoint: EndpointConfig,
    table: str,
    column: str,
) -> int:
    """``COUNT(*) - COUNT(DISTINCT col) WHERE col IS NOT NULL`` on the dest,
    through the canonical warehouse-SQL engine + table-ref helpers."""
    import sqlalchemy as sa

    from connectors.sql_identifiers import require_safe_identifier
    from services.dest_precount import (
        warehouse_sql_engine,
        warehouse_sql_table_ref,
    )

    table_ref = warehouse_sql_table_ref(
        dialect, dest_cfg, str(endpoint.schema or ""), table
    )
    with warehouse_sql_engine(dialect, dest_cfg) as engine:
        qcol = engine.dialect.identifier_preparer.quote(
            require_safe_identifier(column, preserve_case=True)
        )
        stmt = sa.text(
            f"SELECT COUNT(*) - COUNT(DISTINCT {qcol}) FROM {table_ref} "
            f"WHERE {qcol} IS NOT NULL"
        )
        with engine.connect() as conn:
            return int(conn.execute(stmt).scalar() or 0)


def _run_unique_rule_checks(
    mappings: list[dict] | None,
    *,
    endpoint: EndpointConfig,
) -> list[dict[str, Any]]:
    """Post-load population checks for ``unique`` business-rule contracts.

    Runs once per table at Gate-8 for each mapping carrying a unique contract —
    the writer never sees it (``evaluate_contract`` ignores non-row-local
    types). Entries name the rule and outcome only, never the values.
    """
    contracts: list[tuple[str, dict]] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        target = str(m.get("target") or "").strip()
        if not target:
            continue
        for c in m.get("rule_contracts") or []:
            if isinstance(c, dict) and str(c.get("type") or "") == "unique":
                contracts.append((target, c))
    checks: list[dict[str, Any]] = []
    if not contracts:
        return checks
    from .connector_capabilities import resolve_driver_type

    dest_type = resolve_driver_type(endpoint.format or "")
    dest_cfg = resolve_connector_config(endpoint)
    table = str(endpoint.table or dest_cfg.get("table") or "")
    for target, contract in contracts:
        entry = {
            "rule_id": str(contract.get("rule_id") or ""),
            "type": "unique",
            "column": target,
        }
        dialect = _unique_check_dialect(dest_type)
        if dialect is None or not table:
            checks.append(
                {**entry, "status": "unverified", "reason": "unsupported_destination"}
            )
            continue
        try:
            duplicates = _count_dest_duplicates(
                dialect, dest_cfg, endpoint=endpoint, table=table, column=target
            )
        except Exception as exc:
            # Type name only — the exception text may carry values.
            _logger.warning(
                "unique rule check failed: error_type=%s", type(exc).__name__
            )
            checks.append(
                {**entry, "status": "unverified", "reason": "count_error"}
            )
            continue
        if duplicates:
            checks.append({**entry, "status": "failed", "duplicates": duplicates})
        else:
            checks.append({**entry, "status": "passed"})
    return checks


def _apply_unique_rule_checks(
    payload: dict[str, Any],
    mappings: list[dict] | None,
    *,
    endpoint: EndpointConfig,
    validation_mode: str = "strict",
) -> None:
    """Apply post-load unique-contract outcomes onto a recon payload."""
    rule_checks = _run_unique_rule_checks(mappings, endpoint=endpoint)
    if not rule_checks:
        return
    payload["rule_checks"] = rule_checks
    failed = [c for c in rule_checks if c.get("status") == "failed"]
    if failed:
        payload["passed"] = False
        detail = "; ".join(
            f"business rule {c['rule_id']} failed: "
            f"{c['duplicates']} duplicate value(s) in {c['column']} after load"
            for c in failed
        )
        payload["message"] = f"{payload.get('message') or ''} {detail}".strip()
    unverified = [c for c in rule_checks if c.get("status") == "unverified"]
    if unverified:
        # Same convention as file-export Gate-8: an unverified check must not
        # read as a green pass — unproven, never migration_proven. Strict
        # additionally fails the job (balanced completes, still unproven).
        payload["unverified_rules"] = [
            {"rule_id": c.get("rule_id"), "reason": c.get("reason") or "unverified"}
            for c in unverified
        ]
        payload["unproven"] = True
        payload["migration_proven"] = False
        if str(validation_mode or "").strip().lower() in {"strict", "maximum"}:
            payload["passed"] = False
            details = dict(payload.get("details") or {})
            details["reason"] = "business_rule_unverified"
            payload["details"] = details
        note = (
            f"{len(unverified)} business-rule uniqueness check(s) "
            "unverified for this destination"
        )
        payload["message"] = f"{payload.get('message') or ''} — {note}".strip()


def _count_dest_key_duplicates(
    dialect: str,
    dest_cfg: dict[str, Any],
    *,
    endpoint: EndpointConfig,
    table: str,
    columns: list[str],
) -> int:
    """Number of keys appearing more than once on the destination, through
    the canonical warehouse-SQL engine + table-ref helpers."""
    import sqlalchemy as sa

    from connectors.sql_identifiers import require_safe_identifier
    from services.dest_precount import (
        warehouse_sql_engine,
        warehouse_sql_table_ref,
    )

    table_ref = warehouse_sql_table_ref(
        dialect, dest_cfg, str(endpoint.schema or ""), table
    )
    with warehouse_sql_engine(dialect, dest_cfg) as engine:
        quote = engine.dialect.identifier_preparer.quote
        qcols = ", ".join(
            quote(require_safe_identifier(c, preserve_case=True))
            for c in columns
        )
        stmt = sa.text(
            f"SELECT COUNT(*) FROM (SELECT {qcols} FROM {table_ref} "
            f"GROUP BY {qcols} HAVING COUNT(*) > 1) df_dedupe_dups"
        )
        with engine.connect() as conn:
            return int(conn.execute(stmt).scalar() or 0)


def _run_dedupe_gate_checks(
    report: dict[str, Any],
    ctx: dict[str, Any],
    dest_summary: dict[str, Any],
    sync_mode: str,
) -> list[dict[str, Any]]:
    """B2 Gate-8 checks for recipes holding dedupe steps.

    Ledger balance: a step's rows_in must equal winners out plus losers
    ledgered (diverted + removed) — an unbalanced dedupe ledger means rows
    moved without a named reason. Winner uniqueness: when this run wrote the
    whole destination population and every key column lands on the
    destination, the dest must contain no duplicated key.
    """
    from services.sync_cursor import is_overwrite_sync

    checks: list[dict[str, Any]] = []
    steps = dest_summary.get("shape_dedupe") or []
    out_cols = set(str(c) for c in (dest_summary.get("shape_output_columns") or []))
    db_type = str(ctx.get("engine") or "")
    cfg = ctx.get("cfg") or {}
    table_name = str(ctx.get("table_name") or "")
    schema = str(ctx.get("schema") or "")
    whole_table = is_overwrite_sync(sync_mode) or int(
        dest_summary.get(PRECOUNT_KEY) or 0
    ) == 0
    dialect = _unique_check_dialect(db_type)
    for entry in steps:
        step_no = entry.get("step")
        keys = [str(c) for c in entry.get("keys") or []]
        rule = (
            f"keep={entry.get('keep') or 'first'}"
            + (f" order_by={entry.get('order_by')} {entry.get('order_dir') or 'desc'}"
               if entry.get("order_by") else "")
            + (" merge=non_null" if entry.get("merge") else "")
        )
        # 1. Ledger balance.
        rows_in = int(entry.get("rows_in") or 0)
        winners = int(entry.get("rows_out") or 0)
        losers = int(entry.get("rows_diverted") or 0) + int(
            entry.get("rows_removed") or 0
        )
        if rows_in == winners + losers:
            checks.append(
                {
                    "step": step_no,
                    "check": "ledger_balance",
                    "status": "passed",
                    "detail": f"{rows_in} in = {winners} winner(s) + {losers} loser(s)",
                }
            )
        else:
            checks.append(
                {
                    "step": step_no,
                    "check": "ledger_balance",
                    "status": "failed",
                    "detail": (
                        f"dedupe step {step_no}: {rows_in} row(s) in but "
                        f"{winners} winner(s) + {losers} loser(s) ledgered"
                    ),
                }
            )
        # 2. Winner uniqueness on the destination.
        base = {"step": step_no, "check": "winner_uniqueness", "keys": keys}
        if not keys:
            checks.append({**base, "status": "unverified", "reason": "no keys declared"})
            continue
        missing = [k for k in keys if k not in out_cols]
        if missing:
            checks.append(
                {
                    **base,
                    "status": "unverified",
                    "reason": (
                        f"key column(s) {missing} do not reach the destination "
                        "under their recipe names — cannot verify winner "
                        "uniqueness"
                    ),
                }
            )
            continue
        if not whole_table:
            checks.append(
                {
                    **base,
                    "status": "unverified",
                    "reason": (
                        "destination held prior rows this run did not write — "
                        "dedupe keys are not a load contract, so winner "
                        "uniqueness cannot be isolated to this run's rows"
                    ),
                }
            )
            continue
        if dialect is None or not table_name:
            checks.append(
                {**base, "status": "unverified", "reason": "unsupported_destination"}
            )
            continue
        try:
            endpoint = EndpointConfig(
                format=str(db_type or ""),
                schema=schema,
                table=table_name,
            )
            dups = _count_dest_key_duplicates(
                dialect, cfg, endpoint=endpoint, table=table_name, columns=keys
            )
        except Exception as exc:
            _logger.warning(
                "dedupe winner-uniqueness check failed: error_type=%s",
                type(exc).__name__,
            )
            checks.append(
                {**base, "status": "unverified", "reason": "count_error"}
            )
            continue
        if dups:
            checks.append(
                {
                    **base,
                    "status": "failed",
                    "detail": (
                        f"dedupe step {step_no} ({rule}): {dups} key(s) hold "
                        "more than one winner on the destination"
                    ),
                }
            )
            continue
        checks.append(
            {
                **base,
                "status": "passed",
                "detail": f"{winners} unique winner key(s) under {rule}",
            }
        )
        # 3. Winner ordering: re-derive the extremal ordering tuple per key
        # from an independent source re-read and compare it against the row
        # the destination kept. Uniqueness alone passed DD-1's class — a run
        # could keep '9' over '10' and still verify.
        checks.append(
            _dedupe_winner_ordering_check(
                entry,
                step_no,
                keys,
                rule,
                dialect=dialect,
                dest_cfg=cfg,
                dest_schema=schema,
                dest_table=table_name,
                ctx=ctx,
            )
        )
    return checks


def _dedupe_winner_ordering_check(
    entry: dict[str, Any],
    step_no: Any,
    keys: list[str],
    rule: str,
    *,
    dialect: str | None,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    ctx: dict[str, Any],
) -> dict[str, Any]:
    """Prove the kept winner per key from an independent source re-read.

    Applies to ordering-based rules (order_by/then_by) on whole-table runs:
    the source is queried for each key's extremal ordering tuple under the
    declared directions and the destination's winner must carry that tuple.
    Arrival-based rules (keep=first, bare keep=last) and merge steps (fill
    can legitimately change an ordering column's value on the winner) are
    honestly unverified — SQL cannot express them.
    """
    base = {"step": step_no, "check": "winner_ordering", "keys": keys}
    order_cols: list[tuple[str, str, str]] = []
    order_by = str(entry.get("order_by") or "")
    opt_nulls = str(entry.get("nulls") or "last").casefold()
    if order_by:
        order_cols.append(
            (order_by, str(entry.get("order_dir") or "desc").casefold(), opt_nulls)
        )
    for t in entry.get("then_by") or []:
        if isinstance(t, dict):
            order_cols.append(
                (
                    str(t.get("column") or ""),
                    str(t.get("dir") or "desc").casefold(),
                    str(t.get("nulls") or opt_nulls).casefold(),
                )
            )
        else:
            order_cols.append(
                (str(t), str(entry.get("order_dir") or "desc").casefold(), opt_nulls)
            )
    order_cols = [(c, d, n) for c, d, n in order_cols if c]
    if not order_cols:
        return {
            **base,
            "status": "unverified",
            "reason": (
                "arrival-based rule (keep=first / last-seen) has no SQL-"
                "expressible winner ordering — uniqueness proven, ordering "
                "identity is not"
            ),
        }
    if entry.get("merge"):
        return {
            **base,
            "status": "unverified",
            "reason": (
                "merge=non_null may fill an ordering column on the winner — "
                "the destination tuple is legitimately not the source extremal"
            ),
        }
    src_db = str(ctx.get("source_db_type") or "")
    src_cfg = ctx.get("source_cfg") or {}
    src_schema = str(ctx.get("source_schema") or "")
    src_table = str(ctx.get("source_table") or "")
    src_dialect = _unique_check_dialect(src_db)
    if (
        not src_table
        or src_dialect is None
        or src_dialect == "clickhouse"
        or dialect is None
        or not dest_table
    ):
        return {**base, "status": "unverified", "reason": "unsupported_source_or_dest"}
    order_types = entry.get("order_types") or {}
    key_types = entry.get("key_types") or {}
    if not order_types:
        return {
            **base,
            "status": "unverified",
            "reason": "declared source types for ordering columns unavailable",
        }
    # Destination column names for the source-space key/ordering columns.
    mappings = ctx.get("mappings") or []
    target_of = {
        str(m.get("source") or ""): str(m.get("target") or "")
        for m in mappings
        if isinstance(m, dict) and m.get("source") and m.get("target")
    }
    dest_keys = [target_of.get(k, k) for k in keys]
    dest_ords = [target_of.get(c, c) for c, _, _ in order_cols]
    try:
        expected = _dedupe_source_extrema(
            src_dialect,
            src_cfg,
            schema=src_schema,
            table=src_table,
            keys=keys,
            order_cols=order_cols,
        )
        dest_rows = _dedupe_dest_winner_tuples(
            dialect,
            dest_cfg,
            schema=dest_schema,
            table=dest_table,
            keys=dest_keys,
            order_cols=dest_ords,
        )
    except Exception as exc:
        _logger.warning(
            "dedupe winner-ordering check failed: error_type=%s", type(exc).__name__
        )
        return {**base, "status": "unverified", "reason": "check_error"}

    def _canon(vals: tuple, cols: list[str], types_map: dict) -> tuple:
        from services.shape_engine import _dedupe_sort_val

        return tuple(
            _dedupe_sort_val(v, types_map.get(c)) for v, c in zip(vals, cols)
        )

    def _key_canon(vals: tuple) -> tuple:
        # Keys canonicalize under their declared types when known — an int
        # key and its text form must join; else text.
        from services.shape_engine import _dedupe_sort_val

        return tuple(
            _dedupe_sort_val(v, key_types.get(c)) for v, c in zip(vals, keys)
        )

    expected_map = {
        _key_canon(tuple(erow[: len(keys)])): _canon(
            tuple(erow[len(keys):]),
            [c for c, _, _ in order_cols],
            order_types,
        )
        for erow in expected
    }
    seen: set[tuple] = set()
    mismatches = 0
    missing = 0
    for drow in dest_rows:
        k = _key_canon(drow[: len(keys)])
        seen.add(k)
        exp = expected_map.get(k)
        if exp is None:
            missing += 1
            continue
        got = _canon(drow[len(keys):], [c for c, _, _ in order_cols], order_types)
        if got != exp:
            mismatches += 1
    extra = len(expected_map) - len(expected_map.keys() & seen)
    if mismatches or missing or extra:
        return {
            **base,
            "status": "failed",
            "detail": (
                f"dedupe step {step_no} ({rule}): winner ordering does not "
                f"match an independent source re-read — {mismatches} key(s) "
                f"keep a non-extremal ordering tuple, {missing} dest key(s) "
                f"have no source key, {extra} source key(s) have no winner"
            ),
        }
    return {
        **base,
        "status": "passed",
        "detail": (
            f"{len(seen)} winner(s) match the source's extremal ordering "
            f"tuple per key under {rule} (independent re-read)"
        ),
    }


def _dedupe_source_extrema(
    dialect: str,
    src_cfg: dict[str, Any],
    *,
    schema: str,
    table: str,
    keys: list[str],
    order_cols: list[tuple[str, str, str]],
) -> list[tuple]:
    """Each key's extremal ordering tuple on the source, via ROW_NUMBER.

    NULL ordering mirrors the shape engine's null rule (DD-4): default
    nulls="last" — a NULL never outranks a real value under either
    direction (``(col IS NULL) ASC``); nulls="first" restores SQL NULLS
    FIRST (``(col IS NULL) DESC``). Expressed dialect-agnostically so
    MySQL and MSSQL, which lack NULLS FIRST/LAST, order identically.
    """
    import sqlalchemy as sa

    from connectors.sql_identifiers import require_safe_identifier
    from services.dest_precount import (
        warehouse_sql_engine,
        warehouse_sql_table_ref,
    )

    table_ref = warehouse_sql_table_ref(dialect, src_cfg, schema, table)
    with warehouse_sql_engine(dialect, src_cfg) as engine:
        quote = engine.dialect.identifier_preparer.quote
        qk = ", ".join(
            quote(require_safe_identifier(c, preserve_case=True)) for c in keys
        )
        order_terms = []
        qo_parts = []
        for col, direction, nulls in order_cols:
            qc = quote(require_safe_identifier(col, preserve_case=True))
            qo_parts.append(qc)
            dir_sql = "DESC" if direction == "desc" else "ASC"
            null_sql = "DESC" if nulls == "first" else "ASC"
            order_terms.append(f"({qc} IS NULL) {null_sql}")
            order_terms.append(f"{qc} {dir_sql}")
        qo = ", ".join(qo_parts)
        stmt = sa.text(
            f"SELECT {qk}, {qo} FROM ("
            f"SELECT {qk}, {qo}, ROW_NUMBER() OVER ("
            f"PARTITION BY {qk} ORDER BY {', '.join(order_terms)}"
            f") df_rn FROM {table_ref}"
            f") df_dedupe_ext WHERE df_rn = 1"
        )
        with engine.connect() as conn:
            return [tuple(r) for r in conn.execute(stmt).fetchall()]


def _dedupe_dest_winner_tuples(
    dialect: str,
    dest_cfg: dict[str, Any],
    *,
    schema: str,
    table: str,
    keys: list[str],
    order_cols: list[str],
) -> list[tuple]:
    """The destination's (key, ordering) tuples as they landed."""
    import sqlalchemy as sa

    from connectors.sql_identifiers import require_safe_identifier
    from services.dest_precount import (
        warehouse_sql_engine,
        warehouse_sql_table_ref,
    )

    table_ref = warehouse_sql_table_ref(dialect, dest_cfg, schema, table)
    with warehouse_sql_engine(dialect, dest_cfg) as engine:
        quote = engine.dialect.identifier_preparer.quote
        qcols = ", ".join(
            quote(require_safe_identifier(c, preserve_case=True))
            for c in list(keys) + list(order_cols)
        )
        with engine.connect() as conn:
            return [
                tuple(r)
                for r in conn.execute(sa.text(f"SELECT {qcols} FROM {table_ref}"))
            ]


def _run_reconciliation_impl(
    *,
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    rows_written: int,
    writer_checksum: str,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None = None,
    source_schema: dict[str, str] | None = None,
    validation_mode: str = "strict",
    source_endpoint: EndpointConfig | None = None,
    source_spool: Any = None,
) -> dict[str, Any]:
    """Verify row counts and checksums against the destination."""
    # Destination facts a row checksum cannot prove (generator watermarks today).
    physical_state: dict[str, Any] = {}
    # Where the source digest came from. Held in a box because the early-return
    # paths below run before it is known, and the report must never claim two
    # independent digests agreed when only the writer's was available.
    digest_provenance: dict[str, str] = {"source": "", "source_scope": ""}
    # Late-bound: populated once driver/schema/table are resolved. _finalize
    # is defined before those names exist; file-export returns must not stamp.
    vector_stamp_ctx: dict[str, Any] = {}
    keyset_stamp_ctx: dict[str, Any] = {}
    scd2_stamp_ctx: dict[str, Any] = {}
    dedupe_stamp_ctx: dict[str, Any] = {}
    # G21/G22 — filled after source/dest engines resolve. Early file-export
    # returns still see mappings so a declared control total cannot skip silently.
    n5_ctx: dict[str, Any] = {
        "mappings": list(mappings or []),
        "rejected_rows": int((dest_summary or {}).get("rejected_rows", 0) or 0),
    }
    # Late-bound with the resolved destination engine and physical types: which
    # columns Gate-8 could only prove at the carrier's granularity.
    carrier_ctx: dict[str, Any] = {}
    # Late-bound reader arguments for naming the changed row when only the
    # whole-population checksum disagrees (no census driver on file/object).
    full_compare_ctx: dict[str, Any] = {}

    def _finalize(payload: dict[str, Any]) -> dict[str, Any]:
        if digest_provenance["source"] and "source_checksum_provenance" not in payload:
            payload = {
                **payload,
                "source_checksum_provenance": digest_provenance["source"],
            }
        if digest_provenance["source_scope"] and "source_checksum_scope" not in payload:
            payload = {
                **payload,
                "source_checksum_scope": digest_provenance["source_scope"],
            }
        if digest_provenance.get("dest_read"):
            payload = {
                **payload,
                "dest_digest_timing": digest_provenance["dest_read"],
            }
        # Property 3 — carry source snapshot id onto the reconcile report /
        # migration certificate surface.
        stamped = _finalize_reconcile(payload, dest_summary=dest_summary)
        msg = str(stamped.get("message") or "")
        assurance = str(stamped.get("assurance_level") or "")
        # Writer-ack / empty dest digest must never keep the reconcile() theatre
        # line "Row fidelity verified — source and target checksums match".
        if msg.lower().startswith("row fidelity verified") and assurance not in {
            "full_checksum",
        }:
            rows = stamped.get("target_rows") or stamped.get("source_rows") or 0
            dest_hash = str(stamped.get("target_checksum") or "").strip()
            if assurance == "write_pass_dest_readback" or dest_hash:
                stamped["message"] = (
                    f"Destination read-back matches the write-pass fingerprint "
                    f"({rows} row(s)). Source warehouse was not independently "
                    "re-read — not migration_proven."
                )
            else:
                stamped["message"] = (
                    f"Writer acknowledgment for {rows} row(s) — source digest was "
                    "the write-pass hash, not an independent dest read-back. "
                    "Not migration_proven."
                )
        if vector_stamp_ctx:
            stamped = stamp_vector_census(
                stamped,
                vector_stamp_ctx.get("cfg"),
                schema=str(vector_stamp_ctx.get("schema") or ""),
                table_name=str(vector_stamp_ctx.get("table_name") or ""),
                dest_engine=str(vector_stamp_ctx.get("engine") or ""),
            )
        if keyset_stamp_ctx:
            stamped = stamp_keyset_census(
                stamped,
                keyset_stamp_ctx.get("cfg"),
                schema=str(keyset_stamp_ctx.get("schema") or ""),
                table_name=str(keyset_stamp_ctx.get("table_name") or ""),
                dest_engine=str(keyset_stamp_ctx.get("engine") or ""),
                key_columns=list(keyset_stamp_ctx.get("key_columns") or []),
                keys=keyset_stamp_ctx.get("keys"),
            )
        leftover_n = dest_summary.get("leftover_deleted") if isinstance(dest_summary, dict) else None
        if isinstance(leftover_n, int) and leftover_n >= 0:
            stamped["leftover_deleted"] = leftover_n
        if scd2_stamp_ctx:
            stamped = stamp_scd2_census(
                stamped,
                scd2_stamp_ctx.get("cfg"),
                schema=str(scd2_stamp_ctx.get("schema") or ""),
                table_name=str(scd2_stamp_ctx.get("table_name") or ""),
                dest_engine=str(scd2_stamp_ctx.get("engine") or ""),
                is_current_column=str(
                    scd2_stamp_ctx.get("is_current") or ""
                ),
            )
            _retained_missing = int(
                (dest_summary or {}).get("retained_missing_rows")
                or (
                    (dest_summary or {}).get("scd2") or {}
                ).get("retained_missing_rows")
                or 0
            )
            if _retained_missing:
                # Ledger identity: read + retained == current for
                # close_missing=off runs.
                stamped["retained_missing_rows"] = _retained_missing
                if isinstance(stamped.get("scd2"), dict):
                    stamped["scd2"]["retained_missing_rows"] = (
                        _retained_missing
                    )
            # B1 Gate-8: read the destination history back row by row —
            # at most one is_current per key, contiguous non-overlapping
            # intervals per key, and 1..n versions when declared. A broken
            # history is a failed verification in every mode; a verifier
            # that cannot run is "unproven", never silently skipped.
            try:
                from services.scd2_engine import verify_scd2_history

                _ds = dest_summary if isinstance(dest_summary, dict) else {}
                _pk_cols = list(
                    (_ds.get("scd2") or {}).get("primary_key_columns")
                    or _ds.get("primary_key_columns")
                    or []
                )
                if _pk_cols:
                    _hist = verify_scd2_history(
                        scd2_stamp_ctx.get("cfg"),
                        str(scd2_stamp_ctx.get("schema") or ""),
                        str(scd2_stamp_ctx.get("table_name") or ""),
                        _pk_cols,
                        _ds.get("scd2_opts")
                        or (_ds.get("scd2") or {}).get("scd2_opts"),
                        expected={
                            "inserted": int(
                                _ds.get("scd2_rows_written")
                                or (_ds.get("scd2") or {}).get("rows_written")
                                or _ds.get("rows_written")
                                or 0
                            ),
                            "closed": int(
                                _ds.get("updated_rows")
                                or (_ds.get("scd2") or {}).get("updated_rows")
                                or 0
                            ),
                            "closed_missing": int(
                                _ds.get("closed_missing_rows")
                                or (_ds.get("scd2") or {}).get(
                                    "closed_missing_rows"
                                )
                                or 0
                            ),
                            # Per-run recount window — every batch and the
                            # close pass stamps its own instant.
                            "run_started_at": (
                                _ds.get("run_started_at")
                                or (_ds.get("scd2") or {}).get(
                                    "run_started_at"
                                )
                            ),
                            # Exact stamps this run wrote — recount by
                            # membership, not the whole window, so an
                            # overlapping writer is never counted.
                            "run_stamps": (
                                _ds.get("run_stamps")
                                or (_ds.get("scd2") or {}).get(
                                    "run_stamps"
                                )
                                or []
                            ),
                        },
                    )
                    stamped["scd2_history"] = _hist
                    if _hist.get("ok") is False:
                        stamped["passed"] = False
                        if _hist.get("unproven"):
                            stamped["unproven"] = True
                            stamped["message"] = (
                                "SCD2 history verification unproven: "
                                + "; ".join(_hist.get("violations") or [])
                            )
                        else:
                            stamped["verification_verdict"] = "failed"
                            stamped["message"] = (
                                "SCD2 history verification failed: "
                                + "; ".join(_hist.get("violations") or [])
                            )
                else:
                    stamped["passed"] = False
                    stamped["unproven"] = True
                    stamped["message"] = (
                        "SCD2 history verification unproven: no primary "
                        "key columns recorded for the run."
                    )
            except Exception as exc:  # noqa: BLE001
                stamped["passed"] = False
                stamped["unproven"] = True
                stamped["message"] = (
                    f"SCD2 history verification unproven: {exc}"
                )
        if dedupe_stamp_ctx:
            # B2 Gate-8: every dedupe step must leave its winner set unique on
            # the declared keys, and its ledger must balance — rows in equal
            # winners out plus losers ledgered to quarantine. A violation is a
            # failed verification in every mode; a check that cannot run is
            # "unproven", never silently skipped.
            try:
                _ded = _run_dedupe_gate_checks(
                    stamped,
                    dedupe_stamp_ctx,
                    dest_summary if isinstance(dest_summary, dict) else {},
                    str(
                        dest_summary.get("sync_mode")
                        or dest_summary.get("effective_sync_mode")
                        or ""
                    ),
                )
                stamped["dedupe_checks"] = _ded
                _ded_fail = [c for c in _ded if c.get("status") == "failed"]
                _ded_unv = [c for c in _ded if c.get("status") == "unverified"]
                if _ded_fail:
                    stamped["passed"] = False
                    stamped["verification_verdict"] = "failed"
                    stamped["message"] = (
                        "Dedupe verification failed: "
                        + "; ".join(
                            str(c.get("detail") or c.get("reason") or "")
                            for c in _ded_fail
                        )
                    )
                if _ded_unv:
                    # DD-5/DD-6: an unverifiable winner-ordering identity
                    # does not unmeasure the destination COUNT(*) or the
                    # checksums — those stay independently proven. The
                    # verdict is partial and the message LEADS with the
                    # unverified aspect; nothing reads "verified".
                    stamped["verification_verdict"] = "partial"
                    _prior = (stamped.get("message") or "").strip()
                    stamped["message"] = "Dedupe winners unverified: " + (
                        "; ".join(
                            str(c.get("reason") or "")
                            for c in _ded_unv
                        )
                    ) + (f" — {_prior}" if _prior else "")
            except Exception as exc:  # noqa: BLE001
                stamped["verification_verdict"] = "partial"
                _prior = (stamped.get("message") or "").strip()
                stamped["message"] = (
                    f"Dedupe winners unverified: check error {exc}"
                    + (f" — {_prior}" if _prior else "")
                )
        # Property 5 — attach L1–L5 ladder when both populations are available.
        try:
            stamped = _maybe_attach_verification_ladder(
                stamped,
                endpoint=endpoint,
                source_endpoint=source_endpoint,
                records=records,
                columns=columns,
                dest_summary=dest_summary,
                mappings=mappings or [],
                validation_mode=validation_mode,
            )
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "verification ladder skipped: %s", exc, exc_info=exc
            )
        if physical_state:
            stamped["physical_state"] = dict(physical_state)
        stamped = _apply_n5_gate8_extensions(stamped, n5_ctx)
        rounded = list(carrier_ctx.get("rounded_columns") or [])
        if rounded:
            # Both digests were taken at the destination carrier's granularity,
            # so a match here proves every cell the destination *can* hold. The
            # fractional seconds it cannot hold are a declared narrowing, and
            # saying so is the difference between honest proof and a green that
            # over-claims.
            stamped["carrier_rounded_columns"] = rounded
            named = ", ".join(
                f"{c.get('column')} {c.get('source_type')} → {c.get('target_type')}"
                for c in rounded[:3]
            )
            more = f" (+{len(rounded) - 3} more)" if len(rounded) > 3 else ""
            note = (
                f" Instants on {len(rounded)} column(s) were compared at the "
                f"destination carrier's granularity — {named}{more} — because "
                "that column cannot store the source's fractional seconds; "
                "those dropped digits are a declared narrowing, not proven "
                "cell fidelity."
            )
            stamped["message"] = f"{str(stamped.get('message') or '').rstrip()}{note}"
        stamped = _name_row_on_checksum_only_failure(stamped, full_compare_ctx)
        stamped = _localize_checksum_mismatch(stamped, dest_summary)
        stamped = qualify_multi_stream_reconciliation(stamped, dest_summary)
        stamped = _attach_match_summary(stamped, dest_summary)
        if (
            stamped.get("passed")
            and not stamped.get("unproven")
            and not stamped.get("verification_verdict")
        ):
            # V-1: a fully proven run carries an explicit structured verdict —
            # only failed / partial / unproven runs may differ. A count-only
            # engine copy proves counts, not values: with no real digest it
            # stamps "partial", never a green "verified" (X-41 honesty ask).
            # X-41b: a one-sided digest proves nothing — the count-only
            # engine copy carries a real read-back digest on target_checksum
            # with NO source digest and checksum_match=None, so the old
            # "has a digest" test stamped "verified" over a run that never
            # compared values. Verified requires a measured match on real
            # digests (count tokens on either side are not value proof).
            stamped["verification_verdict"] = (
                "verified" if _report_has_proven_value_match(stamped) else "partial"
            )
        # X-57 tail: a failed reconcile must never ship a null verdict —
        # "failed" is the explicit answer (unproven holds stay unproven).
        if (
            stamped.get("passed") is False
            and not stamped.get("unproven")
            and not stamped.get("verification_verdict")
        ):
            stamped["verification_verdict"] = "failed"
        return stamped

    rejected_rows = int(dest_summary.get("rejected_rows", 0) or 0)
    coerced_null_rows = int(dest_summary.get("coerced_null_rows", 0) or 0)
    rows_skipped = int(dest_summary.get("rows_skipped", 0) or 0)
    # Rows an approved shaping recipe removed on the read. They were read — so
    # they belong to the source population this run counted — and they are
    # deliberately not at the destination, so conservation has to name them
    # instead of reading their absence as short delivery.
    rows_shaped_out = int(dest_summary.get("rows_shaped_out", 0) or 0)
    rows_expanded = int(dest_summary.get("rows_expanded", 0) or 0)
    # Rows a declared source filter removed on the read. The source population
    # this run counted includes them (the source paged and counted them), so the
    # filter has to be stated here for the same reason a recipe does.
    rows_source_filtered = int(dest_summary.get("rows_source_filtered", 0) or 0)
    # The streaming reader counts every row it removed on the read — a declared
    # filter's and a recipe's — for committed pages only, and cumulatively across
    # resumes. When it reports that, it is the authoritative removal total: the
    # recipe's own tally covers this pass alone.
    rows_removed_on_read = int(dest_summary.get("rows_removed_on_read", 0) or 0)
    if (
        "rows_source_filtered" not in dest_summary
        and rows_removed_on_read > rows_shaped_out + rows_source_filtered
    ):
        rows_source_filtered = rows_removed_on_read - rows_shaped_out
    # A resumed pass reads and writes only the tail of the population, while the
    # destination read-back is always full-table. Comparing the two directly
    # reports a mismatch on data that is correct, so the resumed slice must be
    # widened back to the whole population before anything is compared.
    resumed_from = _as_count(dest_summary.get("resumed_from"))
    resume_full_source_rows = _as_count(dest_summary.get("resume_full_source_rows"))
    # Coerced rows are KEPT (a cell became NULL); quarantine hold-outs are absent
    # from the destination. Skipped rows (e.g. stale CDC LSN) are not written.
    dropped_rows = max(rejected_rows - coerced_null_rows, 0)
    # Prefer independent source accounting when the caller provides it
    # (streaming paths should pass source_row_count from the read side).
    source_row_count = dest_summary.get("source_row_count")
    if isinstance(source_row_count, int) and source_row_count >= 0:
        source_rows = source_row_count
    elif records:
        source_rows = len(records)
    elif endpoint.kind != "database":
        # File/object exports have no cell-fidelity Gate-8; writer ack is enough
        # for an *unproven* operational pass below. Do not invent conservation.
        source_rows = int(rows_written or 0) + dropped_rows + rows_skipped
    else:
        # Never invent source_rows from writer ack on database destinations —
        # that circularly balances short reads. Fail closed until the reader
        # stamps a population count.
        return _finalize({
            "passed": False,
            "unproven": True,
            "migration_proven": False,
            "message": (
                "Source row count unmeasured — Gate-8 refuses conservation "
                "invented from writer acknowledgements alone."
            ),
            "source_rows": None,
            "target_rows": rows_written,
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
            "details": {
                "reason": "source_row_count_unmeasured",
                "source_row_count_source": dest_summary.get("source_row_count_source"),
            },
        })
    if resume_full_source_rows:
        source_rows = resume_full_source_rows
    elif resumed_from:
        source_rows += resumed_from
    expected_written = max(
        source_rows
        + rows_expanded
        - dropped_rows
        - rows_skipped
        - rows_shaped_out
        - rows_source_filtered,
        0,
    )
    # Destinations with no read-back are accounted from writer counts, so rows a
    # previous pass already committed have to be added back or a correct resume
    # reads as short delivery.
    rows_written_accounted = rows_written + resumed_from

    if endpoint.kind != "database":
        # Object/file exports have no destination cell read-back. Writer checksum
        # proves bytes landed, not per-cell fidelity. Operational write may pass;
        # never stamp migration_proven / cell-fidelity Gate-8 green.
        # Independent artifact COUNT (re-open the file) is dest cardinality —
        # writer ``rows_written`` never closes conservation.
        checksum = str(writer_checksum or dest_summary.get("checksum") or "").strip()
        payload = stamp_artifact_census(
            {
                "passed": True,
                "unproven": True,
                "skipped_readback": True,
                "migration_proven": False,
                "message": (
                    "File/object export wrote successfully — Gate-8 cell fidelity "
                    "unproven (no destination read-back). "
                    + (
                        f"Writer checksum present ({checksum[:16]}…) — count/bytes only."
                        if checksum
                        else "No writer checksum; treat as operational pass only."
                    )
                ),
                "source_rows": source_rows,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "checksum": checksum,
            },
            dest_summary,
            fmt=endpoint.format,
        )
        counted = payload.get("artifact_row_count")
        if isinstance(counted, int) and counted >= 0:
            payload["message"] = (
                "File/object export wrote successfully — Gate-8 cell fidelity "
                "unproven (no destination cell read-back). Independent artifact "
                f"record count is {counted:,}. Writer acknowledgement is diagnostic."
            )
        cell_compare = _file_export_cell_readback(
            records, columns, mappings, dest_summary, fmt=endpoint.format
        )
        if cell_compare is not None:
            payload["artifact_cell_compare"] = cell_compare
            if not cell_compare.get("passed", True):
                payload["passed"] = False
                payload["message"] = (
                    "Read-back verification failed on the written file "
                    f"{os.path.basename(str(dest_summary.get('path') or ''))!r}: "
                    + _cell_mismatch_text(cell_compare)
                )
        return _finalize(payload)

    from .connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    # Prefer canonical driver (amazon_s3→s3) while preserving catalog type on cfg.
    db_type = resolve_driver_type(
        str(cfg.get("type") or endpoint.format or "")
    ).lower()
    from services.dialect_profiles import schema_from_cfg

    schema = dest_summary.get("schema") or schema_from_cfg(db_type, cfg)
    table_name = (
        dest_summary.get("table")
        or dest_summary.get("prefix")
        or dest_summary.get("index")
        or endpoint.table
        or endpoint.collection
        or ""
    )
    if db_type in VECTOR_IDENTITY_ENGINES:
        vector_stamp_ctx.update(
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            engine=db_type,
        )
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(
        str(
            dest_summary.get("sync_mode")
            or dest_summary.get("effective_sync_mode")
            or ""
        ),
        default="",
    ) == "scd2":
        scd2_stamp_ctx.update(
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            engine=db_type,
            is_current=str(
                (dest_summary.get("scd2_opts") or {}).get("is_current")
                or ""
            ),
        )

    if isinstance(dest_summary, dict) and dest_summary.get("shape_dedupe"):
        dedupe_stamp_ctx.update(
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            engine=db_type,
        )

    mapping_dicts = mappings or [{"source": col, "target": col} for col in columns]
    dest_types = _dest_types_from_mappings(mapping_dicts)
    n5_ctx["mappings"] = [dict(m) for m in mapping_dicts if isinstance(m, dict)]
    n5_ctx["dest_db_type"] = db_type
    n5_ctx["dest_cfg"] = dict(cfg)
    n5_ctx["dest_schema"] = str(schema or "")
    n5_ctx["dest_table"] = str(table_name or "")
    n5_ctx["rejected_rows"] = rejected_rows
    n5_ctx["orphan_fk_rows"] = int(
        (dest_summary or {}).get("orphan_fk_rows") or 0
    ) if isinstance(dest_summary, dict) else 0
    if source_endpoint is not None and source_endpoint.kind == "database":
        src_cfg = resolve_connector_config(source_endpoint)
        src_type = resolve_driver_type(
            str(src_cfg.get("type") or source_endpoint.format or "")
        ).lower()
        from services.dialect_profiles import schema_from_cfg as _n5_schema_from_cfg

        n5_ctx["source_cfg"] = dict(src_cfg)
        n5_ctx["source_db_type"] = src_type
        n5_ctx["source_schema"] = str(
            _n5_schema_from_cfg(src_type, src_cfg) or source_endpoint.schema or ""
        )
        n5_ctx["source_table"] = str(
            source_endpoint.table or src_cfg.get("table") or ""
        )
        # The dedupe winner-ordering proof re-reads the source independently —
        # it needs the same coordinates plus the mapping names.
        dedupe_stamp_ctx.update(
            source_cfg=dict(src_cfg),
            source_db_type=src_type,
            source_schema=n5_ctx["source_schema"],
            source_table=n5_ctx["source_table"],
            mappings=n5_ctx["mappings"],
        )
    # The write pass already picked the type basis it fingerprinted on
    # (mapping/logical types overlaid with the physical carrier) and stamps it
    # as gate8_digest_types — the read-back must hash against the same basis
    # or a correct load mismatches in both directions (TEXT carrier degrades
    # and source-declared overrides alike).
    stamped_digest_types = dest_summary.get("gate8_digest_types")
    if isinstance(stamped_digest_types, dict) and stamped_digest_types:
        dest_types = {
            str(k): str(v) for k, v in stamped_digest_types.items() if v
        }
    # Prefer physical types stamped by the writer when present.
    physical = dest_summary.get("column_types") or dest_summary.get("target_types")
    dest_types = overlay_physical_dest_types(dest_types, physical)
    # The plan's target_type is Map's intent; the carrier the rows landed in is
    # what the digests must be taken against. A pre-existing destination column
    # contradicts the plan (declared DATETIME(6), physical datetime) and hashing
    # the plan's precision failed correct loads with two opaque hashes.
    if not physical and table_name and endpoint.kind == "database":
        try:
            from services.dest_physical_types import (
                apply_physical_temporal_precision,
            )

            dest_types = apply_physical_temporal_precision(
                dest_types, db_type, cfg, table=str(table_name)
            )
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "physical destination temporal precision unavailable: %s",
                exc,
                exc_info=exc,
            )
    if source_schema:
        try:
            from services.transform_resolver import attach_transforms_to_mappings

            mapping_dicts = attach_transforms_to_mappings(
                mapping_dicts,
                column_types=source_schema,
                dest_types=dest_types,
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    try:
        from services.carrier_instant import carrier_rounded_columns

        carrier_ctx["rounded_columns"] = carrier_rounded_columns(
            mapping_dicts,
            source_schema=source_schema,
            dest_types=dest_types,
            dest_engine=db_type,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "carrier granularity disclosure skipped: %s", exc, exc_info=exc
        )

    target_cols = _mapped_targets(mapping_dicts, columns)
    pk_cols = list(
        dest_summary.get("primary_key_columns")
        or dest_summary.get("conflict_columns")
        or []
    )
    # Whether the *destination* stands behind this key — a declared PK/unique
    # constraint — as opposed to a key named by the stream contract or inferred
    # by Map. Naming a key is not enforcing it: a contract primary_key on an
    # append-only write leaves a keyless table free to hold the same key twice,
    # so it may align a sample but must never scope a digest. Resolved from the
    # destination catalog at the append branch below, where it is used.
    # Quarantine replay / upsert writers may stamp written_ids without PK meta.
    # Resolve identity from Map so keyed Gate-8 can re-scope the target digest.
    if not pk_cols and mapping_dicts:
        try:
            from services.primary_key import resolve_identity_key

            _src_pk, tgt_pk = resolve_identity_key(
                mappings=mapping_dicts,
                source_columns=list(columns or []),
                dest_kind=str(db_type or endpoint.format or ""),
                validation_mode=validation_mode,
                purpose="uniqueness",
            )
            if tgt_pk:
                pk_cols = [str(tgt_pk)]
                dest_summary.setdefault("primary_key_columns", list(pk_cols))
                dest_summary.setdefault("conflict_columns", list(pk_cols))
        except Exception as exc:
            logging.getLogger(__name__).debug(
                "Gate-8 identity resolve skipped: %s", exc, exc_info=exc
            )
    # Complete overwrite snapshot + dest PK: split MISSING_TARGET from
    # EXTRA_TARGET. Incremental CDC must not pass a batch as S (that would
    # invent leftover dest keys and look like inferred deletes).
    from services.sync_cursor import is_overwrite_sync

    key_tuples = None
    if (
        is_overwrite_sync(
            str(
                dest_summary.get("sync_mode")
                or dest_summary.get("effective_sync_mode")
                or ""
            )
        )
        and pk_cols
        and db_type
        not in {"pgvector", "pinecone", "qdrant", "weaviate", "milvus", "email"}
        and isinstance(source_rows, int)
        and not resumed_from
    ):
        if records and source_rows == len(records):
            key_tuples = records_to_key_tuples(
                records, [str(c) for c in pk_cols], mapping_dicts
            )
        else:
            stamped = dest_summary.get(OVERWRITE_SOURCE_KEYS_KEY)
            if isinstance(stamped, list) and len(stamped) == source_rows:
                try:
                    key_tuples = [tuple(t) for t in stamped]
                except TypeError:
                    key_tuples = None
        if key_tuples:
            # Complete overwrite snapshot MERGE: hard-DELETE dest keys not
            # in S *before* dest COUNT / checksum. Incremental CDC never
            # enters this gate. Measure-without-apply is DMS EXTRA_TARGET;
            # apply-without-this-gate is Airbyte issue #6383.
            try:
                from services.row_conservation import apply_inferred_leftover_deletes

                _removed_keys: list[tuple] = []
                deleted = apply_inferred_leftover_deletes(
                    db_type=db_type,
                    cfg=_leftover_cfg_protecting_run_objects(
                        db_type, cfg, dest_summary, schema=str(schema or ""),
                        table_name=str(table_name or ""),
                    ),
                    schema=str(schema or ""),
                    table_name=str(table_name or ""),
                    key_columns=[str(c) for c in pk_cols],
                    keys=key_tuples,
                    complete_snapshot=True,
                    sync_mode=str(
                        dest_summary.get("sync_mode")
                        or dest_summary.get("effective_sync_mode")
                        or "full_refresh_overwrite"
                    ),
                    leftover_out=_removed_keys,
                )
                if deleted:
                    dest_summary["leftover_deleted"] = int(deleted)
                    if _removed_keys:
                        dest_summary["leftover_deleted_keys"] = [
                            "\x1f".join(str(p) for p in t)
                            for t in _removed_keys[:32]
                        ]
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "Overwrite leftover MERGE skipped: %s", exc, exc_info=exc
                )
            keyset_stamp_ctx.update(
                cfg=cfg,
                schema=schema,
                table_name=table_name,
                engine=db_type,
                key_columns=[str(c) for c in pk_cols],
                keys=key_tuples,
            )
    try:
        _probe_pk_cols = [str(c) for c in pk_cols if c]
        _route_probe = [
            (str(_rn), str(_rinfo.get("table") or ""), dict(_rinfo.get("map") or {}))
            for _rn, _rinfo in sorted(
                (dest_summary.get("routes") or {}).items()
            )
            if isinstance(_rinfo, dict) and _rinfo.get("table")
        ]
        if _route_probe:
            # A routed run never writes the base table — probe each route
            # table, translating source pk names through that route's map
            # (id -> cust_id). Probing the base name logs a false warning.
            _route_states: dict[str, Any] = {}
            for _rn2, _rtable2, _rmap2 in _route_probe:
                _pk2 = [
                    str(_rmap2.get(c) or c)
                    for c in _probe_pk_cols
                ]
                _st2 = _identity_watermark_evidence(
                    db_type=db_type,
                    cfg=cfg,
                    schema=str(schema or ""),
                    table=_rtable2,
                    pk_cols=_pk2,
                )
                if _st2:
                    _route_states[_rn2] = _st2
            if _route_states:
                physical_state["identity_watermark"] = {
                    "supported": True,
                    "verified": all(
                        bool(s.get("verified")) for s in _route_states.values()
                    ),
                    "routes": _route_states,
                }
        else:
            identity_state = _identity_watermark_evidence(
                db_type=db_type,
                cfg=cfg,
                schema=str(schema or ""),
                table=str(table_name or ""),
                pk_cols=_probe_pk_cols,
            )
            if identity_state:
                physical_state["identity_watermark"] = identity_state
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "identity watermark verification skipped: %s", exc, exc_info=exc
        )
        physical_state["identity_watermark"] = {
            "supported": True,
            "verified": False,
            "reason": f"probe failed: {exc}",
        }

    try:
        schema_state = _schema_state_evidence(
            source_endpoint=source_endpoint,
            db_type=db_type,
            cfg=cfg,
            schema=str(schema or ""),
            table=str(table_name or ""),
        )
        if schema_state:
            physical_state["schema_objects"] = schema_state
            keys, unparsed = _source_foreign_keys(schema_state)
            n5_ctx["source_has_fks"] = bool(keys or unparsed)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "physical schema comparison skipped: %s", exc, exc_info=exc
        )
        schema_state = {}
        physical_state["schema_objects"] = {
            "verified": False,
            "reason": f"comparison failed: {exc}",
        }

    try:
        ri_source_schema = ""
        if source_endpoint is not None and source_endpoint.kind == "database":
            from services.dialect_profiles import schema_from_cfg

            from .connector_capabilities import resolve_driver_type

            ri_src_cfg = resolve_connector_config(source_endpoint)
            ri_src_type = resolve_driver_type(
                str(ri_src_cfg.get("type") or source_endpoint.format or "")
            ).lower()
            ri_source_schema = str(schema_from_cfg(ri_src_type, ri_src_cfg) or "")
        from .stream_foreign_keys import fk_route_policy

        if fk_route_policy(endpoint) == "ignore":
            # The operator chose foreign_keys=ignore: rows copied as read, no
            # parent relationship promised at the destination (round 11).
            ri_state = {
                "verified": False,
                "asked": False,
                "reason": "foreign_keys=ignore on this route",
                "relations": [],
                "orphan_rows": 0,
            }
        else:
            ri_state = _referential_integrity_evidence(
                db_type=db_type,
                cfg=cfg,
                schema=str(schema or ""),
                table=str(table_name or ""),
                schema_state=schema_state,
                source_schema=ri_source_schema,
            )
        if int(ri_state.get("orphan_rows") or 0) > 0 and source_endpoint is not None:
            _classify_ri_anomaly_origin(ri_state, source_endpoint, schema_state)
        physical_state["referential_integrity"] = ri_state
        n5_ctx["source_has_fks"] = bool(
            ri_state.get("asked") or ri_state.get("relations")
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "destination referential integrity probe failed: %s", exc, exc_info=exc
        )
        # A probe that raised proves nothing either way; G22 must not read it
        # as "no foreign keys declared".
        physical_state["referential_integrity"] = {
            "verified": False,
            "asked": True,
            "probe_error": True,
            "reason": f"probe failed: {exc}",
        }
        n5_ctx["source_has_fks"] = True

    # The writer digest of a resumed pass covers the tail it wrote, not the
    # population. Recompute from the full source when the caller re-supplied it;
    # otherwise leave it empty and decline the comparison further down rather
    # than compare two different scopes.
    source_checksum_scope_note = ""
    source_checksum_scope_label = "after resume"
    if resumed_from and not (
        resume_full_source_rows and (records or source_spool is not None)
    ):
        source_checksum_scope_note = (
            f"Resumed after {resumed_from:,} previously committed row(s): this pass "
            "read only the remaining slice, so no source digest covering the whole "
            "population is available to compare against the full-table destination "
            "digest."
        ) + " Re-run without resume for full_checksum population proof."
    elif _engine_count_proof_only(dest_summary) is not None:
        source_checksum_scope_label = "by engine copy"
        counted = (
            "the destination key join against the staged population"
            if _keyed_join_proof(dest_summary)
            else "destination COUNT(*) against the source snapshot COUNT(*)"
        )
        source_checksum_scope_note = (
            "The copy ran inside the engines: no row entered this process, so the "
            f"only proof the route produced is {counted}. There is no source value "
            "digest to compare against a destination one. Set "
            "DATAFLOW_ENGINE_DIGEST=1 to digest both populations in-engine for "
            "full_checksum proof."
        )

    source_checksum_provenance = ""
    if source_checksum_scope_note:
        source_checksum = ""
    elif _cdc_source_image_gate(dest_summary):
        # Changelog last-batch writer ack is not the live source table.
        # Engine digest (below) is the comparable population digest.
        source_checksum = ""
        source_checksum_provenance = ""
    elif str(dest_summary.get("checksum_mode") or "") == "inline_write_pass" and writer_checksum:
        # Phase F1 fingerprints are remapped source rows hashed during the write,
        # not the destination writer's ack copied onto both sides.
        source_checksum = str(writer_checksum)
        source_checksum_provenance = SOURCE_DIGEST_WRITE_PASS
        digest_provenance["source"] = source_checksum_provenance
    elif (
        str(dest_summary.get("checksum_mode") or "") == "source_reread"
        and writer_checksum
    ):
        # Second warehouse scan after the write. Streaming hands records=[] so
        # _compute_source_checksum would otherwise stamp writer_ack on this
        # digest and refuse full_checksum — the hole that kept Snowflake→Postgres
        # at Trust 86 / writer-ack after an independent re-read.
        source_checksum = str(writer_checksum)
        source_checksum_provenance = SOURCE_DIGEST_SOURCE_REREAD
        digest_provenance["source"] = source_checksum_provenance
    else:
        digest_records = records
        if records and pk_cols and dest_summary.get(CENSUS_KEY):
            # The keyed upsert hard-DELETEd tombstoned keys, so the destination
            # holds the live population only. Hashing the tombstoned rows here
            # compares two different scopes and fails a correct run — same owner
            # the write path strips with.
            digest_records, tombstones_excluded = live_records_for_digest(
                records,
                key_columns=[str(c) for c in pk_cols if c],
                mappings=mapping_dicts,
            )
            if tombstones_excluded:
                digest_provenance["source_scope"] = (
                    f"live_population_excluding_{tombstones_excluded}_tombstones"
                )
        from services.primary_key import sync_collapses_in_file_duplicates

        if (
            digest_records
            and pk_cols
            and sync_collapses_in_file_duplicates(
                str(dest_summary.get("sync_mode") or dest_summary.get("effective_sync_mode") or "")
            )
        ):
            from services.duplicate_policy import collapse_keep_last

            source_keys: list[str] = []
            for col in pk_cols:
                src_name = str(col)
                for item in mapping_dicts:
                    if str(item.get("target") or "") == str(col) and item.get("source"):
                        src_name = str(item.get("source"))
                        break
                source_keys.append(src_name)
            digest_records, collapsed_n = collapse_keep_last(
                list(digest_records), source_keys
            )
            if collapsed_n:
                digest_provenance["source_scope"] = (
                    f"last_wins_excluding_{collapsed_n}_in_file_duplicates"
                )
        source_checksum, source_checksum_provenance = _compute_source_checksum(
            digest_records,
            columns,
            mapping_dicts,
            source_schema,
            # A resumed writer digest covers the tail only; recompute from the
            # full population the caller re-supplied.
            "" if resumed_from else writer_checksum,
            target_cols=target_cols,
            dest_db_type=db_type,
            dest_types=dest_types,
            validation_mode=validation_mode,
            destination_pk_columns=[str(c) for c in pk_cols if c] or None,
            source_spool=source_spool,
        )
        digest_provenance["source"] = source_checksum_provenance

    # Mirror (inferred-delete) and SCD2 transfers already compute an active-row
    # checksum while applying history/soft deletes; use it directly so closed or
    # deleted rows do not fail strict reconciliation. The streaming staging path
    # surfaces these at the top level; the buffered database path nests them
    # under the "scd2"/"mirror" keys.
    active_checksum = (dest_summary or {}).get("active_checksum")
    active_rows = (dest_summary or {}).get("active_rows") if active_checksum else None
    if not active_checksum:
        for sub_key in ("mirror", "scd2"):
            sub_summary = (dest_summary or {}).get(sub_key)
            if sub_summary and sub_summary.get("active_checksum"):
                active_rows = sub_summary.get("active_rows")
                active_checksum = sub_summary["active_checksum"]
                break
    if active_checksum and source_checksum_scope_note:
        return _finalize(
            _merge_cardinality_only_report(
                source_rows=source_rows,
                active_rows=int(active_rows or 0),
                active_checksum=str(active_checksum),
                rejected_rows=rejected_rows,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                note=source_checksum_scope_note,
            )
        )
    if active_checksum:
        # B1-E5 — close_missing=False retains deleted keys as current: they
        # are real target rows the run never wrote. Expected-current must
        # count them, and the comparable digest covers only current rows
        # keyed by this run's source.
        _retained = int(
            (dest_summary or {}).get("retained_missing_rows")
            or ((dest_summary or {}).get("scd2") or {}).get(
                "retained_missing_rows"
            )
            or 0
        )
        _scoped = (
            (dest_summary or {}).get("source_scoped_checksum")
            or ((dest_summary or {}).get("scd2") or {}).get(
                "source_scoped_checksum"
            )
        )
        _snapshot_digest = (
            (dest_summary or {}).get("source_snapshot_checksum")
            or ((dest_summary or {}).get("scd2") or {}).get(
                "source_snapshot_checksum"
            )
        )
        _scoped_pair = bool(
            _retained and _scoped and _snapshot_digest
        )
        report = reconcile(
            source_rows=source_rows,
            target_rows=int(active_rows or 0),
            source_checksum=(
                str(_snapshot_digest) if _scoped_pair else source_checksum
            ),
            target_checksum=(
                str(_scoped) if _scoped_pair else str(active_checksum)
            ),
            rejected_rows=rejected_rows,
            strict_checksum=True,
            allow_extra_rows=False,
            sample_compare=None,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            rows_shaped_out=rows_shaped_out,
            rows_source_filtered=rows_source_filtered,
            rows_expanded=rows_expanded,
            retained_rows=_retained,
        )
        out = report.to_dict()
        if _retained:
            if _scoped_pair and bool(out.get("checksum_match")):
                # B1-E5a: the digest covered source-keyed rows only — name
                # that population, not the full current count.
                _scoped_rows = int(
                    (dest_summary or {}).get("source_scoped_rows")
                    or ((dest_summary or {}).get("scd2") or {}).get(
                        "source_scoped_rows"
                    )
                    or source_rows
                )
                out["message"] = (
                    "Row fidelity verified — source and target checksums "
                    f"match ({_scoped_rows} source-keyed rows; "
                    f"{_retained} retained key(s) kept current — "
                    "close_missing=off)"
                )
                out["source_checksum_provenance"] = (
                    "staging_snapshot_digest"
                )
            else:
                out["message"] = (
                    str(out.get("message") or "")
                    + f" ({_retained} retained deleted key(s) kept current — "
                    "close_missing=off"
                    + (
                        "; digest covers source-keyed rows only)"
                        if _scoped_pair
                        else ")"
                    )
                ).strip()
                if _scoped_pair:
                    out["source_checksum_provenance"] = (
                        "staging_snapshot_digest"
                    )
        return _finalize(out)

    # Request a real read-back; if the verifier is unavailable we will detect
    # the negative row count and surface a softer "writer only" result.
    # The target digest is always full-population. Balanced mode used to cap it
    # at 5000 rows, which hashed an arbitrary 5000-row prefix and compared it
    # against the full source digest — every balanced transfer above 5000 rows
    # reported a checksum mismatch on data that was byte-identical. Validation
    # mode governs fail-closed severity (`strict_checksum`), never the scope of
    # the comparison; the digest streams through FingerprintAccumulator, which
    # spills to disk, so full scope stays memory-bounded at any row count.
    checksum_limit = 0

    from services.sync_cursor import is_overwrite_sync

    sync_mode_early = str(
        dest_summary.get("sync_mode")
        or dest_summary.get("effective_sync_mode")
        or ""
    )
    allow_extra_early = (
        not is_overwrite_sync(sync_mode_early)
        and sync_mode_early.lower() not in {"full_refresh_mirror", "mirror", "scd2"}
    )
    # Pre-write destination cardinality, stamped by the write adapter. Without
    # it an append into a non-empty table has no cardinality proof at all.
    raw_before = dest_summary.get(PRECOUNT_KEY)
    rows_before = int(raw_before) if isinstance(raw_before, int) else None
    written_ids = [
        str(x)
        for x in (dest_summary.get("written_ids") or [])
        if x is not None and str(x) != ""
    ] or None
    # Streaming transfers pass records=[] — use the bounded sample the writer
    # stashed so append/upsert Gate-8 can still prove key-aligned fidelity.
    sample_records = list(records or [])
    if not sample_records:
        stashed = (
            dest_summary.get("reconcile_sample")
            or dest_summary.get("sample_records")
            or []
        )
        if isinstance(stashed, list):
            sample_records = [r for r in stashed if isinstance(r, dict)]
    pk_column = str(pk_cols[0]) if len(pk_cols) == 1 else None
    # X-OW1: derive the batch key list from whichever row collection this path
    # actually holds — engine-copy runs may carry full `records` without a
    # writer-stashed sample, and without keys the verify pass cannot re-scope
    # the destination digest to the written batch.
    if allow_extra_early and pk_column and not written_ids:
        key_source_rows = sample_records or records or []
        if key_source_rows:
            written_ids = [
                str(x)
                for x in _source_key_values(
                    key_source_rows,
                    sort_key=pk_column,
                    mappings=mapping_dicts,
                    limit=500,
                )
                if x is not None and str(x) != ""
            ] or None

    # Both digests computed by the engines when they will render identically —
    # the same population coverage at a fraction of the cost, since neither side
    # has to bring rows into Python to hash them.
    engine_digests = None
    routed_tables = [
        (str(name), str(info.get("table") or ""))
        for name, info in sorted((dest_summary.get("routes") or {}).items())
        if isinstance(info, dict) and info.get("table")
    ]
    # A server-to-server COPY never brought rows into Python, so it computed both
    # digests itself — the source one inside the same snapshot it copied. Those
    # are the only two comparable values for such a run: recomputing either side
    # here would compare two different algorithms and always disagree.
    paired = _writer_supplied_engine_digests(dest_summary)
    # X-OW1: an append into an occupied destination is only comparable on the
    # staged population's keys — the destination legitimately holds rows this
    # run never wrote. Whole-table digests (writer-supplied or re-read) count
    # those pre-existing rows as corruption; the keyed census owns this scope.
    append_scope_occupied = bool(_keyed_join_proof(dest_summary) and rows_before)
    if paired is not None and not source_checksum_scope_note and not append_scope_occupied:
        # G-1i: the writer's dest digest was computed inside the copy
        # transaction — post-write tampering is invisible to it. Re-digest
        # the destination at verify time (same engine digest, same column
        # order). When a PostgreSQL destination cannot be re-read, the
        # verdict must degrade to count-only — never verified on a stale
        # write-time digest.
        reread = _reread_dest_engine_digest(
            paired,
            db_type=db_type,
            dest_summary=dest_summary,
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            mappings=mapping_dicts,
        )
        if reread is not None:
            paired = reread
            engine_digests = _with_reread_dest_count(
                paired,
                db_type=db_type,
                cfg=cfg,
                schema=schema,
                table_name=table_name,
                rows_before=rows_before,
            )
            if engine_digests is not None:
                digest_provenance["dest_read"] = "verify_time_engine_digest"
        elif db_type == "postgresql":
            source_checksum_scope_label = "count only"
            source_checksum_scope_note = (
                "The destination value digest could not be re-read at verify "
                "time — a write-time digest cannot see post-write changes, so "
                "this verdict is row-count only, never value-verified."
            )
        else:
            engine_digests = _with_reread_dest_count(
                paired,
                db_type=db_type,
                cfg=cfg,
                schema=schema,
                table_name=table_name,
                rows_before=rows_before,
            )
            if engine_digests is not None:
                digest_provenance["dest_read"] = (
                    "write_time_engine_digest_no_verify_reread"
                )
    elif (_engine_digest_enabled() or _cdc_source_image_gate(dest_summary)) and (
        not source_checksum_scope_note
        # X-37: the engine-copy count-only note is precisely what the flag is
        # for — with DATAFLOW_ENGINE_DIGEST=1 the verify pass re-reads both
        # populations in-engine for full_checksum proof instead of degrading
        # to count-only. Only the resume window makes a whole-population
        # digest non-comparable, so that scope still blocks.
        or (
            _engine_digest_enabled()
            and _engine_count_proof_only(dest_summary) is not None
            and not (
                resume_full_source_rows
                and (records or source_spool is not None)
            )
        )
    ) and not append_scope_occupied:
        engine_digests = _engine_population_digests(
            source_endpoint=source_endpoint,
            dest_cfg=cfg,
            dest_db_type=db_type,
            dest_schema=schema,
            dest_table=table_name,
            mappings=mapping_dicts,
            source_schema=source_schema,
            dest_types=dest_types,
        )

    if routed_tables:
        # The router partitioned the run into per-route tables; the base table
        # name was never written to. Verify each route table independently —
        # per-route rows+checksums land on the route ledger — and let the
        # ROUTE_PARTITIONED scope grade conservation at the final reconcile.
        engine_digests = None
    if engine_digests is not None:
        source_checksum, target_checksum, target_rows = engine_digests
        # Both sides re-read independently, in full — the strongest source a
        # digest can have here, and in particular not the writer's own account.
        source_checksum_provenance = SOURCE_DIGEST_ENGINE_POPULATION
        digest_provenance["source"] = source_checksum_provenance
        # A digest now exists — the count-only scope note (and its
        # "Set DATAFLOW_ENGINE_DIGEST=1" prompt) no longer applies (X-37).
        source_checksum_scope_note = ""
        source_checksum_scope_label = "by engine digest"
    elif routed_tables:
        target_rows = 0
        target_checksum = ""
        route_readback: dict[str, Any] = {}
        readback_ok = True
        for _rname, _rtable in routed_tables:
            if db_type == "postgresql":
                # One scan yields both the canonical digest and the additive
                # multiset digest — the additive form proves an append into an
                # occupied route table (post − pre == this run's rows, RT-6).
                from services.reconciliation import verify_postgres_route_table

                _r_rows, _r_checksum, _r_additive = verify_postgres_route_table(
                    host=str(cfg.get("host") or ""),
                    port=int(cfg.get("port") or 5432),
                    database=str(cfg.get("database") or ""),
                    username=str(cfg.get("username") or cfg.get("user") or ""),
                    password=str(cfg.get("password") or ""),
                    schema=schema,
                    connection_string=str(cfg.get("connection_string") or ""),
                    ssl=bool(cfg.get("ssl") or cfg.get("sslmode")),
                    table_name=_rtable,
                    dest_types=None,
                    # Compare only the columns this run wrote — a route table
                    # owned by the customer may carry extra columns (a
                    # default, a generated value) that were never part of the
                    # write (P0-G1 loaded_at case).
                    target_columns=(
                        (dest_summary.get("route_expected") or {})
                        .get(_rname, {})
                        .get("columns")
                    ) or None,
                )
            else:
                _r_additive = ""
                _r_rows, _r_checksum = verify_target(
                    db_type,
                    cfg,
                    schema=schema,
                    table_name=_rtable,
                    fallback_rows=-1,
                    fallback_checksum="",
                    target_columns=None,
                    limit=checksum_limit,
                    dest_types=None,
                    written_ids=None,
                    pk_column=None,
                )
            route_readback[_rname] = {
                "table": _rtable,
                "verified_rows": _r_rows,
                "checksum": _r_checksum,
            }
            if _r_additive:
                route_readback[_rname]["additive_checksum"] = _r_additive
            if isinstance(_r_rows, int) and _r_rows >= 0:
                target_rows += _r_rows
            else:
                readback_ok = False
        # Value-level per-route proof: the write pass fingerprinted each
        # route's mapped rows (route_expected); compare the dest read-back
        # digest against the expected per-route digest. The _df_route tag
        # column is never a destination column, so no exclusion is needed.
        route_expected = dest_summary.get("route_expected") or {}
        route_precount = dest_summary.get("route_precount") or {}
        route_predigest = dest_summary.get("route_predigest") or {}
        from services.sync_cursor import is_overwrite_sync, normalize_sync_mode

        _r_sync = normalize_sync_mode(
            str(dest_summary.get("sync_mode") or ""), default=""
        )
        # "full_refresh" substring matching treated full_refresh_append as a
        # replacing sync and compared this run's digest against a table that
        # still held prior rows — a guaranteed false mismatch (RT-6).
        _r_replacing = is_overwrite_sync(_r_sync)
        # Upsert-class modes land exactly this run's expected set: the final
        # table state IS the run's rows, so the canonical digest compares
        # directly regardless of pre-count (RT-7).
        _r_idempotent = _r_sync in {"incremental_deduped", "upsert", "mirror"}
        route_digest_all = True
        _route_digest_compared = 0
        for _rname2, _rinfo in route_readback.items():
            _exp = route_expected.get(_rname2)
            # The run's write-pass digest is comparable only when the table
            # holds exactly this run's rows: a replacing sync emptied it, an
            # upsert made the table equal the expected set, or the per-route
            # pre-count says it started empty.
            _pre_rows = int(route_precount.get(_rname2) or 0)
            _comparable = _r_replacing or _r_idempotent or _pre_rows == 0
            _rinfo["expected_checksum"] = str(_exp.get("checksum") or "") if _exp else ""
            if not _comparable:
                # Append into an occupied table: prove the delta, not the
                # whole table. post-digest − pre-digest must equal this run's
                # additive digest, and post-count − pre-count the run's rows.
                _pre_add = str((route_predigest.get(_rname2) or {}).get("additive") or "")
                _post_add = str(_rinfo.get("additive_checksum") or "")
                _run_add = str((_exp or {}).get("additive") or "")
                _run_rows = int((_exp or {}).get("rows") or 0)
                if _pre_add and _post_add and _run_add:
                    _route_digest_compared += 1
                    _delta_ok = (
                        (int(_post_add, 16) - int(_pre_add, 16)) % (1 << 256)
                        == int(_run_add, 16)
                    )
                    _count_ok = int(_rinfo.get("verified_rows") or -1) - _pre_rows == _run_rows
                    _match = _delta_ok and _count_ok
                    _rinfo["digest_match"] = _match
                    _rinfo["digest_note"] = (
                        "additive digest: post-write minus pre-write equals "
                        "this run's rows" if _match else
                        "additive digest mismatch — the table's post-minus-pre "
                        "delta does not equal this run's rows"
                    )
                    if not _match:
                        route_digest_all = False
                    continue
                _rinfo["digest_match"] = None
                _rinfo["digest_note"] = (
                    "not comparable — table held rows before this run and no "
                    "additive digest was measured"
                )
                continue
            _route_digest_compared += 1
            _match = bool(
                _exp
                and _rinfo.get("checksum")
                and _exp.get("checksum") == _rinfo.get("checksum")
            )
            _rinfo["digest_match"] = _match
            if not _match:
                route_digest_all = False
        if _route_digest_compared == 0:
            route_digest_all = None  # unproven — not a false alarm
        # Keyed modes (deduped/upsert/mirror): each key must live in exactly
        # one route table. A row that changed route keeps a stale copy in its
        # previous table unless the write purged it — assert uniqueness
        # across all route tables so Gate-8 catches any that survive (P1-B).
        if _r_idempotent and pk_cols and db_type == "postgresql" and readback_ok:
            from services.primary_key import identity_source_columns
            from services.reconciliation import verify_pg_route_key_uniqueness

            _pk_srcs = identity_source_columns(
                [str(c) for c in pk_cols], mapping_dicts
            ) or [str(c) for c in pk_cols]
            _routes_meta = dest_summary.get("routes") or {}
            _key_tables: list[tuple[str, list[str]]] = []
            for _rname3, _rtable3 in routed_tables:
                _rmap3 = (
                    (_routes_meta.get(_rname3) or {}).get("map") or {}
                    if isinstance(_routes_meta.get(_rname3), dict)
                    else {}
                )
                if _rmap3:
                    _rpk = [
                        str(_rmap3.get(s) or "") for s in _pk_srcs
                    ]
                else:
                    _rpk = [str(c) for c in pk_cols]
                if all(_rpk):
                    _key_tables.append((_rtable3, _rpk))
            if _key_tables:
                _dups = verify_pg_route_key_uniqueness(
                    host=str(cfg.get("host") or ""),
                    port=int(cfg.get("port") or 5432),
                    database=str(cfg.get("database") or ""),
                    username=str(cfg.get("username") or cfg.get("user") or ""),
                    password=str(cfg.get("password") or ""),
                    schema=schema,
                    connection_string=str(cfg.get("connection_string") or ""),
                    ssl=bool(cfg.get("ssl") or cfg.get("sslmode")),
                    route_tables=_key_tables,
                )
                if _dups is None:
                    dest_summary["route_key_uniqueness"] = "unproven"
                elif _dups:
                    route_digest_all = False
                    dest_summary["route_key_duplicates"] = [
                        [str(v) for v in d] for d in _dups[:5]
                    ]
                    for _rinfo3 in route_readback.values():
                        _rinfo3["key_uniqueness"] = False
                        _rinfo3["digest_note"] = (
                            "a key exists in more than one route table — a "
                            "stale copy survived a route move"
                        )
                else:
                    dest_summary["route_key_uniqueness"] = "verified"
        dest_summary["route_verification"] = route_readback
        dest_summary["route_digest_match"] = route_digest_all
        if not readback_ok:
            target_rows = -1
    else:
        # Always full-table first for SQL. Keyed batch proof is a fallback when the
        # sink legitimately has extras (upsert/append into non-empty) — never for a
        # first load where target_rows == source_rows (would fingerprint a sample).
        target_rows, target_checksum = verify_target(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            fallback_rows=-1,
            fallback_checksum="",
            target_columns=target_cols,
            limit=checksum_limit,
            dest_types=dest_types,
            written_ids=None,
            pk_column=None,
        )

    if db_type == "pgvector":
        # Physical embedding COUNT(*) is chunk cardinality, not source-row
        # conservation. Do not run Gate-8 as reader == vector COUNT(*) —
        # 2 documents / 5 chunks would fail (or invent a surplus if stuffed
        # as dest population). Identity COUNT(DISTINCT source_id) is stamped
        # in _finalize. Embeddings are opaque; the metadata is not (D209):
        # every carried source value is re-read and compared.
        measured = isinstance(target_rows, int) and target_rows >= 0
        from services.pgvector_document_verify import gate8_document_report

        doc_report = gate8_document_report(
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            mappings=mapping_dicts,
            source_rows=source_rows,
            source_endpoint=source_endpoint,
            read_sample=_read_target_sample_retrying,
            run_records=records,
            source_schema=source_schema,
            base={
                "source_rows": source_rows,
                "source_checksum": source_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
            },
            target_rows=target_rows if measured else None,
            # A row digest over id/content/embedding/metadata is not
            # comparable with the source's; the value proof replaces it.
            target_checksum="",
        )
        if doc_report is not None:
            return _finalize(doc_report)
        return _finalize({
            "passed": measured,
            "unproven": True,
            "skipped_readback": True,
            "migration_proven": False,
            "message": (
                "pgvector write completed — Gate-8 embedding cell fidelity "
                "unproven (opaque vectors). Independent identity is "
                "COUNT(DISTINCT source_id); physical vector COUNT(*) and "
                "writer chunk-upsert acknowledgement are diagnostic."
                if measured
                else (
                    "pgvector destination read-back unavailable — identity "
                    "COUNT(DISTINCT source_id) could not be compared. Writer "
                    "chunk-upsert acknowledgement is not destination proof."
                )
            ),
            "source_rows": source_rows,
            "target_rows": target_rows if measured else None,
            "source_checksum": source_checksum,
            "target_checksum": target_checksum if measured else "",
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
        })

    if db_type in VECTOR_IDENTITY_ENGINES:
        # dest-only REST engines: no SQL chunk COUNT.
        # Never stuff writer upsert ack into target_rows — that is the
        # dest_only writer_ack lie. Identity is stamped in _finalize.
        return _finalize({
            "passed": True,
            "unproven": True,
            "skipped_readback": True,
            "migration_proven": False,
            "message": (
                f"{db_type} write completed — Gate-8 embedding cell fidelity "
                "unproven (opaque vectors). Independent identity is "
                "COUNT(DISTINCT source_id); collection rowCount / "
                "points_count and writer chunk-upsert acknowledgement "
                "are diagnostic."
            ),
            "source_rows": source_rows,
            "target_rows": None,
            "source_checksum": source_checksum,
            "target_checksum": "",
            "rejected_rows": rejected_rows,
            "coerced_null_rows": coerced_null_rows,
            "rows_skipped": rows_skipped,
        })

    strict_checksum = validation_mode in ("strict", "maximum")

    if (
        source_checksum_scope_note
        and target_rows >= 0
        and not _cdc_source_image_gate(dest_summary)
    ):
        # Resumed streaming pass: the destination digest covers the whole
        # population, the source digest could only cover the resumed tail. Prove
        # cardinality and say plainly that population fidelity is not proven —
        # never compare two different populations and call the difference
        # corruption.
        from services.reconciliation import ReconciliationReport
        from services.row_conservation import KeyCensus

        expected_rows = max(source_rows - dropped_rows - rows_skipped, 0)
        keyed_census = (
            KeyCensus.from_mapping(dest_summary.get(CENSUS_KEY))
            if _keyed_join_proof(dest_summary)
            else None
        )
        if keyed_census is not None and rows_before is not None:
            # Keyed engine copy into an occupied table: the identity is
            # ``dest == dest_before + inserts - deletes``, exactly.
            expected_rows = int(rows_before) + int(keyed_census.expected_delta)
            balanced = target_rows == expected_rows
            source_checksum_scope_label = (
                f"by engine copy (dest_before {int(rows_before):,} + inserts "
                f"{keyed_census.inserts:,} - deletes {keyed_census.deletes:,})"
            )
        else:
            balanced = target_rows == expected_rows or (
                allow_extra_early and target_rows >= expected_rows
            )
        # X-22 live: a full-equality engine-copy count proof
        # (dest==source snapshot, or dest keys join the staged population)
        # describes COMPARABLE populations — stamping WHOLE_TABLE_NOT_COMPARABLE
        # skipped L3's checksum, suppressed L4/L5 localization, and let a
        # failed L2 ride out as 'verified' on row_count assurance. Only the
        # append scope (dest == before + staging) and resumed tails are
        # genuinely incomparable.
        _count_scope = str(dest_summary.get("proof_scope") or "")
        _populations_comparable = any(
            known in _count_scope
            for known in (
                "dest_count_equals_source_snapshot",
                "dest_pk_join_equals_staging",
            )
        )
        # X-134: an append first load into a table this run created (or an
        # empty one) has no pre-existing rows to excuse it — dest == staged
        # population exactly, so the key census is a valid value compare.
        # Only a later append run is incomparable (dest = before + delta).
        if _is_append_first_load(
            sync_mode_early, rows_before, target_rows, expected_rows, dest_summary
        ):
            _populations_comparable = True
        # X-68: a count-only engine copy proves counts, not values. When the
        # two populations are comparable and both sides are census engines,
        # run the (pk, row-hash) key census as the value compare — a
        # value-changing tamper must fail, never ride out as "partial".
        _census_verdict: dict[str, Any] | None = None
        _census_note = ""
        _census_unverified_reason: str | None = None
        if (
            balanced
            and _populations_comparable
            and source_checksum_scope_label == "by engine copy"
        ):
            _changed, _missing, _extra, _reason = _generic_key_census(
                source_endpoint=source_endpoint,
                endpoint=endpoint,
                dest_summary=dest_summary,
                mappings=mapping_dicts,
            )
            if _reason is not None:
                # The census could not run: no value compare happened at all.
                # Partial stays honest — but only with a user-visible finding.
                _census_unverified_reason = (
                    "no value compare ran — " + _reason
                )
            else:
                # extra_in_dest is a defect only under the exact-count scope —
                # on the pk-join scope pre-existing dest rows legitimately hold
                # keys that are not in this run's staged population.
                _extra_defect = (
                    _extra if "dest_count_equals_source_snapshot" in _count_scope else []
                )
                if _changed or _missing or _extra_defect:
                    _culprit: dict[str, list[str]] = {}
                    if _changed:
                        _culprit["changed"] = [str(k) for k in _changed]
                    if _missing:
                        _culprit["missing_in_dest"] = [str(k) for k in _missing]
                    if _extra_defect:
                        _culprit["extra_in_dest"] = [str(k) for k in _extra_defect]
                    _census_verdict = {
                        "passed": False,
                        "verification_verdict": "failed",
                        "culprit_keys": _culprit,
                        "message": (
                            "Destination key census found differing row(s) on a "
                            "count-only engine copy: "
                            + ", ".join(
                                f"{len(v)} {k}" for k, v in _culprit.items()
                            )
                            + " — value mismatch, not a count problem. "
                            + source_checksum_scope_note
                        ),
                    }
                else:
                    _census_note = (
                        " Destination key census compared the mapped columns "
                        "key-by-key on both sides — all values agree."
                    )
        if _census_verdict is None and not _census_note and balanced:
            # D212: no value compare ran. A text column whose values cannot
            # round-trip (Unicode-capable source on a code-page carrier) makes
            # the count-only verdict a failure — never green over a lossy write.
            from services.value_readback import count_only_text_risk_verdict

            _census_verdict = count_only_text_risk_verdict(
                source_endpoint=source_endpoint,
                endpoint=endpoint,
                mappings=mapping_dicts,
                reason=_census_unverified_reason,
            )
        if _census_verdict is not None:
            _base = ReconciliationReport(
                passed=False,
                source_rows=source_rows,
                target_rows=target_rows,
                source_checksum="",
                target_checksum=target_checksum,
                rejected_rows=rejected_rows,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                checksum_scope=(
                    _count_scope if _populations_comparable else WHOLE_TABLE_NOT_COMPARABLE
                ),
                message=_census_verdict["message"],
            ).to_dict()
            _base["culprit_keys"] = _census_verdict["culprit_keys"]
            _base["verification_verdict"] = "failed"
            if _census_verdict.get("value_compare"):
                _base["value_compare"] = _census_verdict["value_compare"]
            else:
                _base["value_compare"] = {"ran": True, "method": "key_census",
                                          "passed": False}
            return _finalize(_base)
        _count_only_report = ReconciliationReport(
            passed=balanced,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum="",
            target_checksum=target_checksum,
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            checksum_scope=(
                _count_scope
                if _populations_comparable
                else WHOLE_TABLE_NOT_COMPARABLE
            ),
            message=(
                (
                    f"Row count verified {source_checksum_scope_label}: "
                    f"{target_rows:,} row(s) on the destination for "
                    f"{source_rows:,} source row(s). "
                    if balanced
                    else (
                        f"Row count mismatch {source_checksum_scope_label}: expected "
                        f"{expected_rows:,} row(s) on the destination, found "
                        f"{target_rows:,}. "
                    )
                )
                + source_checksum_scope_note
                + _census_note
            ),
        ).to_dict()
        if _census_unverified_reason:
            # X-68: never a green-looking partial with no value compare and no
            # stated reason — the operator must see that nothing compared
            # values on this run.
            _count_only_report["value_compare"] = {
                "ran": False,
                "reason": _census_unverified_reason,
            }
        elif _census_note:
            _count_only_report["value_compare"] = {"ran": True, "method": "key_census"}
        return _finalize(_count_only_report)

    sample_compare = None
    # A router partitioned the run into per-route tables — there is no single
    # destination table to compare against, and ``target_cols`` still carries
    # the recipe's internal ROUTE_COLUMN tag, which is never a destination
    # column. Per-route verify_target above already proved each table's rows
    # and checksum (RT-1).
    if sample_records and table_name and target_cols and not routed_tables:
        sort_key = _sort_key_for_columns(target_cols, mapping_dicts)
        key_values = _source_key_values(
            sample_records,
            sort_key=sort_key,
            mappings=mapping_dicts,
            limit=min(50, len(sample_records)),
        )
        # Compare EVERY mapped column. Truncating to 20 falsely treated columns
        # 21+ as NULL (Mongo→MySQL users with 24 fields: Gate-8 failed on
        # referral_invite_modal_dismissed with source 0 vs invented NULL).
        try:
            target_sample = _read_target_sample_retrying(
                "sample compare",
                db_type,
                cfg,
                schema=schema,
                table_name=table_name,
                columns=target_cols or None,
                limit=min(50, len(sample_records)),
                sort_key=sort_key,
                key_values=key_values or None,
            )
        except TargetSampleUnavailable as exc:
            # A failed read is not "no rows to compare". Skipping Gate-8 here
            # used to report a clean reconcile while the destination was
            # unreachable — the exact silent-pass the proof bar forbids.
            unavailable = bool(getattr(exc, "transient", False))
            return _finalize({
                "passed": False,
                "verification_unavailable": unavailable,
                "message": (
                    "Gate-8 sample compare unavailable: could not read destination "
                    f"sample ({exc}). Refusing to treat a failed read as fidelity proof."
                    + (
                        " The committed rows were kept; re-run verification once the "
                        "destination is reachable."
                        if unavailable
                        else ""
                    )
                ),
                "source_rows": source_rows,
                "target_rows": target_rows,
                "source_checksum": source_checksum,
                "target_checksum": target_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "sample_compare": {"passed": False, "error": str(exc)},
            })
        if target_sample:
            sample_compare = sample_compare_rows(
                sample_records,
                target_sample,
                mapping_dicts,
                target_columns=target_cols,
                sample_size=min(50, len(sample_records)),
                sort_key=sort_key,
                dest_db_type=db_type,
                dest_types=dest_types,
                duplicate_policy=str(dest_summary.get("duplicate_policy") or ""),
                inserted_keys=dest_summary.get("inserted_keys"),
                source_locale=str(dest_summary.get("number_locale") or ""),
            )
            full_compare_ctx.update(
                db_type=db_type, cfg=cfg, schema=schema, table_name=table_name,
                target_cols=target_cols, sort_key=sort_key, mappings=mapping_dicts,
                dest_types=dest_types, records=sample_records,
                duplicate_policy=str(dest_summary.get("duplicate_policy") or ""),
                source_locale=str(dest_summary.get("number_locale") or ""),
                source_endpoint=source_endpoint,
            )

    # CDC delete proof: stashed PKs must be absent on destination read-back.
    delete_pks = [
        str(k)
        for k in (dest_summary.get("reconcile_deletes") or [])
        if k is not None and str(k) != ""
    ]
    if delete_pks and table_name and target_cols and strict_checksum:
        sort_key = _sort_key_for_columns(target_cols, mapping_dicts)
        if sort_key:
            try:
                still_present = _read_target_sample_retrying(
                    "delete proof",
                    db_type,
                    cfg,
                    schema=schema,
                    table_name=table_name,
                    columns=[sort_key],
                    limit=len(delete_pks),
                    sort_key=sort_key,
                    key_values=delete_pks,
                )
            except TargetSampleUnavailable as exc:
                return _finalize({
                    "passed": False,
                    "verification_unavailable": bool(getattr(exc, "transient", False)),
                    "message": (
                        "Gate-8 delete proof unavailable: could not read destination "
                        f"keys ({exc}). Refusing to treat a failed read as proof that "
                        "deleted PKs are absent."
                    ),
                    "source_rows": source_rows,
                    "target_rows": target_rows,
                    "source_checksum": source_checksum,
                    "target_checksum": target_checksum,
                    "rejected_rows": rejected_rows,
                    "coerced_null_rows": coerced_null_rows,
                })
            if still_present:
                return _finalize({
                    "passed": False,
                    "message": (
                        f"Gate-8 delete proof failed: {len(still_present)} deleted "
                        "PK(s) still present on destination after CDC delete"
                    ),
                    "source_rows": source_rows,
                    "target_rows": target_rows,
                    "source_checksum": source_checksum,
                    "target_checksum": target_checksum,
                    "rejected_rows": rejected_rows,
                    "coerced_null_rows": coerced_null_rows,
                    "rows_skipped": rows_skipped,
                    "delete_keys_checked": len(delete_pks),
                    "delete_keys_still_present": len(still_present),
                })

    # No read-back verifier available for this destination.
    if target_rows < 0:
        # dest_only sinks (milvus, pinecone, …) have no independent SQL
        # identity read-back by design — fail-closed strict mode would ban
        # every production write. pgvector identity is stamped above; do not
        # accept writer-ack as dest population for that driver.
        dest_only = False
        try:
            from src.transfer.connector_capabilities import _DRIVER_CAPS

            dest_only = bool(_DRIVER_CAPS.get(db_type, {}).get("dest_only"))
        except Exception:
            dest_only = False
        # SaaS / Kafka: keyed sample compare is independent enough for reverse-ETL
        # (Hightouch/Census row sync status) when full-table COUNT is unavailable.
        if (
            sample_compare
            and sample_compare.get("passed")
            and int(sample_compare.get("compared") or 0) > 0
            and rows_written_accounted == expected_written
        ):
            return _finalize({
                "passed": True,
                "message": (
                    f"Gate-8 sample screening compared "
                    f"{int(sample_compare.get('compared') or 0)} "
                    f"key-aligned field(s) for '{db_type}' "
                    f"({rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + ") — screening only, not population proof"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written_accounted,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "sample_compare": sample_compare,
                "assurance_level": "sample_screening",
            })
        if strict_checksum and not dest_only:
            return _finalize({
                "passed": False,
                "message": (
                    "Strict reconciliation requires an independent destination read-back; "
                    f"verifier unavailable for '{db_type}'"
                ),
                "source_rows": source_rows,
                "target_rows": -1,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "sample_compare": sample_compare,
            })
        if rows_written_accounted == expected_written:
            # Warehouse/SQL dests advertise an independent COUNT/digest. A
            # missing verifier there is a Gate-8 failure, not a green writer-ack.
            # dest_only / no-read sinks (email, Redis, Kafka) still ack.
            _warehouse_readback = {
                "postgresql",
                "mysql",
                "snowflake",
                "bigquery",
                "sqlserver",
                "oracle",
                "sqlite",
                "generic_sql",
                "databricks",
                "mongodb",
            }
            dest_key = str(db_type or "").strip().lower()
            if dest_key in _warehouse_readback and not dest_only:
                return _finalize({
                    "passed": False,
                    "unproven": True,
                    "message": (
                        f"Writer acknowledged {rows_written:,} rows but Gate-8 has no "
                        f"independent destination read-back for '{db_type}'. "
                        "Not migration_proven."
                    ),
                    "source_rows": source_rows,
                    "target_rows": -1,
                    "source_checksum": source_checksum,
                    "target_checksum": "",
                    "rejected_rows": rejected_rows,
                    "coerced_null_rows": coerced_null_rows,
                    "rows_skipped": rows_skipped,
                    "sample_compare": sample_compare,
                })
            return _finalize({
                "passed": True,
                "message": (
                    f"Transfer verified by writer: {rows_written:,} rows written"
                    + (f", {rejected_rows:,} rejected" if rejected_rows else "")
                    + (f", {rows_skipped:,} skipped" if rows_skipped else "")
                    + " (read-back verifier not available for this destination)"
                ),
                "source_rows": source_rows,
                "target_rows": rows_written_accounted,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "sample_compare": sample_compare,
            })
        report = reconcile(
            source_rows=source_rows,
            target_rows=rows_written_accounted,
            source_checksum=source_checksum,
            target_checksum="",
            rejected_rows=rejected_rows,
            strict_checksum=False,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            rows_shaped_out=rows_shaped_out,
            rows_source_filtered=rows_source_filtered,
            rows_expanded=rows_expanded,
            sample_compare=sample_compare,
        )
        return _finalize(report.to_dict())

    # Data loss signal: the target table holds fewer rows than we just wrote.
    # A CDC update of an existing key is a write that does not add a row, so
    # dest COUNT is below the event ack on a correct stream. The source image
    # count further down is the population. Comparing the blank CDC source
    # digest to the full-table dest digest here failed MySQL→Postgres after
    # the snapshot and the live update had both landed.
    if target_rows < rows_written_accounted and not _cdc_source_image_gate(
        dest_summary
    ):
        report = reconcile(
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            rejected_rows=rejected_rows,
            strict_checksum=strict_checksum,
            sample_compare=sample_compare,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            rows_shaped_out=rows_shaped_out,
            rows_source_filtered=rows_source_filtered,
            rows_expanded=rows_expanded,
        )
        return _finalize(report.to_dict())

    # We have a verified read-back. Extra dest rows are legitimate for append /
    # upsert into a non-empty sink; overwrite/mirror/replace must not soft-pass
    # extras (Airbyte/Fivetran-class honesty: mode-aware reconcile).
    sync_mode = sync_mode_early
    allow_extra = not is_overwrite_sync(sync_mode)
    if sync_mode.lower() in {"full_refresh_mirror", "mirror", "scd2"}:
        allow_extra = False

    # An incremental poll that reads nothing has nothing to reconcile: no batch
    # was written, so there is no batch digest to compare, and the whole-table
    # digest of a sink that already held rows never equals an empty source
    # digest. Comparing them failed every quiet poll of every scheduled
    # incremental sync — a red run for the normal outcome, which buries the runs
    # that are actually broken. The proof of a no-op is that the destination did
    # not move.
    if (
        allow_extra
        and source_rows == 0
        and int(rows_written or 0) == 0
        and dropped_rows == 0
    ):
        if rows_before is None:
            return _finalize({
                "passed": False,
                "unproven": True,
                "message": (
                    "No new source rows since the last watermark — nothing written, "
                    f"but pre-write destination count was not measured "
                    f"(destination now holds {target_rows:,} row(s)). "
                    "Not migration_proven."
                ),
                "source_rows": 0,
                "target_rows": target_rows,
                "source_checksum": source_checksum,
                "target_checksum": target_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "assurance_level": "none",
            })
        if target_rows == rows_before:
            _noop_verify = _covered_keyed_noop_verify(
                endpoint=endpoint,
                source_endpoint=source_endpoint,
                dest_summary=dest_summary,
                mappings=mappings,
                sync_mode=sync_mode,
                rows_before=rows_before,
                target_rows=target_rows,
                pk_column=pk_column,
                db_type=str(db_type or ""),
            )
            if _noop_verify is not None:
                _noop_verify.setdefault("source_rows", 0)
                _noop_verify.setdefault("target_rows", target_rows)
                _noop_verify.setdefault("source_checksum", source_checksum)
                _noop_verify.setdefault("target_checksum", target_checksum)
                _noop_verify.setdefault("rejected_rows", rejected_rows)
                _noop_verify.setdefault("coerced_null_rows", coerced_null_rows)
                _noop_verify.setdefault("rows_skipped", rows_skipped)
                return _finalize(_noop_verify)
            return _finalize({
                "passed": True,
                "message": (
                    "No new source rows since the last watermark — nothing written, "
                    f"destination unchanged at {target_rows:,} row(s)."
                ),
                "source_rows": 0,
                "target_rows": target_rows,
                "source_checksum": source_checksum,
                "target_checksum": target_checksum,
                "rejected_rows": rejected_rows,
                "coerced_null_rows": coerced_null_rows,
                "rows_skipped": rows_skipped,
                "assurance_level": NO_OP_DEST_UNCHANGED,
            })

    # Streaming append/upsert soft-pass of extra dest rows without a stashed
    # sample cannot claim key-aligned proof (Airbyte/Fivetran honesty bar).
    # A whole-population engine digest pair (server-to-server COPY, or both
    # sides re-read in the engines) already compared every mapped cell, so no
    # sample is owed — a sample is a subset of the proof already in hand.
    is_streaming = bool(dest_summary.get("streaming"))
    if (
        strict_checksum
        and is_streaming
        and allow_extra
        and int(rows_written or 0) > 0
        and not sample_compare
        and not sample_records
        and engine_digests is None
        and not routed_tables
        and db_type
        not in {"pinecone", "qdrant", "weaviate", "milvus", "pgvector", "email"}
    ):
        from services.reconciliation import ReconciliationReport

        return _finalize(ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                "Gate-8 refused: streaming write completed but the writer recorded "
                "no source sample, so the write cannot be proven key by key. "
                "Re-run the transfer; if it repeats, report this job."
            ),
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
        ).to_dict())

    # Upsert/append into a larger sink: whole-table digests are not comparable to
    # the batch. Re-fingerprint destination WHERE pk IN (batch keys) while keeping
    # full-table cardinality for the operator report.
    expected_batch = max(source_rows - dropped_rows - rows_skipped, 0)
    keyed_scope = ""
    # Upsert/append/quarantine-replay into a non-empty table: whole-table digests
    # are not comparable. Keyed fingerprint of written_ids proves the batch for
    # balanced and strict alike (strict_checksum only governs fail-closed severity
    # inside reconcile(), not whether we may re-scope the target digest).
    # The keyed digest is only comparable to the source digest when the key list
    # covers the *whole* batch. `written_ids` is capped (writer stash / bounded
    # sample), so a 100-row append whose stash held 50 keys re-read 50 rows and
    # compared them to the 100-row source digest — a guaranteed mismatch
    # reported as "checksum mismatch (strict)" on a correct load. A partial key
    # list is a diagnostic sample, not a scope: fall through to the append delta.
    keys_cover_batch = bool(written_ids) and len(written_ids) >= expected_batch
    # Engine digests already compared both whole populations in their engines;
    # swapping the target for a keyed Python fingerprint would compare two
    # different algorithms and always disagree (an append that also added a
    # column failed Gate-8 on matching tables).
    if (
        allow_extra
        and pk_column
        and keys_cover_batch
        and engine_digests is None
        and target_rows > expected_batch
        and source_checksum
        and target_checksum
        and str(db_type) in KEYED_READBACK_ENGINES
    ):
        _keyed_rows, keyed_checksum = verify_target(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            fallback_rows=target_rows,
            fallback_checksum=target_checksum,
            target_columns=target_cols,
            limit=checksum_limit,
            dest_types=dest_types,
            written_ids=written_ids,
            pk_column=pk_column,
        )
        # A key-scoped digest is only comparable when the key identifies *one*
        # destination row. An append (no merge, no enforced constraint) can put
        # the same key in twice: appending one batch twice left two rows per key,
        # the keyed read-back returned 2x the batch, and its digest could never
        # equal the source's — a correct append failed itself with two hex
        # strings. Merge writes own their conflict target, and a declared
        # PK/unique constraint rejects the second copy; an identity inferred from
        # Map guarantees neither, so an append keeps the delta as its identity.
        keys_identify_one_row = not sync_mode_appends_without_key_resolution(
            sync_mode
        ) or _destination_enforces_single_key(
            db_type,
            cfg,
            schema=schema,
            table_name=table_name,
            key_column=pk_column,
        )
        if keyed_checksum and keys_identify_one_row:
            target_checksum = keyed_checksum
            keyed_scope = WRITTEN_BATCH_KEYS

    cdc_poll_drift: dict[str, Any] = {}
    if (
        _cdc_source_image_gate(dest_summary)
        and engine_digests is None
        and not keyed_scope
    ):
        # Same-engine catch-up used engine digest above. Cross-engine (or
        # digest unavailable) must not compare last-batch ack to full dest.
        # A finished value scan checks that each source row's fingerprint
        # is on the dest. If the scan cannot finish, stay on COUNT and say
        # value fidelity was not compared.
        keyed_scope = CDC_SOURCE_IMAGE_COUNT
        if source_endpoint is not None and source_endpoint.kind == "database":
            from services.cdc_value_digest import (
                CdcValueScanIncomplete,
                prove_cdc_values,
            )
            from services.reconciliation import ReconciliationReport

            src_cfg_v = resolve_connector_config(source_endpoint)
            src_type_v = resolve_driver_type(
                str(src_cfg_v.get("type") or source_endpoint.format or "")
            )
            try:
                proof = prove_cdc_values(
                    source_type=src_type_v,
                    source_cfg=src_cfg_v,
                    source_table=str(
                        source_endpoint.table or source_endpoint.collection or ""
                    ),
                    dest_type=str(db_type or ""),
                    dest_cfg=dict(cfg),
                    dest_table=str(table_name or ""),
                    mappings=list(mapping_dicts or []),
                    dest_types=dest_types,
                    **_cdc_poll_window(dest_summary),
                )
            except CdcValueScanIncomplete as exc:
                return _finalize(
                    ReconciliationReport(
                        passed=False,
                        source_rows=source_rows,
                        target_rows=target_rows if isinstance(target_rows, int) else 0,
                        source_checksum="",
                        target_checksum="",
                        message=(
                            f"{exc} A matching row count does not prove the "
                            "destination cells. At-least-once upsert. Not "
                            "platform exactly-once."
                        ),
                        checksum_scope=CDC_SOURCE_IMAGE_COUNT,
                        checksum_match=False,
                        population_proof=False,
                    ).to_dict()
                )
            if proof is not None:
                keyed_scope = CDC_SOURCE_IMAGE_VALUES
                source_checksum = proof.source_digest
                target_checksum = proof.dest_digest
                cdc_poll_drift = proof.drift

    # A keyed merge into an occupied destination grows it by ``inserts -
    # deletes``, not by the batch. When the batch digest could not be re-scoped
    # by key (batch larger than the key stash, or a destination without keyed
    # read-back) the writer's key census is the cardinality identity; grading
    # the batch size as the expected delta failed every correct upsert whose
    # events touched existing keys.
    keyed_delta: int | None = None
    if allow_extra and not keyed_scope:
        from services.row_conservation import KIND_KEYED, KeyCensus, conservation_kind

        census = KeyCensus.from_mapping(dest_summary.get(CENSUS_KEY))
        if (
            census is not None
            and conservation_kind(sync_mode, dest_count_before=rows_before) == KIND_KEYED
        ):
            keyed_delta = int(census.expected_delta)

    report = reconcile(
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        rejected_rows=rejected_rows,
        strict_checksum=strict_checksum,
        allow_extra_rows=allow_extra,
        sample_compare=sample_compare,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
        rows_shaped_out=rows_shaped_out,
        rows_source_filtered=rows_source_filtered,
        rows_expanded=rows_expanded,
        target_rows_before=rows_before,
        checksum_scope=keyed_scope
        or (ROUTE_PARTITIONED if routed_tables else ""),
        keyed_expected_delta=keyed_delta,
        route_digest_match=dest_summary.get("route_digest_match"),
        rows_written=int(rows_written) if rows_written is not None else None,
    )
    if cdc_poll_drift:
        from services.incremental_drift import attach_drift

        return _finalize(
            attach_drift(report.to_dict(), cdc_poll_drift, resource=str(table_name or ""))
        )
    return _finalize(report.to_dict())


def _cdc_poll_window(dest_summary: dict[str, Any] | None) -> dict[str, Any]:
    """The query-CDC poll window the run stamped, for the CDC value proof (D188).

    Log CDC stamps none: its image is the whole table. A poll reads
    ``cursor > watermark`` only, so its proof is scoped to that window.
    """
    from services.incremental_drift import RunWindow

    window = RunWindow.from_summary(dest_summary)
    keys = [
        str(k)
        for k in ((dest_summary or {}).get("cdc_poll_key_columns") or [])
        if str(k).strip()
    ]
    if window is None or not keys:
        return {}
    return {"window": window, "key_columns": keys}


def _leftover_cfg_protecting_run_objects(
    db_type: str,
    cfg: dict[str, Any],
    dest_summary: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> dict[str, Any]:
    """Leftover-MERGE config that never rewrites the objects this run wrote.

    A whole-object overwrite leaves no prior rows in its own objects, so a key
    outside the source found there was added after the write. Rewriting it
    away would verify a repaired object instead of the one that was written.
    """
    from services.dest_precount import _object_store_kind

    if _object_store_kind(db_type) not in {"s3", "gcs", "adls"}:
        return cfg
    from connectors.object_store_common import (
        normalize_object_base_key,
        object_parts_prefix,
    )
    from connectors.object_store_leftover import (
        LEFTOVER_PROTECTED_OBJECTS,
        LEFTOVER_PROTECTED_PREFIXES,
    )

    exact = [str(dest_summary.get("key") or "")]
    prefixes: list[str] = []
    try:
        base = normalize_object_base_key(
            table_name, schema, str(cfg.get("file_format") or "")
        )
        exact.append(base)
        prefixes.append(object_parts_prefix(base))
    except ValueError:
        pass
    out = dict(cfg)
    out[LEFTOVER_PROTECTED_OBJECTS] = [k for k in exact if k]
    out[LEFTOVER_PROTECTED_PREFIXES] = [p for p in prefixes if p]
    return out


def _readback_named_a_row(recon: dict[str, Any]) -> bool:
    """True when a failed read-back compare already names the differing row."""
    for name in ("artifact_cell_compare", "full_keyed_compare", "sample_compare"):
        compare = recon.get(name)
        if not isinstance(compare, dict) or compare.get("passed") is not False:
            continue
        for mismatch in compare.get("mismatches") or []:
            if isinstance(mismatch, dict) and (
                mismatch.get("key") not in (None, "") or mismatch.get("row") not in (None, "")
            ):
                return True
    return False


def _cell_mismatch_text(compare: dict[str, Any]) -> str:
    first = (compare.get("mismatches") or [None])[0]
    if not isinstance(first, dict):
        return str(first or "value mismatch in read-back")
    where = f"row {first.get('key')}" if first.get("key") else f"row #{first.get('row')}"
    return (
        f"{where} column {first.get('source')}→{first.get('target')}: "
        f"source={first.get('source_value')!r} dest={first.get('target_value')!r}"
    )


def _file_export_cell_readback(
    records: Any,
    columns: list[str],
    mappings: list[dict] | None,
    dest_summary: dict[str, Any] | None,
    *,
    fmt: str | None,
) -> dict[str, Any] | None:
    """Every-row cell compare of the final local export against what was sent.

    Re-opens the artifact on disk through the file reader after the last write,
    so the file the user downloads is the file that is checked.
    """
    summary = dest_summary or {}
    path = str(summary.get("path") or summary.get("export_path") or "")
    if not path or not os.path.isfile(path) or not isinstance(records, list) or not records:
        return None
    from services.dest_precount import UnmeasuredArtifact, _iter_artifact_records

    mapping_dicts = mappings or [{"source": c, "target": c} for c in columns]
    pk = list(summary.get("primary_key_columns") or summary.get("conflict_columns") or [])
    from pathlib import Path

    from services.fixed_width_layout import FixedWidthError, iter_fixed_width_dicts

    try:
        if path.endswith(".fwf") or str(fmt or "").lower() in {"fixed_width", "fwf"}:
            # The layout lives in a sidecar beside the file, found from its path.
            rows = list(iter_fixed_width_dicts(Path(path)))
        else:
            with open(path, "rb") as fh:
                rows = list(_iter_artifact_records(fh, name=os.path.basename(path), fmt=fmt))
    except (UnmeasuredArtifact, FixedWidthError, OSError, ValueError) as exc:
        return {"passed": False, "mismatches": [f"written file could not be re-read: {exc}"]}
    targets = [str(m.get("target")) for m in mapping_dicts if m.get("target")]
    return sample_compare_rows(
        list(records),
        rows,
        mapping_dicts,
        target_columns=targets or None,
        sample_size=len(records),
        sort_key=pk[0] if len(pk) == 1 else None,
        dest_db_type=str(fmt or ""),
        dest_types=_dest_types_from_mappings(mapping_dicts),
        source_locale=str(summary.get("number_locale") or ""),
    )

def run_reconciliation(
    *,
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    rows_written: int,
    writer_checksum: str,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None = None,
    source_schema: dict[str, str] | None = None,
    validation_mode: str = "strict",
    source_endpoint: EndpointConfig | None = None,
    source_spool: Any = None,
    shape_recipe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Verify row counts and checksums against the destination.

    Thin wrapper: unique business-rule contracts are enforced post-load exactly
    once per call — no early-return path inside the impl may skip them.
    """
    started = time.monotonic()
    try:
        recon = _run_reconciliation_with_rules(
            endpoint=endpoint,
            records=records,
            columns=columns,
            rows_written=rows_written,
            writer_checksum=writer_checksum,
            dest_summary=dest_summary,
            mappings=mappings,
            source_schema=source_schema,
            validation_mode=validation_mode,
            source_endpoint=source_endpoint,
            source_spool=source_spool,
        )
    except BaseException as exc:
        emit_event(
            _logger, "gate8.verify.completed", outcome="errored",
            duration_ms=(time.monotonic() - started) * 1000.0, error=exc,
            destination_type=_endpoint_type(endpoint), validation_mode=validation_mode,
        )
        raise
    if isinstance(dest_summary, dict) and dest_summary.get("shape_recipe_hash"):
        # G-1: on a shaped run both digests derive from the same stream — a
        # dropped or silently skipped recipe step leaves them equally
        # unshaped and "matching". Prove the recipe's declared output is
        # what the destination actually holds.
        try:
            proof = _transform_applied_proof(
                shape_recipe=shape_recipe,
                dest_summary=dest_summary,
                source_endpoint=source_endpoint,
                endpoint=endpoint,
                columns=columns,
                mappings=mappings,
                source_schema=source_schema,
                recon=recon,
            )
        except Exception as exc:
            proof = {
                "status": "unverifiable",
                "reason": f"transform-applied proof errored: {exc!s}"[:300],
            }
        if proof:
            # Plain statement of which compare ran — exact per-row multiset
            # joins at any size, not a sampled or digest shortcut.
            proof.setdefault(
                "compare_mode",
                "exact per-row multiset joins (disk-backed, all rows)",
            )
            recon["transform_applied"] = proof
            if proof.get("status") == "failed":
                steps_txt = ", ".join(proof.get("culprit_steps") or [])
                recon["passed"] = False
                recon["verification_verdict"] = "failed"
                non_recipe = list(proof.get("non_recipe_drift_columns") or [])
                if non_recipe:
                    cols_txt = ", ".join(str(c) for c in non_recipe[:8])
                    if len(non_recipe) > 8:
                        cols_txt += "…"
                    why_txt = (
                        f"Destination drift on column {cols_txt} "
                        f"is not explained by any recipe step"
                    )
                else:
                    why_txt = (
                        f"{steps_txt} did not apply as declared"
                        if steps_txt
                        else str(proof.get('reason') or "the recipe output differs")
                    )
                head = "Transform-applied proof failed: the destination does not hold the approved recipe's output"
                cell = recon.get("artifact_cell_compare")
                if isinstance(cell, dict) and cell.get("passed") is False:
                    # File/object destinations: the re-read artifact already
                    # differs from what was sent — that tamper is the real
                    # cause, not a recipe/transform defect. Lead with the
                    # read-back wording so operators check the destination
                    # file, not their declared transforms.
                    head = "Read-back verification failed: the written file changed after send"
                recon["message"] = (
                    f"{head} — {why_txt} (recipe "
                    f"{proof.get('recipe_hash')})."
                    f"{proof.get('key_note') or ''}"
                )
            elif proof.get("status") == "verified":
                prov = proof.get("provenance") or "recipe re-applied"
                note = (
                    f"transform output verified — {prov} "
                    f"({proof.get('rows')} rows)"
                )
                if (
                    proof.get("scoped_to_run_keys")
                    and "per-cell fidelity is NOT proven"
                    in str(recon.get("message") or "")
                ):
                    # M-2: the key-scoped read-back DID prove this run's
                    # rows cell-for-cell — say so, and drop the stale
                    # NOT-proven disclaimer + the overwrite upsell.
                    msg = str(recon["message"])
                    msg = msg.replace(
                        "per-cell fidelity is NOT proven",
                        f"{proof.get('rows')} row(s) proven "
                        "cell-for-cell against the recipe; "
                        "pre-existing rows untouched",
                    )
                    msg = msg.replace(
                        " Use overwrite, or upsert with a primary key, "
                        "for full_checksum proof.",
                        "",
                    )
                    recon["message"] = msg
                if recon.get("message"):
                    recon["message"] = f"{recon['message']} ({note})"
            elif proof.get("status") == "unverifiable" or (
                proof.get("status") == "skipped" and not proof.get("covered_by")
            ):
                if recon.get("verification_verdict") in (None, "verified"):
                    recon["verification_verdict"] = "partial"
                label = "unverifiable" if proof.get("status") == "unverifiable" else "not applicable"
                cell = recon.get("artifact_cell_compare")
                if isinstance(cell, dict) and cell.get("passed") is False:
                    # The re-read file already differs from what was sent, so
                    # the proof's divergence is that change, not source drift.
                    proof["reason"] = (
                        "the written file differs from what was sent (see the "
                        "read-back failure); recipe application not assessed"
                    )
                note = f"transform proof {label} — {proof.get('reason')}"
                if recon.get("message"):
                    recon["message"] = f"{recon['message']} ({note})"
    # X-37: with DATAFLOW_ENGINE_DIGEST=1 the L3 ladder already re-read both
    # populations and digested them with the same canonical algorithm — a
    # real full_checksum on an engine-copy route, not count-only. Promote it
    # over the "no rows entered this process" stamp instead of telling the
    # operator to set the flag they already set.
    if _engine_digest_enabled() and not recon.get("checksum_match"):
        _l3_run = recon.get("verification_ladder") or {}
        _l3_layer = (_l3_run.get("layers") or {}).get("L3") or {}
        _l3_details = _l3_layer.get("details") or {}
        _l3_src = str(_l3_details.get("source_checksum") or "")
        _l3_dst = str(_l3_details.get("target_checksum") or "")
        if (
            _l3_src
            and _l3_dst
            and not _l3_details.get("skipped")
            and str(_l3_details.get("algorithm") or "")
        ):
            recon["source_checksum"] = _l3_src
            recon["target_checksum"] = _l3_dst
            recon["checksum_match"] = _l3_src == _l3_dst
            # full_checksum names the *matching* coverage — stamping it on a
            # proven mismatch lets a failed job read as verified (the verdict
            # field carries the failure; the label must not contradict it).
            if recon["checksum_match"]:
                recon["assurance_level"] = "full_checksum"
                if recon.get("passed") and recon.get("post_write_pending"):
                    # The count-only stamp this promotion supersedes also
                    # said "post-write proof pending". Leaving it made a
                    # verified, fully digested engine-copy run classify as
                    # pre_write_only and report migration_proven=false.
                    recon["phase"] = "post_write_verified"
                    recon["coverage"] = "full_checksum"
                    recon["post_write_pending"] = False
            recon["checksum_scope"] = "verify_time_population_digest"
            # X-41b: _finalize already stamped verification_verdict="partial"
            # before this promotion ran — a proven two-sided digest match
            # upgrades it to "verified"; a proven mismatch stays failed.
            if recon["checksum_match"]:
                if recon.get("verification_verdict") in (None, "partial"):
                    recon["verification_verdict"] = "verified"
            else:
                recon["verification_verdict"] = "failed"
            _msg = str(recon.get("message") or "")
            _cut = _msg.find(" There is no source value digest")
            # The count-only scope note starts two sentences earlier — leaving
            # "…the only proof the route produced is destination COUNT(*)."
            # next to "…digested (full_checksum)" is contradictory (X-39).
            _note_start = _msg.find(" The copy ran inside the engines:")
            if _note_start >= 0 and (_cut < 0 or _note_start < _cut):
                _cut = _note_start
            if _cut >= 0:
                # The L4/L5 localization the ladder appends on a fail sits
                # after the scope note — it is not part of the note and must
                # survive (X-39). Find it directly; the note's own wording
                # varies by scope (engine copy vs resume).
                _tail = ""
                for _marker in (" L4/L5 localized", " L4 localized"):
                    _at = _msg.find(_marker, _cut)
                    if _at >= 0:
                        _tail = _msg[_at:]
                        break
                if recon["checksum_match"]:
                    recon["message"] = (
                        _msg[:_cut]
                        + " Both populations were re-read and digested "
                        "order-independently at verify time; the digests match "
                        "(full_checksum)."
                        + _tail
                    )
                else:
                    # Failure-first on a FAILED job: lead with the digest
                    # mismatch and the localized cell; the row-count fact is
                    # secondary, not the headline.
                    _count_fact = _msg[:_cut].strip()
                    recon["message"] = (
                        "Checksum mismatch (full_checksum): both populations "
                        "were re-read and digested order-independently at "
                        "verify time; the digests mismatch."
                        + _tail
                        + (f" {_count_fact}" if _count_fact else "")
                    )
            # The rewrites above rebuild the message from the count-only note's
            # head, dropping whatever a later gate appended after it. A G22
            # block/warn is a finding in its own right (matching digests do
            # not prove parents exist) and must stay in the operator's text.
            _ri_gate = recon.get("g22_dest_referential_integrity")
            if isinstance(_ri_gate, dict) and _ri_gate.get("status") in {"block", "warn"}:
                _ri_msg = str(_ri_gate.get("message") or "").strip()
                if _ri_msg and _ri_msg not in str(recon.get("message") or ""):
                    recon["message"] = f"{str(recon.get('message') or '').rstrip()} {_ri_msg}".strip()
    # Unshaped digest failures only: for shaped runs the transform_applied
    # proof names its own culprit keys via culprit_keys_by_kind; hashing raw
    # src vs shaped dest here would flag every transformed row. And this is a
    # failure-path namer — on a passing count-only run ``checksum_match=False``
    # means "digest unproven", not "mismatch", so there is no culprit to name
    # (and no "(source 'kafka' has no census driver)" clause on a completed
    # job's message, M17).
    if (
        recon.get("checksum_match") is False
        and recon.get("passed") is not True
        and not recon.get("culprit_keys")
        and not recon.get("transform_applied")
    ):
        _loc_rows = (
            (recon.get("verification_ladder") or {}).get("localization") or {}
        ).get("rows") or []
        _by_kind: dict[str, list[str]] = {}
        for _r in _loc_rows:
            _pk = _r.get("pk")
            if _pk is None:
                continue
            if _r.get("reason") == "missing_side":
                _kind = (
                    "missing_in_dest"
                    if _r.get("source_present")
                    else "extra_in_dest"
                )
            else:
                _kind = "changed"
            if sum(len(v) for v in _by_kind.values()) >= 32:
                break
            _by_kind.setdefault(_kind, []).append(str(_pk))
        if _by_kind:
            recon["culprit_keys"] = _by_kind
        else:
            _name_gate8_culprits(
                recon,
                source_endpoint=source_endpoint,
                endpoint=endpoint,
                dest_summary=dest_summary,
                mappings=mappings,
            )
            if not recon.get("culprit_keys"):
                # Never leave a detected mismatch with empty culprits and no
                # explanation: name why keys could not be produced. The
                # census itself names the gate it hit (no recorded
                # single-column PK, missing table names, no mappings); this
                # fallback only covers an unreadable census driver.
                if not recon.get("culprit_keys_unavailable_reason"):
                    recon["culprit_keys_unavailable_reason"] = (
                        "culprit keys could not be named — destination key "
                        "census requires both sides readable by a census "
                        "driver (postgres/mysql/mssql/duckdb/sqlite)"
                    )
                if recon.get("message") and not _readback_named_a_row(recon):
                    recon["message"] = (
                        f"{recon['message']} "
                        f"({recon['culprit_keys_unavailable_reason']})"
                    )
    # D188 — message rewrites above rebuild the text from its head; the
    # out-of-window drift sentence is a finding and must stay in it.
    from services.incremental_drift import DRIFT_KEY, drift_sentence

    _drift_text = drift_sentence(recon.get(DRIFT_KEY) or {})
    if _drift_text and _drift_text not in str(recon.get("message") or ""):
        recon["message"] = f"{str(recon.get('message') or '').rstrip()} {_drift_text}".strip()
    verify_ms = (time.monotonic() - started) * 1000.0
    # The wall time Gate-8 itself took — job_phase_timings reports it as verify.
    recon["verify_seconds"] = round(verify_ms / 1000.0, 3)
    _emit_gate8_verdict(
        recon, endpoint=endpoint, validation_mode=validation_mode,
        rows_written=rows_written, duration_ms=verify_ms,
    )
    return recon


def _endpoint_type(endpoint: Any) -> str | None:
    return str(getattr(endpoint, "format", "") or "") or None


def _gate8_verdict(recon: dict[str, Any]) -> tuple[str, str, str | None]:
    """(verdict, outcome, reason_code) of the *verification*, checked before ``passed``.

    An unreadable destination or an unproven population also sets
    ``passed=False`` (fail closed), but nothing was shown wrong — that is a
    hold, not a mismatch. A passing job whose destination has no read-back by
    design (``skipped_readback``: file/object exports, opaque vector sinks) is
    an expected ``skipped`` — neither proof nor an alert. Only a measured
    disagreement is ``failed``. The job effect is reported as ``job_outcome``.
    """
    if recon.get("verification_unavailable"):
        return "unavailable", "held", None
    if recon.get("unproven") and recon.get("passed") and recon.get("skipped_readback"):
        return "unproven", "skipped", "dest_not_verifiable"
    if recon.get("unproven"):
        return "unproven", "held", None
    if not recon.get("passed"):
        return "failed", "failed", None
    return "passed", "succeeded", None


def _emit_gate8_verdict(
    recon: dict[str, Any] | None,
    *,
    endpoint: Any,
    validation_mode: str,
    rows_written: int,
    duration_ms: float,
) -> None:
    recon = recon if isinstance(recon, dict) else {}
    verdict, outcome, reason_code = _gate8_verdict(recon)
    rule_checks = [c for c in recon.get("rule_checks") or [] if isinstance(c, dict)]
    emit_event(
        _logger, "gate8.verify.completed", outcome=outcome, duration_ms=duration_ms,
        verdict=verdict,
        reason_code=reason_code,
        job_outcome="succeeded" if recon.get("passed") else "failed",
        destination_type=_endpoint_type(endpoint),
        validation_mode=validation_mode,
        rows_written=rows_written,
        source_rows=recon.get("source_rows"),
        target_rows=recon.get("target_rows"),
        rejected_rows=recon.get("rejected_rows"),
        checksum_match=recon.get("checksum_match"),
        verification_unavailable=bool(recon.get("verification_unavailable")),
        skipped_readback=bool(recon.get("skipped_readback")),
        rule_checks_failed=sum(1 for c in rule_checks if c.get("status") == "failed"),
    )


def _bind_endpoints_to_last_stream(
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    dest_summary: dict[str, Any] | None,
) -> tuple[EndpointConfig, EndpointConfig | None]:
    """Point Gate-8 at the table the multi-stream summary describes.

    ``run_non_cdc_multi_stream_sequential`` hands back the *last* stream's
    summary (digests, counts, ``table``) but restores the request's primary
    endpoints, so Gate-8 would digest the last table's written population
    against the first table's source and destination rows and call a correct
    job a checksum mismatch. Rebind copies of both endpoints to that last
    stream so every figure Gate-8 compares describes one table.
    """
    summary = dest_summary or {}
    streams = summary.get("streams")
    if not summary.get("multi_stream") or not isinstance(streams, list) or len(streams) < 2:
        return endpoint, source_endpoint
    last = streams[-1]
    name = str(last.get("name") or "").strip() if isinstance(last, dict) else ""
    if not name:
        return endpoint, source_endpoint

    def _bound(ep: EndpointConfig | None) -> EndpointConfig | None:
        if ep is None or getattr(ep, "kind", "") != "database":
            return ep
        bound = copy.copy(ep)
        if getattr(ep, "format", "") == "mongodb" or (
            getattr(ep, "collection", None) and not getattr(ep, "table", None)
        ):
            bound.collection = name
        elif getattr(ep, "table", None):
            bound.table = name
        else:
            return ep
        return bound

    return _bound(endpoint) or endpoint, _bound(source_endpoint)


def _run_reconciliation_with_rules(
    *,
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    rows_written: int,
    writer_checksum: str,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None,
    source_schema: dict[str, str] | None,
    validation_mode: str,
    source_endpoint: EndpointConfig | None,
    source_spool: Any,
) -> dict[str, Any]:
    endpoint, source_endpoint = _bind_endpoints_to_last_stream(
        endpoint, source_endpoint, dest_summary
    )
    recon = _run_reconciliation_impl(
        endpoint=endpoint,
        records=records,
        columns=columns,
        rows_written=rows_written,
        writer_checksum=writer_checksum,
        dest_summary=dest_summary,
        mappings=mappings,
        source_schema=source_schema,
        validation_mode=validation_mode,
        source_endpoint=source_endpoint,
        source_spool=source_spool,
    )
    if not (recon or {}).get("rule_checks"):
        _apply_unique_rule_checks(
            recon, mappings, endpoint=endpoint, validation_mode=validation_mode
        )
    return recon
