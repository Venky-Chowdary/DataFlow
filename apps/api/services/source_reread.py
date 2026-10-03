"""Independent source re-read for Gate-8 — Fivetran/HVR Compare class.

Write-pass fingerprints hash the same remapped rows the writer just sent.
Dest read-back of those bytes proves the warehouse stored what we sent, not
that we read the source correctly. ``full_checksum`` / ``migration_proven``
require a second source digest that did not travel with the write.

This module owns two decisions so stream.py cannot drift:

* **When** to re-read (operator env, file→warehouse, and warehouse full refresh
  including same-engine). Incremental and CDC stay on the write-pass.
* **How** to page the re-read. Snapshot-scan sources must not OFFSET-page
  (O(n²) and skip/duplicate under concurrent writes) — the same cliff the
  extract already closed.
"""

from __future__ import annotations

from typing import Any, Final

from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES

#: Destinations that can independently SELECT the written population.
WAREHOUSE_VERIFY_DESTS: Final[frozenset[str]] = SNAPSHOT_SCAN_SOURCES

#: File sources a second parse can fingerprint. Excel/CSV/JSON is the product
#: route; the warehouse set above does not include them, so file→Postgres used
#: to keep the write-pass digest and could not earn ``migration_proven``.
FILE_REREAD_SOURCES: Final[frozenset[str]] = frozenset(
    {
        "csv",
        "tsv",
        "json",
        "jsonl",
        "ndjson",
        "excel",
        "parquet",
        "avro",
        "orc",
        "xml",
        "yaml",
        "fixed_width",
    }
)

#: Sources the Gate-8 re-read loop knows how to page. A source outside this set
#: keeps the write-pass digest even when ``should_reread_source`` says yes, so
#: the write pass must not skip its inline fingerprints for those routes.
REREAD_SCAN_SOURCES: Final[frozenset[str]] = frozenset(
    {
        "postgresql",
        "redshift",
        "mysql",
        "snowflake",
        "bigquery",
        "sqlite",
        "generic_sql",
        "mongodb",
        "s3",
        "gcs",
        "adls",
    }
)

_PG_FAMILY: Final[frozenset[str]] = frozenset(
    {"postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase", "redshift"}
)
_MYSQL_FAMILY: Final[frozenset[str]] = frozenset({"mysql", "mariadb", "tidb"})
_MSSQL_FAMILY: Final[frozenset[str]] = frozenset({"sqlserver", "mssql", "azure_sql"})


def engine_family(engine: str | None) -> str:
    """Canonical family so ``postgres`` and ``postgresql`` do not look heterogeneous."""
    raw = (engine or "").strip().lower()
    if raw in _PG_FAMILY:
        return "postgresql"
    if raw in _MYSQL_FAMILY:
        return "mysql"
    if raw in _MSSQL_FAMILY:
        return "sqlserver"
    return raw


def _env_reread_mode() -> str:
    from services.brand_env import getenv_brand

    raw = (getenv_brand("RECONCILE_SOURCE_REREAD", "auto") or "auto").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return "off"
    if raw in {"1", "true", "yes", "on"}:
        return "on"
    return "auto"


def should_reread_source(
    *,
    src_type: str,
    dest_type: str,
    incremental: bool = False,
    partial_write_pass: bool = False,
) -> bool:
    """True when Gate-8 must open a second source scan.

    A partial write-pass (resume covering only the tail) always re-reads —
    comparing a session digest to a full destination is a false mismatch.
    ``DATAFLOW_RECONCILE_SOURCE_REREAD=0`` still cannot suppress that.

    Auto (default): warehouse → warehouse full refresh, including same-engine.
    HVR Compare checksums a fresh source read against the target; a write-pass
    digest is the bytes we just sent, not that second read. Incremental and
    CDC stay on the write-pass — a second scan of a moving table is a false
    mismatch, and CDC remains at-least-once upsert.
    """
    if partial_write_pass:
        return True
    mode = _env_reread_mode()
    if mode == "off":
        return False
    if mode == "on":
        return True
    if incremental:
        return False
    src = engine_family(src_type)
    dest = engine_family(dest_type)
    raw_src = (src_type or "").strip().lower()
    raw_dest = (dest_type or "").strip().lower()
    if not src or not dest:
        return False
    # Membership uses the family so ``postgres`` and ``mssql`` are the same
    # engines as ``postgresql`` and ``sqlserver``. Object stores stay on the
    # write-pass unless the operator forces a second scan: a full object
    # download is not the warehouse SELECT this policy is for.
    if src not in SNAPSHOT_SCAN_SOURCES and raw_src not in SNAPSHOT_SCAN_SOURCES:
        return False
    if dest not in WAREHOUSE_VERIFY_DESTS and raw_dest not in WAREHOUSE_VERIFY_DESTS:
        return False
    return True


def should_reread_file_source(
    *,
    file_type: str,
    dest_type: str,
    incremental: bool = False,
    partial_write_pass: bool = False,
) -> bool:
    """True when a file load must parse the source a second time.

    Same policy as :func:`should_reread_source`: a partial write-pass always
    re-reads, ``RECONCILE_SOURCE_REREAD=0`` cannot suppress that, and ``=1``
    forces the second parse. Auto re-reads a full file refresh into a
    warehouse that can be SELECTed back. File→file and incremental file
    loads keep the write-pass digest.
    """
    if partial_write_pass:
        return True
    mode = _env_reread_mode()
    if mode == "off":
        return False
    if mode == "on":
        return True
    if incremental:
        return False
    kind = (file_type or "").strip().lower()
    if kind not in FILE_REREAD_SOURCES:
        return False
    if (dest_type or "").strip().lower() not in WAREHOUSE_VERIFY_DESTS:
        return False
    return True


def align_source_populations(write_pass: Any, reread: Any) -> dict[str, Any]:
    """Identity alignment of a write-pass fingerprint set and a second source read.

    Both arguments are fingerprint accumulators. Alignment is equality of the
    ``(row key, fingerprint)`` multisets — same keys, same cell hashes, same
    cardinality. The value digest ignores keys, so this check is what stops a
    second parse of the same cells under different identities from looking proven.

    Call before ``digest()``. ``identity_digest`` does not consume the accumulator.
    """
    write_rows = int(getattr(write_pass, "total", 0) or 0)
    reread_rows = int(getattr(reread, "total", 0) or 0)
    write_id = str(write_pass.identity_digest())
    reread_id = str(reread.identity_digest())
    aligned = bool(write_rows > 0 and write_rows == reread_rows and write_id == reread_id)
    if aligned:
        reason = "write_pass_and_reread_identity_digests_match"
    elif write_rows == 0 or reread_rows == 0:
        reason = "empty_population"
    elif write_rows != reread_rows:
        reason = "row_count_differs"
    else:
        reason = "identity_digest_differs"
    return {
        "identity_hash_aligned": aligned,
        "write_pass_rows": write_rows,
        "reread_rows": reread_rows,
        "write_pass_identity_digest": write_id,
        "reread_identity_digest": reread_id,
        "reason": reason,
    }


def align_identity_digests(
    *,
    write_pass_identity_digest: str,
    reread_identity_digest: str,
    write_pass_rows: int,
    reread_rows: int,
    write_pass_value_digest: str = "",
    reread_value_digest: str = "",
) -> dict[str, Any]:
    """Same alignment when the write-pass pairs were already digested.

    The CSV COPY fast path fingerprints while it maps, then closes the
    accumulator. It keeps the identity digest. A later re-read compares that
    stored digest, the row counts, and the value digests.
    """
    write_id = str(write_pass_identity_digest or "").strip()
    reread_id = str(reread_identity_digest or "").strip()
    write_n = int(write_pass_rows or 0)
    reread_n = int(reread_rows or 0)
    value_write = str(write_pass_value_digest or "").strip()
    value_reread = str(reread_value_digest or "").strip()
    values_agree = (not value_write or not value_reread) or value_write == value_reread
    aligned = bool(
        write_n > 0
        and write_n == reread_n
        and write_id
        and write_id == reread_id
        and values_agree
    )
    if aligned:
        reason = "write_pass_and_reread_identity_digests_match"
    elif not write_id or not reread_id:
        reason = "identity_digest_missing"
    elif write_n != reread_n:
        reason = "row_count_differs"
    elif not values_agree:
        reason = "value_digest_differs"
    else:
        reason = "identity_digest_differs"
    return {
        "identity_hash_aligned": aligned,
        "write_pass_rows": write_n,
        "reread_rows": reread_n,
        "write_pass_identity_digest": write_id,
        "reread_identity_digest": reread_id,
        "reason": reason,
    }


def reread_pagination_plan(
    *,
    src_type: str,
    incremental: bool = False,
) -> dict[str, Any]:
    """How the independent re-read pages.

    ``mode=scan`` carries a fresh ``scan_state`` so ``_read_batch`` holds one
    SELECT/find + fetchmany/getmore. ``use_offset`` is False on that path —
    the reader ignores the numeric offset.
    """
    if incremental:
        return {"mode": "cursor_or_offset", "scan_state": None, "use_offset": True}
    if src_type in SNAPSHOT_SCAN_SOURCES:
        return {"mode": "scan", "scan_state": {}, "use_offset": False}
    return {"mode": "offset", "scan_state": None, "use_offset": True}
