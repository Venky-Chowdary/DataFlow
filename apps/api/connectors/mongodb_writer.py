"""MongoDB bulk writer — CSV file to collection with checkpoint batches."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import InvalidOperation
from typing import Any

from services.brand_env import getenv_brand

from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _conflict_key_identity,
    _is_nullish_conflict_key,
    _rejected_row_count,
    build_mapped_rows_with_details,
    compare_lsn,
    gate8_writer_meta,
    identity_marker,
    lsn_is_newer,
    resolve_target_columns,
    row_checksum,
    reject_on_strict_policy,
    sanitize_identifier,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from services.document_instant import (
    cell_exceeds_document_instant,
    transform_narrows_to_calendar_day,
)
from services.value_serializer import json_loads_exact

from connectors.destination_naming import writer_identifier
from connectors.mongo_field_paths import insert_document, path_view, target_refusal

logger = logging.getLogger(__name__)

#: A mapped target is a field path (``address.city``), kept exactly; SQL
#: identifier rules renamed it ``address_city`` (Document Modeler PR1, G11).
_field = writer_identifier("mongodb")


def bind_mongo_json_document(value: Any) -> Any:
    """Parse a Mongo JSON/OBJECT/ARRAY/VARIANT cell.

    Numbers match ``json_loads_exact``. Invalid text refuses — never store the
    raw string as a silent invent. Non-finite constants refuse.
    """
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        def _reject(name: str) -> None:
            raise ValueError(f"non-finite JSON constant: {name}")

        try:
            return json_loads_exact(value, parse_constant=_reject)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError(
                f"MongoDB JSON refused {value!r} "
                "(refuse silent string invent)"
            ) from exc
    raise ValueError(
        f"MongoDB JSON refused {value!r} "
        "(refuse silent pass-through invent)"
    )


def _mongo_conflict_key(doc: Mapping[str, Any], pk_cols: list[str]) -> tuple[Any, ...]:
    """Canonical PK tuple so extract reader-null matches dest None."""
    return tuple(_conflict_key_identity(doc.get(c)) for c in pk_cols)


def _mongo_incomplete_pk_cols(doc: Mapping[str, Any], pk_cols: list[str]) -> list[str]:
    """Conflict-key columns that cannot identify a dense Mongo upsert."""
    return [c for c in pk_cols if _is_nullish_conflict_key(doc.get(c))]


def _mongo_insert_id_is_null(doc: Mapping[str, Any]) -> bool:
    """True when insert would store reader-null as a real ``_id``.

    Only Python None was refused. After extract emits SQL_NULL_SENTINEL,
    insert_many stored the wire spelling as document identity.
    Empty string stays a present key (not extract NULL).
    """
    from services.value_serializer import is_reader_null_cell

    return "_id" in doc and is_reader_null_cell(doc.get("_id"))


def _target_is_temporal(target_type: str) -> bool:
    """True when the destination cell holds BSON's single date/time carrier."""
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        normalize_logical_type,
    )

    return normalize_logical_type(target_type) in {LOGICAL_DATE, LOGICAL_DATETIME}


def _declares_calendar_day(
    mapping: Mapping[str, Any], source_type: str = ""
) -> bool:
    """True when the resolved source type declares a calendar day."""
    from services.type_system import LOGICAL_DATE, normalize_logical_type

    return normalize_logical_type(source_type) == LOGICAL_DATE


def _is_pure_date_value(value: Any) -> bool:
    """True for a date value with no clock component."""
    import re
    from datetime import date as _date
    from datetime import datetime as _datetime

    from connectors.sql_temporal import parse_sql_date

    if isinstance(value, _datetime):
        return False
    if isinstance(value, _date):
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if re.search(r"(?:[Tt]\s*\d|\s+\d{1,2}:\d{2})", text):
        return False
    return parse_sql_date(text) is not None


def declared_source_is_instant(source_type: str) -> bool:
    """True when the source type is an offset-bearing instant.

    A naive Python value of that type already lost ``tzinfo`` in the driver.
    MySQL TIMESTAMP is read after the session is pinned to UTC, and live
    introspect stamps it ``TIMESTAMPTZ``. Stamping UTC keeps the clock.
    Bare ``TIMESTAMP`` / ``DATETIME`` / ``TIMESTAMP_NTZ`` stay wall-clock and
    still need the operator's UTC contract.
    """
    from services.type_system import datetime_timezone_polarity

    return datetime_timezone_polarity(source_type) in {"tz", "ltz"}


def _mapping_source_type(
    mapping: Mapping[str, Any], column_types: Mapping[str, str]
) -> str:
    """Source DDL for one mapping. The mapping stamp wins over the schema."""
    declared = mapping.get("source_type") or mapping.get("sourceType") or ""
    if isinstance(declared, str) and declared.strip():
        return declared.strip()
    source_name = str(mapping.get("source") or "")
    if not source_name:
        return ""
    looked = column_types.get(source_name)
    if looked is None:
        looked = column_types.get(source_name.lower(), "")
    return str(looked or "").strip()


def _row_projection_column_types(
    mappings: list[dict[str, Any]],
    column_types: Mapping[str, str],
    dest_types: Mapping[str, Any],
) -> dict[str, str]:
    """Keep BSON-date cells raw until the writer applies per-cell policy."""
    from services.type_system import LOGICAL_DATE, normalize_logical_type

    projected = dict(column_types)
    for mapping in mappings:
        target_name = _field(mapping.get("target") or mapping.get("source"))
        target_type = (
            dest_types.get(target_name)
            or dest_types.get(target_name.lower())
            or dest_types.get(target_name.upper())
            or mapping.get("target_type")
            or mapping.get("targetType")
            or ""
        )
        transform = str(mapping.get("transform") or "").strip().lower()
        if (
            normalize_logical_type(str(target_type)) == LOGICAL_DATE
            and transform in {"", "none", "null", "identity"}
        ):
            source_name = str(mapping.get("source") or "")
            if source_name:
                projected[source_name] = "VARCHAR"
    return projected


# MongoDB commands handle ~1000-document batches most reliably through proxies
# and serverless tiers. 20k-document single calls can hit socket/proxy limits.
MONGO_WRITE_BATCH_SIZE = int(getenv_brand("MONGO_BATCH_SIZE", "1000"))


def _fetch_mongo_physical_types(
    coll: Any,
    target_cols: list[str],
    *,
    sample_limit: int = 50,
) -> tuple[dict[str, str], Exception | None]:
    """Sample live BSON types for mapped fields (majority vote).

    Empty collections return ``({}, None)`` — Map stamps remain authoritative
    for create-new. Probe failures return ``({}, exc)`` so writers can
    fail-closed on auth (never soft-empty → Map invent).
    """
    from services.schema_introspect import (
        _finalize_mongodb_type,
        _sample_logical_type,
    )

    wanted = {str(c) for c in target_cols if c}
    if not wanted:
        return {}, None
    type_counts: dict[str, dict[str, int]] = {c: {} for c in wanted}
    try:
        for doc in coll.find().limit(int(sample_limit)):
            if not isinstance(doc, dict):
                continue
            # Mapped targets are field paths: ``address.city`` is read inside
            # the ``address`` sub-document, not as a top-level key.
            for key, val in path_view(doc, wanted).items():
                inferred = _sample_logical_type(val, key)
                if not inferred or val is None:
                    continue
                tc = type_counts[key]
                tc[inferred] = int(tc.get(inferred, 0)) + 1
    except Exception as exc:
        logger.debug("Mongo physical type sample failed", exc_info=True)
        return {}, exc
    physical: dict[str, str] = {}
    for col, counts in type_counts.items():
        if not counts:
            continue
        carrier = _finalize_mongodb_type(counts)
        if carrier:
            physical[col] = carrier
            physical.setdefault(col.lower(), carrier)
            physical.setdefault(col.upper(), carrier)
    return physical, None


def _absent_field_map_stamps(
    mapped_data_cols: list[str],
    physical: dict[str, str],
    live_dest: Any,
    target_cols: list[str],
    mappings: list,
    column_types: dict[str, str] | None,
    logical_types: list[str],
) -> dict[str, str]:
    """Map stamps for mapped fields no live probe or BSON sample carries."""
    from connectors.writer_common import resolve_mapping_dest_types

    live = live_dest if isinstance(live_dest, dict) else {}

    def _known(col: str, types: dict) -> bool:
        return any(
            str(types.get(k) or "").strip() for k in (col, col.lower(), col.upper())
        )

    absent = [
        c for c in mapped_data_cols if not _known(c, physical) and not _known(c, live)
    ]
    if not absent:
        return {}
    stamped = resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types or {},
        logical_types=logical_types,
        live_types=None,
        default="VARCHAR",
    )
    return {c: str(stamped.get(c) or "").strip() for c in absent if str(stamped.get(c) or "").strip()}


def _mongo_rematerialize_if_physical_differs(
    *,
    physical: dict[str, str],
    dest_types: dict[str, str],
    target_cols: list[str],
    headers: list[str],
    data_rows: list,
    mappings: list,
    column_types: dict[str, str] | None,
    logical_types: list[str],
    policy: Any,
    conflict_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
    force_remap: bool = False,
) -> tuple[list[tuple], list[str], list[dict], dict[str, str]] | None:
    """Rebuild mapped rows from source when live BSON carriers differ from Map.

    ``force_remap`` covers deferred Map under partial Studio (empty / invent risk).
    """
    from connectors.writer_common import rematerialize_live_dest_types

    live_dest_types = rematerialize_live_dest_types(
        physical, list(target_cols or []), product="MongoDB"
    )
    if live_dest_types is None:
        return None
    carriers_differ = any(
        str(dest_types.get(c) or "").strip().upper()
        != str(live_dest_types.get(c) or "").strip().upper()
        for c in target_cols
    )
    if not carriers_differ and not force_remap:
        return None
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=_row_projection_column_types(
            mappings, column_types or {}, live_dest_types
        ),
        dest_types=live_dest_types,
        error_policy=policy,
        preserve_case=True,
        dest_kind="mongodb",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=destination_column_nullability,
    )
    return (
        mapped_rows,
        list(transform_errors or []),
        rejected_details,
        live_dest_types,
    )


@dataclass
class WriteResult(_WriteResult):
    driver: str = "pymongo"


def _connection_string(
    host: str,
    port: int,
    username: str,
    password: str,
    connection_string: str,
    database: str = "",
    ssl: bool = False,
    auth_source: str = "",
) -> str:
    from connectors.mongodb_common import normalize_mongodb_connection_string

    return normalize_mongodb_connection_string(
        connection_string,
        database=database,
        host=host,
        port=port,
        username=username,
        password=password,
        ssl=ssl,
        auth_source=auth_source,
    )


def _idempotent_insert_many(coll, docs: list[dict]) -> int:
    """Insert documents; duplicate-key errors count as already-present.

    Missing ``_id`` is allowed — MongoDB assigns ObjectIds. We never invent a
    content-hash ``_id`` from row bytes (that collapsed natural keys and hid
    collisions). For deterministic identity, map ``_id`` or use upsert with
    ``conflict_columns`` (at-least-once insert may duplicate on replay).
    """
    from pymongo.errors import BulkWriteError

    # Defense-in-depth: never inject synthetic identity keys before insert.
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if _mongo_insert_id_is_null(doc):
            raise ValueError(
                "MongoDB insert refused null `_id` — map a real identity or omit "
                "`_id` for server-assigned ObjectId (refuse null PK invent)"
            )

    try:
        result = coll.insert_many(docs, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as bwe:
        details = bwe.details or {}
        write_errors = details.get("writeErrors", [])
        non_dup = [e for e in write_errors if e.get("code") != 11000]
        if non_dup:
            raise
        # All errors were duplicate keys; those rows are already present.
        return len(docs)


def _mongo_cancel_checkpoint(
    rows_written: int,
    *,
    source_rows_consumed: int,
    rejected_details: list[dict[str, Any]],
) -> None:
    """Poll the cancel flag between ``insert_many`` batches (QA r12 D5).

    MongoDB commits every batch on its own, so a cancelled chunk is a committed
    prefix nothing can roll back. The cancel therefore carries what that prefix
    covers — the source rows consumed through the last committed batch and the
    quarantine rows among them — for the engine to checkpoint, so Resume
    continues after the committed documents instead of refusing for want of a
    durable checkpoint (``services.job_status.checkpoint_partial_chunk_on_cancel``).
    """
    from services.cancel_check import cancel_checkpoint
    from services.error_handling import TransferCancelled

    try:
        cancel_checkpoint(rows_written)
    except TransferCancelled as exc:
        consumed = int(source_rows_consumed or 0)
        exc.chunk_source_rows_consumed = consumed  # type: ignore[attr-defined]
        exc.chunk_rejected_details = [  # type: ignore[attr-defined]
            d
            for d in rejected_details
            if isinstance(d, dict) and 0 < int(d.get("row") or 0) <= consumed
        ]
        raise


def _widen_sampled_mongo_decimals(dest_types, mappings, kwargs: dict) -> dict:
    """A sample DECIMAL(p,s) is not a Mongo catalog bound.

    Mongo stores Decimal128. Rejecting a later value because the first page
    fit DECIMAL(4,2) drops rows the document can hold. Operator overrides and
    a declared relational domain stay as stamped. Only decimal and float
    stamps are rewritten: a DATE target must stay a date when the source
    sample is text, or the document stores a string and drops the instant.
    """
    from services.decimal_observe import upgrade_sampled_decimal_stamp
    from services.dest_schema_authority import apply_sampled_profile_to_dest_types
    from services.type_system import normalize_logical_type

    original = {str(k): str(v) for k, v in dict(dest_types or {}).items()}
    numeric = {
        name: current
        for name, current in original.items()
        if normalize_logical_type(current) in {"decimal", "float"}
    }
    if not numeric:
        if type(dest_types) is dict or not dest_types:
            return original
        return type(dest_types)(original)
    profiled = apply_sampled_profile_to_dest_types(
        numeric, mappings, dest_db="mongodb"
    )
    by_target: dict[str, dict] = {}
    for mapping in mappings or []:
        if not isinstance(mapping, dict):
            continue
        target = str(mapping.get("target") or "").strip()
        if target:
            by_target[target] = mapping
            by_target.setdefault(target.casefold(), mapping)
    fallback_db = str(kwargs.get("source_db") or kwargs.get("source_format") or "")
    widened = dict(original)
    for name, current in profiled.items():
        mapping = by_target.get(name) or by_target.get(str(name).casefold()) or {}
        if mapping.get("user_override") or mapping.get("userOverride"):
            widened[name] = str(current)
            continue
        source_db = str(mapping.get("source_db") or fallback_db)
        widened[name] = upgrade_sampled_decimal_stamp(
            str(current),
            src_type=str(mapping.get("source_type") or current),
            dest_db="mongodb",
            source_db=source_db,
        )
    if type(dest_types) is dict or not dest_types:
        return widened
    return type(dest_types)(widened)


def _upsert_update(doc: dict, pk_cols: list[str]) -> dict:
    """The update document for one upserted row.

    ``_id`` never goes in ``$set``: MongoDB refuses any update that rewrites it
    (code 66), even to a numerically equal value of another BSON type (a stored
    ``NumberLong(5)`` matched by an int ``5``), which ``ReplaceOne`` accepted.
    As the conflict key it is only in the filter, which seeds it on insert; a
    mapped ``_id`` off the key is written on insert only (``$setOnInsert``), so
    a match keeps the stored ``_id`` and a differing value is left for Gate-8 to
    report instead of failing the batch.

    Dotted keys stay dotted (``{"$set": {"address.city": ...}}``): MongoDB
    then updates only that field inside the sub-document and keeps its
    siblings, the same G15 contract one level down.
    """
    if "_id" not in doc:
        return {"$set": doc}
    fields = {k: v for k, v in doc.items() if k != "_id"}
    update: dict = {"$set": fields} if fields else {}
    if "_id" not in pk_cols or not fields:
        update["$setOnInsert"] = {"_id": doc["_id"]}
    return update


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,  # usually not used in MongoDB, but we keep signature consistent
    connection_string: str,
    ssl: bool,
    table_name: str,  # collection name
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    backfill_new_fields: bool = False,
    auth_source: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del backfill_new_fields
    try:
        import pymongo
        # ``import pymongo`` does not load the errors submodule. An except
        # clause is evaluated while another exception is already active, so
        # ``pymongo.errors.PyMongoError`` raises AttributeError and hides the
        # original failure when this process has not imported the submodule yet.
        import pymongo.errors
        from pymongo.errors import PyMongoError
        from pymongo import MongoClient  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver, stub_writes_allowed
        from connectors.stub_writer import simulate_stub_write

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=schema or "db",
                checksum="", chunks_completed=0,
                error=require_driver("pymongo", "pymongo"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=schema or "db",
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=schema or "db",
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    target_cols, logical_types = resolve_target_columns(
        mappings,
        column_types,
        preserve_case=True,
        table_exists=False if create_table else None,
        identifier=_field,
    )
    path_error = target_refusal(target_cols)
    if path_error:
        # Refused, never renamed: Validate and Execute apply the same rule.
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=schema or "db",
            checksum="", chunks_completed=0, error=path_error,
        )
    if not target_cols:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "db",
            checksum="",
            chunks_completed=0,
            error="No column mappings",
        )

    collection_name = sanitize_identifier(table_name, preserve_case=True)
    db_name = database or schema or "test"
    # Prefer Studio-probed live field types over Map stamps (invent cliff).
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="MongoDB",
    )
    dest_types = _widen_sampled_mongo_decimals(dest_types, mappings, _kwargs)
    row_column_types = _row_projection_column_types(
        mappings, column_types, dest_types
    )
    policy = transform_error_policy(error_policy)

    try:
        from connectors.mongodb_common import _mongo_client

        conn_str = _connection_string(host, port, username, password, connection_string, database, ssl, auth_source)
        # Reuse a cached MongoClient per connection string to avoid paying the
        # connection handshake cost on every batch.
        client = _mongo_client(conn_str)

        db = client[db_name]
        try:
            existing = set(db.list_collection_names(filter={"name": collection_name}))
        except TypeError:
            # Older pymongo / limited clients without filter=.
            existing = {
                n for n in db.list_collection_names() if n == collection_name
            }
        except AttributeError:
            # Test doubles / stripped clients — allow create path; deny-create fails closed.
            if not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection_name,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        "MongoDB client cannot list collections to verify "
                        f"{collection_name!r} exists (create_table disabled)"
                    ),
                )
            existing = set()
        collection_existed = collection_name in existing
        if not create_table and not collection_existed:
            # Mongo creates collections on first write — deny-create must probe first.
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=(
                    f"MongoDB collection {collection_name!r} is missing "
                    "and create_table is disabled"
                ),
            )
        # Create-new: partial Studio must not soft-bind Map VARCHAR.
        if studio_err and not collection_existed:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=studio_err,
            )
        coll = db[collection_name]

        # Existing collection: sample BSON carriers and rematerialize when they
        # differ from Map/Studio stamps (VARCHAR→Decimal128 invent cliff).
        if collection_existed:
            from connectors.saas_common import is_auth_error

            physical, sample_exc = _fetch_mongo_physical_types(coll, target_cols)
            if sample_exc is not None and is_auth_error(sample_exc):
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection_name,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"MongoDB BSON sample auth failed: {sample_exc} — "
                        "refuse Map VARCHAR bind (empty→NULL invent risk)."
                    ),
                )
            mapped_data_cols = [c for c in target_cols if c and c != "_id"]
            if sample_exc is None and studio_err:
                # A field neither the live probe nor the BSON sample holds is a
                # new field (additive drift under propagate_columns): Map's
                # stamp is its only carrier, there is no physical one to
                # contradict. Treating it as a partial-Studio gap refused the
                # run (QA r12 D10).
                stamps = _absent_field_map_stamps(
                    mapped_data_cols, physical, live_dest, target_cols,
                    mappings, column_types, logical_types,
                )
                if stamps:
                    physical = {**physical, **stamps}
                    live_dest = {**dict(live_dest or {}), **stamps}
                    dest_types, studio_err = resolve_studio_or_map_dest_types(
                        target_cols,
                        mappings,
                        column_types,
                        logical_types=logical_types,
                        studio_types=live_dest,
                        product="MongoDB",
                    )
                    dest_types = _widen_sampled_mongo_decimals(dest_types, mappings, _kwargs)
                    row_column_types = _row_projection_column_types(
                        mappings, column_types, dest_types
                    )
            try:
                doc_count = int(coll.estimated_document_count())
            except Exception:
                try:
                    doc_count = int(coll.count_documents({}))
                except Exception:
                    doc_count = -1
            # Empty collection: Map stamps OK for first fill unless partial Studio
            # (no BSON to rematerialize — refuse Map VARCHAR invent for gaps).
            if studio_err and doc_count == 0:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection_name,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=studio_err,
                )
            # Non-empty / unknown count: every mapped field needs BSON sample or
            # Studio carrier (partial sample must not leave Map VARCHAR invent gaps).
            if mapped_data_cols and (doc_count > 0 or doc_count < 0):
                from connectors.writer_common import require_physical_types_for_existing_table

                effective_physical = dict(physical)
                if isinstance(live_dest, dict):
                    for c in mapped_data_cols:
                        if effective_physical.get(c) or effective_physical.get(
                            str(c).lower()
                        ) or effective_physical.get(str(c).upper()):
                            continue
                        st = str(live_dest.get(c) or "").strip()
                        if st:
                            effective_physical[c] = st
                phys_err = require_physical_types_for_existing_table(
                    table_existed=True,
                    physical=effective_physical,
                    dialect_label="MongoDB",
                    target_cols=mapped_data_cols,
                    destination_kind="mongodb",
                    physical_sampled=sample_exc is None,
                )
                if phys_err:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=collection_name,
                        target_schema=db_name,
                        checksum="",
                        chunks_completed=0,
                        error=phys_err,
                    )
                physical = effective_physical
            _force_remap = bool(studio_err)
            remat = _mongo_rematerialize_if_physical_differs(
                physical=physical,
                dest_types=dest_types,
                target_cols=target_cols,
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                column_types=column_types,
                logical_types=logical_types,
                policy=policy,
                conflict_columns=conflict_columns,
                destination_column_nullability=_kwargs.get(
                    "destination_column_nullability"
                ),
                force_remap=_force_remap,
            )
            if remat is not None:
                mapped_rows, transform_errors, rejected_details, dest_types = remat
            elif _force_remap:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=collection_name,
                    target_schema=db_name,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        "MongoDB live BSON incomplete for mapped fields — "
                        "refuse Map VARCHAR rematerialize invent. Re-run "
                        "destination schema introspect and retry."
                    ),
                )
            else:
                mapped_rows, transform_errors, rejected_details = (
                    build_mapped_rows_with_details(
                        headers=headers,
                        data_rows=data_rows,
                        mappings=mappings,
                        target_cols=target_cols,
                        column_types=row_column_types,
                        dest_types=dest_types,
                        error_policy=policy,
                        preserve_case=True,
                        dest_kind="mongodb",
                        destination_pk_columns=list(conflict_columns or []) or None,
                        destination_column_nullability=_kwargs.get(
                            "destination_column_nullability"
                        ),
                    )
                )
        else:
            mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=row_column_types,
                dest_types=dest_types,
                error_policy=policy,
                preserve_case=True,
                dest_kind="mongodb",
                destination_pk_columns=list(conflict_columns or []) or None,
                destination_column_nullability=_kwargs.get("destination_column_nullability"),
            )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)

        tgt_types = [str(dest_types.get(c) or "").strip() for c in target_cols]
        from connectors.writer_common import apply_write_quarantine_matrix

        mapped_rows = apply_write_quarantine_matrix(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="MongoDB",
            mappings=mappings,
        )
        rejected_rows = _rejected_row_count(data_rows, mapped_rows, rejected_details, policy)
        # FAIL_JOB / strict abort after matrix — matrix may add abort-class rejects.
        _map_abort = reject_on_strict_policy(policy, rejected_details, "MongoDB", transform_errors)
        if _map_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
                rejected_rows=rejected_rows,
                rejected_details=list(rejected_details),
                warnings=transform_errors,
            )

        from datetime import date as _date
        from datetime import datetime as _datetime

        from bson.decimal128 import Decimal128

        # The write's own resolver, with the destination known: on BSON the
        # ``date`` token is the instant carrier, so a calendar-day cast stamped
        # before the destination was known must not pick the midnight branch
        # below (D232, D278). Same SSOT build_mapped_rows binds through.
        from services.transform_resolver import resolve_transform

        transform_by_col = {
            _field(m.get("target") or m.get("source")): (
                resolve_transform(m, column_types=column_types, dest_kind="mongodb")
                if m.get("transform")
                else ""
            )
            for m in mappings
        }
        # Columns whose operator accepted the UTC-normalize risk. BSON date is an
        # instant, so a zoneless source has to be stamped with *some* zone; the
        # refusal below is of a *silent* choice, not of the choice itself.
        # ``resolve_timezone_policy`` names this POLICY_UTC_INVENT and marks it
        # requires_contract, so an acknowledged column is exactly the case the
        # policy says may proceed — and it is recorded on the summary.
        utc_normalize_ack = {
            _field(m.get("target") or m.get("source"))
            for m in mappings
            if m.get("risk_acknowledged") or m.get("riskAcknowledged")
        }
        utc_normalized_cols: set[str] = set()
        # Columns the source declares as a calendar day. A date has no time of
        # day and therefore no zone to invent: UTC midnight is the one instant
        # every driver reads back as the same date. Refusing it as "naive"
        # blocked plain DATE columns behind a contract that answers a question
        # the value never raised.
        calendar_day_cols = {
            _field(m.get("target") or m.get("source"))
            for m in mappings
            if _declares_calendar_day(m, _mapping_source_type(m, column_types))
        }
        from services.type_system import (
            LOGICAL_DATETIME,
            LOGICAL_DATE,
            LOGICAL_TIME,
            normalize_logical_type,
        )

        pure_date_target_cols = set()
        for mapping in mappings:
            source_type = _mapping_source_type(mapping, column_types)
            source_logical = normalize_logical_type(source_type)
            target_name = _field(mapping.get("target") or mapping.get("source"))
            target_logical = normalize_logical_type(
                str(
                    dest_types.get(target_name)
                    or mapping.get("target_type")
                    or mapping.get("targetType")
                    or ""
                )
            )
            if (
                source_logical not in {LOGICAL_DATETIME, LOGICAL_TIME}
                and target_logical == LOGICAL_DATE
            ):
                pure_date_target_cols.add(
                    _field(mapping.get("target") or mapping.get("source"))
                )
        calendar_day_truncation_contract_cols = set()
        from services.migration_risk_contract import mapping_has_clearing_risk_contract

        for mapping in mappings:
            column = _field(mapping.get("target") or mapping.get("source"))
            contract = mapping.get("risk_contract") or mapping.get("riskContract")
            if (
                column in calendar_day_cols
                and isinstance(contract, dict)
                and mapping_has_clearing_risk_contract(mapping)
                and (
                    contract.get("expected_precision_loss")
                    or contract.get("expected_truncation")
                )
            ):
                calendar_day_truncation_contract_cols.add(column)
        # Polarity is a property of the column, not the cell. Resolving it
        # once keeps a million-row write off the DDL regex.
        instant_cols = {
            _field(m.get("target") or m.get("source"))
            for m in mappings
            if declared_source_is_instant(_mapping_source_type(m, column_types))
        }

        def _to_bson(
            value: Any, stype: str, transform: str = "", column: str = ""
        ) -> Any:
            from services.value_serializer import absent_sql_bind, is_missing_sentinel

            # Preserve DF_MISSING through coercion so sparse upsert can omit
            # the field — never convert the sentinel into a live BSON value.
            if is_missing_sentinel(value):
                return value
            handled, bound = absent_sql_bind(value)
            if handled:
                return bound
            from services.value_serializer import document_bind_value

            value = document_bind_value(value)
            upper = stype.upper()
            # An explicit mapping transform overrides the inferred source type so
            # values like decimal strings are stored as the correct BSON type.
            if upper in {"FLOAT", "DOUBLE", "FLOAT64", "REAL", "FLOAT32", "FLOAT16"}:
                from connectors.sql_bind import coerce_float_wire

                return coerce_float_wire(value, ddl_type=upper or "FLOAT")
            if transform in {"decimal", "currency", "percentage"} or upper in {"DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC"}:
                if isinstance(value, str) and not str(value).strip():
                    raise ValueError(
                        f"MongoDB DECIMAL refused empty string {value!r} "
                        "(refuse silent Decimal128 invent / field wipe)"
                    )
                from connectors.sql_bind import coerce_decimal_wire

                coerced = coerce_decimal_wire(value, ddl_type=upper or "DECIMAL")
                return Decimal128(str(coerced))
            if transform == "integer" or upper in {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SERIAL", "BIGSERIAL"}:
                from decimal import Decimal

                if isinstance(value, bool):
                    iv = int(value)
                elif isinstance(value, int):
                    iv = value
                elif isinstance(value, float):
                    if not value.is_integer():
                        raise ValueError(
                            f"cannot coerce non-integral float {value!r} to INTEGER "
                            "without truncation"
                        )
                    iv = int(value)
                elif isinstance(value, Decimal):
                    if value != value.to_integral_value():
                        raise ValueError(
                            f"cannot coerce non-integral decimal {value!r} to INTEGER "
                            "without truncation"
                        )
                    iv = int(value)
                elif isinstance(value, str):
                    try:
                        iv = int(value)
                    except (ValueError, TypeError) as exc:
                        raise ValueError(
                            f"MongoDB INTEGER refused {value!r} "
                            "(refuse silent string invent)"
                        ) from exc
                else:
                    try:
                        iv = int(value)
                    except (ValueError, TypeError) as exc:
                        raise ValueError(
                            f"MongoDB INTEGER refused {value!r} "
                            "(refuse silent pass-through invent)"
                        ) from exc
                # BSON supports signed 64-bit ints; fall back to Decimal128 or
                # string when a value overflows.
                if iv > 2**63 - 1 or iv < -(2**63):
                    try:
                        return Decimal128(str(iv))
                    except (InvalidOperation, ValueError, TypeError):
                        return str(iv)
                return iv
            if transform == "boolean" or upper in {"BOOLEAN", "BOOL"}:
                from connectors.sql_bind import coerce_boolean_wire

                coerced = coerce_boolean_wire(value, as_int=False)
                if not isinstance(coerced, bool):
                    raise ValueError(
                        f"MongoDB BOOLEAN refused {value!r} "
                        "(refuse invent via pass-through)"
                    )
                return coerced
            if _target_is_temporal(upper) and (
                transform_narrows_to_calendar_day(transform)
                or column in calendar_day_cols
                or (
                    column in pure_date_target_cols
                    and _is_pure_date_value(value)
                )
            ):
                from connectors.sql_temporal import (
                    coerce_sql_temporal,
                    parse_sql_date,
                    parse_sql_datetime,
                )
                from datetime import timezone as _tz

                parsed = parse_sql_datetime(value, wall_clock=True)
                if (
                    parsed is not None
                    and any(
                        (
                            parsed.hour,
                            parsed.minute,
                            parsed.second,
                            parsed.microsecond,
                        )
                    )
                    and column in calendar_day_cols
                    and column not in calendar_day_truncation_contract_cols
                ):
                    raise ValueError(
                        "non-midnight value for logical DATE would truncate "
                        "time-of-day — refuse silent truncation without a "
                        "covering Risk Contract"
                    )
                if parsed is None and parse_sql_date(value) is None:
                    raise ValueError(
                        f"cannot parse {value!r} as a pure logical DATE"
                    )
                coerced = coerce_sql_temporal(value, "DATE")
                # DATE → BSON Date as UTC midnight (calendar date instant), never
                # leave naive for PyMongo local-TZ invent.
                if coerced is None:
                    return None
                if isinstance(coerced, _datetime):
                    d = coerced.date()
                    return _datetime(d.year, d.month, d.day, tzinfo=_tz.utc)
                if isinstance(coerced, _date):
                    return _datetime(
                        coerced.year, coerced.month, coerced.day, tzinfo=_tz.utc
                    )
                raise ValueError(
                    f"MongoDB DATE refused {value!r} (refuse silent pass-through invent)"
                )
            # A "DATE" carrier with a datetime transform is Mongo's single BSON
            # date, which stores a full instant. Narrowing it truncated every
            # timestamp to midnight even though the carrier could hold the time.
            # Every date/datetime spelling names BSON's one carrier —
            # ``TIMESTAMPTZ(6)`` / ``DATETIME2(7)`` echoed from the source
            # included. Matching only the bare tokens wrote those as strings.
            if _target_is_temporal(upper):
                from connectors.sql_temporal import coerce_sql_temporal

                # BSON Date is a UTC instant. Coerce as TIMESTAMPTZ so ISO-Z keeps
                # tzinfo — DATETIME would strip offset to naive then falsely refuse.
                try:
                    coerced = coerce_sql_temporal(value, "TIMESTAMPTZ")
                except ValueError:
                    coerced = coerce_sql_temporal(value, "DATETIME")
                if coerced is None:
                    return None
                if isinstance(coerced, _datetime):
                    # A declared instant (TIMESTAMPTZ / TIMESTAMP_TZ / LTZ) that
                    # arrives naive already had its offset stripped by the
                    # driver. Stamp UTC and keep the clock. A wall-clock source
                    # still needs an accepted UTC-normalize risk — stamping one
                    # quietly would shift polarity.
                    if coerced.tzinfo is None:
                        from datetime import timezone as _tzu

                        if column in instant_cols:
                            coerced = coerced.replace(tzinfo=_tzu.utc)
                        elif column not in utc_normalize_ack:
                            raise ValueError(
                                "MongoDB date/time refused naive wall-clock — "
                                "provide offset/Z, or accept the UTC-normalize "
                                "risk on this column (refuse silent UTC invent)"
                            )
                        else:
                            utc_normalized_cols.add(column)
                            coerced = coerced.replace(tzinfo=_tzu.utc)
                    # BSON date counts milliseconds; the driver drops anything
                    # finer without a word. Judge the raw cell so a seven-digit
                    # string is not first truncated to Python's six.
                    # The batch guard below already refused an unacknowledged
                    # one; this stays as the per-cell backstop.
                    if cell_exceeds_document_instant(value) and column not in ms_ack_cols:
                        raise ValueError(
                            f"MongoDB date refused {value!r}: BSON date keeps "
                            "milliseconds and this value has finer digits — map "
                            "to a string, or accept the millisecond-truncation "
                            "risk on this column (refuse silent truncation)"
                        )
                    from datetime import timezone as _tz

                    return coerced.astimezone(_tz.utc)
                raise ValueError(
                    f"MongoDB DATETIME refused {value!r} "
                    "(refuse silent pass-through invent)"
                )
            if upper in {"BINARY", "BYTEA", "BLOB", "VARBINARY"}:
                from connectors.sql_bind import coerce_binary_wire
                from bson.binary import Binary as _Bin

                if isinstance(value, bytes):
                    return _Bin(value)
                # Fail-closed — never UTF-8-invent bytes.
                return _Bin(coerce_binary_wire(value))
            if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
                from services.json_number_capacity import json_number_refusal

                # D206: BSON has no int past int64 and Decimal128 stops at 34
                # digits (wider fell back to a string). Refuse, never round.
                refusal = json_number_refusal(value, engine="mongodb", ddl_type=upper)
                if refusal:
                    raise ValueError(refusal)
                return bind_mongo_json_document(value)
            if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
                from connectors.sql_bind import coerce_uuid_wire

                return coerce_uuid_wire(value)
            if upper in {"OBJECTID", "OBJECT_ID"}:
                from bson.objectid import ObjectId as _Oid

                if isinstance(value, _Oid):
                    return value
                text = str(value).strip()
                if len(text) == 24 and _Oid.is_valid(text):
                    return _Oid(text)
                raise ValueError(
                    f"cannot coerce {value!r} to MongoDB ObjectId "
                    "(expect 24-char hex)"
                )
            if upper == "TIME":
                from connectors.sql_temporal import bind_time_iso

                return bind_time_iso(value)
            return value

        # BSON coercion is fail-closed (ObjectId hex, non-integral ints, binary
        # wire). Letting a ValueError escape aborted the whole write with
        # rows_written=0, so one bad cell lost every good row. Hold the row out
        # and record why instead — quarantine, never silent drop, never abort.
        from connectors.writer_common import (
            append_write_quarantine_detail,
            partition_dense_upsert_rows,
        )
        from services.value_serializer import cell_to_string as _cell_to_string

        if write_mode == "upsert" and conflict_columns:
            pk_for_part = [str(c) for c in conflict_columns if str(c) and str(c) in target_cols]
            if pk_for_part:
                # Partition before BSON coerce / same-key fold — empty keys must
                # never collapse onto None/"" and mass-touch destination docs.
                mapped_rows = partition_dense_upsert_rows(
                    mapped_rows,
                    pk_for_part,
                    target_cols=target_cols,
                    rejected_details=rejected_details,
                    policy=policy,
                )

        # D148: BSON date keeps milliseconds. Every guarded cell is checked
        # before anything is bound; an unacknowledged sub-millisecond value
        # refuses the whole batch — never a partial write, never a truncation.
        from services.millisecond_fit import (
            TRUNCATED_KEY,
            guard_batch,
            narrowing_acknowledged,
            truncation_warnings,
        )

        ms_ack_cols = {
            _field(m.get("target") or m.get("source"))
            for m in mappings
            if narrowing_acknowledged(m)
        }
        ms_guard = guard_batch(
            mapped_rows,
            target_cols,
            [
                c
                for c, t in zip(target_cols, tgt_types)
                if _target_is_temporal(t)
                and c not in calendar_day_cols
                and not transform_narrows_to_calendar_day(transform_by_col.get(c, ""))
            ],
            acknowledged=ms_ack_cols,
            carrier_label="MongoDB date",
        )
        if ms_guard.refusal:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=ms_guard.refusal,
                rejected_rows=rejected_rows,
                rejected_details=list(rejected_details),
                warnings=transform_errors,
            )

        typed_rows: list[tuple] = []
        # 1-based source row of each typed row: hold-outs leave gaps, and a
        # cancel reports the source rows its committed batches covered.
        typed_src_rows: list[int] = []
        for row_idx, row in enumerate(mapped_rows):
            cells: list[Any] = []
            hold_out = False
            # Coerce with live-aware dest types (tgt_types), not Map-only
            # logical_types — otherwise quarantine can pass live DDL while
            # BSON invents a different wire shape (Bugbot invent cliff).
            for i, (v, t) in enumerate(zip(row, tgt_types)):
                col = target_cols[i] if i < len(target_cols) else f"col_{i}"
                try:
                    cells.append(
                        _to_bson(v, t, transform_by_col.get(col, ""), column=col)
                    )
                except (ValueError, TypeError, InvalidOperation) as exc:
                    append_write_quarantine_detail(
                        rejected_details,
                        {
                            "row": row_idx + 1,
                            "column": col,
                            "target": col,
                            "value": _cell_to_string(v)[:120],
                            "reason": f"BSON coercion rejected the value — {exc}",
                            "policy": "coerce_null" if policy == "coerce_null" else "write_quarantine",
                            "chars": [],
                        },
                        mapped_row=list(row),
                        target_cols=target_cols,
                    )
                    if policy == "coerce_null":
                        cells.append(None)
                        continue
                    hold_out = True
                    break
            if hold_out:
                continue
            typed_rows.append(tuple(cells))
            typed_src_rows.append(row_idx + 1)

        _bson_abort = reject_on_strict_policy(
            policy, rejected_details, "MongoDB", transform_errors=None
        )
        if _bson_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=0,
                error=_bson_abort,
                rejected_rows=_rejected_row_count(
                    data_rows, typed_rows, rejected_details, policy
                ),
                rejected_details=rejected_details,
            )
        rejected_rows = _rejected_row_count(data_rows, typed_rows, rejected_details, policy)

        total = len(typed_rows)
        # MongoDB writes are split into smaller server-friendly batches.
        mongo_batch_size = max(1, min(MONGO_WRITE_BATCH_SIZE, CHUNK_SIZE))
        chunks = max(1, (total + mongo_batch_size - 1) // mongo_batch_size)
        written = 0
        skipped_total = 0
        # Older images of a key this upsert kept the latest of (E2-023): Gate-8
        # expects one row per key, not every version read.
        collapsed_total = 0
        key_index_ready = False
        from services.duplicate_policy import current_match_action

        # skip_duplicates (dedupe_skip): insert absent keys only; kept keys are
        # rows_skipped with a Gate-8 marker, never counted as written.
        skip_stored = current_match_action() == "skip"
        kept_markers: list[str] = []

        for chunk_idx in range(chunks):
            start = chunk_idx * mongo_batch_size
            batch = typed_rows[start : start + mongo_batch_size]
            if not batch:
                break
            # QA r12 D5: stop the NEXT insert_many when the job was cancelled,
            # as the SQL writers do (D102 / D168). One engine chunk is twenty
            # 1,000-document batches; without this a cancelled job kept
            # writing to the end of the chunk. ``written`` is what this call
            # already committed, so the cancel reports it (D199 then counts
            # the collection itself).
            _mongo_cancel_checkpoint(
                written + skipped_total,
                source_rows_consumed=typed_src_rows[start - 1] if start else 0,
                rejected_details=rejected_details,
            )

            # Convert row tuples to documents; omit Missing, bind reader-null
            # as None (shared present_field_bindings).
            from connectors.writer_common import present_field_bindings

            docs: list[dict[str, Any]] = [
                present_field_bindings(dict(zip(target_cols, row))) for row in batch
            ]

            # Preserve MongoDB ObjectId identity when a 24-char hex _id is present.
            from bson.objectid import ObjectId

            for doc in docs:
                if "_id" in doc and isinstance(doc["_id"], str):
                    v = doc["_id"]
                    if len(v) == 24 and ObjectId.is_valid(v):
                        try:
                            doc["_id"] = ObjectId(v)
                        except Exception as exc:
                            logger.warning("Exception suppressed: %s", exc, exc_info=exc)

            if write_mode == "upsert" and conflict_columns:
                from pymongo import UpdateOne
                from pymongo.errors import BulkWriteError

                requested_keys = [str(c) for c in conflict_columns if str(c)]
                pk_cols = [c for c in requested_keys if c in target_cols]
                missing_keys = [c for c in requested_keys if c not in target_cols]
                if not pk_cols or missing_keys:
                    return WriteResult(
                        ok=False,
                        rows_written=written,
                        table_name=collection_name,
                        target_schema=db_name,
                        checksum="",
                        chunks_completed=chunk_idx,
                        error=(
                            "MongoDB upsert requires mapped conflict columns; missing: "
                            f"{missing_keys or requested_keys}"
                        ),
                        rejected_rows=len(rejected_details),
                        rejected_details=list(rejected_details),
                        warnings=transform_errors,
                    )

                if not key_index_ready:
                    # Every upsert filters on the conflict key, and the CDC LSN
                    # prefetch queries it too. Without an index on those fields
                    # each one is a collection scan, so throughput decays as the
                    # collection grows (measured: 598 rows/s at 3K down to ~31
                    # rows/s at 60K). Creating it is idempotent server-side.
                    # On a collection this run creates, the index is made UNIQUE:
                    # the upsert merge is then server-enforced unique on the key,
                    # which is what the D101 destination-key gate verifies on the
                    # next run. On an operator's existing collection it stays a
                    # non-unique throughput index — declaring uniqueness there is
                    # undisclosed DDL that must be approved at Validate, and can
                    # fail on existing duplicates. A collection handle that
                    # cannot create one (a restricted role, or a driver stand-in
                    # without the method) must not fail the write.
                    try:
                        coll.create_index(
                            [(c, pymongo.ASCENDING) for c in pk_cols],
                            unique=not collection_existed,
                        )
                    except (PyMongoError, AttributeError) as exc:
                        logger.warning(
                            "could not index Mongo conflict key %s on %s: %s",
                            pk_cols,
                            collection_name,
                            exc,
                        )
                    key_index_ready = True

                # Deduplicate within the batch on the conflict key, keeping the
                # highest _df_lsn so ``bulk_write(ordered=False)`` does not apply
                # same-PK updates in an undefined order.
                kept: dict[tuple, dict[str, Any]] = {}
                for doc in docs:
                    key = _mongo_conflict_key(doc, pk_cols)
                    prev = kept.get(key)
                    if skip_stored and prev is not None:
                        continue  # skip_duplicates keeps the first image of a key
                    if (
                        DF_LSN_COL not in target_cols
                        or prev is None
                        or compare_lsn(doc.get(DF_LSN_COL), prev.get(DF_LSN_COL)) >= 0
                    ):
                        kept[key] = doc
                if write_mode == "upsert" and pk_cols:
                    collapsed_total += max(0, len(docs) - len(kept))
                docs = list(kept.values())

                # Pre-fetch existing _df_lsn values for the batch keys so stale
                # redelivery cannot regress destination state under CDC.
                existing_lsn: dict[tuple, Any] = {}
                use_lsn_guard = DF_LSN_COL in target_cols
                if use_lsn_guard:
                    try:
                        batch_filters = []
                        for doc in docs:
                            filt = {c: doc.get(c) for c in pk_cols}
                            if not _mongo_incomplete_pk_cols(filt, pk_cols):
                                batch_filters.append(filt)
                        if batch_filters:
                            projection = {DF_LSN_COL: 1}
                            projection.update({c: 1 for c in pk_cols})
                            for existing in coll.find({"$or": batch_filters}, projection):
                                key = _mongo_conflict_key(existing, pk_cols)
                                existing_lsn[key] = existing.get(DF_LSN_COL)
                    except PyMongoError as exc:
                        # Fail closed: without the prior LSN map we cannot prove
                        # idempotency, so we must not write this batch.
                        return WriteResult(
                            ok=False,
                            rows_written=written,
                            rows_skipped=skipped_total,
                            table_name=collection_name,
                            target_schema=db_name,
                            checksum="",
                            chunks_completed=chunk_idx,
                            error=(
                                "MongoDB CDC LSN prefetch failed; cannot guarantee "
                                f"idempotent delivery: {exc}"
                            ),
                            rejected_rows=len(rejected_details),
                            rejected_details=list(rejected_details),
                            warnings=transform_errors,
                        )

                ops = []
                op_keys: list[tuple] = []
                skipped_stale = 0
                for doc_idx, doc in enumerate(docs):
                    filt = {c: doc.get(c) for c in pk_cols}
                    missing_cols = _mongo_incomplete_pk_cols(filt, pk_cols)
                    if missing_cols:
                        detail = {
                            "row": start + doc_idx + 1,
                            "column": ",".join(missing_cols),
                            "target": ",".join(pk_cols),
                            "value": "",
                            "reason": (
                                "Mongo upsert skipped — incomplete conflict key "
                                f"{missing_cols}; refuse silent omit"
                            ),
                            "policy": "write_fail" if policy == "fail" else "write_quarantine",
                            "chars": [],
                        }
                        rejected_details.append(detail)
                        if policy == "fail":
                            return WriteResult(
                                ok=False,
                                rows_written=written,
                                table_name=collection_name,
                                target_schema=db_name,
                                checksum="",
                                chunks_completed=chunk_idx,
                                error=detail["reason"],
                                rejected_rows=len(rejected_details),
                                rejected_details=list(rejected_details),
                                warnings=transform_errors,
                            )
                        continue
                    if use_lsn_guard:
                        incoming_lsn = doc.get(DF_LSN_COL)
                        key = _mongo_conflict_key(doc, pk_cols)
                        prior_lsn = existing_lsn.get(key)
                        if incoming_lsn is not None and not lsn_is_newer(incoming_lsn, prior_lsn):
                            skipped_stale += 1
                            continue
                    # $set the mapped fields, never ReplaceOne: replacing the
                    # document deletes every field this mapping does not write
                    # (the client's own fields, an earlier mapping's fields, or
                    # fields a partial CDC image omits). Same contract as the
                    # SQL upsert (G15): dest-only columns stay off SET. A NULL
                    # mapped value is written as null.
                    # skip_duplicates: $setOnInsert writes only when no document
                    # matches the key — a stored document is left as stored.
                    ops.append(
                        UpdateOne(
                            filt,
                            {"$setOnInsert": doc} if skip_stored else _upsert_update(doc, pk_cols),
                            upsert=True,
                        )
                    )
                    op_keys.append(tuple(filt[c] for c in pk_cols))
                outcome = None
                if ops:
                    try:
                        outcome = coll.bulk_write(ops, ordered=False)
                    except BulkWriteError as exc:
                        # Unordered: every other operation was applied. Count
                        # them before the batch error fails the write.
                        details = exc.details or {}
                        if skip_stored:
                            # A matched key was kept as stored, not written.
                            failed = {int(e.get("index", -1)) for e in details.get("writeErrors") or []}
                            inserted = {int(u.get("index", -1)) for u in details.get("upserted") or []}
                            held = [
                                op_keys[n]
                                for n in range(len(ops))
                                if n not in inserted and n not in failed
                            ]
                            kept_markers.extend(identity_marker(k) for k in held)
                            written += int(details.get("nUpserted") or 0)
                            skipped_total += skipped_stale + len(held)
                        else:
                            written += int(details.get("nUpserted") or 0) + int(
                                details.get("nMatched") or 0
                            )
                        raise
                if skip_stored and outcome is not None:
                    inserted = set((getattr(outcome, "upserted_ids", None) or {}).keys())
                    held = [op_keys[n] for n in range(len(ops)) if n not in inserted]
                    kept_markers.extend(identity_marker(k) for k in held)
                    skipped_stale += len(held)
                    written -= len(held)
                written += len(ops)
                skipped_total += skipped_stale
            else:
                # Insert (and overwrite, which recreates then inserts): dotted
                # targets become sub-documents. Upsert keeps them dotted in
                # $set above, so sibling fields inside a sub-document survive.
                written += _idempotent_insert_many(
                    coll, [insert_document(doc) for doc in docs]
                )
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written + skipped_total)

        # Re-check FAIL_JOB / strict after mid-write incomplete-PK quarantine.
        _final_abort = reject_on_strict_policy(
            policy, rejected_details, "MongoDB", transform_errors
        )
        if _final_abort:
            return WriteResult(
                ok=False,
                # Honest count: earlier chunks may already be committed.
                rows_written=written,
                rows_skipped=skipped_total,
                table_name=collection_name,
                target_schema=db_name,
                checksum="",
                chunks_completed=chunks,
                error=_final_abort,
                rejected_rows=len(rejected_details),
                rejected_details=list(rejected_details),
                coerced_null_rows=coerced_null_rows,
                warnings=transform_errors,
            )
        return WriteResult(
            ok=True,
            rows_written=written,
            rows_skipped=skipped_total,
            table_name=collection_name,
            target_schema=db_name,
            checksum=row_checksum(
                mapped_rows,
                target_cols,
                dest_db_type="mongodb",
            ),
            chunks_completed=chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written - skipped_total),
            rejected_details=list(rejected_details),
            coerced_null_rows=coerced_null_rows,
            warnings=[
                *transform_errors,
                *(
                    f"{col}: zoneless values written as UTC under the accepted "
                    "UTC-normalize risk"
                    for col in sorted(utc_normalized_cols)
                ),
                *truncation_warnings(ms_guard.truncated, "BSON"),
                # G11: the operator's legacy_field_policy=write_new, on the run.
                *(str(a) for a in (_kwargs.get("legacy_field_ack") or [])),
            ],
            meta={
                **gate8_writer_meta(mapped_rows, target_cols),
                **({"collapsed_duplicate_rows": collapsed_total} if collapsed_total else {}),
                **({TRUNCATED_KEY: dict(ms_guard.truncated)} if ms_guard.truncated else {}),
                **({"skipped_identity_keys": kept_markers} if kept_markers else {}),
            },
        )
    except (PyMongoError, ValueError, TypeError, KeyError, OSError) as exc:
        return WriteResult(
            ok=False,
            rows_written=written if "written" in locals() else 0,
            rows_skipped=skipped_total if "skipped_total" in locals() else 0,
            meta=(
                {"skipped_identity_keys": list(kept_markers)}
                if locals().get("kept_markers")
                else {}
            ),
            table_name=table_name,
            target_schema=schema or "db",
            checksum="",
            chunks_completed=0,
            error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
            warnings=transform_errors if "transform_errors" in locals() else [],
        )
