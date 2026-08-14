"""Elasticsearch index writer — bulk indexing."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import json_default

from connectors.elasticsearch_reader import _client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    gate8_writer_meta,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)

logger = logging.getLogger(__name__)


def _fetch_es_physical_types(
    client: Any, index: str, target_cols: list[str]
) -> tuple[dict[str, str], Exception | None]:
    """Read committed index mapping → logical carriers for rematerialize.

    ``get_mapping`` responses are keyed by concrete index names; aliases resolve
    to those keys — never assume ``mapping[alias]`` exists.

    Returns ``(physical, None)`` on success or ``({}, exc)`` on probe failure
    so callers can fail-closed on auth (never soft-empty → Map invent).
    """
    from services.schema_introspect import _es_mapping_type

    wanted = {str(c) for c in target_cols if c}
    if not wanted:
        return {}, None
    try:
        mapping = client.indices.get_mapping(index=index)
    except Exception as exc:
        logger.debug("Elasticsearch get_mapping failed for %s", index, exc_info=True)
        return {}, exc
    props: dict = {}
    if isinstance(mapping, dict):
        # Prefer exact name, then any concrete index body (alias → real index).
        body = mapping.get(index)
        if isinstance(body, dict):
            props = (body.get("mappings") or {}).get("properties") or {}
        if not props:
            for _concrete, body in mapping.items():
                if not isinstance(body, dict):
                    continue
                candidate = (body.get("mappings") or {}).get("properties") or {}
                if candidate:
                    props = candidate
                    break
    physical: dict[str, str] = {}
    for name, info in (props or {}).items():
        if name not in wanted:
            continue
        if not isinstance(info, dict):
            info = {"type": "text"}
        es_type = str(info.get("type") or "text")
        if es_type == "nested":
            carrier = "ARRAY<JSON>"
        elif es_type == "object" or (not es_type and info.get("properties")):
            carrier = "JSON"
        else:
            carrier = _es_mapping_type(es_type)
        physical[name] = carrier
        physical.setdefault(name.lower(), carrier)
        physical.setdefault(name.upper(), carrier)
    return physical, None


def _es_rematerialize_if_physical_differs(
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
    """Rebuild mapped rows when live mapping carriers differ from Map stamps.

    ``force_remap`` covers deferred Map under partial Studio (invent risk).
    Requires full live coverage of ``target_cols`` (no Map VARCHAR gap-fill).
    Callers may overlay known dynamic-mapping props onto ``dest_types`` first.
    """
    from connectors.writer_common import rematerialize_live_dest_types

    live_dest_types = rematerialize_live_dest_types(
        physical, list(target_cols or []), product="Elasticsearch"
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
        column_types=column_types,
        dest_types=live_dest_types,
        preserve_case=True,
        error_policy=policy,
        dest_kind="elasticsearch",
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
    driver: str = "elasticsearch-py"


def _to_es_value(value: Any, source_type: str) -> Any:
    """Convert transform-engine values to Elasticsearch-native JSON shapes.

    Binary is stored as base64 text (JSON-safe); invalid base64 / UUID raise
    ValueError so the writer can quarantine — never invent UTF-8 bytes or a
    random UUID (Airbyte/Fivetran fail-closed class).
    """
    from services.value_serializer import is_missing_sentinel

    # STOP_COLUMN / coerce_null omit — callers must skip projecting this cell.
    # Returning None here would write JSON null and wipe prior _source fields.
    if is_missing_sentinel(value):
        raise ValueError("DF_MISSING must be omitted from Elasticsearch _source")
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DECIMAL", "NUMERIC", "NUMBER", "BIGNUMERIC"}:
        from connectors.sql_bind import coerce_decimal_wire

        if isinstance(value, str) and not str(value).strip():
            raise ValueError(
                f"Elasticsearch DECIMAL refused empty string {value!r} "
                "(refuse silent null invent / field wipe)"
            )
        # Keep as string — float64 would silently lose precision (no quarantine).
        return str(coerce_decimal_wire(value, ddl_type=upper))
    if upper in {"FLOAT", "DOUBLE", "FLOAT64", "REAL"}:
        from connectors.sql_bind import coerce_float_wire

        if isinstance(value, str) and not str(value).strip():
            raise ValueError(
                f"Elasticsearch FLOAT refused empty string {value!r} "
                "(refuse silent null invent / field wipe)"
            )
        return coerce_float_wire(value, ddl_type=upper)
    if upper in {
        "INTEGER",
        "INT",
        "INT32",
        "INT64",
        "LONG",
        "SHORT",
        "BYTE",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
    }:
        from connectors.sql_bind import coerce_integer_wire

        if isinstance(value, str) and not str(value).strip():
            raise ValueError(
                f"Elasticsearch {upper} refused empty string {value!r} "
                "(refuse silent null invent / field wipe)"
            )
        return coerce_integer_wire(value, ddl_type=upper)
    if upper in {"BOOLEAN", "BOOL"}:
        from connectors.sql_bind import coerce_boolean_wire

        coerced = coerce_boolean_wire(value, as_int=False)
        if not isinstance(coerced, bool):
            raise ValueError(
                f"Elasticsearch BOOLEAN refused unrecognized value {value!r} "
                "(refuse invent via bool())"
            )
        return coerced
    if upper in {
        "DATE",
        "TIME",
        "DATETIME",
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "DATETIMEOFFSET",
    }:
        from connectors.sql_temporal import coerce_sql_temporal

        if isinstance(value, str) and not str(value).strip():
            raise ValueError(
                f"Elasticsearch {upper} refused empty string {value!r} "
                "(refuse silent null invent / field wipe)"
            )
        return coerce_sql_temporal(value, upper)
    if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
        from connectors.sql_bind import coerce_uuid_wire

        return coerce_uuid_wire(value)
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
        import base64

        from connectors.sql_bind import coerce_binary_wire

        raw = coerce_binary_wire(value)
        return base64.b64encode(raw).decode("ascii") if raw is not None else None
    if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
        # ES dynamic mapping can only assign one JSON kind per field; storing the
        # JSON as a string keeps the transfer lossless and avoids object/array
        # collisions when the same logical column contains mixed JSON shapes.
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=json_default)
        return value
    return value


def _resolve_doc_id(
    source: dict[str, Any],
    *,
    conflict_columns: list[str],
    target_cols: list[str],
) -> str | None:
    """Deterministic document identity for idempotent upsert/retry.

    Prefer explicit ``_id``, then configured conflict/PK columns (including
    composite keys), then a single ``id`` field when present.
    """
    if "_id" in source and source.get("_id") is not None and str(source.get("_id")).strip() != "":
        return str(source["_id"])
    configured = [c for c in conflict_columns if c]
    if configured:
        # Fail closed on partial composite identity: every configured PK part must
        # resolve into the document. Shrinking to present keys alone would collide
        # distinct composite rows onto the same partial _id.
        from connectors.writer_common import resolve_conflict_targets

        try:
            keys = resolve_conflict_targets(configured, list(source.keys()), strict=True)
        except ValueError:
            return None
        parts: list[str] = []
        for col in keys:
            val = source.get(col)
            if val is None or str(val).strip() == "":
                return None
            parts.append(str(val).strip())
        return "|".join(parts) if len(parts) > 1 else parts[0]
    for alias in ("id", "ID", "Id", "pk", "PK"):
        if alias in source and source.get(alias) is not None and str(source.get(alias)).strip() != "":
            return str(source[alias])
    for col in target_cols:
        if col.lower() in {"id", "pk", "doc_id", "document_id"} and col in source:
            val = source.get(col)
            if val is not None and str(val).strip() != "":
                return str(val)
    return None


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    api_key: str = "",
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    conflict_columns: list[str] | None = None,
    write_mode: str = "insert",
    **_kwargs: Any,
) -> WriteResult:
    del schema, backfill_new_fields
    policy = transform_error_policy(error_policy)
    index = table_name or database
    cfg = {
        "host": host, "port": port, "username": username, "password": password,
        "connection_string": connection_string, "ssl": ssl, "api_key": api_key,
    }
    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="Elasticsearch",
    )
    # Defer map/quarantine until after index probe — rematerialize when live
    # mapping carriers differ from Map/Studio stamps.
    mapped_rows: list = []
    errors: list = []
    rejected_details: list = []
    tgt_types: list[str] = []

    conflict = [c for c in (conflict_columns or []) if c]
    mode = (write_mode or "insert").lower()
    # Elasticsearch is key-addressed: auto-generated ids break idempotent retry /
    # upsert / CDC and hide collisions. Always require resolvable document identity.
    requires_identity = True

    client = _client(cfg)
    try:
        index_exists = bool(client.indices.exists(index=index))
        if not index_exists and not create_table:
            # Clusters with action.auto_create_index would otherwise invent the index.
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=index,
                target_schema=host or "localhost",
                checksum="",
                chunks_completed=0,
                error=(
                    f"Elasticsearch index {index!r} is missing and "
                    "create_table is disabled"
                ),
            )
        # Create-new index: partial Studio must not soft-bind Map VARCHAR.
        if studio_err and not index_exists:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=index,
                target_schema=host or "localhost",
                checksum="",
                chunks_completed=0,
                error=studio_err,
            )
        if create_table and not index_exists:
            # Use one shard and zero replicas for predictable test/CI behavior
            # and to avoid blowing through small cluster shard limits.
            client.indices.create(
                index=index,
                body={"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
            )

        if index_exists:
            from connectors.saas_common import is_auth_error

            physical, map_exc = _fetch_es_physical_types(client, index, target_cols)
            if map_exc is not None:
                # Document store — never cite information_schema. Auth and other
                # get_mapping failures both refuse (cannot prove live field types).
                kind = "auth" if is_auth_error(map_exc) else "probe"
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=index,
                    target_schema=host or "localhost",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Elasticsearch get_mapping {kind} failed: {map_exc} — "
                        "refuse Map bind without index mapping (get_mapping)."
                    ),
                )
            mapped_data_cols = [c for c in target_cols if c and c != "_id"]
            # ES/OpenSearch is not relational: empty/partial properties mean
            # dynamic mapping, not SQL empty→NULL invent. Overlay Studio + live
            # props when present; unmapped fields keep Map stamps. Partial Studio
            # still fail-closes via force_remap below (audit §2.5).
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
            # Dynamic mapping: overlay known props onto Map stamps without
            # requiring full relational coverage (unmapped fields stay Map).
            for c in mapped_data_cols:
                hit = (
                    effective_physical.get(c)
                    or effective_physical.get(str(c).lower())
                    or effective_physical.get(str(c).upper())
                )
                if hit and str(hit).strip():
                    dest_types[c] = str(hit).strip()
            _force_remap = bool(studio_err)
            remat = _es_rematerialize_if_physical_differs(
                physical=effective_physical if effective_physical else physical,
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
                mapped_rows, errors, rejected_details, dest_types = remat
            elif _force_remap:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=index,
                    target_schema=host or "localhost",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        "Elasticsearch live mapping incomplete for mapped fields — "
                        "refuse Map VARCHAR rematerialize invent. Re-run "
                        "destination schema introspect and retry."
                    ),
                )
            else:
                mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
                    headers=headers,
                    data_rows=data_rows,
                    mappings=mappings,
                    target_cols=target_cols,
                    column_types=column_types,
                    dest_types=dest_types,
                    preserve_case=True,
                    error_policy=policy,
                    dest_kind="elasticsearch",
                    destination_pk_columns=list(conflict_columns or []) or None,
                    destination_column_nullability=_kwargs.get(
                        "destination_column_nullability"
                    ),
                )
        else:
            mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                preserve_case=True,
                error_policy=policy,
                dest_kind="elasticsearch",
                destination_pk_columns=list(conflict_columns or []) or None,
                destination_column_nullability=_kwargs.get(
                    "destination_column_nullability"
                ),
            )

        from connectors.writer_common import apply_write_quarantine_matrix, reject_on_strict_policy

        # Partial Studio: never soft-fill quarantine carriers from Map logicals
        # after force_remap (empty→NULL invent on typed ES props).
        if studio_err:
            missing = [
                c
                for c in target_cols
                if c and not str(dest_types.get(c) or "").strip()
            ]
            if missing:
                sample = ", ".join(repr(c) for c in missing[:12])
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=index,
                    target_schema=host or "localhost",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"Elasticsearch mapped field(s) {sample} lack live carriers "
                        "under partial Studio — refuse Map logical quarantine invent. "
                        "Re-run destination schema introspect and retry."
                    ),
                )
            tgt_types = [str(dest_types.get(c) or "").strip() for c in target_cols]
        else:
            tgt_types = [
                str(
                    dest_types.get(c)
                    or (logical_types[i] if i < len(logical_types) else "")
                    or ""
                )
                for i, c in enumerate(target_cols)
            ]
        mapped_rows = apply_write_quarantine_matrix(
            mapped_rows,
            target_cols,
            tgt_types,
            rejected_details,
            policy,
            dialect_label="Elasticsearch",
            mappings=list(mappings or []) or None,
        )
        _map_abort = reject_on_strict_policy(
            policy, rejected_details, "Elasticsearch", errors
        )
        if _map_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=index,
                target_schema=host or "localhost",
                checksum="",
                chunks_completed=0,
                error=_map_abort or f"Transform errors: {'; '.join(errors[:3])}",
                warnings=errors[:10],
                rejected_rows=len(
                    {d.get("row") for d in rejected_details if d.get("row") is not None}
                ),
                rejected_details=list(rejected_details),
            )

        if conflict:
            from connectors.writer_common import (
                partition_dense_upsert_rows,
                resolve_conflict_targets,
            )

            # Resolve to the Map spelling first. _resolve_doc_id already matches
            # these names case-insensitively, so a PK the operator wrote in a
            # different case reaches the document id fine but would miss
            # target_cols.index here and quarantine every row before identity
            # resolution ever ran. Non-strict: an unresolvable name is left to
            # the id resolver, which refuses a partial key on its own terms.
            partition_keys = resolve_conflict_targets(conflict, target_cols, strict=False)
            if partition_keys:
                mapped_rows = partition_dense_upsert_rows(
                    mapped_rows,
                    partition_keys,
                    target_cols=target_cols,
                    rejected_details=rejected_details,
                    policy=policy,
                )

        from elasticsearch.helpers import bulk

        identity_missing = 0
        actions: list[dict[str, Any]] = []
        from services.value_serializer import is_missing_sentinel

        for row_idx, row in enumerate(mapped_rows):
            try:
                # Omit DF_MISSING — never project JSON null (would wipe prior fields).
                source: dict[str, Any] = {}
                for i, value in enumerate(row):
                    if is_missing_sentinel(value):
                        continue
                    wire = (
                        tgt_types[i]
                        if i < len(tgt_types) and str(tgt_types[i] or "").strip()
                        else (
                            ""
                            if studio_err
                            else (
                                logical_types[i] if i < len(logical_types) else ""
                            )
                        )
                    )
                    if studio_err and not str(wire or "").strip():
                        raise ValueError(
                            f"Elasticsearch field {target_cols[i]!r} lacks live "
                            "carrier under partial Studio — refuse Map logical "
                            "wire invent"
                        )
                    source[target_cols[i]] = _to_es_value(value, wire)
            except (ValueError, TypeError) as cell_exc:
                rejected_details.append({
                    "row": row_idx + 1,
                    "column": "",
                    "target": index,
                    "value": "",
                    "reason": str(cell_exc)[:500],
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                    "chars": [],
                })
                if policy == "fail":
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=index,
                        target_schema=host or "localhost",
                        checksum="",
                        chunks_completed=0,
                        error=str(cell_exc)[:500],
                        rejected_details=list(rejected_details),
                    )
                continue
            doc_id = _resolve_doc_id(
                source,
                conflict_columns=conflict,
                target_cols=target_cols,
            )
            source.pop("_id", None)
            if doc_id is None:
                identity_missing += 1
                rejected_details.append({
                    "row": row_idx + 1,
                    "column": ",".join(conflict) if conflict else "_id",
                    "target": index,
                    "value": "",
                    "reason": (
                        "elasticsearch requires document identity — map a primary "
                        "key to _id or configure conflict_columns"
                    ),
                    "policy": "write_fail" if policy == "fail" else "write_quarantine",
                })
                continue
            # Upsert/update/merge: update+doc merges present fields only —
            # never full _source replace (would wipe unmapped destination fields).
            if mode in {"upsert", "update", "merge"}:
                action = {
                    "_op_type": "update",
                    "_index": index,
                    "_id": str(doc_id),
                    "doc": source,
                    "doc_as_upsert": True,
                }
            else:
                action = {"_index": index, "_source": source, "_id": str(doc_id)}
                # Insert/append must not silently overwrite existing docs (index vs create).
                if mode in {"insert", "append", "create"}:
                    action["_op_type"] = "create"
            actions.append(action)

        written, bulk_errors = bulk(client, actions, raise_on_error=False) if actions else (0, [])
        try:
            client.indices.refresh(index=index)
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        if on_checkpoint:
            on_checkpoint(1, 1, written)

        # Materialize bulk item failures into rejected_details so control-plane
        # JSONL / dest DLQ see them — never return ok=True with silent drops.
        bulk_details: list[dict[str, Any]] = []
        for err in bulk_errors or []:
            if not isinstance(err, dict):
                bulk_details.append({
                    "row": "",
                    "column": "",
                    "target": "",
                    "value": "",
                    "reason": str(err)[:500],
                    "policy": policy,
                })
                continue
            inner = next(iter(err.values()), {}) if err else {}
            if not isinstance(inner, dict):
                inner = {}
            reason = inner.get("error") or err
            if isinstance(reason, dict):
                reason = reason.get("reason") or reason.get("type") or str(reason)
            bulk_details.append({
                "row": str(inner.get("_id") or ""),
                "column": "",
                "target": index,
                "value": "",
                "reason": f"elasticsearch bulk: {reason}"[:500],
                "policy": policy,
            })

        all_rejected = list(rejected_details) + bulk_details
        fail_closed = policy == "fail" and bool(bulk_details or identity_missing)
        if requires_identity and identity_missing > 0 and written == 0:
            fail_closed = True
        # Re-check FAIL_JOB / strict after mid-write identity/bulk rejects.
        _final_abort = reject_on_strict_policy(policy, all_rejected, "Elasticsearch")
        if _final_abort:
            fail_closed = True
        err_msg = None
        if fail_closed:
            # Identity failure is the primary operator signal (audit §2.5) —
            # prefer it over the generic strict-policy summary.
            if identity_missing and written == 0:
                err_msg = (
                    f"elasticsearch blocked: {identity_missing} row(s) "
                    "lack document identity — set Primary key on Map"
                )
            elif _final_abort:
                err_msg = _final_abort
            elif bulk_errors:
                err_msg = f"elasticsearch bulk rejected {len(bulk_errors)} item(s)"
            else:
                err_msg = "elasticsearch write failed"
        return WriteResult(
            ok=not fail_closed,
            # Honest count: mid-write may have landed docs before abort-class reject.
            rows_written=written,
            table_name=index,
            target_schema=host or "localhost",
            checksum=row_checksum(
                mapped_rows, target_cols, dest_db_type="elasticsearch"
            )
            if not fail_closed
            else "",
            chunks_completed=1,
            error=err_msg,
            warnings=(errors + [str(e) for e in (bulk_errors or [])[:5]])[:10],
            rejected_rows=len({str(d.get("row")) for d in all_rejected if d.get("row") not in (None, "")}),
            rejected_details=list(all_rejected),
            meta=gate8_writer_meta(mapped_rows, target_cols) if not fail_closed else {},
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=index,
            target_schema=host or "",
            checksum="",
            chunks_completed=0,
            error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
        )
    finally:
        try:
            client.close()
        except Exception as exc:
            logger.warning("Exception suppressed: %s", exc, exc_info=exc)
