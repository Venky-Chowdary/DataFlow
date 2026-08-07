"""BigQuery bulk writer — insert_rows_json + optional MERGE upsert with ``_df_lsn``."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable

from services.type_system import materialize_dest_ddl

from connectors.driver_guard import stub_writes_allowed
from connectors.stub_writer import simulate_stub_write
from connectors.writer_common import (
    CHUNK_SIZE,
    DF_LSN_COL,
    _coerced_null_row_count,
    _rejected_row_count,
    assert_sparse_upsert_has_pk,
    bind_sql_mapped_rows_with_quarantine,
    build_mapped_rows_with_details,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    null_safe_merge_on,
    quarantine_currency_markers_into_numeric,
    quarantine_unfit_arrays,
    quarantine_unfit_binaries,
    quarantine_unfit_bitstrings,
    quarantine_unfit_booleans,
    quarantine_unfit_decimals,
    quarantine_unfit_enum_set,
    quarantine_unfit_integers,
    quarantine_unfit_json,
    quarantine_unfit_specialty_types,
    quarantine_unfit_strings,
    quarantine_unfit_temporals,
    quarantine_unfit_years,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    sparse_present_bindings,
    reject_on_strict_policy,
    resolve_conflict_targets,
    split_dense_sparse_rows,
    transform_error_policy,
)
from connectors.writer_common import (
    WriteResult as _WriteResult,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "google-cloud-bigquery"


# BigQuery fixed-point platform caps (SchemaField precision/scale kwargs).
_BQ_NUMERIC_MAX_P, _BQ_NUMERIC_MAX_S = 38, 9
_BQ_BIGNUMERIC_MAX_P, _BQ_BIGNUMERIC_MAX_S = 76, 38


def _bq_fixed_point_spec(
    inferred: str,
) -> tuple[str, int | None, int | None] | None:
    """Map≡CREATE fixed-point: ``(NUMERIC|BIGNUMERIC|STRING, p|None, s|None)``.

    Explicit Map ``NUMERIC(p,s)`` keeps NUMERIC polarity within (38,9).
    ``DECIMAL`` / ``NUMBER`` / ``BIGNUMERIC`` stamps become BIGNUMERIC with the
    approved ``(p,s)``. Over BIGNUMERIC caps → STRING (fail-closed; no invent).
    Bare ``DECIMAL`` / ``BIGNUMERIC`` → bare BIGNUMERIC (platform default).
    Returns None when ``inferred`` is not a fixed-point carrier.
    """
    import re

    from services.type_system import strip_identity_qualifier

    raw = strip_identity_qualifier(inferred).strip()
    if not raw:
        return None
    m = re.match(
        r"^(BIGNUMERIC|BIGDECIMAL|NUMERIC|DECIMAL|NUMBER)\s*"
        r"(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$",
        raw,
        re.IGNORECASE,
    )
    if not m:
        # Legalize logicals (e.g. money aliases) via Map≡CREATE materialize SSOT.
        wire = materialize_dest_ddl("bigquery", raw)
        m = re.match(
            r"^(BIGNUMERIC|NUMERIC)\s*"
            r"(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$",
            wire,
            re.IGNORECASE,
        )
        if not m:
            return None
        family = m.group(1).upper()
        if m.group(2) is None:
            return family, None, None
        p = int(m.group(2))
        s = int(m.group(3) or 0)
        if p > _BQ_BIGNUMERIC_MAX_P or s > _BQ_BIGNUMERIC_MAX_S:
            return "STRING", None, None
        return family, p, s

    family = m.group(1).upper()
    if m.group(2) is None:
        if family == "NUMERIC":
            return "NUMERIC", None, None
        return "BIGNUMERIC", None, None
    p = int(m.group(2))
    s = int(m.group(3) or 0)
    if p > _BQ_BIGNUMERIC_MAX_P or s > _BQ_BIGNUMERIC_MAX_S:
        return "STRING", None, None
    if family == "NUMERIC":
        if p <= _BQ_NUMERIC_MAX_P and s <= _BQ_NUMERIC_MAX_S:
            return "NUMERIC", p, s
        # Explicit NUMERIC that exceeds NUMERIC caps — promote to BIGNUMERIC
        # keeping the Map (p,s), never invent (76,38).
        return "BIGNUMERIC", p, s
    return "BIGNUMERIC", p, s


def bq_type(inferred: str) -> str:
    """Map logical / Map stamp to a BigQuery SchemaField ``field_type`` string.

    Never returns parameterized type names — the Python client rejects
    ``BIGNUMERIC(p,s)`` / ``STRING(n)`` as ``field_type``. Precision/scale and
    max_length are applied via :func:`bq_schema_field` kwargs (Map≡CREATE).

    Over BIGNUMERIC capacity fails closed to STRING (no silent invent).

    Map≡CREATE: legalization goes through ``materialize_dest_ddl`` — never blind
    ``ddl_type`` (which invents TIMESTAMP→DATETIME for approved Map stamps).
    """
    import re

    fp = _bq_fixed_point_spec(inferred)
    if fp is not None:
        return fp[0]

    # Legalize non-BQ carriers (BINARY→BYTES, VARCHAR→STRING, TIMESTAMP keep).
    wire = materialize_dest_ddl("bigquery", inferred)
    widthed = re.match(r"(STRING|BYTES)\s*\(\s*\d+\s*\)", wire, re.IGNORECASE)
    if widthed:
        return widthed.group(1).upper()
    # Reject illegal parameterized leftovers as field_type.
    if re.match(r"^[A-Z_][A-Z0-9_]*\s*\(", wire, re.IGNORECASE):
        base = wire.split("(", 1)[0].strip().upper()
        return base or wire
    return wire.upper() if wire else wire


def bq_schema_field(bigquery_mod: Any, col: str, inferred: str) -> Any:
    """Build SchemaField honoring Map ``(p,s)`` / ``max_length`` stamps.

    Map≡CREATE: explicit NUMERIC/BIGNUMERIC(p,s) become SchemaField
    ``precision`` / ``scale`` — never bare platform invent (76,38) / (38,9).
    """
    from services.type_system import (
        parse_binary_carrier_width,
        parse_string_carrier_width,
    )

    field_type = bq_type(inferred)
    kwargs: dict[str, Any] = {}
    if field_type in {"NUMERIC", "BIGNUMERIC"}:
        fp = _bq_fixed_point_spec(inferred)
        if fp is not None and fp[1] is not None and fp[2] is not None:
            kwargs["precision"] = int(fp[1])
            kwargs["scale"] = int(fp[2])
    elif field_type == "STRING":
        width = parse_string_carrier_width(materialize_dest_ddl("bigquery", inferred))
        if width is None:
            width = parse_string_carrier_width(inferred)
        if width is not None and width > 0:
            kwargs["max_length"] = int(width)
    elif field_type == "BYTES":
        width = parse_binary_carrier_width(materialize_dest_ddl("bigquery", inferred))
        if width is None:
            width = parse_binary_carrier_width(inferred)
        if width is not None and width > 0:
            kwargs["max_length"] = int(width)
    return bigquery_mod.SchemaField(col, field_type, **kwargs)


def resolve_bigquery_decimal_target_types(
    target_cols: list[str],
    logical_types: list[str],
    table_schema: list[Any] | None = None,
) -> list[str]:
    """Prefer physical SchemaField (p,s / max_length / REPEATED); else Map/ddl wire.

    After Map≡CREATE, physical NUMERIC/BIGNUMERIC fields carry the approved
    ``(p,s)``. Quarantine must gate on that stamp (or mapped ddl) so append
    paths never silently overflow into streaming/load errors. REPEATED fields
    must rematerialize as ``ARRAY<T>`` — never scalar INT/STRING invent.
    """
    by_name: dict[str, Any] = {}
    if table_schema:
        for field in table_schema:
            name = getattr(field, "name", None)
            if name:
                by_name[str(name)] = field
                by_name[str(name).lower()] = field
                by_name[str(name).upper()] = field

    out: list[str] = []
    for col, logical in zip(target_cols, logical_types):
        field = by_name.get(col) or by_name.get(str(col).lower()) or by_name.get(str(col).upper())
        if field is not None:
            carrier = _bigquery_physical_field_carrier(field)
            if carrier:
                out.append(carrier)
                continue
            # Empty field_type on live schema — never invent STRING; use Map ddl.
        # No physical field yet — legalize Map stamp to BQ wire for quarantine.
        fp = _bq_fixed_point_spec(logical)
        if fp is not None and fp[0] != "STRING" and fp[1] is not None and fp[2] is not None:
            out.append(f"{fp[0]}({fp[1]},{fp[2]})")
        elif fp is not None and fp[0] != "STRING":
            out.append(fp[0])
        else:
            out.append(materialize_dest_ddl("bigquery", logical))
    return out


def _bigquery_physical_field_carrier(field: Any) -> str:
    """Live SchemaField → bind carrier; REPEATED becomes ARRAY<T>; empty type refuses."""
    ftype = str(getattr(field, "field_type", "") or "").upper().strip()
    if not ftype:
        return ""
    mode = str(getattr(field, "mode", "") or "").upper().strip()
    if ftype in {"NUMERIC", "BIGNUMERIC", "DECIMAL"}:
        precision = getattr(field, "precision", None)
        scale = getattr(field, "scale", None)
        if precision is not None and scale is not None:
            base = f"{ftype}({int(precision)},{int(scale)})"
        else:
            base = ftype
    elif ftype == "STRING":
        max_len = getattr(field, "max_length", None)
        if max_len is not None and int(max_len) > 0:
            base = f"STRING({int(max_len)})"
        else:
            base = "STRING"
    elif ftype == "BYTES":
        max_len = getattr(field, "max_length", None)
        if max_len is not None and int(max_len) > 0:
            base = f"BYTES({int(max_len)})"
        else:
            base = "BYTES"
    elif ftype in {"RECORD", "STRUCT"}:
        base = "STRUCT"
    else:
        base = ftype
    if mode == "REPEATED" and not base.upper().startswith("ARRAY<"):
        return f"ARRAY<{base}>"
    return base


def build_bigquery_merge_sql(
    target_table: str,
    staging_table: str,
    target_cols: list[str],
    conflict_columns: list[str],
    *,
    lsn_column: str | None = None,
) -> str:
    """Build a BigQuery MERGE for PK upsert with optional monotonic LSN guard."""
    from connectors.writer_common import resolve_conflict_targets

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        raise ValueError("BigQuery MERGE requires conflict_columns present in target_cols")
    on_clause = null_safe_merge_on(
        conflict,
        left_alias="T",
        right_alias="S",
        quote_column=lambda c: f"`{c}`",
    )
    update_cols = [c for c in target_cols if c not in conflict]
    set_clause = ", ".join(f"T.`{c}` = S.`{c}`" for c in update_cols) or "T.`{0}` = S.`{0}`".format(
        conflict[0]
    )
    matched = "WHEN MATCHED"
    if lsn_column and lsn_column in target_cols:
        from connectors.writer_common import bigquery_lsn_match_predicate

        matched += f" AND {bigquery_lsn_match_predicate('T', 'S', lsn_column)}"
    matched += f" THEN UPDATE SET {set_clause}"  # nosec B608
    insert_cols = ", ".join(f"`{c}`" for c in target_cols)
    insert_vals = ", ".join(f"S.`{c}`" for c in target_cols)
    return (
        f"MERGE `{target_table}` T\n"
        f"USING `{staging_table}` S\n"
        f"ON {on_clause}\n"
        f"{matched}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def _bq_query_param_type(bq_t: str) -> str:
    t = (bq_t or "STRING").upper().split("(")[0].strip()
    if t in {"INTEGER", "INT64"}:
        return "INT64"
    if t in {"FLOAT", "FLOAT64"}:
        return "FLOAT64"
    if t in {"BOOL", "BOOLEAN"}:
        return "BOOL"
    if t in {"NUMERIC", "BIGNUMERIC", "DECIMAL"}:
        return "NUMERIC"
    if t in {"TIMESTAMP", "DATETIME", "DATE", "TIME", "BYTES", "JSON"}:
        return t
    return "STRING"


def _bq_normalize_present(
    present: dict[str, Any],
    target_cols: list[str],
    bq_types: list[str],
) -> dict[str, Any]:
    """Normalize sparse present bindings the same way dense BQ records are."""
    from connectors.warehouse_temporal import records_for_bigquery

    cols = [c for c in target_cols if c in present]
    if not cols:
        return {}
    types = [bq_types[target_cols.index(c)] for c in cols]
    row = tuple(present[c] for c in cols)
    recs = records_for_bigquery([row], cols, types)
    return recs[0] if recs else {}


def _bq_sdk():
    """Lazy BigQuery SDK import — kept patchable for unit tests without the client."""
    from google.cloud import bigquery

    return bigquery


def _bq_apply_sparse_upsert(
    client: Any,
    table_id: str,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[tuple],
    bq_types: list[str],
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> tuple[int, int, list[tuple]]:
    """Per-row BigQuery DML omitting DF_MISSING — never SET col=NULL for absent CDC fields."""
    from connectors.writer_common import (
        DF_LSN_COL,
        assert_sparse_upsert_has_pk,
        materialize_sparse_row_for_checksum,
        sparse_present_bindings,
    )
    from services.cdc_effectively_once import should_apply_pk_row
    from services.value_serializer import cell_to_string

    bigquery = _bq_sdk()
    from connectors.writer_common import resolve_conflict_targets

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        raise ValueError("sparse BigQuery upsert requires conflict_columns")
    type_by_col = {c: bq_types[i] for i, c in enumerate(target_cols)}
    written = 0
    skipped = 0
    checksum_rows: list[tuple] = []
    for row_idx, row in enumerate(sparse_rows):
        raw_present = sparse_present_bindings(row, target_cols)
        # Pre-bind preferred. Residual refuse / empty PK → quarantine, not
        # silent skip and not batch-abort.
        try:
            present = _bq_normalize_present(
                raw_present,
                target_cols,
                bq_types,
            )
            assert_sparse_upsert_has_pk(present, conflict)
        except ValueError as exc:
            if rejected_details is not None:
                sample = ""
                try:
                    sample = cell_to_string(
                        next(iter(raw_present.values()), "")
                    )[:120]
                except Exception:
                    sample = ""
                rejected_details.append(
                    {
                        "row": row_idx,
                        "column": "*",
                        "value": sample,
                        "reason": str(exc)[:300],
                        "policy": policy,
                    }
                )
            continue
        non_pk = {k: v for k, v in present.items() if k not in conflict}

        def _params(values: dict[str, Any], prefix: str) -> list[Any]:
            out = []
            for col, val in values.items():
                out.append(
                    bigquery.ScalarQueryParameter(
                        f"{prefix}{col}",
                        _bq_query_param_type(type_by_col.get(col, "STRING")),
                        val,
                    )
                )
            return out

        pk_where = " AND ".join(f"`{c}` = @pk_{c}" for c in conflict)
        pk_params = _params({c: present[c] for c in conflict}, "pk_")
        select_cols = ", ".join(f"`{c}`" for c in target_cols)
        sel = (
            f"SELECT {select_cols} FROM `{table_id}` WHERE {pk_where}"  # nosec B608
        )
        job = client.query(
            sel,
            job_config=bigquery.QueryJobConfig(query_parameters=pk_params),
        )
        existing_rows = list(job.result())
        existing_tuple = tuple(existing_rows[0]) if existing_rows else None
        existing = (
            dict(zip(target_cols, existing_tuple)) if existing_tuple is not None else None
        )
        if (
            existing is not None
            and DF_LSN_COL in present
            and DF_LSN_COL in target_cols
        ):
            if not should_apply_pk_row(
                existing_lsn=existing.get(DF_LSN_COL),
                incoming_lsn=present[DF_LSN_COL],
            ).applied:
                skipped += 1
                continue

        if non_pk:
            set_clause = ", ".join(f"`{c}` = @v_{c}" for c in non_pk)
            upd = (
                f"UPDATE `{table_id}` SET {set_clause} WHERE {pk_where}"  # nosec B608
            )
            job = client.query(
                upd,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=_params(non_pk, "v_") + pk_params
                ),
            )
            job.result()
            affected = getattr(job, "num_dml_affected_rows", None)
            if affected and affected > 0:
                written += 1
                checksum_rows.append(
                    materialize_sparse_row_for_checksum(present, existing, target_cols)
                )
                continue

        cols = list(present.keys())
        col_sql = ", ".join(f"`{c}`" for c in cols)
        val_sql = ", ".join(f"@v_{c}" for c in cols)
        ins = (
            f"INSERT INTO `{table_id}` ({col_sql}) VALUES ({val_sql})"  # nosec B608
        )
        try:
            job = client.query(
                ins,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=_params(present, "v_")
                ),
            )
            job.result()
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
        except Exception:
            if not non_pk:
                raise
            set_clause = ", ".join(f"`{c}` = @v_{c}" for c in non_pk)
            upd = (
                f"UPDATE `{table_id}` SET {set_clause} WHERE {pk_where}"  # nosec B608
            )
            job = client.query(
                upd,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=_params(non_pk, "v_") + pk_params
                ),
            )
            job.result()
            written += 1
            checksum_rows.append(
                materialize_sparse_row_for_checksum(present, existing, target_cols)
            )
    return written, skipped, checksum_rows


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
    warehouse: str,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    service_account: str = "",
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    create_table: bool = True,
    **_kwargs: Any,
) -> WriteResult:
    dest_nullability = _kwargs.get("destination_column_nullability")
    live_dest_types = _kwargs.get("destination_column_types")
    del username, password, ssl, warehouse, _kwargs
    from connectors.writer_common import resolve_writer_backfill

    backfill_new_fields = resolve_writer_backfill(
        backfill_new_fields=backfill_new_fields,
        mappings=mappings,
    )
    project_id = database or host
    dataset_id = schema or "dataflow"
    table_name = sanitize_identifier(table_name)
    policy = transform_error_policy(error_policy)
    conflict_columns = list(conflict_columns or [])

    try:
        from google.cloud import bigquery  # noqa: F401
    except ImportError:
        from connectors.driver_guard import require_driver

        if not stub_writes_allowed():
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
                checksum="", chunks_completed=0,
                error=require_driver("google.cloud.bigquery", "google-cloud-bigquery"),
                driver="none",
            )
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=dataset_id,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=dataset_id,
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    from connectors.bigquery_conn import _is_local_endpoint

    is_local, _ = _is_local_endpoint(host, connection_string)
    creds_ref = (service_account or connection_string or "").strip()
    has_creds = bool(creds_ref) and not creds_ref.lower().startswith(("http://", "https://"))
    if stub_writes_allowed() and not is_local and not has_creds:
        # No live endpoint/credentials and dev/test stubs are allowed.
        rows, checksum, chunks = simulate_stub_write(
            data_rows=data_rows, table_name=table_name, target_schema=dataset_id,
            on_checkpoint=on_checkpoint,
        )
        return WriteResult(
            ok=True, rows_written=rows, table_name=table_name, target_schema=dataset_id,
            checksum=checksum, chunks_completed=chunks, driver="stub",
        )

    from connectors.writer_common import sample_values_by_source_from_batch

    batch_samples = sample_values_by_source_from_batch(headers, data_rows, mappings)
    target_cols, logical_types = resolve_target_columns(
        mappings,
        column_types,
        sample_values_by_source=batch_samples,
        # Deny-create must match existing DDL; create-new keeps empty dest types.
        # Unknown existence must stay None — never invent True on append.
        table_exists=False if create_table else None,
    )
    if not target_cols:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
            checksum="", chunks_completed=0, error="No column mappings",
        )
    # Prefer Studio-probed live DDL over Map stamps; physical schema may refine later.
    # Partial Studio must not soft-fill Map VARCHAR for create-new gaps.
    from connectors.writer_common import resolve_studio_or_map_dest_types

    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        studio_types=live_dest_types if isinstance(live_dest_types, dict) else None,
        product="BigQuery",
    )

    try:
        from google.cloud import bigquery

        from connectors.bigquery_conn import get_client, _is_local_endpoint

        is_local, _ = _is_local_endpoint(host, connection_string)

        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            service_account=service_account,
            host=host,
            port=port,
            connection_string=connection_string,
        )
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        # CREATE/ADD must honor Studio/live dest_types — never Map logical_types invent.
        schema_fields = [
            bq_schema_field(
                bigquery,
                col,
                str(dest_types.get(col) or (logical_types[i] if i < len(logical_types) else "STRING")),
            )
            for i, col in enumerate(target_cols)
        ]
        dataset_ref = f"{project_id}.{dataset_id}"
        # Probe existence first — create_table=True + exists_ok must still
        # rematerialize against live DDL when the table already exists.
        # Only treat NotFound/404 as missing; permission/throttle fail closed.
        table_existed = False
        physical_schema = None
        try:
            physical_schema = list(client.get_table(table_id).schema)
            table_existed = True
        except Exception as probe_exc:
            msg = str(probe_exc).lower()
            exc_name = type(probe_exc).__name__.lower()
            not_found = (
                "404" in msg
                or "not found" in msg
                or "notfound" in exc_name
                or "does not exist" in msg
            )
            if not not_found:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"BigQuery table probe failed for {table_id!r} — refuse Map bind "
                        f"without physical schema (empty→NULL invent risk): {probe_exc}"
                    ),
                )
            physical_schema = None
            table_existed = False

        if not table_existed:
            # Create-new: partial Studio must not soft-bind Map VARCHAR for gaps.
            if studio_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=studio_err,
                )
            if not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"BigQuery table {table_id!r} is missing or inaccessible "
                        "and create_table is disabled"
                    ),
                )
            existing_datasets = {ds.dataset_id for ds in client.list_datasets()}
            if dataset_id not in existing_datasets:
                client.create_dataset(bigquery.Dataset(dataset_ref))
            table = bigquery.Table(table_id, schema=schema_fields)
            client.create_table(table, exists_ok=True)
            # Re-probe after exists_ok — concurrent/pre-existing tables must still
            # overlay live DDL (probe race must not invent Map VARCHAR bind).
            try:
                physical_schema = list(client.get_table(table_id).schema)
                table_existed = True
            except Exception as post_exc:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"BigQuery table {table_id!r} create/exists_ok succeeded but "
                        f"physical schema re-probe failed — refuse Map bind: {post_exc}"
                    ),
                )
        elif create_table:
            # Table exists — ensure dataset path still works for backfill below;
            # do not recreate with Map stamps (would invent carriers).
            pass

        if backfill_new_fields:
            table = client.get_table(table_id)
            existing = {f.name for f in table.schema}
            new_fields = []
            for i, col in enumerate(target_cols):
                if col in existing:
                    continue
                typ = str(dest_types.get(col) or "").strip()
                if not typ:
                    explicit = ""
                    if i < len(mappings):
                        explicit = str(
                            mappings[i].get("target_type")
                            or mappings[i].get("dest_type")
                            or ""
                        ).strip()
                    # Partial Studio: explicit Map stamp OK; never invent from
                    # source/logical alone (generic_sql additive parity).
                    if explicit and (
                        studio_err
                        or (isinstance(live_dest_types, dict) and live_dest_types)
                    ):
                        typ = explicit
                        dest_types[col] = typ
                    elif studio_err or (
                        isinstance(live_dest_types, dict) and live_dest_types
                    ):
                        return WriteResult(
                            ok=False,
                            rows_written=0,
                            table_name=table_name,
                            target_schema=dataset_id,
                            checksum="",
                            chunks_completed=0,
                            error=(
                                f"BigQuery additive column {col!r} lacks Studio/live "
                                "type and Map target_type under partial destination "
                                "schema — refuse Map VARCHAR ADD invent. Stamp the "
                                "column on Map or disable backfill_new_fields."
                            ),
                        )
                    else:
                        typ = (
                            logical_types[i]
                            if i < len(logical_types)
                            else "STRING"
                        )
                new_fields.append(bq_schema_field(bigquery, col, typ))
            if new_fields:
                table.schema = list(table.schema) + new_fields
                client.update_table(table, ["schema"])
                # Refresh physical after additive evolve.
                try:
                    physical_schema = list(client.get_table(table_id).schema)
                    table_existed = True
                except Exception as refresh_exc:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table_name,
                        target_schema=dataset_id,
                        checksum="",
                        chunks_completed=0,
                        error=(
                            f"BigQuery physical schema refresh after backfill failed "
                            f"for {table_id!r} — refuse Map bind: {refresh_exc}"
                        ),
                    )

        # Existing / post-create table: physical schema BEFORE map/transform so
        # live types beat Map stamps (BOOLEAN→STRING invent cliff).
        if table_existed:
            from connectors.writer_common import require_physical_types_for_existing_table

            if physical_schema is None:
                try:
                    physical_schema = list(client.get_table(table_id).schema)
                except Exception as schema_exc:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table_name,
                        target_schema=dataset_id,
                        checksum="",
                        chunks_completed=0,
                        error=(
                            f"BigQuery physical schema introspection failed for existing "
                            f"table {table_id!r} — refuse silent Map VARCHAR bind "
                            f"(empty→NULL invent risk): {schema_exc}"
                        ),
                    )
            if not physical_schema:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"BigQuery physical schema empty for existing table {table_id!r} — "
                        "refuse silent Map VARCHAR bind (empty→NULL invent risk). "
                        "Re-check dataset/table permissions and retry."
                    ),
                )
            physical_map = {}
            for f in physical_schema:
                name = str(getattr(f, "name", "") or "")
                if not name:
                    continue
                carrier = _bigquery_physical_field_carrier(f)
                if not carrier:
                    continue
                physical_map[name] = carrier
                physical_map[name.lower()] = carrier
                physical_map[name.upper()] = carrier
            if not physical_map:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"BigQuery physical schema empty for existing table {table_id!r} — "
                        "refuse silent Map VARCHAR bind (empty→NULL invent risk). "
                        "Re-check dataset/table permissions and retry."
                    ),
                )
            overlay_err = require_physical_types_for_existing_table(
                table_existed=True,
                physical=physical_map,
                dialect_label="BigQuery",
                target_cols=target_cols,
            )
            if overlay_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=dataset_id,
                    checksum="",
                    chunks_completed=0,
                    error=overlay_err,
                )
            live_types = resolve_bigquery_decimal_target_types(
                target_cols, logical_types, physical_schema
            )
            dest_types = {
                target_cols[i]: live_types[i] for i in range(len(target_cols))
            }

        # Rebuild CREATE/MERGE staging schema from final dest_types (post-backfill
        # + physical overlay) — never keep pre-backfill Map logical carriers.
        schema_fields = [
            bq_schema_field(
                bigquery,
                col,
                str(
                    dest_types.get(col)
                    or (logical_types[i] if i < len(logical_types) else "STRING")
                ),
            )
            for i, col in enumerate(target_cols)
        ]

        mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            error_policy=policy,
            dest_kind="bigquery",
            destination_pk_columns=list(conflict_columns or []) or None,
            destination_column_nullability=dest_nullability,
        )
        # Prefer physical table (p,s) so append into NUMERIC never silent-overflows.
        if physical_schema is None:
            try:
                physical_schema = list(client.get_table(table_id).schema)
            except Exception:
                physical_schema = None
        decimal_target_types = resolve_bigquery_decimal_target_types(
            target_cols, logical_types, physical_schema
        )
        mapped_rows = quarantine_currency_markers_into_numeric(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_decimals(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            dialect_label="BigQuery NUMERIC",
            dest_db="bigquery",
        )
        mapped_rows = quarantine_unfit_years(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_booleans(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_temporals(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_specialty_types(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_integers(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            dialect_label="BigQuery INTEGER",
        )
        mapped_rows = quarantine_unfit_bitstrings(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_binaries(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            dialect_label="BigQuery BYTES",
        )
        mapped_rows = quarantine_unfit_enum_set(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
        )
        mapped_rows = quarantine_unfit_strings(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            dialect_label="BigQuery STRING",
        )
        # BigQuery REPEATED columns reject NULL elements and arrays of arrays, so
        # the array gate must run here — the scalar gates above cannot see inside
        # an ARRAY payload.
        mapped_rows = quarantine_unfit_arrays(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            dialect_label="BigQuery",
        )
        mapped_rows = quarantine_unfit_json(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            dialect_label="BigQuery JSON",
        )
        sparse_rows: list[tuple] = []
        rows_for_checksum: list[tuple] = list(mapped_rows)
        try:
            conflict = resolve_conflict_targets(conflict_columns, target_cols)
        except ValueError as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=dataset_id,
                checksum="",
                chunks_completed=0,
                error=str(exc),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )
        if write_mode == "upsert" and conflict:
            mapped_rows, sparse_rows = split_dense_sparse_rows(mapped_rows)
            if DF_LSN_COL in target_cols:
                mapped_rows = dedupe_rows_by_pk_and_lsn(
                    mapped_rows, conflict, target_cols
                )
            else:
                mapped_rows = dedupe_rows(mapped_rows, conflict, target_cols)
        elif write_mode == "upsert" and conflict_columns and not conflict:
            # Operator supplied PKs that do not resolve onto Map targets — refuse
            # rather than split-and-drop sparse CDC rows on the append path.
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=dataset_id,
                checksum="",
                chunks_completed=0,
                error=(
                    "BigQuery upsert conflict_columns do not match mapped targets "
                    f"{list(conflict_columns)!r} vs {target_cols!r} — refuse silent "
                    "sparse CDC drop"
                ),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )
        # Map may stamp VARCHAR while physical DDL is DATE/INT — empty must
        # quarantine before records_for_bigquery / sparse DML (no batch abort).
        # Module-level bind import — never late-import inside write_mapped_rows
        # (UnboundLocalError class that aborted MySQL→Postgres).
        mapped_rows = bind_sql_mapped_rows_with_quarantine(
            mapped_rows,
            target_cols,
            decimal_target_types,
            rejected_details,
            policy,
            engine="bigquery",
            dialect_label="BigQuery",
            mappings=mappings,
        )
        if sparse_rows:
            sparse_rows = bind_sql_mapped_rows_with_quarantine(
                sparse_rows,
                target_cols,
                decimal_target_types,
                rejected_details,
                policy,
                engine="bigquery",
                dialect_label="BigQuery",
                mappings=mappings,
            )
        rejected_rows = _rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy, sparse_rows=sparse_rows
        )
        coerced_null_rows = _coerced_null_row_count(rejected_details, policy)
        _map_abort = reject_on_strict_policy(policy, rejected_details, 'BigQuery', transform_errors)
        if _map_abort:
            return WriteResult(
                ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
                checksum="", chunks_completed=0,
                error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
                rejected_rows=rejected_rows,
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

        from connectors.warehouse_temporal import (
            quarantine_from_bigquery_errors,
            records_for_bigquery,
        )

        # Prefer live/quarantine carriers (ARRAY<T> for REPEATED) over Map stamps —
        # bq_type() strips to SchemaField names and would invent scalar STRING wire.
        bq_types = []
        for t in decimal_target_types:
            raw = str(t or "").strip()
            if raw.upper().startswith("ARRAY<") or raw.upper().startswith("STRUCT"):
                bq_types.append(raw)
            else:
                bq_types.append(bq_type(raw))
        total = len(mapped_rows)
        chunks = max(1, (total + CHUNK_SIZE - 1) // CHUNK_SIZE) if total else 0
        written = 0
        rows_skipped = 0
        chunks_completed = 0
        use_merge = write_mode == "upsert" and bool(conflict)
        if sparse_rows and not use_merge:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table_name,
                target_schema=dataset_id,
                checksum="",
                chunks_completed=0,
                error=(
                    "BigQuery sparse CDC rows require upsert MERGE with resolvable "
                    "conflict_columns — refuse silent drop of omit-from-SET updates"
                ),
                rejected_details=rejected_details,
                warnings=transform_errors,
            )

        sparse_checksum_rows: list[tuple] = []
        if sparse_rows and use_merge:
            sparse_written, sparse_skipped, sparse_checksum = _bq_apply_sparse_upsert(
                client,
                table_id,
                target_cols,
                conflict,
                sparse_rows,
                bq_types,
                rejected_details=rejected_details,
                policy=policy,
            )
            written += sparse_written
            rows_skipped += sparse_skipped
            sparse_checksum_rows = list(sparse_checksum)

        if use_merge:
            if mapped_rows:
                from connectors.writer_common import partition_dense_upsert_rows

                before_dense = len(mapped_rows)
                mapped_rows = partition_dense_upsert_rows(
                    mapped_rows,
                    conflict,
                    target_cols=target_cols,
                    rejected_details=rejected_details,
                    policy=policy,
                )
                rows_skipped += before_dense - len(mapped_rows)
            # Ack checksum = rows that MERGE/land (partitioned dense + sparse images).
            rows_for_checksum = list(mapped_rows) + sparse_checksum_rows
            if mapped_rows:
                staging_name = sanitize_identifier(f"{table_name}_stg_{uuid.uuid4().hex[:8]}")
                staging_id = f"{project_id}.{dataset_id}.{staging_name}"
                staging = bigquery.Table(staging_id, schema=schema_fields)
                client.create_table(staging, exists_ok=True)
                try:
                    if is_local:
                        # goccy/bigquery-emulator does not support load jobs; streaming
                        # inserts into a staging table are immediately MERGE-readable.
                        for chunk_idx in range(chunks):
                            start = chunk_idx * CHUNK_SIZE
                            batch = mapped_rows[start : start + CHUNK_SIZE]
                            if not batch:
                                break
                            records = records_for_bigquery(batch, target_cols, bq_types)
                            errors = client.insert_rows_json(staging_id, records)
                            if errors:
                                raise RuntimeError(f"BigQuery staging insert errors: {errors[:3]}")
                            merge_sql = build_bigquery_merge_sql(
                                table_id,
                                staging_id,
                                target_cols,
                                conflict,
                                lsn_column=DF_LSN_COL if DF_LSN_COL in target_cols else None,
                            )
                            client.query(merge_sql).result()
                            written += len(batch)
                            chunks_completed = chunk_idx + 1
                            if on_checkpoint:
                                on_checkpoint(chunks_completed, chunks, written)
                    else:
                        # Load jobs (not streaming inserts) so staging is immediately MERGE-readable.
                        load_config = bigquery.LoadJobConfig(
                            schema=schema_fields,
                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        )
                        for chunk_idx in range(chunks):
                            start = chunk_idx * CHUNK_SIZE
                            batch = mapped_rows[start : start + CHUNK_SIZE]
                            if not batch:
                                break
                            records = records_for_bigquery(batch, target_cols, bq_types)
                            load_job = client.load_table_from_json(
                                records, staging_id, job_config=load_config
                            )
                            load_job.result()
                            merge_sql = build_bigquery_merge_sql(
                                table_id,
                                staging_id,
                                target_cols,
                                conflict,
                                lsn_column=DF_LSN_COL if DF_LSN_COL in target_cols else None,
                            )
                            client.query(merge_sql).result()
                            written += len(batch)
                            chunks_completed = chunk_idx + 1
                            if on_checkpoint:
                                on_checkpoint(chunks_completed, chunks, written)
                finally:
                    client.delete_table(staging_id, not_found_ok=True)
        else:
            for chunk_idx in range(chunks):
                start = chunk_idx * CHUNK_SIZE
                batch = mapped_rows[start : start + CHUNK_SIZE]
                if not batch:
                    break
                records = records_for_bigquery(batch, target_cols, bq_types)
                errors = client.insert_rows_json(table_id, records)
                if errors:
                    details, bad = quarantine_from_bigquery_errors(
                        errors, batch, target_cols, row_offset=start, policy=policy,
                    )
                    if policy in {"quarantine", "coerce_null"} and bad:
                        rejected_details.extend(details)
                        transform_errors.extend(d["reason"] for d in details[:5])
                        # Streaming insert commits good rows; count only those.
                        written += len(batch) - len(bad)
                    elif policy == "fail":
                        return WriteResult(
                            ok=False,
                            rows_written=written,
                            table_name=table_name,
                            target_schema=dataset_id,
                            checksum="",
                            chunks_completed=chunk_idx,
                            error=str(errors[:2]),
                            rejected_rows=rejected_rows + len(bad),
                            rejected_details=rejected_details + details,
                            warnings=transform_errors,
                        )
                    else:
                        # Unknown policy: fail closed with details, no silent drop.
                        return WriteResult(
                            ok=False,
                            rows_written=written,
                            table_name=table_name,
                            target_schema=dataset_id,
                            checksum="",
                            chunks_completed=chunk_idx,
                            error=str(errors[:2]),
                            rejected_details=rejected_details + details,
                            warnings=transform_errors,
                        )
                else:
                    written += len(batch)
                chunks_completed = chunk_idx + 1
                if on_checkpoint:
                    on_checkpoint(chunks_completed, chunks, written)

        _final_abort = reject_on_strict_policy(policy, rejected_details, "BigQuery")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=table_name,
                target_schema=dataset_id,
                checksum="",
                chunks_completed=chunks_completed or chunks,
                error=_final_abort,
                rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
                rejected_details=rejected_details,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                warnings=transform_errors,
            )

        return WriteResult(
            ok=True, rows_written=written, table_name=table_name, target_schema=dataset_id,
            checksum=row_checksum(
                rows_for_checksum,
                target_cols,
                dest_db_type="bigquery",
                dest_types=dest_types,
            ),
            chunks_completed=chunks_completed or chunks,
            rejected_rows=max(rejected_rows, len(data_rows) - written - rows_skipped),
            rejected_details=rejected_details,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            warnings=transform_errors,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=dataset_id,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=rejected_details if "rejected_details" in locals() else [],
        )
