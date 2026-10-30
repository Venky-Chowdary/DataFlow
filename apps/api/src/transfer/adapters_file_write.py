from __future__ import annotations

import csv
import io
from typing import Any

from services.value_serializer import cell_to_string
from src.transfer.models import EndpointConfig
from src.transfer.adapters import (
    FileExportMapBlocked,
    _CONTAINER_EXPORT_FORMATS,
)

def write_destination_file(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    *,
    source_format: str | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    validation_mode: str = "strict",
) -> tuple[bytes, str, dict]:
    """Write records to CSV, JSON, JSONL, TSV, YAML, FWF, or a container format."""
    import sys
    from pathlib import Path

    _api_root = Path(__file__).resolve().parents[2]
    if str(_api_root) not in sys.path:
        sys.path.insert(0, str(_api_root))
    from connectors.writer_common import (
        resolve_target_columns,
        transform_error_policy_for_validation_mode,
    )
    from services.format_converter import can_convert, convert_rows

    fmt = (endpoint.format or "json").lower()
    # ndjson is a JSON Lines alias for the conversion engine
    if fmt == "ndjson":
        fmt = "jsonl"
    src_fmt = (source_format or fmt).lower()
    if src_fmt == "ndjson":
        src_fmt = "jsonl"
    types = column_types or {}

    export_columns = columns
    export_records = records
    transform_errors: list[str] = []
    rejected_details: list[dict[str, Any]] = []

    if mappings:
        from connectors.writer_common import (
            apply_write_quarantine_matrix,
            build_mapped_rows_with_details,
            reject_on_strict_policy,
            transform_error_policy,
        )
        from connectors.source_row_spool import matrix_row_from_record

        headers = columns
        data_rows = [matrix_row_from_record(rec, headers) for rec in records]
        target_cols, _ = resolve_target_columns(mappings, types)
        error_policy = transform_error_policy_for_validation_mode(validation_mode)
        mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=types,
            error_policy=error_policy,
            dest_types={
                str(m.get("target") or ""): str(
                    m.get("target_type") or m.get("dest_type") or types.get(m.get("source") or "", "")
                )
                for m in mappings
                if str(m.get("target") or "").strip()
            },
        )
        # Same typed quarantine matrix as SQL/object writers — never export oversize/
        # unfit cells that transforms left as strings.
        dest_type_list = [
            str(
                next(
                    (
                        m.get("target_type") or m.get("dest_type") or types.get(m.get("source") or "", "")
                        for m in mappings
                        if str(m.get("target") or "") == col
                    ),
                    types.get(col, ""),
                )
                or ""
            )
            for col in target_cols
        ]
        if mapped_rows and any(str(t).strip() for t in dest_type_list):
            policy = transform_error_policy(error_policy)
            mapped_rows = apply_write_quarantine_matrix(
                mapped_rows,
                target_cols,
                dest_type_list,
                rejected_details,
                policy,
                dialect_label="file_export",
                mappings=list(mappings) or None,
            )
        # Keep DF_MISSING through export — JSON/JSONL omit keys; dense CSV/grid
        # render empty via to_delimited_value. Never force-null invent before serialize.
        abort = reject_on_strict_policy(error_policy, rejected_details, "file_export")
        if abort:
            raise FileExportMapBlocked(
                abort,
                rejected_details=rejected_details,
                rejected_rows=len(rejected_details),
            )
        export_columns = target_cols
        export_records = [dict(zip(target_cols, row)) for row in mapped_rows]

    if mappings:
        export_dest_types = {
            str(m.get("target") or ""): str(
                m.get("target_type")
                or m.get("dest_type")
                or types.get(m.get("source") or "", "")
            )
            for m in mappings
            if str(m.get("target") or "").strip()
        }
    else:
        export_dest_types = dict(types)

    from connectors.writer_common import to_delimited_value

    def _export_grid_cell(val: Any, col: str) -> str:
        """One CSV/grid cell — reader-null is empty, never the extract token."""
        parsed = to_delimited_value(val, col, export_dest_types)
        if parsed is None:
            return ""
        if isinstance(parsed, str):
            return parsed
        return cell_to_string(parsed)

    def _export_summary(
        filename: str,
        *,
        mime: str | None = None,
        converted_from: str | None = None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "format": fmt,
            "filename": filename,
            "rows": len(export_records),
            "transform_errors": transform_errors[:10],
            "mapped": bool(mappings),
            "rejected_details": list(rejected_details),
            "rejected_rows": len(rejected_details),
        }
        if mime is not None:
            out["mime"] = mime
        if converted_from is not None:
            out["converted_from"] = converted_from
        return out

    grid = [
        [_export_grid_cell(rec.get(col), col) for col in export_columns]
        for rec in export_records
    ]

    # JSON/JSONL must use omit-aware serialization below — the grid convert path
    # renders DF_MISSING / reader-null as empty (never the extract token).
    #
    # A container format is decided by the *destination*, never by whether the
    # source's own format can be converted into it: a database source has no
    # file format to convert from, so ``can_convert("postgresql", "avro")`` is
    # false and the export used to fall through to the JSON writer — landing
    # JSON bytes under an ``.avro`` name that no Avro reader can open. An empty
    # population is still that container, not an empty JSON array.
    container = fmt in _CONTAINER_EXPORT_FORMATS
    if fmt not in {"json", "jsonl", "yaml", "fixed_width", "fwf"} and (
        container or (can_convert(src_fmt, fmt) and grid)
    ):
        content, mime = convert_rows(
            export_columns,
            grid,
            source_format="csv" if container else src_fmt,
            target_format=fmt,
        )
        ext = (
            "tsv"
            if fmt == "tsv"
            else "xlsx"
            if fmt == "excel"
            else "parquet"
            if fmt == "parquet"
            else "avro"
            if fmt == "avro"
            else "orc"
            if fmt == "orc"
            else "xml"
            if fmt == "xml"
            else fmt
            if fmt in ("csv", "jsonl")
            else "json"
        )
        filename = f"export.{ext}"
        return (
            content,
            filename,
            _export_summary(
                filename=filename,
                mime=mime,
                converted_from=src_fmt if src_fmt != fmt else None,
            ),
        )

    def _json_export_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from connectors.writer_common import present_field_bindings, to_json_value

        # Kafka/object-store class: omit STOP_COLUMN / sparse CDC keys entirely.
        # Reader-null binds as None then JSON null — never the extract token.
        return [
            {
                c: to_json_value(v, c, export_dest_types, exact_decimal=True)
                for c, v in present_field_bindings(r).items()
            }
            for r in rows
        ]

    export_mime = ""
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=export_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(
            [
                {c: _export_grid_cell(r.get(c), c) for c in export_columns}
                for r in export_records
            ]
        )
        content = buf.getvalue().encode("utf-8")
        filename = "export.csv"
        export_mime = "text/csv"
    elif fmt == "tsv":
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=export_columns, delimiter="\t", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(
            [
                {c: _export_grid_cell(r.get(c), c) for c in export_columns}
                for r in export_records
            ]
        )
        content = buf.getvalue().encode("utf-8")
        filename = "export.tsv"
        export_mime = "text/tab-separated-values"
    elif fmt == "jsonl":
        records = _json_export_records(export_records)
        from services.value_serializer import json_dumps_exact_numbers

        lines = [json_dumps_exact_numbers(r) for r in records]
        content = "\n".join(lines).encode("utf-8")
        filename = "export.jsonl"
        export_mime = "application/x-ndjson"
    elif fmt == "yaml":
        from services.yaml_tabular import dump_yaml_records

        yaml_rows = [
            {c: _export_grid_cell(r.get(c), c) for c in export_columns}
            for r in export_records
        ]
        content = dump_yaml_records(yaml_rows, export_columns)
        filename = "export.yaml"
        export_mime = "application/yaml"
    elif fmt in {"fixed_width", "fwf"}:
        from services.fixed_width_layout import (
            dump_fixed_width_records,
            resolve_dest_export_layout,
        )

        extra = endpoint.extra if isinstance(endpoint.extra, dict) else {}
        write_opts = extra.get("write_options") if isinstance(extra.get("write_options"), dict) else {}
        declared = extra.get("fixed_width_layout")
        if declared in (None, "", (), []):
            declared = write_opts.get("fixed_width_layout")
        layout = resolve_dest_export_layout(
            declared=declared,
            columns=export_columns,
            dest_types=export_dest_types,
        )
        fwf_rows = [
            {c: _export_grid_cell(r.get(c), c) for c in export_columns}
            for r in export_records
        ]
        content = dump_fixed_width_records(fwf_rows, layout)
        filename = "export.fwf"
        export_mime = "text/plain"
        fmt = "fixed_width"
    else:
        if fmt not in {"json", "csv", "tsv", "jsonl", "yaml", "fixed_width"}:
            raise ValueError(
                f"File export format '{fmt}' is not supported — refusing to "
                "write JSON bytes under that name"
            )
        records = _json_export_records(export_records)
        from services.value_serializer import json_dumps_exact_numbers

        content = json_dumps_exact_numbers(records, indent=2).encode("utf-8")
        filename = "export.json"
        export_mime = "application/json"
    return (
        content,
        filename,
        _export_summary(filename, mime=export_mime or None),
    )
