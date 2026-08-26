"""Write-quarantine passes for exotic destination carriers (Phase F8 extraction).

Bit strings, binary/BLOB widths, ENUM/SET membership and the specialty wire
shapes (geography, interval, vector, XML, inet/cidr, macaddr, ltree) share one
contract with the rest of the matrix in :mod:`connectors.writer_common`: a cell
the destination cannot store is held out with an actionable reason, never
silently truncated. They live here so the shared writer trunk stays inside its
frozen size budget; ``writer_common`` re-exports them, so no call site changes.

Helpers from the trunk are imported inside the functions: the trunk re-exports
this module, and a module-level import back into it would be a cycle.
"""

from __future__ import annotations

import re
from typing import Any


def _append_write_quarantine_detail(*args: Any, **kwargs: Any) -> None:
    from connectors.writer_common import append_write_quarantine_detail

    append_write_quarantine_detail(*args, **kwargs)


def _binary_storage_bytes(value: Any) -> bytes | None:
    from connectors.writer_common import binary_storage_bytes

    return binary_storage_bytes(value)

def quarantine_unfit_bitstrings(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells that are not valid 0/1 bitstrings or exceed BIT(n)/VARBIT(n).

    BIT destinations must not receive base64/UTF-8 invent (BYTEA path).
    """

    from connectors.sql_bind import coerce_bitstring_wire
    from services.type_system import (
        is_bitstring_carrier,
        is_varying_bitstring_carrier,
        parse_bitstring_width,
    )
    from services.value_serializer import cell_to_string

    bit_cols: list[tuple[int, str]] = []
    for i, typ in enumerate(target_types):
        if is_bitstring_carrier(typ):
            bit_cols.append((i, typ))
    if not bit_cols:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, typ in bit_cols:
            from connectors.writer_common import _unfit_cell_absent

            if _unfit_cell_absent(cells, col_idx):
                continue
            try:
                bits = coerce_bitstring_wire(
                    cells[col_idx],
                    width=parse_bitstring_width(typ),
                    varying=is_varying_bitstring_carrier(typ),
                )
            except ValueError as exc:
                sample = cell_to_string(cells[col_idx])[:120]
                _append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": f"{exc} — quarantined",
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
                if policy == "coerce_null":
                    from services.value_serializer import DF_MISSING_SENTINEL
                    cells[col_idx] = DF_MISSING_SENTINEL
                else:
                    hold_out = True
                    break
                continue
            if bits is not None:
                cells[col_idx] = bits
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_binaries(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dialect_label: str = "VARBINARY",
) -> list[tuple]:
    """Hold out / NULL cells that overflow BINARY(n) or fail base64 wire decode.

    Preflight samples miss production outliers; silent truncate / UTF-8 invent
    is forbidden. Invalid base64 is quarantined (not re-encoded).
    BIT/VARBIT columns are handled by ``quarantine_unfit_bitstrings``.
    """

    from services.type_system import (
        is_bitstring_carrier,
        normalize_logical_type,
        parse_binary_carrier_width,
    )

    bin_cols: list[tuple[int, int | None, str]] = []
    for i, typ in enumerate(target_types):
        if normalize_logical_type(typ) != "binary":
            continue
        if is_bitstring_carrier(typ):
            continue
        width = parse_binary_carrier_width(typ)
        bin_cols.append((i, width, typ))
    if not bin_cols:
        return mapped_rows

    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, width, typ in bin_cols:
            from connectors.writer_common import _unfit_cell_absent

            if _unfit_cell_absent(cells, col_idx):
                continue
            raw = _binary_storage_bytes(cells[col_idx])
            if raw is None:
                sample = cell_to_string(cells[col_idx])[:120]
                _append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"binary wire is not valid base64 for {dialect_label} "
                    "— quarantined (refuse silent UTF-8 encode)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
                if policy == "coerce_null":
                    from services.value_serializer import DF_MISSING_SENTINEL
                    cells[col_idx] = DF_MISSING_SENTINEL
                else:
                    hold_out = True
                    break
                continue
            if width is not None and len(raw) > width:
                sample = cell_to_string(cells[col_idx])[:120]
                _append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"binary length {len(raw)} exceeds {dialect_label}({width}) "
                    "— quarantined (would truncate on write)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=row,
                target_cols=target_cols,
            )
                if policy == "coerce_null":
                    from services.value_serializer import DF_MISSING_SENTINEL
                    cells[col_idx] = DF_MISSING_SENTINEL
                else:
                    hold_out = True
                    break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def quarantine_unfit_enum_set(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    set_joiner: str = ",",
) -> list[tuple]:
    """Hold out / NULL cells outside a destination ENUM/SET member domain.

    MySQL non-strict ENUM stores invalid values as '' — silent wipe. Fail closed.
    HubSpot checkbox / Salesforce multipicklist use ``set_joiner=';'``.
    """

    from services.type_system import parse_enum_or_set_ordered_members
    from services.value_serializer import cell_to_string

    domain_cols: list[tuple[int, str, str]] = []
    for i, typ in enumerate(target_types):
        parsed = parse_enum_or_set_ordered_members(typ)
        if not parsed:
            continue
        kind, members = parsed
        if not members:
            continue
        domain_cols.append((i, kind, typ))
    if not domain_cols:
        return mapped_rows

    from connectors.sql_bind import coerce_enum_wire, coerce_set_wire

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, kind, typ in domain_cols:
            from connectors.writer_common import _unfit_cell_absent

            if _unfit_cell_absent(cells, col_idx):
                continue
            try:
                if kind == "ENUM":
                    cells[col_idx] = coerce_enum_wire(cells[col_idx], ddl_type=typ)
                else:
                    cells[col_idx] = coerce_set_wire(
                        cells[col_idx], ddl_type=typ, joiner=set_joiner
                    )
                continue
            except ValueError:
                raw = cell_to_string(cells[col_idx])
            sample = raw[:120]
            _append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": (
                    f"value not in {kind} domain — quarantined "
                    "(MySQL would store '' / drop SET members silently)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=cells,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out

def _specialty_column_kind(type_str: str) -> str | None:
    """Return specialty kind when destination DDL needs wire-shape quarantine.

    String/VARCHAR carriers (Databricks/Iceberg) are skipped for geo/interval —
    quarantine applies only when the destination expects specialty bind.
    """
    from services.type_system import (
        normalize_logical_type,
        parse_vector_dimension,
        specialty_carrier_base,
    )

    logical = normalize_logical_type(type_str)
    if logical == "geography":
        return "geography"
    if logical == "interval":
        return "interval"
    if logical == "vector" and parse_vector_dimension(type_str) is not None:
        return "vector"
    upper = (type_str or "").upper()
    if re.search(
        r"\b(GEOGRAPHY|GEOMETRY|SDO_GEOMETRY|POINT|LINESTRING|POLYGON|"
        r"MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\b",
        upper,
    ):
        return "geography"
    if re.search(r"\bINTERVAL\b", upper):
        return "interval"
    # Network / text / geometric specialty — bind refuse must be mirrored in the
    # matrix so object-store / SaaS paths never green empty HSTORE then crash.
    spec = specialty_carrier_base(type_str)
    if spec in {"INET", "IPV4", "IPV6", "IP"}:
        return "inet"
    if spec == "CIDR":
        return "cidr"
    if spec in {"MACADDR", "MACADDR8"}:
        return "macaddr"
    if spec in {"XML", "XMLTYPE"}:
        return "xml"
    if spec == "LTREE":
        return "ltree"
    if spec:
        # HSTORE / TSVECTOR / POINT / BOX / RANGE / OID / PG_LSN / …
        return "bind"
    return None


def quarantine_unfit_specialty_types(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Hold out cells unfit for GEOGRAPHY / INTERVAL / VECTOR / INET / … sinks.

    Specialty types travel as identity payloads (WKT/GeoJSON/ISO-8601/float lists).
    Fail-closed — never invent empty geometry, wrong interval family, or pad/truncate
    embedding dimensions (pgvector/Snowflake VECTOR reject wrong width).
    """
    specialty_cols: list[tuple[int, str, str]] = []
    for i, typ in enumerate(target_types):
        kind = _specialty_column_kind(typ)
        if kind:
            specialty_cols.append((i, kind, typ))
    if not specialty_cols:
        return mapped_rows

    from connectors.sql_bind import (
        coerce_cidr_wire,
        coerce_inet_wire,
        coerce_ltree_wire,
        coerce_macaddr_wire,
        coerce_xml_wire,
    )
    from services.schema_inference import (
        geography_wire_srid,
        interval_wire_family,
        is_geography_wire,
        is_interval_wire,
    )
    from services.type_system import (
        interval_family,
        parse_geography_srid,
        parse_vector_dimension,
        parse_vector_length,
        specialty_carrier_base,
    )
    from services.value_serializer import cell_to_string

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, kind, typ in specialty_cols:
            from connectors.writer_common import _unfit_cell_absent

            if _unfit_cell_absent(cells, col_idx):
                continue
            reason = ""
            ok = True
            if kind == "geography":
                ok = is_geography_wire(cells[col_idx])
                if ok:
                    dest_srid = parse_geography_srid(typ)
                    wire_srid = geography_wire_srid(cells[col_idx])
                    if (
                        dest_srid is not None
                        and wire_srid is not None
                        and dest_srid != wire_srid
                    ):
                        ok = False
                        reason = (
                            f"geography SRID mismatch wire={wire_srid} dest={dest_srid} "
                            "— quarantined (refuse silent reproject)"
                        )
            elif kind == "interval":
                ok = is_interval_wire(cells[col_idx])
                if ok:
                    dest_fam = interval_family(typ)
                    wire_fam = interval_wire_family(cells[col_idx])
                    if dest_fam and wire_fam and dest_fam != wire_fam:
                        ok = False
                        reason = (
                            f"interval family mismatch wire={wire_fam} dest={dest_fam} "
                            "— quarantined (YEAR-MONTH ↔ DAY-SECOND collapse)"
                        )
            elif kind == "vector":
                dest_dim = parse_vector_dimension(typ)
                wire_len = parse_vector_length(cells[col_idx])
                if dest_dim is None:
                    ok = True
                elif wire_len is None:
                    ok = False
                    reason = (
                        f"value is not a parseable VECTOR({dest_dim}) payload "
                        "— quarantined (refuse invent embedding)"
                    )
                elif wire_len != dest_dim:
                    ok = False
                    reason = (
                        f"vector length {wire_len} ≠ destination VECTOR({dest_dim}) "
                        "— quarantined (refuse pad/truncate embedding)"
                    )
            elif kind in {"inet", "cidr", "macaddr", "xml", "ltree"}:
                try:
                    if kind == "inet":
                        coerce_inet_wire(cells[col_idx])
                    elif kind == "cidr":
                        coerce_cidr_wire(cells[col_idx])
                    elif kind == "macaddr":
                        eui64 = specialty_carrier_base(typ) == "MACADDR8"
                        coerce_macaddr_wire(cells[col_idx], eui64=eui64)
                    elif kind == "xml":
                        coerce_xml_wire(cells[col_idx])
                    else:
                        coerce_ltree_wire(cells[col_idx])
                except ValueError as exc:
                    ok = False
                    reason = str(exc)[:300]
            elif kind == "bind":
                from connectors.sql_bind import normalize_sql_bind_value

                try:
                    normalize_sql_bind_value(cells[col_idx], typ, engine="")
                except ValueError as exc:
                    ok = False
                    reason = str(exc)[:300]
            if ok:
                continue
            sample = cell_to_string(cells[col_idx])[:120]
            _append_write_quarantine_detail(
                rejected_details,
                {
                    "row": row_idx + 1,
                    "column": target_cols[col_idx],
                    "target": target_cols[col_idx],
                    "value": sample,
                    "reason": reason
                    or (
                    f"value is not a valid {kind} wire payload "
                    "— quarantined (would fail destination bind or invent a cast)"
                    ),
                    "policy": "write_quarantine",
                    "chars": [],
                },
                mapped_row=row,
                target_cols=target_cols,
            )
            if policy == "coerce_null":
                from services.value_serializer import DF_MISSING_SENTINEL
                cells[col_idx] = DF_MISSING_SENTINEL
            else:
                hold_out = True
                break
        if hold_out:
            continue
        out.append(tuple(cells))
    return out
