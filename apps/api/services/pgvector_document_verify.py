"""Gate-8 for the pgvector document layout: every source value, read back (D209).

The document layout keeps each mapped source value inside ``metadata`` JSONB.
Gate-8 used to stop at the document count and call the cells "unproven
(opaque vectors)". The embedding is opaque; the metadata is not. This re-reads
the source independently, reads one metadata object per document back from the
destination, and requires every source row to be present with every carried
field equal — SQL NULL as JSON null, '' as '', unicode byte for byte.
"""

from __future__ import annotations

import itertools
import json
import logging
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)


def _canon(value: Any) -> str:
    """One comparable spelling for a source cell and its JSON read-back."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)):
        try:
            dec = Decimal(str(value))
        except InvalidOperation:
            return f"s:{value}"
        if dec == dec.to_integral_value():
            return f"n:{dec.quantize(Decimal(1))}"
        return f"n:{dec.normalize()}"
    if isinstance(value, (dict, list)):
        return "j:" + json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, (datetime, date, time)):
        return f"s:{value.isoformat()}"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"s:{bytes(value).hex()}"
    text = str(value)
    stripped = text.strip()
    if stripped[:1] in {"{", "["}:
        try:
            parsed = json.loads(stripped)
        except ValueError:
            parsed = None
        if isinstance(parsed, (dict, list)):
            return _canon(parsed)
    return f"s:{text}"


_NUMERIC_TYPES = ("INT", "DEC", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE", "REAL", "MONEY")


def _is_numeric_type(declared: Any) -> bool:
    text = str(declared or "").strip().upper()
    return bool(text) and text.startswith(_NUMERIC_TYPES)


def _is_number(value: Any) -> bool:
    if value is None or isinstance(value, (bool, str, bytes, dict, list)):
        return False
    try:
        return Decimal(str(value)).is_finite()
    except (InvalidOperation, ValueError):
        return False


def _canon_numeric(value: Any) -> str:
    """A numeric field read as text ('1000.00') and stored as a JSON number agree."""
    if value is None or isinstance(value, (bool, dict, list)):
        return _canon(value)
    try:
        dec = Decimal(str(value).strip())  # str, Decimal128, numpy scalars
    except InvalidOperation:
        return _canon(value)
    return _canon(dec) if dec.is_finite() else _canon(value)


def _canon_text_or_temporal(value: Any, numeric: bool = False) -> set[str]:
    """Spellings a source temporal may legitimately take in JSON."""
    if numeric:
        return {_canon_numeric(value)}
    if isinstance(value, (datetime, date, time)):
        iso = value.isoformat()
        return {f"s:{iso}", f"s:{iso.replace('T', ' ')}", f"s:{value}"}
    return {_canon(value)}


def numeric_keys(
    mappings: list[dict[str, Any]], source_schema: Mapping[str, Any] | None = None
) -> set[str]:
    """Metadata keys whose declared type is numeric (mapping or source schema)."""
    schema = dict(source_schema or {})
    out: set[str] = set()
    for m in mappings or []:
        source = str(m.get("source") or "").strip()
        if any(
            _is_numeric_type(t)
            for t in (m.get("source_type"), m.get("target_type"), schema.get(source))
        ):
            out.add(str(m.get("target") or source).strip())
    return out


_TRUE_SPELLINGS = frozenset({"1", "true", "t", "yes", "y"})
_FALSE_SPELLINGS = frozenset({"0", "false", "f", "no", "n"})


def _canon_bool(value: Any) -> str:
    """A boolean field read as 0/1 (MySQL/MariaDB TINYINT(1), BIT(1)) equals its JSON bool."""
    if value is None or isinstance(value, bool):
        return _canon(value)
    if isinstance(value, (bytes, bytearray)) and len(value) == 1:
        value = value[0]
    if isinstance(value, (int, Decimal)) and value in (0, 1):
        return _canon(bool(value))
    text = str(value).strip().lower()
    if text in _TRUE_SPELLINGS:
        return _canon(True)
    if text in _FALSE_SPELLINGS:
        return _canon(False)
    return _canon(value)


def boolean_keys(mappings: list[dict[str, Any]]) -> set[str]:
    """Metadata keys the plan declares BOOLEAN (the writer stores a JSON bool)."""
    from services.type_system import normalize_logical_type

    out: set[str] = set()
    for m in mappings or []:
        if normalize_logical_type(str(m.get("target_type") or "")) == "boolean":
            source = str(m.get("source") or "").strip()
            out.add(str(m.get("target") or source).strip())
    return out


def carried_fields(
    mappings: list[dict[str, Any]], options: Mapping[str, Any]
) -> list[tuple[str, str]]:
    """``(source, metadata key)`` for every mapped field the metadata carries."""
    excluded = {str(c) for c in (options.get("exclude_pii_columns") or []) if c}
    only = {str(c) for c in (options.get("metadata_columns") or []) if c}
    embed = str(options.get("embedding_column") or "").strip()
    out: list[tuple[str, str]] = []
    for m in mappings or []:
        source = str(m.get("source") or "").strip()
        key = str(m.get("target") or source).strip()
        if not source or not key:
            continue
        names = {source, key}
        if names & excluded or (embed and embed in names):
            continue
        if only and not (names & only):
            continue
        out.append((source, key))
    return out


def read_document_metadata(cfg: Mapping[str, Any], schema: str, table: str) -> list[dict[str, Any]]:
    """One metadata object per document (its first chunk)."""
    from psycopg2 import sql

    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 5432),
        database=str(cfg.get("database") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        ssl=bool(cfg.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT DISTINCT ON (source_id) metadata FROM {}.{} "
                    "ORDER BY source_id, chunk_index"
                ).format(sql.Identifier(schema), sql.Identifier(table))
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for (meta,) in rows:
        if isinstance(meta, str):
            meta = json.loads(meta)
        out.append(meta if isinstance(meta, dict) else {})
    return out


def compare_documents(
    source_rows: list[Mapping[str, Any]],
    documents: list[Mapping[str, Any]],
    fields: list[tuple[str, str]],
    numeric: set[str] | None = None,
    boolean: set[str] | None = None,
) -> dict[str, Any]:
    """Every source row must be a document whose carried fields equal it.

    A field is compared by numeric value when it is declared numeric, or when
    either side holds a real number for it (the writer binds a declared
    DECIMAL read as '1000.00' to the JSON number 1000.0). Text-only fields
    compare byte for byte, so '007' never equals '7'.
    """
    numeric = set(numeric or set())
    boolean = set(boolean or set())
    for source, key in fields:
        if any(_is_number(m.get(key, m.get(source))) for m in documents) or any(
            _is_number(r.get(source)) for r in source_rows
        ):
            numeric.add(key)
    # A declared BOOLEAN read as 0/1 is stored as a JSON bool: not a number.
    numeric -= boolean

    def canon_for(key: str) -> Callable[[Any], str]:
        if key in boolean:
            return _canon_bool
        return _canon_numeric if key in numeric else _canon

    def dest_key(meta: Mapping[str, Any]) -> tuple[str, ...] | None:
        cells = []
        for source, key in fields:
            canon = canon_for(key)
            if key in meta:
                cells.append(canon(meta[key]))
            elif source in meta:
                cells.append(canon(meta[source]))
            else:
                return None
        return tuple(cells)

    dest = Counter(k for k in (dest_key(m) for m in documents) if k is not None)
    missing_keys = sum(1 for m in documents if dest_key(m) is None)
    unmatched: list[dict[str, Any]] = []
    for row in source_rows:
        options = [
            [_canon_bool(row.get(source))]
            if key in boolean
            else sorted(_canon_text_or_temporal(row.get(source), key in numeric))
            for source, key in fields
        ]
        hit = next(
            (cand for cand in itertools.product(*options) if dest.get(cand)), None
        )
        if hit is None:
            unmatched.append({s: row.get(s) for s, _k in fields})
            continue
        dest[hit] -= 1
    return {
        "source_rows": len(source_rows),
        "documents": len(documents),
        "fields": [k for _s, k in fields],
        "unmatched_source_rows": len(unmatched),
        "documents_missing_a_field": missing_keys,
        "examples": [
            {k: (None if v is None else str(v)[:120]) for k, v in row.items()}
            for row in unmatched[:3]
        ],
        "document_examples": [
            {k: (None if v is None else str(v)[:120]) for k, v in dict(m).items() if not str(k).startswith("_df_")}
            for m in documents[:2]
        ] if unmatched else [],
        "passed": not unmatched,
    }


def verify_document_values(
    *,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    mappings: list[dict[str, Any]],
    options: Mapping[str, Any],
    source_rows: Any,
    read_source: Callable[[int], list[dict[str, Any]] | None],
    source_schema: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Value proof for a document table, or ``None`` when it could not be measured."""
    if not isinstance(source_rows, int) or source_rows < 0:
        return None
    fields = carried_fields(mappings, options)
    if not fields:
        return None
    try:
        src = read_source(source_rows)
        if src is None:
            return None
        docs = read_document_metadata(cfg, schema, table_name)
    except Exception as exc:  # noqa: BLE001 — the count-only report stands
        logger.warning("pgvector document value proof unavailable: %s", type(exc).__name__)
        return None
    return compare_documents(
        src, docs, fields, numeric_keys(mappings, source_schema), boolean_keys(mappings)
    )


def gate8_document_report(
    *,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    mappings: list[dict[str, Any]],
    source_rows: Any,
    source_endpoint: Any,
    read_sample: Callable[..., list[dict[str, Any]]],
    run_records: list[dict[str, Any]] | None,
    base: dict[str, Any],
    source_schema: Mapping[str, Any] | None = None,
    target_rows: Any,
    target_checksum: str,
) -> dict[str, Any] | None:
    """The Gate-8 report for a document table, or ``None`` to keep the count-only one."""
    from services.pgvector_layout import document_options

    provenance: dict[str, str] = {}
    proof = verify_document_values(
        cfg=cfg,
        schema=schema,
        table_name=table_name,
        mappings=mappings,
        options=document_options(cfg),
        source_rows=source_rows,
        source_schema=source_schema,
        read_source=lambda total: _source_for_proof(
            source_endpoint, mappings, total, read_sample, run_records, provenance
        ),
    )
    if proof is None:
        return None
    if provenance.get("source"):
        proof = {**proof, "source_provenance": provenance["source"]}
    report = {
        **base,
        "target_rows": target_rows,
        "target_checksum": target_checksum,
        "document_value_proof": proof,
        "skipped_readback": False,
        "migration_proven": False,
    }
    fields = ", ".join(proof.get("fields") or [])
    if not proof["passed"]:
        return {
            **report,
            "passed": False,
            "unproven": False,
            "message": (
                f"pgvector document metadata does not hold {proof['unmatched_source_rows']} "
                f"of {proof['source_rows']} source row(s) value for value "
                f"(fields: {fields}); first: {proof['examples'][:1]}"
            ),
        }
    return {
        **report,
        "passed": True,
        "unproven": False,
        "message": (
            f"pgvector document metadata holds every source value of "
            f"{proof['source_rows']} row(s) (fields: {fields}; NULL as JSON null). "
            "Embedding vectors are opaque and not compared."
        ),
    }


def read_source_population(
    source: Any,
    mappings: list[dict[str, Any]],
    total: int,
    read_sample: Callable[..., list[dict[str, Any]]],
) -> list[dict[str, Any]] | None:
    """The whole source table when it holds exactly the rows this run read.

    A filtered, windowed (incremental) or limited run read part of the table;
    its rows cannot be told apart here, so the proof is left to the count.
    """
    if source is None or str(getattr(source, "kind", "") or "") != "database":
        return None
    table = str(getattr(source, "table", "") or "").strip()
    columns = [str(m.get("source") or "") for m in mappings or [] if str(m.get("source") or "").strip()]
    if not table or not columns:
        return None
    from services.dialect_profiles import schema_from_cfg
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(source)
    engine = resolve_driver_type(str(cfg.get("type") or getattr(source, "format", "") or "")).lower()
    rows = read_sample(
        "pgvector document proof (source)",
        engine,
        cfg,
        schema=schema_from_cfg(engine, cfg),
        table_name=table,
        columns=columns,
        limit=total + 1,
    )
    rows = [r for r in rows or [] if isinstance(r, dict)]
    return rows if len(rows) == total else None


def _source_for_proof(
    source: Any,
    mappings: list[dict[str, Any]],
    total: int,
    read_sample: Callable[..., list[dict[str, Any]]],
    run_records: list[dict[str, Any]] | None,
    provenance: dict[str, str],
) -> list[dict[str, Any]] | None:
    """The independent source re-read when it is this run's population, else the run's rows.

    An incremental, filtered or limited run read part of the table. Its own
    rows as read (before the writer) are then what the metadata must hold.
    """
    rows = read_source_population(source, mappings, total, read_sample)
    if rows is not None:
        provenance["source"] = "independent_source_reread"
        return rows
    records = [r for r in run_records or [] if isinstance(r, dict)]
    if records and len(records) == total:
        provenance["source"] = "run_records_as_read"
        return records
    return None
