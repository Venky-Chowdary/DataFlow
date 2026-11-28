"""Oracle-specific Gate-8 read-back.

Oracle is the one supported destination where the *identity* of the object the
writer filled is not knowable from the name the operator typed: unquoted DDL
folds to upper case while quoted DDL is stored verbatim, so ``users`` and
``"users"`` are two tables. Reconciliation that folds the name reads the wrong
object — a written table then reports "sample unavailable" or, worse, a clean
count from a table this job never touched. Every read here resolves the stored
spelling through :mod:`services.sql_object_identity` first.

Split out of :mod:`services.reconciliation` so the dialect's rules (LOB
comparison limits, ``FETCH FIRST``, catalog identity) live in one place.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from connectors.oracle_tstz import (
    oracle_tz_kinds,
    oracle_tz_positions,
    oracle_tz_select_list,
    oracle_tz_to_char,
    parse_oracle_tz_row,
)
from services.readback_projection import project_readback
from services.reconciliation import (
    canonical_checksum_from_iter,
    keyed_readback_sa_clause,
    keyed_readback_scope,
    sa_streaming_result,
)

logger = logging.getLogger(__name__)

#: Destination type strings routed to this module.

#: Oracle column types that cannot appear in ORDER BY / IN comparisons.
_ORACLE_LOB_TYPES: Final = {"CLOB", "NCLOB", "BLOB", "LONG", "LONG RAW", "XMLTYPE"}


def is_oracle_destination(db_type: str, dest: dict[str, Any]) -> bool:
    """True when this destination is Oracle, including generic SQL URLs."""
    from src.transfer.engine_family import family_or_kind

    if family_or_kind(db_type) == "oracle":
        return True
    return db_type == "generic_sql" and str(
        dest.get("connection_string") or ""
    ).lower().startswith("oracle")


class OracleCatalogUnreadable(RuntimeError):
    """The Oracle catalog did not answer; the object's spelling is unknown."""


def oracle_catalog_identity(
    conn: Any,
    table_name: str,
    schema: str | None,
    columns: list[str] | None = None,
) -> tuple[str | None, str | None, dict[str, str]]:
    """Resolve the stored spelling of an Oracle table, owner and columns.

    Guessing the fold made Gate-8 read back ``"USERS"`` from a table the writer
    had just filled as ``"users"``: rows landed, then the job failed as "sample
    unavailable". Both sides now ask the catalog what the object is actually
    called instead of folding.

    Returns ``(owner, table, {requested_column: stored_column})``. Anything the
    catalog does not know is returned unresolved — never invented.
    """
    from services.sql_object_identity import resolve_object_identity

    # The caller knows this is Oracle: ask Oracle's catalog directly rather
    # than sniffing the connection (a proxy without ``.dialect`` fell to the
    # generic inspector and read as unreadable on a healthy catalog).
    ident = resolve_object_identity(conn, table_name, schema, columns=columns, dialect="oracle")
    if ident.catalog_unreadable:
        # "Could not ask" is not "not there": a read-back that folded the name
        # on a blip would verify the wrong object or report it missing (D266).
        raise OracleCatalogUnreadable(
            f"Oracle catalog could not be read for {schema + '.' if schema else ''}{table_name}"
        )
    if ident.known_missing:
        return None, None, {}
    return ident.schema, ident.table, dict(ident.columns)


def oracle_comparable_expr(
    conn: Any, table_name: str, schema: str | None, column: str
) -> str:
    """SQL expression for ``column`` usable as an Oracle comparison key.

    LOB columns raise ORA-22848 in ORDER BY / IN. ``DBMS_LOB.SUBSTR`` of 4000
    characters raises ORA-06502 when those characters exceed 4000 bytes, so
    the order key is a 1000-character prefix. Equality of a sort key uses
    ``DBMS_LOB.COMPARE`` on the whole value, which does not return a VARCHAR2.
    """
    import sqlalchemy as sa

    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    quoted = quote_sql_identifier(require_safe_identifier(column, preserve_case=True))
    try:
        # Case-insensitive match: a quoted lower-case table/column created by an
        # older run is the same physical object as the folded spelling.
        row = conn.execute(
            sa.text(
                "SELECT data_type FROM all_tab_columns "
                "WHERE UPPER(table_name) = :t AND UPPER(column_name) = :c "
                "AND (:s IS NULL OR UPPER(owner) = :s)"
            ),
            {
                "t": str(table_name or "").upper(),
                "c": str(column or "").upper(),
                "s": str(schema).upper() if schema else None,
            },
        ).first()
    except Exception:
        return quoted
    data_type = str((row or [""])[0] or "").upper()
    if data_type in _ORACLE_LOB_TYPES:
        # 1000 characters stays inside VARCHAR2's 4000-byte SQL limit (ORA-06502).
        return f"DBMS_LOB.SUBSTR({quoted}, 1000, 1)"
    return quoted


_ORACLE_TS_BIND_FORMAT: Final = "YYYY-MM-DD HH24:MI:SS.FF9"
_ORACLE_TSTZ_BIND_FORMAT: Final = "YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM"


def oracle_temporal_key_sql(conn: Any, table: str, owner: str | None, column: str) -> str:
    """``TO_TIMESTAMP[_TZ](:{bind}, fmt)`` for a temporal key column, else ``""`` (D335).

    A text bind against an Oracle DATE/TIMESTAMP is converted with the session
    format; the ISO ``T`` of a stored watermark is ORA-01861. The conversion is
    explicit here and the bound text is normalised by :func:`oracle_temporal_key_text`.
    """
    import sqlalchemy as sa

    try:
        row = conn.execute(
            sa.text(
                "SELECT data_type FROM all_tab_columns "
                "WHERE UPPER(table_name) = :t AND UPPER(column_name) = :c "
                "AND (:s IS NULL OR UPPER(owner) = :s)"
            ),
            {
                "t": str(table or "").upper(),
                "c": str(column or "").upper(),
                "s": str(owner).upper() if owner else None,
            },
        ).first()
    except Exception:  # noqa: BLE001 - an unread catalog keeps the plain bind
        return ""
    data_type = str((row or [""])[0] or "").upper()
    if "TIME ZONE" in data_type:
        return "TO_TIMESTAMP_TZ({bind}, '" + _ORACLE_TSTZ_BIND_FORMAT + "')"
    if data_type == "DATE" or data_type.startswith("TIMESTAMP"):
        return "TO_TIMESTAMP({bind}, '" + _ORACLE_TS_BIND_FORMAT + "')"
    return ""


def oracle_temporal_key_text(value: Any, *, zoned: bool) -> Any:
    """ISO timestamp text in the spelling the explicit conversion parses."""
    import re

    m = re.match(
        r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?\s*(Z|[+-]\d{2}:?\d{2})?$",
        str(value or "").strip(),
    )
    if not m:
        return value
    day, clock, frac, offset = m.groups()
    text = f"{day} {clock}.{(frac or '0')[:9]}"
    if not zoned:
        # An NTZ column holds the civil digits; the writer strips an offset.
        return text
    if not offset or offset == "Z":
        offset = "+00:00"
    elif ":" not in offset:
        offset = f"{offset[:3]}:{offset[3:]}"
    return f"{text} {offset}"


def oracle_probe_description(conn: Any, table_ref: str) -> Any:
    """Driver column description of ``table_ref`` (zero-row probe)."""
    import sqlalchemy as sa

    probe = conn.execute(sa.text(f"SELECT * FROM {table_ref} WHERE 1=0"))  # nosec B608
    return getattr(getattr(probe, "cursor", None), "description", None)


def verify_oracle_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent Oracle read-back for Gate-8 (write-location ''≡NULL fingerprints).

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "oracle",
            "host": host or "",
            "port": int(port or 1521),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
        }
        engine = get_sqlalchemy_engine(cfg)
        sch = (schema or username or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect="oracle")
        with engine.connect() as conn:
            # Verify the object the writer filled — see oracle_catalog_identity.
            owner, stored_table, pk_map = oracle_catalog_identity(
                conn, table_name, sch, [pk_column] if pk_column else None
            )
            if stored_table:
                table_ref = quote_table_ref(
                    stored_table,
                    schema=owner or sch,
                    dialect="oracle",
                    preserve_case=True,
                )
            if pk_column and pk_map.get(pk_column):
                pk_column = pk_map[pk_column]
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            # The driver reads TSTZ naive; read back the instant that landed (D155).
            description = oracle_probe_description(conn, table_ref)
            tz_positions = oracle_tz_positions(description)
            select_list = oracle_tz_select_list(description)
            select = sa.text(f"SELECT {select_list} FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="oracle")
                select = sa.text(
                    f"SELECT {select_list} FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns, projected = project_readback(
                names,
                target_columns,
                (parse_oracle_tz_row(row, tz_positions) for row in result),
            )
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="oracle",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Oracle reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


# A single VARCHAR2 bind of a 40k character key raises ORA-01460.
_ORACLE_BIND_CHARS = 1000


def _oracle_clob_equals(quoted_col: str, index: int, key: str) -> tuple[str, dict[str, str]]:
    """``DBMS_LOB.COMPARE`` of ``key`` using binds that fit in VARCHAR2."""
    text = key or ""
    pieces = [
        text[offset:offset + _ORACLE_BIND_CHARS]
        for offset in range(0, max(len(text), 1), _ORACLE_BIND_CHARS)
    ]
    names: list[str] = []
    bound: dict[str, str] = {}
    for piece_i, piece in enumerate(pieces):
        name = f"k{index}_{piece_i}"
        names.append(f"TO_CLOB(:{name})")
        bound[name] = piece
    return f"DBMS_LOB.COMPARE({quoted_col}, {' || '.join(names)}) = 0", bound


def read_oracle_target_sample(
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[Any],
    sort_key: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Ordered destination sample from the Oracle object the writer filled."""
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import (
        quote_column_list,
        quote_sql_identifier,
        quote_table_ref,
        require_safe_identifier,
    )

    lim = max(1, int(limit or 50))
    ora_col_sql = (
        "*"
        if cols == ["*"]
        else quote_column_list(
            [require_safe_identifier(c, preserve_case=True) for c in cols]
        )
    )
    sch = (schema or dest.get("schema") or dest.get("username") or "").strip() or None
    table_ref = quote_table_ref(table_name, schema=sch, dialect="oracle")
    engine = get_sqlalchemy_engine(
        {
            "type": "oracle",
            "host": dest.get("host", ""),
            "port": int(dest.get("port") or 1521),
            "database": dest.get("database", ""),
            "username": dest.get("username", ""),
            "password": dest.get("password", ""),
            "connection_string": dest.get("connection_string", ""),
            "schema": schema or dest.get("schema") or "",
        }
    )
    with engine.connect() as conn:
        # Read back the object the writer actually filled: ask the catalog for
        # its stored spelling instead of folding the name.
        owner, stored_table, col_map = oracle_catalog_identity(
            conn,
            table_name,
            sch,
            None if cols == ["*"] else list(cols),
        )
        read_table = stored_table or table_name
        read_owner = owner or sch
        if stored_table:
            table_ref = quote_table_ref(
                read_table, schema=read_owner, dialect="oracle", preserve_case=True
            )
        description = oracle_probe_description(conn, table_ref)
        if cols == ["*"]:
            ora_col_sql = oracle_tz_select_list(description)
            tz_positions = oracle_tz_positions(description)
        else:
            # Same TSTZ rule as the checksum read-back (D155), per named column.
            kinds_by_name = {
                str(description[i][0]).upper(): kind
                for i, kind in oracle_tz_kinds(description).items()
            }
            stored = [
                require_safe_identifier(col_map.get(c, c), preserve_case=True)
                for c in cols
            ]
            col_kinds = {
                i: kinds_by_name[name.upper()]
                for i, name in enumerate(stored)
                if name.upper() in kinds_by_name
            }
            tz_positions = frozenset(col_kinds)
            if col_map or tz_positions:
                quoted = [quote_sql_identifier(name) for name in stored]
                ora_col_sql = ", ".join(
                    f"{oracle_tz_to_char(q, col_kinds[i])} AS {q}" if i in col_kinds else q
                    for i, q in enumerate(quoted)
                )
        sort_key_stored = col_map.get(sort_key, sort_key) if sort_key else ""
        # Oracle rejects a LOB wherever it needs a comparison key (ORA-22848),
        # so ordering or ``IN``-filtering on a CLOB — what an unbounded source
        # text column becomes here — made a written table read back as "sample
        # unavailable". Compare on the leading 4000 characters instead, and
        # order by ROWID when the caller supplied no key at all.
        key_expr = (
            oracle_comparable_expr(conn, read_table, read_owner, sort_key_stored)
            if sort_key
            else ""
        )
        ora_order = key_expr or "ROWID"
        lob_order = key_expr.startswith("DBMS_LOB.SUBSTR")
        if keys and sort_key and lob_order:
            # The whole sort key, not the 1000-character order prefix (D362).
            # One TO_CLOB bind of a 40k key raises ORA-01460. Each bind stays
            # inside VARCHAR2 and the pieces are concatenated.
            quoted_col = key_expr[key_expr.find("(") + 1:key_expr.find(",")]
            params: dict[str, Any] = {"lim": lim}
            clauses: list[str] = []
            for i, key in enumerate(keys):
                clause, bound = _oracle_clob_equals(quoted_col, i, str(key))
                clauses.append(clause)
                params.update(bound)
            where = " OR ".join(clauses)
            sql = (
                f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                f"WHERE {where} "
                f"ORDER BY {ora_order}, ROWID FETCH FIRST :lim ROWS ONLY"
            )
        elif keys and sort_key:
            params: dict[str, Any] = {f"k{i}": k for i, k in enumerate(keys)}
            params["lim"] = lim
            placeholders = ",".join(f":k{i}" for i in range(len(keys)))
            temporal = oracle_temporal_key_sql(conn, read_table, read_owner, sort_key_stored)
            if temporal:
                zoned = "TO_TIMESTAMP_TZ" in temporal
                params.update(
                    {f"k{i}": oracle_temporal_key_text(k, zoned=zoned) for i, k in enumerate(keys)}
                )
                placeholders = ",".join(
                    temporal.format(bind=f":k{i}") for i in range(len(keys))
                )
            sql = (
                f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                f"WHERE {key_expr} IN ({placeholders}) "
                f"ORDER BY {ora_order} FETCH FIRST :lim ROWS ONLY"
            )
        else:
            params = {"lim": lim}
            sql = (
                f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                f"ORDER BY {ora_order} FETCH FIRST :lim ROWS ONLY"
            )
        try:
            result = conn.execute(sa.text(sql), params)
        except Exception as exc:
            if "01460" not in str(exc):
                raise
            raise RuntimeError(
                "Oracle rejected a key longer than VARCHAR2 (ORA-01460). "
                "The comparison was split into 1000-character binds and still failed."
            ) from exc
        names = list(cols) if cols and cols != ["*"] else list(result.keys())
        rows = result.fetchall()
        return [dict(zip(names, parse_oracle_tz_row(row, tz_positions))) for row in rows]
