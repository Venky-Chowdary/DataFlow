from __future__ import annotations

from typing import Any
from services import schema_introspect as _si
from services.unique_key_introspect import (
    _oracle_fetch_unique_keys,
    _sqlserver_fetch_unique_keys,
)
from services.engine_pool import release_engine
from services.schema_introspect import (
    apply_identity_probe,
    attach_declared_catalog_type,
    logger,
    logging,
    probe_check_constraints,
    probe_physical_storage,
    probe_secondary_indexes,
    re,
)

def _oracle_apply_timestamp_scale(dtype: str, scale: Any) -> str:
    """Put ALL_TAB_COLUMNS.DATA_SCALE onto a TIMESTAMP that omitted ``(n)``.

    For a timestamp column, ``DATA_SCALE`` is the fractional-second precision.
    ``DATA_TYPE`` is often the bare word ``TIMESTAMP`` (or ``TIMESTAMP WITH
    TIME ZONE``). NUMBER precision is already stitched the same way; leaving
    the scale off here is what made a created ``TIMESTAMP(7)`` read back as
    the Oracle default of 6.
    """
    text = str(dtype or "")
    upper = text.upper()
    if "TIMESTAMP" not in upper or "(" in upper or scale is None:
        return text
    try:
        fsp = int(scale)
    except (TypeError, ValueError):
        return text
    if not 0 <= fsp <= 9:
        return text
    return re.sub(r"(?i)\bTIMESTAMP\b", f"TIMESTAMP({fsp})", text, count=1)


def _oracle_to_logical(dtype: str) -> str:
    """Map Oracle data_type (+ optional precision/scale) to logical carriers.

    NUMBER(p,0) → INTEGER when p ≤ 18; else DECIMAL(p,0). NUMBER(p,s) → DECIMAL(p,s).
    BINARY_FLOAT/DOUBLE → FLOAT. Oracle DATE includes time-of-day → TIMESTAMP.
    """
    raw = (dtype or "").strip()
    d = raw.upper().replace(" ", "")
    # NUMBER(p,s) / FLOAT(p) carriers
    m = re.match(r"^NUMBER\((\d+)(?:,(\d+))?\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(2) is not None and int(m.group(2)) == 0:
            return zero_scale_numeric_carrier(int(m.group(1)))
        if m.group(2) is not None:
            return f"DECIMAL({m.group(1)},{m.group(2)})"
        return f"DECIMAL({m.group(1)})"
    if d == "NUMBER" or d.startswith("NUMBER("):
        return "DECIMAL"
    # Oracle ANSI FLOAT(p) is NUMBER-backed binary precision (bare FLOAT = 126
    # binary digits ~ 38 decimal), not IEEE-64. Reading it as DOUBLE silently
    # drops it to a 53-bit mantissa, so the declared carrier is preserved and
    # the destination decides how to hold it.
    m_float = re.match(r"^FLOAT(?:\((\d+)\))?$", d)
    if m_float:
        return f"FLOAT({m_float.group(1)})" if m_float.group(1) else "FLOAT"
    if d in {"BINARY_FLOAT", "BINARY_DOUBLE"} or d.startswith("FLOAT"):
        from services.type_system import float_width_carrier

        return float_width_carrier(d) or ("BINARY_DOUBLE" if "DOUBLE" in d else "FLOAT")
    if d in {"INTEGER", "INT", "SMALLINT", "BIGINT"}:
        from services.type_system import integer_width_carrier

        return integer_width_carrier(d) or "BIGINT"
    if d == "BOOLEAN":
        return "BOOLEAN"
    if d == "DATE":
        return "TIMESTAMP"  # Oracle DATE is datetime
    if "TIMESTAMP" in d:
        # ALL_TAB_COLUMNS often spells the type as TIMESTAMP(6) or
        # TIMESTAMP(6) WITH TIME ZONE. Dropping (n) made the next incremental
        # compare a source TIMESTAMP_TZ(7) to a bare live carrier, which this
        # product reads as Oracle's default of 6 and then blocks as a narrowing.
        suffix = ""
        m_fsp = re.search(r"TIMESTAMP\((\d+)\)", d)
        if m_fsp:
            fsp = max(0, min(9, int(m_fsp.group(1))))
            suffix = f"({fsp})"
        if "WITHLOCALTIMEZONE" in d:
            return f"TIMESTAMP_LTZ{suffix}"
        if "WITHTIMEZONE" in d:
            return f"TIMESTAMP_TZ{suffix}"
        return f"TIMESTAMP_NTZ{suffix}"
    if "INTERVAL" in raw.upper():
        # Preserve Oracle leading-field / fractional-second precision
        # (INTERVAL DAY(3) TO SECOND(6) — ANSI/Oracle contract).
        raw_u = re.sub(r"\s+", " ", raw.upper()).strip()
        m_ym = re.match(
            r"INTERVAL YEAR(?:\((\d+)\))? TO MONTH(?:\((\d+)\))?",
            raw_u,
        )
        if m_ym:
            if m_ym.group(1):
                return f"INTERVAL YEAR({m_ym.group(1)}) TO MONTH"
            return "INTERVAL YEAR TO MONTH"
        m_ds = re.match(
            r"INTERVAL DAY(?:\((\d+)\))? TO SECOND(?:\((\d+)\))?",
            raw_u,
        )
        if m_ds:
            if m_ds.group(1) or m_ds.group(2):
                day_p = m_ds.group(1) or "2"
                sec_p = m_ds.group(2) or "6"
                return f"INTERVAL DAY({day_p}) TO SECOND({sec_p})"
            return "INTERVAL DAY TO SECOND"
        if "YEAR" in raw_u and "MONTH" in raw_u:
            return "INTERVAL YEAR TO MONTH"
        if any(tok in raw_u for tok in ("DAY", "SECOND", "HOUR", "MINUTE")):
            return "INTERVAL DAY TO SECOND"
        return "INTERVAL"
    # VARCHAR2(n BYTE|CHAR) — BYTE is Oracle default; multi-byte UTF-8 can
    # truncate under BYTE while CHAR semantics still fit (Informatica-class bug).
    m = re.match(r"^(VARCHAR2|NVARCHAR2|CHAR|NCHAR)\((\d+)(?:(BYTE|CHAR))?\)$", d)
    if m:
        fixed = m.group(1) in {"CHAR", "NCHAR"}
        national = m.group(1) in {"NVARCHAR2", "NCHAR"}
        if national:
            prefix = "NCHAR" if fixed else "NVARCHAR"
        else:
            prefix = "CHAR" if fixed else "VARCHAR"
        unit = (m.group(3) or "").upper()
        if unit in {"BYTE", "CHAR"}:
            return f"{prefix}({m.group(2)} {unit})"
        return f"{prefix}({m.group(2)})"
    if d in {"CLOB", "NCLOB", "LONG", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"}:
        return "TEXT"
    if d in {"BLOB", "RAW", "LONGRAW", "BFILE"} or d.startswith("RAW("):
        return "BINARY"
    if d == "JSON":
        return "JSON"
    if d in {"XMLTYPE", "XML"}:
        return "XMLTYPE"
    if d in {"ROWID", "UROWID"}:
        return d
    if "SDO_GEOMETRY" in d:
        return "SDO_GEOMETRY"
    if d == "GEOGRAPHY":
        return "GEOGRAPHY"
    if d == "GEOMETRY":
        return "GEOMETRY"
    return "TEXT"


def _sqlserver_to_logical(dtype: str) -> str:
    """Map SQL Server type_name (+ optional (p,s)) to logical carriers."""
    raw = (dtype or "").strip()
    d = raw.lower()
    m = re.match(r"^(decimal|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(3) is not None and int(m.group(3)) == 0:
            return zero_scale_numeric_carrier(int(m.group(2)))
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d.startswith("decimal") or d.startswith("numeric"):
        return "DECIMAL"
    if d == "money":
        return "MONEY"  # → DECIMAL(19,4) via normalize/ddl; keep token for money quarantine
    if d == "smallmoney":
        return "SMALLMONEY"
    if d in {"float", "real"}:
        from services.type_system import float_width_carrier

        return float_width_carrier(d) or "FLOAT"
    if d in {"int", "bigint", "smallint", "tinyint"}:
        from services.type_system import integer_width_carrier

        # SQL Server INT is unambiguously 32-bit. The shared carrier widens the
        # ambiguous INT/INTEGER keyword to BIGINT (never-narrower invent), which
        # would turn a read int32 column into a BIGINT on the destination and
        # lose the declared width — so name it explicitly, as PostgreSQL int4 is.
        if d == "int":
            return "INT4"
        return integer_width_carrier(d) or "BIGINT"
    if d == "bit":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if d == "time" or d.startswith("time("):
        m = re.search(r"time\s*\(\s*(\d+)\s*\)", d)
        return f"TIME({m.group(1)})" if m else "TIME"
    if d == "datetimeoffset" or d.startswith("datetimeoffset"):
        # Offset-pinned (SQL Server) — TIMESTAMP_TZ polarity, not session LTZ.
        m = re.search(r"datetimeoffset\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMP_TZ({m.group(1)})" if m else "TIMESTAMP_TZ"
    if d.startswith("datetime2"):
        m = re.search(r"datetime2\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMP_NTZ({m.group(1)})" if m else "TIMESTAMP_NTZ"
    # Minute accuracy + Microsoft rounding — keep carrier (wave 70).
    if d == "smalldatetime":
        return "SMALLDATETIME"
    if d in {"datetime"} or d.startswith("datetime"):
        return "TIMESTAMP_NTZ"
    if d == "uniqueidentifier":
        return "UUID"
    if d == "hierarchyid":
        # Path polarity `/1/2/` — PG create-new maps to LTREE (wave 67).
        return "HIERARCHYID"
    if d == "json":
        return "JSON"
    if d == "xml":
        # Preserve XML specialty (not opaque TEXT) — PG/Oracle XML bind SSOT.
        return "XML"
    if d == "sql_variant":
        # Opaque typed union — JSONB envelope on PG create-new (wave 69).
        return "SQL_VARIANT"
    if d == "geography":
        return "GEOGRAPHY"
    if d == "geometry":
        return "GEOMETRY"
    m = re.match(r"^(varbinary|binary)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"{m.group(1).upper()}({m.group(2)})"
    # SQL Server TIMESTAMP is ROWVERSION (8-byte concurrency token), NOT datetime.
    # HVR/Estuary map it to BYTEA — never invent a clock type (wave 66).
    if d in {"rowversion", "timestamp"}:
        return "ROWVERSION"
    if d in {"binary", "varbinary", "image"}:
        return "BINARY"
    if "varbinary" in d and "(max)" in d:
        return "BINARY"
    if "binary" in d:
        return "BINARY"
    # Prefer parametric carriers when length was folded into dtype (e.g. varchar(50)).
    m = re.match(r"^(n?varchar|n?char)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        base = m.group(1).lower()
        width = m.group(2)
        if base.startswith("n"):
            return f"N{'CHAR' if 'char' in base and 'varchar' not in base else 'VARCHAR'}({width})"
        return f"{'CHAR' if base == 'char' else 'VARCHAR'}({width})"
    # National (max) is UTF-16. Code-page (max) stays TEXT. Bare ``nvarchar``
    # must be decided before any substring test: ``"varchar" in "nvarchar"``
    # is true, and that misread NVARCHAR(MAX) as VARCHAR, then cp1252, and
    # quarantined 山田 / Łukasz on a column that holds them.
    if d in {"ntext", "sysname"} or (
        d.startswith("n") and "(max)" in d and "char" in d
    ):
        return "NVARCHAR(MAX)" if d != "sysname" else "NVARCHAR"
    if d in {"text"} or ("(max)" in d and not d.startswith("n")):
        return "TEXT"
    if d in {"nvarchar"}:
        return "NVARCHAR"
    if d == "nchar":
        return "NCHAR"
    if d == "varchar":
        return "VARCHAR"
    if d == "char":
        return "CHAR"
    return "TEXT"


def _introspect_oracle(**kwargs) -> dict[str, Any]:
    """Oracle ALL_TAB_COLUMNS introspect with NUMBER(p,s) / FLOAT honesty."""
    try:
        import sqlalchemy as sa
        from connectors.generic_sql import _engine, connection_options
    except Exception:
        return {
            "ok": False,
            "error": "Install oracledb/SQLAlchemy for Oracle introspection",
            "columns": [],
            "tables": [],
        }

    table = (kwargs.get("table") or "").strip()
    schema = (kwargs.get("schema") or kwargs.get("username") or "").strip().upper()
    cfg = {
        # Connection-affecting extras first: service name / SID / TLS decide
        # which instance answers, and the explicit keys below stay authoritative.
        **connection_options(kwargs),
        "type": "oracle",
        "host": kwargs.get("host") or "",
        "port": int(kwargs.get("port") or 1521),
        "database": kwargs.get("database") or "",
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
        "schema": schema,
        "connection_string": kwargs.get("connection_string") or "",
        "ssl": bool(kwargs.get("ssl", True)),
    }
    try:
        engine = _engine(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    try:
        with engine.connect() as conn:
            tables: list[str] = []
            if not table:
                if schema:
                    rows = conn.execute(
                        sa.text(
                            "SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name"
                        ),
                        {"owner": schema},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        sa.text("SELECT table_name FROM user_tables ORDER BY table_name")
                    ).fetchall()
                tables = [r[0] for r in rows]
                return {"ok": True, "columns": [], "tables": tables, "schema": schema}

            owner = schema or (kwargs.get("username") or "").upper()
            tables = [table.upper()]
            # Stored spelling inside the requested owner: a quoted lower-case
            # table read as "does not exist" under the folded name, so Validate
            # planned create-new (and skipped the destination PK / duplicate
            # gate) for a table that was really there. Same-owner resolution
            # only — cross-owner healing stays behind ``strict_namespace``.
            from services.sql_object_identity import resolve_object_identity

            _ident = resolve_object_identity(conn, table, owner)
            resolved_table = _ident.table if _ident.exists else table.upper()
            if _ident.exists and _ident.schema:
                owner = _ident.schema
            tables = [resolved_table]
            # VIRTUAL_COLUMN / IDENTITY_COLUMN — client INSERT must omit ALWAYS.
            # ALL_TAB_COLS (not ALL_TAB_COLUMNS) is the view that exposes
            # VIRTUAL_COLUMN; the join used to fail with ORA-00904 on every
            # Oracle and silently degrade to the identity-blind fallback, so a
            # virtual or identity column looked like an ordinary insertable one.
            _oracle_col_sql = """
                    SELECT atc.column_name, atc.data_type, atc.data_precision, atc.data_scale,
                           atc.nullable, atc.char_length, atc.char_used,
                           atc.virtual_column, atc.identity_column,
                           ic.generation_type, atc.data_default
                    FROM all_tab_cols atc
                    LEFT JOIN all_tab_identity_cols ic
                      ON atc.owner = ic.owner
                     AND atc.table_name = ic.table_name
                     AND atc.column_name = ic.column_name
                    WHERE atc.owner = :owner AND atc.table_name = :table
                      AND atc.hidden_column = 'NO'
                    ORDER BY atc.column_id
                    """
            try:
                col_rows = conn.execute(
                    sa.text(_oracle_col_sql),
                    {"owner": owner, "table": resolved_table},
                ).fetchall()
            except Exception:
                # Pre-12c / limited grants: fall back without identity join.
                _oracle_col_sql = """
                    SELECT column_name, data_type, data_precision, data_scale, nullable,
                           char_length, char_used, virtual_column, 'NO', NULL, data_default
                    FROM all_tab_cols
                    WHERE owner = :owner AND table_name = :table
                      AND hidden_column = 'NO'
                    ORDER BY column_id
                    """
                try:
                    col_rows = conn.execute(
                        sa.text(_oracle_col_sql),
                        {"owner": owner, "table": resolved_table},
                    ).fetchall()
                except Exception:
                    _oracle_col_sql = """
                        SELECT column_name, data_type, data_precision, data_scale, nullable,
                               char_length, char_used, 'NO', 'NO', NULL, data_default
                        FROM all_tab_columns
                        WHERE owner = :owner AND table_name = :table
                        ORDER BY column_id
                        """
                    col_rows = conn.execute(
                        sa.text(_oracle_col_sql),
                        {"owner": owner, "table": resolved_table},
                    ).fetchall()
            # Destination probes must NOT heal across owners: another schema's
            # columns would mark a create-new target as already existing.
            if not col_rows and not bool(kwargs.get("strict_namespace")):
                found = conn.execute(
                    sa.text(
                        """
                        SELECT owner, table_name FROM all_tables
                        WHERE UPPER(table_name) = UPPER(:table)
                        ORDER BY CASE
                          WHEN owner = :owner THEN 0
                          ELSE 1
                        END
                        FETCH FIRST 5 ROWS ONLY
                        """
                    ),
                    {"table": table, "owner": owner},
                ).fetchall()
                for found_owner, found_table in found or []:
                    try:
                        col_rows = conn.execute(
                            sa.text(_oracle_col_sql),
                            {"owner": found_owner, "table": found_table},
                        ).fetchall()
                    except Exception:
                        col_rows = []
                    if col_rows:
                        owner = found_owner
                        resolved_table = str(found_table)
                        break
            columns: list[dict] = []
            for row in col_rows:
                name, data_type, precision, scale, nullable = row[:5]
                char_length = row[5] if len(row) > 5 else None
                char_used = row[6] if len(row) > 6 else None
                virtual_col = row[7] if len(row) > 7 else "NO"
                identity_col = row[8] if len(row) > 8 else "NO"
                generation = row[9] if len(row) > 9 else None
                data_default = row[10] if len(row) > 10 else None
                dtype = str(data_type or "")
                dtype_u = dtype.upper()
                if dtype_u == "NUMBER" and precision is not None:
                    if scale is not None:
                        dtype = f"NUMBER({int(precision)},{int(scale)})"
                    else:
                        dtype = f"NUMBER({int(precision)})"
                elif dtype_u == "NUMBER" and (
                    (scale is not None and int(scale) == 0)
                    or str(identity_col or "").upper() == "YES"
                ):
                    # Oracle reports an unconstrained NUMBER for an identity
                    # column (precision and scale both NULL), but the identity
                    # sequence only ever yields integers. Read bare it became a
                    # fractional DECIMAL, so the key landed in a column with
                    # decimal places that no destination will generate into.
                    # 38 is Oracle's maximum precision.
                    dtype = "NUMBER(38,0)"
                elif (
                    dtype_u in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}
                    and char_length is not None
                    and int(char_length) > 0
                ):
                    unit = "CHAR" if str(char_used or "").upper() == "C" else "BYTE"
                    # NVARCHAR2/NCHAR are always character semantics in Oracle.
                    if dtype_u in {"NVARCHAR2", "NCHAR"}:
                        unit = "CHAR"
                    dtype = f"{dtype_u}({int(char_length)} {unit})"
                dtype = _oracle_apply_timestamp_scale(dtype, scale)
                logical = _oracle_to_logical(dtype)
                if str(virtual_col or "").upper() == "YES":
                    logical = f"{logical} GENERATED ALWAYS"
                elif str(identity_col or "").upper() == "YES":
                    gen = str(generation or "").upper()
                    if "ALWAYS" in gen and "DEFAULT" not in gen:
                        logical = f"{logical} GENERATED ALWAYS"
                    else:
                        logical = f"{logical} GENERATED BY DEFAULT"
                col_ora: dict[str, Any] = {
                    "name": name,
                    "inferred_type": logical,
                    "nullable": str(nullable).upper() == "Y",
                    "default": (
                        str(data_default).strip()
                        if data_default is not None
                        and str(data_default).strip() not in ("", "NULL")
                        else None
                    ),
                    "is_identity": str(identity_col or "").upper() == "YES",
                    "data_type": dtype,
                }
                attach_declared_catalog_type(
                    col_ora, str(dtype or ""), dest_db="oracle"
                )
                columns.append(col_ora)
            if columns:
                apply_identity_probe("oracle", conn, owner, resolved_table, columns)
            unique_meta = (
                _oracle_fetch_unique_keys(conn, owner, resolved_table)
                if columns
                else {"primary_key_columns": [], "unique_keys": []}
            )
            foreign_keys, foreign_keys_meta = (
                _si._fetch_foreign_keys("oracle", conn, owner, resolved_table)
                if columns
                else ([], None)
            )
            return {
                "ok": True,
                "columns": columns,
                "tables": tables,
                "schema": owner,
                "primary_key_columns": unique_meta.get("primary_key_columns") or [],
                "unique_keys": unique_meta.get("unique_keys") or [],
                "foreign_keys": foreign_keys,
                "foreign_keys_meta": foreign_keys_meta,
                "physical_storage": (
                    probe_physical_storage("oracle", conn, owner, resolved_table).to_dict()
                    if columns
                    else None
                ),
                "check_constraints_meta": (
                    probe_check_constraints("oracle", conn, owner, resolved_table).to_dict()
                    if columns
                    else None
                ),
                "indexes_meta": (
                    probe_secondary_indexes("oracle", conn, owner, resolved_table).to_dict()
                    if columns
                    else None
                ),
            }
    except Exception as exc:
        logger.warning("oracle introspect failed", exc_info=True)
        try:
            from connectors.generic_sql import introspect_table_schema

            info = introspect_table_schema(cfg, table)
            if info.get("ok") and info.get("columns"):
                for col in info["columns"]:
                    inferred = str(col.get("inferred_type") or "").lower()
                    if inferred in {"float", "double"}:
                        col["inferred_type"] = "FLOAT"
                    elif inferred.startswith("decimal"):
                        col["inferred_type"] = col["inferred_type"].upper() if "(" in inferred else "DECIMAL"
                return info
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    finally:
        try:
            release_engine(engine)
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)


def _object_kind(table_type: Any) -> str:
    """``information_schema`` TABLE_TYPE as the listing's ``table`` / ``view`` label."""
    return "view" if str(table_type or "").upper() == "VIEW" else "table"


def _introspect_sqlserver(**kwargs) -> dict[str, Any]:
    """SQL Server INFORMATION_SCHEMA introspect with FLOAT≠DECIMAL honesty."""
    try:
        import sqlalchemy as sa
        from connectors.generic_sql import _engine, connection_options
    except Exception:
        return {
            "ok": False,
            "error": "Install pyodbc/SQLAlchemy for SQL Server introspection",
            "columns": [],
            "tables": [],
        }

    table = (kwargs.get("table") or "").strip()
    schema = (kwargs.get("schema") or "dbo").strip()
    cfg = {
        # Carry TLS/driver keywords: Driver 18 verifies by default, so an
        # operator-declared certificate or trust flag has to reach the probe.
        **connection_options(kwargs),
        "type": "sqlserver",
        "host": kwargs.get("host") or "",
        "port": int(kwargs.get("port") or 1433),
        "database": kwargs.get("database") or "",
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
        "schema": schema,
        "connection_string": kwargs.get("connection_string") or "",
        "ssl": bool(kwargs.get("ssl", True)),
    }
    try:
        engine = _engine(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    try:
        with engine.connect() as conn:
            tables: list[str] = []
            if not table:
                # Views are listed beside tables, each with its kind (M18).
                listed = conn.execute(
                    sa.text(
                        """
                        SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND TABLE_SCHEMA = :schema
                        ORDER BY TABLE_NAME
                        """
                    ),
                    {"schema": schema},
                ).fetchall()
                tables = [r[0] for r in listed]
                kinds = {r[0]: _object_kind(r[1]) for r in listed}
                # Default dbo with no tables is the intermittent empty list:
                # the objects live in another user schema. Qualify those names
                # so the operator can pick them. A schema the operator named
                # that really is empty stays empty.
                if not tables and schema.lower() in {"", "dbo"}:
                    other = [
                        r
                        for r in conn.execute(
                            sa.text(
                                """
                                SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                                  AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')
                                ORDER BY TABLE_SCHEMA, TABLE_NAME
                                """
                            )
                        ).fetchall()
                        if r[1]
                    ]
                    names = [
                        str(r[1])
                        if str(r[0] or "").lower() in {"", "dbo"}
                        else f"{r[0]}.{r[1]}"
                        for r in other
                    ]
                    tables = names
                    kinds = {n: _object_kind(r[2]) for n, r in zip(names, other)}
                return {
                    "ok": True,
                    "columns": [],
                    "tables": tables,
                    "table_types": kinds,
                    "schema": schema,
                }

            tables = [table]
            col_rows = conn.execute(
                sa.text(
                    """
                    SELECT
                      c.COLUMN_NAME,
                      c.DATA_TYPE,
                      c.NUMERIC_PRECISION,
                      c.NUMERIC_SCALE,
                      c.CHARACTER_MAXIMUM_LENGTH,
                      c.DATETIME_PRECISION,
                      c.COLLATION_NAME,
                      c.IS_NULLABLE,
                      c.COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = :schema AND c.TABLE_NAME = :table
                    ORDER BY c.ORDINAL_POSITION
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchall()
            if not col_rows and not bool(kwargs.get("strict_namespace")):
                found = conn.execute(
                    sa.text(
                        """
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE'
                          AND LOWER(TABLE_NAME) = LOWER(:table)
                        ORDER BY CASE
                          WHEN TABLE_SCHEMA = :schema THEN 0
                          WHEN TABLE_SCHEMA = 'dbo' THEN 1
                          ELSE 2
                        END
                        """
                    ),
                    {"table": table, "schema": schema},
                ).fetchall()
                for found_schema, found_table in found or []:
                    col_rows = conn.execute(
                        sa.text(
                            """
                            SELECT
                              c.COLUMN_NAME,
                              c.DATA_TYPE,
                              c.NUMERIC_PRECISION,
                              c.NUMERIC_SCALE,
                              c.CHARACTER_MAXIMUM_LENGTH,
                              c.DATETIME_PRECISION,
                              c.COLLATION_NAME,
                              c.IS_NULLABLE,
                              c.COLUMN_DEFAULT
                            FROM INFORMATION_SCHEMA.COLUMNS c
                            WHERE c.TABLE_SCHEMA = :schema AND c.TABLE_NAME = :table
                            ORDER BY c.ORDINAL_POSITION
                            """
                        ),
                        {"schema": found_schema, "table": found_table},
                    ).fetchall()
                    if col_rows:
                        schema = found_schema
                        break
            columns: list[dict] = []
            for row in col_rows:
                (
                    name,
                    data_type,
                    precision,
                    scale,
                    char_len,
                    dt_prec,
                    collation,
                    nullable,
                ) = tuple(row)[:8]
                column_default = row[8] if len(row) > 8 else None
                dtype = str(data_type or "")
                base = dtype.lower()
                if base in {"decimal", "numeric"} and precision is not None:
                    if scale is not None:
                        dtype = f"{base}({int(precision)},{int(scale)})"
                    else:
                        dtype = f"{base}({int(precision)})"
                elif base in {
                    "varchar",
                    "nvarchar",
                    "char",
                    "nchar",
                    "binary",
                    "varbinary",
                } and char_len is not None:
                    # -1 is (max). Leaving the bare token made nvarchar(max)
                    # look like VARCHAR and dropped national characters.
                    try:
                        width = int(char_len)
                    except (TypeError, ValueError):
                        width = 0
                    if width < 0:
                        dtype = f"{base}(max)"
                    elif width > 0:
                        dtype = f"{base}({width})"
                elif base in {"time", "datetime2", "datetimeoffset"} and dt_prec is not None:
                    dtype = f"{base}({int(dt_prec)})"
                logical = _sqlserver_to_logical(dtype)
                coll = str(collation or "").strip()
                if coll:
                    from services.type_system import normalize_logical_type

                    if normalize_logical_type(logical) in {"string", "text"}:
                        logical = f"{logical} COLLATE {coll}"
                col_mssql: dict[str, Any] = {
                    "name": name,
                    "inferred_type": logical,
                    "nullable": str(nullable).upper() == "YES",
                    "default": (
                        str(column_default)
                        if column_default is not None
                        else None
                    ),
                    "data_type": dtype,
                    "collation": coll,
                }
                attach_declared_catalog_type(
                    col_mssql, str(dtype or ""), dest_db="sqlserver"
                )
                columns.append(col_mssql)
            # IDENTITY columns: INFORMATION_SCHEMA does not expose them, so a
            # SQL Server source looked like a plain BIGINT key and the
            # destination was created without a generator — the client's first
            # insert after cutover then had no key to use. Seed/increment are
            # measured from sys.identity_columns (sql_variant decoded).
            if columns:
                apply_identity_probe("sqlserver", conn, schema, table, columns)

            # Computed columns are not insertable — annotate GENERATED ALWAYS
            # so writers omit them (same path as PG/MySQL identity).
            if columns:
                try:
                    computed_rows = conn.execute(
                        sa.text(
                            """
                            SELECT c.name
                            FROM sys.computed_columns cc
                            JOIN sys.columns c
                              ON cc.object_id = c.object_id AND cc.column_id = c.column_id
                            JOIN sys.tables t ON t.object_id = cc.object_id
                            JOIN sys.schemas s ON s.schema_id = t.schema_id
                            WHERE s.name = :schema AND t.name = :table
                            """
                        ),
                        {"schema": schema, "table": table},
                    ).fetchall()
                    computed_names = {str(r[0]) for r in (computed_rows or []) if r and r[0]}
                    for col in columns:
                        if col["name"] in computed_names:
                            typ = str(col.get("inferred_type") or "")
                            if "GENERATED ALWAYS" not in typ.upper():
                                col["inferred_type"] = f"{typ} GENERATED ALWAYS"
                except Exception:
                    pass
            unique_meta = (
                _sqlserver_fetch_unique_keys(conn, schema, table)
                if columns
                else {"primary_key_columns": [], "unique_keys": []}
            )
            foreign_keys, foreign_keys_meta = (
                _si._fetch_foreign_keys("sqlserver", conn, schema, table)
                if columns
                else ([], None)
            )
            return {
                "ok": True,
                "columns": columns,
                "tables": tables,
                "schema": schema,
                "primary_key_columns": unique_meta.get("primary_key_columns") or [],
                "unique_keys": unique_meta.get("unique_keys") or [],
                "foreign_keys": foreign_keys,
                "foreign_keys_meta": foreign_keys_meta,
                "physical_storage": (
                    probe_physical_storage("sqlserver", conn, schema, table).to_dict()
                    if columns
                    else None
                ),
                "check_constraints_meta": (
                    probe_check_constraints("sqlserver", conn, schema, table).to_dict()
                    if columns
                    else None
                ),
                "indexes_meta": (
                    probe_secondary_indexes("sqlserver", conn, schema, table).to_dict()
                    if columns
                    else None
                ),
            }
    except Exception as exc:
        logger.warning("sqlserver introspect failed", exc_info=True)
        try:
            from connectors.generic_sql import introspect_table_schema

            return introspect_table_schema(cfg, table)
        except Exception:
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    finally:
        try:
            release_engine(engine)
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
