from __future__ import annotations

from typing import Any
from connectors.generic_sql import (
    ChDateTime64,
    ChNullable,
    TrinoTimestamp,
    _DialectNativeType,
    _ExactJSON,
    _MSSQL_WIRES,
    _MYSQL_WIRES,
    _OracleTimestamp,
    _is_oracle_wire,
    materialize_dest_ddl,
    mssql,
    mysql,
    oracle,
    postgresql,
    re,
    sa,
)

def _type_repr(type_obj: Any) -> str:
    try:
        return str(type_obj).lower()
    except (TypeError, ValueError):
        return ""


def _logical_type_from_sa(col_type: Any) -> str:
    """Map a SQLAlchemy type instance to a Datawrap logical type."""
    from services.type_system import normalize_logical_type

    if col_type is None:
        return "string"

    repr_ = _type_repr(col_type)

    # Direct dialect UUID types
    if "uuid" in repr_:
        return "uuid"
    if isinstance(col_type, (sa.UUID,)):
        return "uuid"
    if isinstance(col_type, postgresql.UUID):
        return "uuid"

    if isinstance(col_type, (sa.ARRAY,)):
        return "array"

    if isinstance(col_type, (sa.JSON,)):
        return "json"

    if isinstance(col_type, (sa.LargeBinary, sa.BINARY)):
        return "binary"

    if isinstance(col_type, (sa.Boolean,)):
        return "boolean"

    # MySQL-style TINYINT(1) is conventionally boolean.
    if "tinyint" in repr_ and getattr(col_type, "display_width", 0) == 1:
        return "boolean"

    # IEEE floats must stay FLOAT — never collapse into DECIMAL/NUMBER.
    if isinstance(col_type, (sa.Float, sa.Double, sa.REAL)):
        return "float"
    if any(
        tok in repr_
        for tok in ("float", "double", "real", "binary_float", "binary_double")
    ) and "decimal" not in repr_ and "numeric" not in repr_ and "number" not in repr_:
        return "float"

    # Fixed-point before integer: ``oracle.NUMBER`` subclasses *both* Numeric and
    # Integer, so an Integer-first check read NUMBER(12,2) money as ``integer``
    # and dropped the scale — every Oracle source column came back integral and
    # the create-new target then collapsed to TEXT.
    if isinstance(col_type, (sa.Numeric,)):
        from services.type_system import (
            zero_scale_fits_signed_bigint,
            zero_scale_numeric_carrier,
        )

        precision = getattr(col_type, "precision", None)
        scale = getattr(col_type, "scale", None)
        if precision is not None and scale is not None:
            if int(scale) == 0:
                # Wide NUMERIC(38,0) must stay DECIMAL — never signed BIGINT overflow.
                if zero_scale_fits_signed_bigint(int(precision)):
                    return "integer"
                return zero_scale_numeric_carrier(int(precision))
            return f"DECIMAL({int(precision)},{int(scale)})"
        if precision is not None:
            return f"DECIMAL({int(precision)})"
        if isinstance(col_type, (sa.Integer,)):
            # Unconstrained NUMBER — integral wire, no declared scale to keep.
            return "integer"
        return "decimal"

    # Integer *width* is declared, so it is reported. Collapsing BIGINT,
    # INTEGER and SMALLINT onto one "integer" token made every generic-SQL
    # engine (DuckDB, ClickHouse, Trino, …) describe a BIGINT column as a
    # 32-bit one: an existing BIGINT destination then read as a narrowing and
    # blocked a safe route, and a genuinely INT32 destination read the same
    # way, so a real 2**31 overflow was indistinguishable from a false alarm.
    # The bind range-checks the stamped token too, so a 64-bit column reflected
    # as bare "integer" took int4 bounds and refused every value above 2**31
    # the BIGINT destination stores natively.
    if isinstance(col_type, sa.BigInteger):
        return "BIGINT"
    if isinstance(col_type, sa.SmallInteger):
        return "SMALLINT"
    if isinstance(col_type, sa.Integer):
        return "integer"

    if isinstance(col_type, (sa.DateTime,)):
        # Preserve TIMESTAMPTZ vs NTZ — collapsing both to "datetime" loses TZ polarity
        # on generic Postgres/Trino/warehouse reflection (Airbyte-class honesty gap).
        # mssql.DATETIMEOFFSET subclasses DateTime and leaves ``timezone`` False,
        # so the flag alone reads an offset-storing carrier as NTZ and the writer
        # then quarantines every aware value the column exists to hold.
        # Classic SQL Server DATETIME is 1/300 s. Folding it into the same token
        # as DATETIME2 made preflight believe the column kept seven digits, the
        # write rounded ``.000001`` to ``.000``, and the sample check failed
        # only after the batch had committed.
        type_name = str(getattr(getattr(col_type, "__class__", None), "__name__", "") or "")
        # Compare the class name as written. ``sa.DateTime``.upper() is also
        # DATETIME, and that fold read TIMESTAMPTZ as the 1/300-second SQL
        # Server type before the timezone flag was consulted.
        if type_name == "DATETIME2":
            precision = getattr(col_type, "precision", None)
            digits = int(precision) if isinstance(precision, int) and precision >= 0 else 7
            return f"DATETIME2({digits})"
        if type_name == "DATETIMEOFFSET":
            precision = getattr(col_type, "precision", None)
            digits = int(precision) if isinstance(precision, int) and precision >= 0 else 7
            return f"DATETIMEOFFSET({digits})"
        if type_name == "SMALLDATETIME":
            return "SMALLDATETIME"
        if type_name == "DATETIME":
            return "DATETIME"
        if "datetimeoffset" in (
            f"{type_name} {col_type!r}".lower()
        ):
            return "timestamptz"
        # Oracle TIMESTAMP WITH LOCAL TIME ZONE carries awareness on
        # ``local_timezone`` and leaves ``timezone`` False, so the flag alone
        # read an instant carrier as NTZ and the writer quarantined every aware
        # value the column exists to hold.
        if getattr(col_type, "local_timezone", False):
            return "timestamp_ltz"
        tz = getattr(col_type, "timezone", None)
        if tz is True:
            return "timestamptz"
        if tz is False:
            return "timestamp_ntz"
        return "datetime"

    if isinstance(col_type, (sa.Date,)):
        return "date"

    if isinstance(col_type, (sa.Time,)):
        return "time"

    if isinstance(col_type, (sa.String, sa.Text, sa.CHAR)):
        # Preserve VARCHAR(n)/CHAR(n)/NVARCHAR(n) — bare "string" invents
        # unbounded capacity over live typmod on existing-table overlay.
        length = getattr(col_type, "length", None)
        try:
            n = int(length) if length is not None else None
        except (TypeError, ValueError):
            n = None
        type_name = getattr(getattr(col_type, "__class__", None), "__name__", "").lower()
        module = getattr(getattr(col_type, "__class__", None), "__module__", "").lower()
        national = (
            isinstance(col_type, (sa.Unicode, sa.UnicodeText))
            or "nvarchar" in type_name
            or type_name in {"nchar", "unicodetext", "unicode"}
            or ("mssql" in module and "national" in repr(col_type).lower())
        )
        # NVARCHAR(MAX) reflects with length None. Returning "TEXT" or "string"
        # classified the column as SQL Server VARCHAR (cp1252) and quarantined
        # U+90CE on a national column that holds it.
        if national and (n is None or n <= 0):
            return "NVARCHAR(MAX)"
        if isinstance(col_type, sa.Text) and n is None:
            return "TEXT"
        if n is not None and n > 0:
            if national:
                prefix = "NCHAR" if "char" in type_name and "var" not in type_name else "NVARCHAR"
                return f"{prefix}({n})"
            if isinstance(col_type, sa.CHAR) or (
                "char" in type_name and "var" not in type_name
            ):
                return f"CHAR({n})"
            return f"VARCHAR({n})"
        if "mssql" in module and type_name in {"varchar", "char", "text"}:
            return "VARCHAR(MAX)" if type_name != "char" else "CHAR"
        return "string"

    # Fallback text matching for dialect-specific types not captured above
    # SQL Server specifics BEFORE broad timestamp/datetime matching.
    if "datetimeoffset" in repr_:
        return "timestamptz"
    type_name = getattr(getattr(col_type, "__class__", None), "__name__", "").lower()
    module = getattr(getattr(col_type, "__class__", None), "__module__", "").lower()
    # Specialty BEFORE broad "variant" → JSON (sql_variant must not invent JSON).
    if "hierarchyid" in repr_ or type_name == "hierarchyid":
        return "HIERARCHYID"
    if "sql_variant" in repr_ or type_name in {"sql_variant", "sqlvariant"}:
        return "SQL_VARIANT"
    if type_name == "xml" or (repr_ == "xml" or repr_.endswith(".xml")):
        return "XML"
    if "geography" in repr_ or type_name == "geography":
        return "GEOGRAPHY"
    if "geometry" in repr_ or type_name == "geometry":
        return "GEOMETRY"
    if "rowversion" in repr_ or (
        "mssql" in module and type_name in {"timestamp", "rowversion"}
    ):
        # SQL Server TIMESTAMP is rowversion (binary), not a datetime.
        return "ROWVERSION"
    if "json" in repr_ or "super" in repr_:
        return "json"
    # Snowflake/Databricks VARIANT (not SQL Server sql_variant — handled above).
    if "variant" in repr_ and "sql_variant" not in repr_:
        return "json"
    if "array" in repr_:
        return "array"
    if "uuid" in repr_ or "guid" in repr_ or "uniqueidentifier" in repr_:
        return "uuid"
    if any(
        x in repr_
        for x in ("binary", "blob", "bytea", "varbinary", "image", "raw", "rowversion")
    ):
        return "binary"
    # FLOAT before DECIMAL — "float" must not fall into the numeric/decimal bucket.
    if any(
        x in repr_ for x in ("binary_float", "binary_double", "float", "double", "real")
    ) and not any(x in repr_ for x in ("numeric", "decimal", "number(", "money")):
        return "float"
    if any(x in repr_ for x in ("numeric", "decimal", "number", "money", "smallmoney")):
        return "decimal"
    if any(x in repr_ for x in ("int", "serial", "smallint", "tinyint", "bigint")):
        return "integer"
    if "bool" in repr_ or "bit" in repr_:
        return "boolean"
    if "datetimeoffset" in repr_ or "timestamptz" in repr_ or "with time zone" in repr_:
        return "timestamptz"
    if "without time zone" in repr_ or "timestamp_ntz" in repr_:
        return "timestamp_ntz"
    # ClickHouse DateTime64 / IPv4 / Tuple — preserve carriers (not bare string).
    if any(
        tok in repr_
        for tok in ("datetime64", "ipv4", "ipv6", "tuple(", "array(", "map(")
    ) or (
        "clickhouse" in module
        and any(tok in repr_ for tok in ("datetime", "date", "uuid", "decimal"))
    ):
        from services.schema_introspect import _ch_to_logical

        try:
            original = str(col_type)
        except (TypeError, ValueError):
            original = repr_
        return _ch_to_logical(original)
    if "datetime" in repr_ or "timestamp" in repr_:
        return "datetime"
    if "date" in repr_:
        return "date"
    if "time" in repr_:
        return "time"
    if any(x in repr_ for x in ("char", "varchar", "text", "clob", "string")):
        return "string"

    return normalize_logical_type(repr_)


def _declared_temporal_fsp(logical: str, cap: int) -> int | None:
    """Fractional seconds named on ``logical``, clamped to the engine maximum."""
    from services.type_system import parse_temporal_fractional_precision

    fsp = parse_temporal_fractional_precision(logical)
    if fsp is None:
        return None
    return max(0, min(int(cap), int(fsp)))


def _oracle_timestamp_type(
    logical: str,
    *,
    timezone: bool = False,
    local_timezone: bool = False,
) -> Any:
    """Oracle TIMESTAMP, with ``(n)`` when the stamp declared one.

    A bare declaration stays the stock type (Oracle's own default of 6).
    Declared precision is clamped to 0..9, which is what Oracle accepts.
    """
    fsp = _declared_temporal_fsp(logical, 9)
    if fsp is None or _OracleTimestamp is None:
        return oracle.TIMESTAMP(timezone=timezone, local_timezone=local_timezone)
    return _OracleTimestamp(
        timezone=timezone,
        local_timezone=local_timezone,
        fractional_seconds=fsp,
    )


def _mysql_timestamp_instant(logical: str) -> bool:
    """True when this stamp is MySQL's TIMESTAMP instant, not a wall clock.

    ``TIMESTAMP(n)`` is the physical instant column MySQL create-new stamps
    and INFORMATION_SCHEMA reports. A bare ``TIMESTAMP`` is the ambiguous
    wall-clock spelling (PostgreSQL and Oracle) unless the bound source
    engine is MySQL itself. ``TIMESTAMPTZ`` is that same MySQL column after
    introspect, and only then: a PostgreSQL timestamptz does not fit the
    1970..2038 window, so it stays ``DATETIME(6)``.
    """
    from services.dest_dialect_facts import _normalize_dest_db
    from services.source_engine_scope import active_source_engine

    raw = (logical or "").strip().lower()
    base = re.sub(r"\s*\(\s*\d+\s*\)", "", raw).strip()
    source_is_mysql = _normalize_dest_db(active_source_engine()) == "mysql"
    if base in {"timestamptz", "timestamp with time zone"}:
        return source_is_mysql
    if base != "timestamp":
        return False
    if _declared_temporal_fsp(logical, 6) is not None:
        return True
    return source_is_mysql


def _sub_second_naive_wire(dialect_name: str, db_type: str, logical: str = "") -> Any:
    """Naive-datetime carrier that keeps sub-second precision, else ``None``.

    ``sa.DateTime()`` compiles to Oracle ``DATE`` (whole seconds, no fraction),
    SQL Server ``DATETIME`` (rounded to 1/300 s) and MySQL ``DATETIME``
    (fsp 0 — the fraction is dropped), so a PostgreSQL microsecond stamp lands
    altered and a row checksum can only match by luck. On MySQL that also
    collapsed SCD2 version boundaries: two versions written in the same second
    got one instant, so ``valid_from == valid_to`` and no as-of query could see
    the closed version. ``None`` means the dialect's own default already
    carries fractions.

    A declared Oracle ``(n)`` is kept. A MySQL ``TIMESTAMP(n)`` instant is not
    rewritten to ``DATETIME`` — that rewrite is what turned a same-engine
    TIMESTAMP source into a wall-clock column.
    """
    if _is_oracle_wire(dialect_name, db_type):
        return _oracle_timestamp_type(logical)
    if mssql is not None and (
        (dialect_name or "").lower() == "mssql" or (db_type or "").lower() in _MSSQL_WIRES
    ):
        # Bare DATETIME2 is already SQL Server's seven-digit carrier. Only a
        # declared ``(n)`` is precision-bearing DDL; inventing ``(7)`` on a
        # bare logical made the DDL disagree with the family it was mapped to.
        fsp = _declared_temporal_fsp(logical, 7)
        return mssql.DATETIME2() if fsp is None else mssql.DATETIME2(precision=fsp)
    if mysql is not None and _MYSQL_WIRES & {
        (dialect_name or "").lower(), (db_type or "").lower()
    }:
        if _mysql_timestamp_instant(logical):
            fsp = _declared_temporal_fsp(logical, 6)
            return mysql.TIMESTAMP(fsp=6 if fsp is None else fsp)
        return mysql.DATETIME(fsp=6)
    return None


def _is_mysql_wire(dialect_name: str, db_type: str) -> bool:
    """True when DDL compiles through the MySQL/MariaDB dialect."""
    names = {(dialect_name or "").lower(), (db_type or "").lower()}
    return bool(names & _MYSQL_WIRES)


def _mysql_character_type(logical: str, db_type: str) -> Any | None:
    """Compile a MySQL character stamp without dropping ``CHARACTER SET``.

    ``sa.Unicode(n)`` and ``sa.String(n)`` both render ``VARCHAR(n)`` and
    leave the character set to the server default. On a server whose default
    is utf8mb3 that is the national alias: a 4-byte scalar is refused with
    1366, and the next run reads the column back as
    ``VARCHAR(n) COLLATE UTF8MB3_GENERAL_CI``. The charset has to be an
    attribute of the type. ``NVARCHAR``/``NCHAR`` stay the alias with no
    charset clause — a clause on those tokens is a 1064.

    Returns ``None`` for carriers this path does not own, so numeric,
    temporal, and plain ``VARCHAR(n)`` keep the existing compiler.
    """
    if mysql is None:
        return None
    raw = (logical or "").strip()
    if not raw:
        return None
    from services.type_system import (
        is_fixed_char_carrier,
        is_national_string_carrier,
        string_carrier_length,
    )

    national = is_national_string_carrier(raw)
    has_charset = bool(re.search(r"(?:CHARACTER\s+SET|CHARSET)\s+", raw, re.I))
    if not national and not has_charset:
        return None
    materialized = materialize_dest_ddl(db_type or "mysql", raw, source_type=raw)
    text = (materialized or raw).strip()
    charset_match = re.search(
        r"(?:CHARACTER\s+SET|CHARSET)\s+([A-Za-z0-9_]+)", text, re.I
    )
    charset = charset_match.group(1) if charset_match else ""
    collate_match = re.search(r"\bCOLLATE\s+([A-Za-z0-9_]+)", text, re.I)
    collation = collate_match.group(1) if collate_match else ""
    # SQL Server collation names are not MySQL collations. Stating one fails
    # CREATE (1273). utf8mb4 capacity is the character set.
    if collation and not collation.lower().startswith("utf8mb4"):
        collation = ""
    base = re.sub(
        r"\s+(?:CHARACTER\s+SET|CHARSET)\s+\S+", "", text, flags=re.I
    )
    base = re.sub(r"\s+COLLATE\s+\S+", "", base, flags=re.I).strip()
    head = base.upper().split("(", 1)[0].strip()
    width = string_carrier_length(base)
    kwargs: dict[str, Any] = {}
    if charset:
        kwargs["charset"] = charset
    if collation:
        kwargs["collation"] = collation
    if head in {"NVARCHAR", "NCHAR", "NTEXT"} or (
        is_national_string_carrier(base) and not charset
    ):
        if head == "NCHAR" or is_fixed_char_carrier(base):
            return mysql.NCHAR(width) if width is not None else mysql.NCHAR()
        return mysql.NVARCHAR(width) if width is not None else mysql.NVARCHAR()
    lobs = {
        "TINYTEXT": mysql.TINYTEXT,
        "TEXT": mysql.TEXT,
        "MEDIUMTEXT": mysql.MEDIUMTEXT,
        "LONGTEXT": mysql.LONGTEXT,
    }
    lob = lobs.get(head)
    if lob is not None:
        return lob(**kwargs)
    if head == "CHAR" or is_fixed_char_carrier(base):
        return mysql.CHAR(width, **kwargs) if width is not None else mysql.CHAR(**kwargs)
    if width is not None:
        return mysql.VARCHAR(width, **kwargs)
    if head in {"VARCHAR", "CHARACTER", "CHARACTER VARYING"}:
        return mysql.VARCHAR(**kwargs) if kwargs else mysql.VARCHAR()
    return None


def _sa_type_for_logical(
    logical: str,
    dialect_name: str,
    db_type: str = "",
    *,
    nullable: bool = True,
) -> Any:
    """Map a Datawrap logical type to a SQLAlchemy type that compiles for the engine.

    Accepts carriers like ``DECIMAL(12,4)`` / ``NUMERIC(38,10)`` — bare
    ``t == "decimal"`` matching used to fall through to TEXT and strip scale
    (SQL Server / Oracle / DuckDB greenfield fidelity bug).
    """
    from services.decision_kernel import ddl_type, normalize_logical_type
    from services.type_system import (
        LOGICAL_ARRAY,
        LOGICAL_BINARY,
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_GEOGRAPHY,
        LOGICAL_INTEGER,
        LOGICAL_INTERVAL,
        LOGICAL_JSON,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_TIME,
        LOGICAL_UUID,
        parse_numeric_precision_scale,
    )

    raw = (logical or "string").strip()
    raw_lower = raw.lower()
    t = normalize_logical_type(raw)

    def _maybe_nullable(sa_type: Any) -> Any:
        # ClickHouse: only Nullable(T) when the column is nullable. PK / ORDER BY
        # identity cols must stay bare Int64/DateTime — wrapping everything made
        # empty→NULL legal at DDL and weakened ReplacingMergeTree contracts.
        if dialect_name == "clickhouse" and ChNullable is not None and nullable:
            return ChNullable(sa_type)
        return sa_type

    # TZ polarity from introspection carriers — check before LOGICAL_DATETIME collapse.
    if (
        "timestamptz" in raw_lower
        or "timestamp_tz" in raw_lower
        or "timestamp_ltz" in raw_lower
        or "timestamp with time zone" in raw_lower
        or "timestamp with local time zone" in raw_lower
        or "datetimeoffset" in raw_lower
        or raw_lower.endswith(" with time zone")
        or raw_lower.endswith(" local time zone")
    ):
        # SQL Server DATETIMEOFFSET is TZ-aware — never bind as naive DATETIME2.
        if db_type == "questdb":
            return _maybe_nullable(sa.DateTime())
        if db_type == "trino" and TrinoTimestamp is not None:
            return TrinoTimestamp(precision=3, timezone=True)
        if _is_oracle_wire(dialect_name, db_type):
            # sa.DateTime(timezone=True) compiles to Oracle DATE — second
            # granularity with no zone at all, so a live postgresql->oracle run
            # created DATE for TIMESTAMPTZ and reconciled green only because the
            # fixture held whole-second UTC values. Declared (n) is kept so a
            # TIMESTAMP(7) plan is not created as the Oracle default of 6.
            local = "local time zone" in raw_lower or "_ltz" in raw_lower
            return _maybe_nullable(
                _oracle_timestamp_type(
                    raw, timezone=not local, local_timezone=local
                )
            )
        if mysql is not None and _MYSQL_WIRES & {
            (dialect_name or "").lower(), (db_type or "").lower()
        }:
            # No MySQL carrier holds an offset, so the zone decision is made
            # upstream. sa.DateTime(timezone=True) compiles to fsp-0 DATETIME,
            # dropping the fraction too — a second loss for nothing.
            # A MySQL source's own TIMESTAMP (introspected as TIMESTAMPTZ)
            # is the instant carrier. Every other aware source stays
            # DATETIME(6), which can hold years outside 1970..2038.
            if _mysql_timestamp_instant(raw):
                fsp = _declared_temporal_fsp(raw, 6)
                return _maybe_nullable(mysql.TIMESTAMP(fsp=6 if fsp is None else fsp))
            return _maybe_nullable(mysql.DATETIME(fsp=6))
        if mssql is not None and (
            (dialect_name or "").lower() == "mssql"
            or (db_type or "").lower() in _MSSQL_WIRES
        ):
            # sa.DateTime(timezone=True) compiles to DATETIMEOFFSET with no
            # precision argument. Name the seven digits the column keeps so a
            # PostgreSQL microsecond cannot land on classic DATETIME. The
            # dialect type defaults to timezone=False, which made the bind
            # path strip the offset from a column that stores one.
            fsp = _declared_temporal_fsp(raw, 7)
            return _maybe_nullable(
                mssql.DATETIMEOFFSET(precision=7 if fsp is None else fsp, timezone=True)
            )
        return sa.DateTime(timezone=True)
    if (
        "timestamp_ntz" in raw_lower
        or "timestamp without time zone" in raw_lower
        or "datetime_ntz" in raw_lower
        or " without time zone" in raw_lower
    ):
        sub_second = _sub_second_naive_wire(dialect_name, db_type, raw)
        return _maybe_nullable(sub_second if sub_second is not None else sa.DateTime())

    if t == LOGICAL_INTEGER:
        # Width SSOT = decision_kernel integer_width_carrier / ddl_type.
        # Bare logical ``integer`` must invent 64-bit (never sa.Integer INT32).
        # Explicit INTEGER/INT/INT32 stay 32-bit; BIGINT stays 64-bit.
        from services.decision_kernel import integer_width_carrier
        from services.type_system import integer_bit_width

        int_u = raw.upper().split("(", 1)[0].strip().replace(" ", "")
        if int_u in {"NUMBER", "NUMERIC", "DECIMAL", "DEC"}:
            from services.type_system import (
                parse_numeric_precision_scale as _declared_precision,
            )

            declared, _declared_scale = _declared_precision(raw)
            if declared is not None:
                # A declared NUMBER(p,0) carries its own domain. The 64-bit
                # never-narrower default below is for *ambiguous* widths, and
                # applying it here widened a planned NUMBER(1) boolean carrier
                # to NUMBER(19) — CREATE contradicting the stamped plan.
                return _maybe_nullable(sa.Numeric(int(declared), 0))
        carrier = integer_width_carrier(raw) or ""
        width = integer_bit_width(carrier) if carrier else None
        if int_u in {"INT", "INTEGER"} and db_type:
            # The bare keyword is ambiguous across engines, so the carrier
            # widens it to 64-bit. Once the destination engine is named that
            # ambiguity is gone: SQL Server / PostgreSQL / MySQL INT is int32,
            # and binding it as BIGINT re-widens a column the operator declared
            # 32-bit. Oracle/Snowflake report unbounded and keep the widen.
            from services.numeric_fit import integer_storage_bounds

            bounds = integer_storage_bounds(int_u, dest_db=db_type)
            if bounds and bounds[1] == 2147483647:
                return _maybe_nullable(sa.Integer())
        if int_u in {
            "BIGINT",
            "INT64",
            "LONG",
            "UBIGINT",
            "UINT64",
            "BIGSERIAL",
        } or width == 64 or (carrier.upper() in {"BIGINT", "INT64", "LONG"}):
            return _maybe_nullable(sa.BigInteger())
        # Oracle NUMBER(p,0) normalized as integer — width from precision.
        if int_u == "NUMBER":
            from services.type_system import parse_numeric_precision_scale as _pnps

            np, _ns = _pnps(raw)
            if np is not None and int(np) > 9:
                return _maybe_nullable(sa.BigInteger())
            if np is not None and int(np) <= 4:
                return _maybe_nullable(sa.SmallInteger())
            return _maybe_nullable(sa.Integer())
        if int_u in {"SMALLINT", "INT2", "SMALLSERIAL", "SHORT", "INT16"} or width == 16:
            return _maybe_nullable(sa.SmallInteger())
        if int_u in {"TINYINT", "INT1", "UINT8", "BYTE"} or (width is not None and width <= 8):
            return _maybe_nullable(sa.SmallInteger())
        if int_u in {"INTEGER", "INT", "INT32", "MEDIUMINT", "SERIAL", "SIGNED"} or width == 32:
            return _maybe_nullable(sa.Integer())
        # Bare / unknown integer invent — never-narrower 64-bit (audit ITEM 1).
        return _maybe_nullable(sa.BigInteger())
    if t == LOGICAL_DECIMAL:
        precision, scale = parse_numeric_precision_scale(raw)
        if db_type == "risingwave":
            return sa.Numeric()
        # QuestDB lacks true DECIMAL — CREATE uses DOUBLE; ddl_type stamps
        # DOUBLE so Map/Validate match physical (never silent DECIMAL invent).
        if db_type == "questdb":
            return sa.Double()
        # Preserve source DECIMAL(p,s) when present — never invent TEXT.
        if precision is not None:
            out_scale = 0 if scale is None else int(scale)
            if db_type == "presto":
                return sa.DECIMAL(int(precision), out_scale)
            return _maybe_nullable(sa.Numeric(int(precision), out_scale))
        # Bare DECIMAL/NUMBER — Map≡CREATE must match type_system ddl_type SSOT.
        # Never invent Numeric(38,15) when the destination default is (38,10)
        # (SQL Server / Oracle / Databricks / Synapse).
        dest = (db_type or dialect_name or "").strip() or "generic_sql"
        wire = ddl_type(dest, raw)
        wp, ws = parse_numeric_precision_scale(wire)
        if wp is not None:
            out_scale = 0 if ws is None else int(ws)
            if (db_type or "").lower() == "presto" or dialect_name == "presto":
                return sa.DECIMAL(int(wp), out_scale)
            return _maybe_nullable(sa.Numeric(int(wp), out_scale))
        # PostgreSQL-wire: bare NUMERIC (arbitrary scale, no invent).
        if dialect_name == "postgresql" or (db_type or "").lower() in {
            "postgresql",
            "postgres",
            "cockroachdb",
            "yugabytedb",
            "timescale",
            "timescaledb",
            "supabase",
            "neon",
            "risingwave",
        }:
            return sa.Numeric()
        # Destination has no fixed-point wire (e.g. SQLite TEXT) — do not invent
        # Numeric(38,15); follow ddl_type collapse.
        wire_logical = normalize_logical_type(wire)
        if wire_logical in {LOGICAL_TEXT, LOGICAL_STRING}:
            if "TEXT" in (wire or "").upper():
                return _maybe_nullable(sa.Text())
            return _maybe_nullable(sa.String())
        return _maybe_nullable(sa.Numeric())
    if t == LOGICAL_FLOAT:
        # Approximate IEEE float — never rewrite to fixed-point Numeric.
        # Exact lowercase logical ``float`` → Double (never-narrower invent).
        # Honor Map REAL/FLOAT4/FLOAT stamps (sa.Double invents mantissa widen).
        float_u = raw.upper().split("(", 1)[0].strip()
        if _is_oracle_wire(dialect_name, db_type):
            # Oracle DOUBLE PRECISION / FLOAT(n) are NUMBER-backed: magnitude
            # caps near 1e125, so an IEEE-754 double source (1.5e300) will not
            # fit. BINARY_DOUBLE / BINARY_FLOAT are the true IEEE carriers.
            single = float_u in {
                "REAL",
                "FLOAT4",
                "HALF",
                "FLOAT16",
                "BINARY_FLOAT",
                "FLOAT32",
            }
            return _maybe_nullable(
                oracle.BINARY_FLOAT() if single else oracle.BINARY_DOUBLE()
            )
        if (raw or "").strip() == LOGICAL_FLOAT:
            return _maybe_nullable(sa.Double())
        if float_u in {"REAL", "FLOAT4", "HALF", "FLOAT16", "BINARY_FLOAT", "FLOAT32"}:
            if dialect_name == "postgresql" and hasattr(postgresql, "REAL"):
                return _maybe_nullable(postgresql.REAL())
            return _maybe_nullable(sa.Float())
        if float_u == "FLOAT" and (db_type or "").lower() in {
            "mysql",
            "mariadb",
            "tidb",
            "sqlserver",
            "mssql",
            "databricks",
            "spark",
            "delta",
            "delta_lake",
            "databricks_sql",
        }:
            return _maybe_nullable(sa.Float())
        return _maybe_nullable(sa.Double())
    if t == LOGICAL_BOOLEAN:
        return _maybe_nullable(sa.Boolean())
    if t == LOGICAL_DATE:
        return _maybe_nullable(sa.Date())
    if t == LOGICAL_DATETIME:
        if db_type == "questdb":
            return sa.DateTime()
        # Preserve timezone metadata when the target dialect supports it.
        if dialect_name == "clickhouse":
            return _maybe_nullable(
                ChDateTime64(3) if ChDateTime64 is not None else sa.DateTime()
            )
        if db_type == "trino" and TrinoTimestamp is not None:
            # Trino bare timestamp path — TZ-aware stamps already returned above.
            return TrinoTimestamp(precision=3, timezone=False)
        if db_type == "presto":
            return sa.TIMESTAMP()
        # A catalog token names the physical column. Bare logical ``datetime``
        # stays DATETIME2(7). ``DATETIME`` is classic 1/300 s, ``DATETIME2(n)``
        # keeps n, and ``SMALLDATETIME`` is the minute carrier. Upgrading every
        # token to DATETIME2(7) made the rounding refuse look at the wrong
        # class, so a microsecond was rounded, the read-back failed, and the
        # batch rolled back empty (DEF-R1-001).
        if mssql is not None and (
            (dialect_name or "").lower() == "mssql"
            or (db_type or "").lower() in _MSSQL_WIRES
        ):
            head = raw.strip()
            base = head.split("(", 1)[0].strip()
            base_u = base.upper()
            if base_u == "DATETIME2":
                prec = 7
                prec_match = re.search(r"\((\d+)\)", head)
                if prec_match:
                    prec = max(0, min(7, int(prec_match.group(1))))
                return _maybe_nullable(mssql.DATETIME2(precision=prec))
            if base_u == "SMALLDATETIME":
                return _maybe_nullable(mssql.SMALLDATETIME())
            if base == "DATETIME":
                return _maybe_nullable(mssql.DATETIME())
        sub_second = _sub_second_naive_wire(dialect_name, db_type, raw)
        if sub_second is not None:
            return _maybe_nullable(sub_second)
        # Map≡CREATE: LOGICAL_DATETIME without TZ markers is NTZ wall-clock on
        # Oracle/DuckDB/PG/SQL Server. Databricks TIMESTAMP is session-TZ aware
        # (TIMESTAMP_NTZ already returned naive above) — never invent the wrong
        # polarity. TZ-aware carriers (TIMESTAMPTZ, DATETIMEOFFSET, Oracle
        # WITH [LOCAL] TIME ZONE) already returned timezone=True above.
        db_l = (db_type or "").strip().lower()
        if db_l in {
            "databricks",
            "spark",
            "delta",
            "delta_lake",
            "databricks_sql",
        }:
            base = raw_lower.split("(", 1)[0].strip()
            if base in {"timestamp", "timestamptz"}:
                return sa.DateTime(timezone=True)
        return _maybe_nullable(sa.DateTime())
    if t == LOGICAL_TIME:
        # ClickHouse, QuestDB and Presto (PyHive) do not bind Python time objects
        # reliably; store as string in these engines.
        if dialect_name == "clickhouse" or db_type in (
            "clickhouse",
            "questdb",
            "presto",
        ):
            return _maybe_nullable(sa.String())
        return _maybe_nullable(sa.Time())
    if t == LOGICAL_UUID:
        if db_type == "questdb":
            return sa.Text()
        if db_type == "risingwave":
            return sa.String()
        # ClickHouse stores UUIDs as variable-length String to avoid
        # FixedString(36) padding/failure for non-canonical UUIDs.
        if dialect_name == "clickhouse":
            return _maybe_nullable(sa.String())
        if dialect_name == "postgresql":
            return postgresql.UUID()
        # CREATE must land the carrier the canonical type map stamped on the
        # mapping (``services.type_system.ddl_type``), because the bind route
        # is compiled from that stamp: a UNIQUEIDENTIFIER stamp binds a native
        # ``uuid.UUID``, and pyodbc silently truncates that to 16 characters
        # when the column it lands in is VARCHAR(36).
        if dialect_name == "mssql" and mssql is not None:
            return _maybe_nullable(mssql.UNIQUEIDENTIFIER())
        if dialect_name == "mysql":
            return _maybe_nullable(sa.CHAR(36))
        return _maybe_nullable(sa.String(36))
    if t in (LOGICAL_JSON, LOGICAL_ARRAY):
        # DuckDB: use a custom JSON type that stores compact text and binds
        # Python None as SQL NULL.  Typed ``ARRAY<...>`` carriers still map to
        # ``sa.ARRAY`` for callers that introspect the SQLAlchemy type.
        if db_type == "duckdb":
            if t == LOGICAL_ARRAY:
                match = re.match(r"^(?:ARRAY|LIST)<(.+)>$", raw, re.IGNORECASE)
                if match:
                    element = match.group(1).strip()
                    return sa.ARRAY(
                        _sa_type_for_logical(element, dialect_name, db_type)
                    )
            return _ExactJSON(none_as_null=True)
        if db_type in ("oracle", "clickhouse", "trino", "questdb", "presto"):
            return _maybe_nullable(sa.Text())
        if dialect_name == "postgresql":
            if t == LOGICAL_ARRAY:
                elem = None
                match = re.match(r"^(?:ARRAY|LIST)<(.+)>$", raw, re.IGNORECASE)
                postfix = re.match(r"^(.+?)\s*\[\s*\]\s*$", raw.strip(), re.IGNORECASE)
                if match:
                    elem = match.group(1).strip()
                elif postfix:
                    elem = postfix.group(1).strip()
                if elem:
                    return sa.ARRAY(
                        _sa_type_for_logical(elem, dialect_name, db_type)
                    )
            return postgresql.JSONB()
        return _ExactJSON(none_as_null=True)
    if t == LOGICAL_BINARY:
        if db_type in ("clickhouse", "trino", "questdb", "presto"):
            return _maybe_nullable(sa.Text())
        return sa.LargeBinary()
    if t in (LOGICAL_GEOGRAPHY, LOGICAL_INTERVAL):
        # Typed specialty DDL — never invent TEXT for Oracle SDO_GEOMETRY / PG INTERVAL.
        # String carriers (Databricks/Iceberg/MySQL interval→TEXT) stay Text honestly.
        engine_key = (db_type or dialect_name or "").lower()
        logical_name = "geography" if t == LOGICAL_GEOGRAPHY else "interval"
        native = ddl_type(engine_key, logical_name) if engine_key else ""
        native_logical = normalize_logical_type(native)
        if (
            not native
            or native_logical in {LOGICAL_STRING, LOGICAL_TEXT}
            or native.upper() in {"STRING", "TEXT", "VARCHAR", "NVARCHAR", "VARCHAR2"}
        ):
            return _maybe_nullable(sa.Text())
        if _DialectNativeType is None:
            return _maybe_nullable(sa.Text())
        return _maybe_nullable(_DialectNativeType(native))
    # Specialty carriers that normalize to STRING (HIERARCHYID, SQL_VARIANT, XML,
    # ROWVERSION, …) — DialectNativeType / typed wire via ddl_type; never invent
    # bare Text that rematerializes as Map VARCHAR over live specialty DDL.
    from services.type_system import (
        is_fixed_char_carrier,
        is_national_string_carrier,
        specialty_carrier_base,
        string_carrier_length,
    )

    # Unicode polarity and declared width survive to DDL. Collapsing every string
    # carrier to sa.Text() made SQL Server CREATE a code-page VARCHAR(MAX) for an
    # invented NVARCHAR(64), and a live postgresql->mssql read-back came back with
    # ``中`` rewritten to ``?``. sa.Unicode/UnicodeText are the dialect-neutral
    # national wires (NVARCHAR on SQL Server, NVARCHAR2 on Oracle, VARCHAR on
    # engines that are Unicode-only anyway). MySQL is the exception: those
    # generic types drop CHARACTER SET, and the server default utf8mb3 then
    # refuses a 4-byte scalar the national source held.
    if _is_mysql_wire(dialect_name, db_type):
        mysql_type = _mysql_character_type(raw, db_type or dialect_name)
        if mysql_type is not None:
            return _maybe_nullable(mysql_type)
    if is_national_string_carrier(raw):
        width = string_carrier_length(raw)
        if width is not None:
            if is_fixed_char_carrier(raw):
                return _maybe_nullable(sa.NCHAR(width))
            if _is_oracle_wire(dialect_name, db_type) and oracle is not None:
                # sa.Unicode compiles to Oracle VARCHAR2, so an NVARCHAR2 source
                # column landed as a non-national carrier and the fidelity gate
                # reported the create-new table as a narrowing.
                return _maybe_nullable(oracle.NVARCHAR2(width))
            return _maybe_nullable(sa.Unicode(width))
        if dialect_name == "mssql" or db_type in {"sqlserver", "mssql", "azure_sql"}:
            # sa.UnicodeText compiles to deprecated NTEXT; NVARCHAR(max) is the
            # only supported SQL Server Unicode LOB wire.
            return _maybe_nullable(sa.Unicode())
        return _maybe_nullable(sa.UnicodeText())

    # Declared width survives to DDL for the non-national carriers too: every
    # bounded VARCHAR(n)/CHAR(n) used to land as sa.Text(), so a live
    # postgresql->oracle CREATE turned VARCHAR(64) into CLOB — unindexable, and
    # the destination no longer enforces the source's length contract.
    width = string_carrier_length(raw)
    if width is not None:
        if is_fixed_char_carrier(raw):
            if _is_oracle_wire(dialect_name, db_type) and _DialectNativeType is not None:
                # Oracle CHAR(n) means n *bytes*: a 36-character UUID key in a
                # multibyte charset no longer fits, and the fidelity gate reads
                # the created column back as a narrowing of CHAR(n CHAR).
                return _maybe_nullable(_DialectNativeType(f"CHAR({width} CHAR)"))
            return _maybe_nullable(sa.CHAR(width))
        return _maybe_nullable(sa.String(width))

    spec = specialty_carrier_base(raw)
    if spec:
        engine_key = (db_type or dialect_name or "").lower()
        native = ddl_type(engine_key, raw) if engine_key else spec
        native_logical = normalize_logical_type(native or "")
        native_base = (native or "").upper().split("(", 1)[0].strip()
        # Same-token or remapped specialty (HIERARCHYID, LTREE, SQL_VARIANT, …)
        # before LOGICAL_STRING collapse invents bare Text.
        native_spec = specialty_carrier_base(native)
        if native_spec:
            if _DialectNativeType is None:
                return _maybe_nullable(sa.Text())
            return _maybe_nullable(_DialectNativeType(native))
        stringish = {
            "STRING",
            "TEXT",
            "VARCHAR",
            "NVARCHAR",
            "VARCHAR2",
            "NVARCHAR2",
            "CLOB",
            "NCLOB",
            "CHAR",
            "NCHAR",
        }
        if (
            not native
            or native_logical in {LOGICAL_STRING, LOGICAL_TEXT}
            or native_base in stringish
        ):
            m = re.match(
                r"^(?:N?VAR)?CHAR2?\s*\(\s*(\d+)\s*\)$",
                (native or "").strip(),
                re.IGNORECASE,
            )
            if m:
                return _maybe_nullable(sa.String(int(m.group(1))))
            return _maybe_nullable(sa.Text())
        if native_logical == LOGICAL_BINARY:
            return sa.LargeBinary()
        if native_logical == LOGICAL_UUID:
            if dialect_name == "postgresql":
                return postgresql.UUID()
            if dialect_name == "mssql" and mssql is not None:
                return _maybe_nullable(mssql.UNIQUEIDENTIFIER())
            return _maybe_nullable(sa.String(36))
        if native_logical == LOGICAL_JSON:
            if dialect_name == "postgresql":
                return postgresql.JSONB()
            return _ExactJSON(none_as_null=True)
        if _DialectNativeType is None:
            return _maybe_nullable(sa.Text())
        return _maybe_nullable(_DialectNativeType(native))
    if mssql is not None and (
        dialect_name == "mssql" or (db_type or "").lower() in _MSSQL_WIRES
    ):
        # sa.Text() compiles to SQL Server TEXT, deprecated since 2005 and
        # unusable with most string predicates. VARCHAR(max) is the wire.
        return _maybe_nullable(mssql.VARCHAR(None))
    return _maybe_nullable(sa.Text())
