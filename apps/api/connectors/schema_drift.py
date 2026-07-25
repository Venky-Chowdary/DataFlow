"""Schema-drift handling — add missing columns and widen existing columns.

Used by SQLAlchemy-based writers and by native SQL writers to safely evolve
a destination schema when the source introduces new columns or wider types
(backfill mode).
"""

from __future__ import annotations

import logging
import re
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
from services.type_system import is_lossy_coercion, normalize_logical_type

logger = logging.getLogger(__name__)


def _type_length(type_name: str) -> int | None:
    match = re.search(r"\(\s*(\d+)", type_name or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _numeric_precision_scale(type_name: str) -> tuple[int | None, int | None]:
    m = re.search(r"\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)", type_name or "")
    if not m:
        return (None, None)
    precision = int(m.group(1))
    scale = int(m.group(2)) if m.group(2) is not None else 0
    return (precision, scale)


def _integer_bit_width(type_name: str) -> int | None:
    upper = (type_name or "").upper()
    if "BIGSERIAL" in upper or "BIGINT" in upper or "INT8" in upper:
        return 64
    if "MEDIUMINT" in upper:
        return 24
    if "SMALLSERIAL" in upper or "SMALLINT" in upper or "INT2" in upper:
        return 16
    if "TINYSERIAL" in upper or "TINYINT" in upper or "INT1" in upper:
        return 8
    if "SERIAL" in upper or "INTEGER" in upper or "INT4" in upper or "INT" in upper:
        return 32
    return None


def _integer_max_digits(type_name: str) -> int | None:
    width = _integer_bit_width(type_name)
    if width is None:
        return None
    if width == 8:
        return 4  # -128..127
    if width == 16:
        return 6  # -32768..32767
    if width == 24:
        return 8  # -8388608..8388607
    if width == 32:
        return 11  # -2147483648..2147483647
    if width == 64:
        return 20  # -9223372036854775808..9223372036854775807
    return None


def _float_mantissa_bits(type_name: str) -> int | None:
    upper = (type_name or "").upper()
    if "DOUBLE" in upper:
        return 53
    if "REAL" in upper or "FLOAT" in upper:
        return 24
    return None


def _is_string_like(logical: str) -> bool:
    return logical in {"string", "text"}


def _minimum_string_length_for(old_type: str) -> int | None:
    """Minimum VARCHAR length that can losslessly hold a non-string value."""
    old_logical = normalize_logical_type(old_type)
    if old_logical == "integer":
        return _integer_max_digits(old_type)
    if old_logical == "decimal":
        p, _s = _numeric_precision_scale(old_type)
        if p is None:
            return None
        return p + 2  # sign + decimal point
    if old_logical == "float":
        return 24
    if old_logical in {"date", "time"}:
        return 30
    if old_logical == "boolean":
        return 5
    if old_logical == "uuid":
        return 36
    return None


def is_wider_type(old_type: str, new_type: str) -> bool:
    """True when new_type can hold all values of old_type without loss."""
    old_type = old_type or "VARCHAR"
    new_type = new_type or "VARCHAR"
    old_logical = normalize_logical_type(old_type)
    new_logical = normalize_logical_type(new_type)

    # String / text family: compare length, treating TEXT/unlimited as max.
    if _is_string_like(old_logical) and _is_string_like(new_logical):
        old_len = _type_length(old_type)
        new_len = _type_length(new_type)
        old_is_unlimited = old_logical == "text" or old_len is None
        new_is_unlimited = new_logical == "text" or new_len is None
        if old_is_unlimited:
            return False
        if new_is_unlimited:
            return True
        return new_len > old_len

    if old_logical == new_logical:
        if old_logical == "decimal":
            old_p, old_s = _numeric_precision_scale(old_type)
            new_p, new_s = _numeric_precision_scale(new_type)
            new_unbounded = new_p is None and new_s is None
            old_unbounded = old_p is None and old_s is None
            if new_unbounded and old_unbounded:
                return False  # both unbounded: no effective change
            if new_unbounded:
                return True  # unbounded is wider than any bounded DECIMAL
            if old_unbounded:
                return False  # bounded can never be wider than unbounded
            if new_p is None or new_s is None or old_p is None or old_s is None:
                return False
            return (
                new_p >= old_p and new_s >= old_s and (new_p > old_p or new_s > old_s)
            )
        if old_logical == "integer":
            old_w = _integer_bit_width(old_type)
            new_w = _integer_bit_width(new_type)
            if old_w is None or new_w is None:
                return False
            return new_w > old_w
        if old_logical == "float":
            old_w = _float_mantissa_bits(old_type)
            new_w = _float_mantissa_bits(new_type)
            if old_w is None or new_w is None:
                return False
            return new_w > old_w
        return False

    # Integer -> DECIMAL: new must have enough integer digits.
    if old_logical == "integer" and new_logical == "decimal":
        digits = _integer_max_digits(old_type)
        new_p, new_s = _numeric_precision_scale(new_type)
        if digits is None or new_p is None or new_s is None:
            return False
        return new_p - new_s >= digits

    # Integer -> FLOAT: only safe when float mantissa can exactly represent max int.
    if old_logical == "integer" and new_logical == "float":
        old_w = _integer_bit_width(old_type)
        new_w = _float_mantissa_bits(new_type)
        if old_w is None or new_w is None:
            return False
        return new_w >= old_w

    # DECIMAL -> FLOAT: safe from an overflow/range perspective when the float
    # mantissa can represent the decimal's total digit count (DOUBLE ~ 15 digits).
    if old_logical == "decimal" and new_logical == "float":
        old_p, _old_s = _numeric_precision_scale(old_type)
        new_w = _float_mantissa_bits(new_type)
        if old_p is None or new_w is None:
            return False
        max_exact_digits = 15 if new_w >= 53 else (6 if new_w >= 24 else 0)
        return old_p <= max_exact_digits

    # Cross-logical promotions to string-like: length must be sufficient.
    if _is_string_like(new_logical):
        min_len = _minimum_string_length_for(old_type)
        new_len = _type_length(new_type)
        if new_len is None:
            return True
        if min_len is None:
            return False
        return new_len >= min_len

    # Other cross-logical promotions rely on the type-system safe-promotion list.
    return not is_lossy_coercion(old_type, new_type)


def _information_schema_type_to_str(
    data_type: str,
    char_len: int | None,
    numeric_precision: int | None,
    numeric_scale: int | None,
) -> str:
    """Reconstruct a type string from information_schema metadata."""
    upper = (data_type or "").upper()
    if upper in {
        "CHARACTER VARYING",
        "VARCHAR",
        "CHARACTER",
        "CHAR",
        "NCHAR",
        "NVARCHAR",
    }:
        length = char_len
        if length is not None and length > 0:
            return f"{upper}({length})"
        return upper
    if upper in {"TEXT", "CLOB", "LONGTEXT", "MEDIUMTEXT", "NTEXT"}:
        return "TEXT"
    if upper in {"NUMERIC", "DECIMAL", "NUMBER"}:
        if numeric_precision is not None:
            if numeric_scale is not None and numeric_scale > 0:
                return f"{upper}({numeric_precision},{numeric_scale})"
            return (
                f"{upper}({numeric_precision},0)"
                if numeric_precision is not None
                else upper
            )
        return upper
    if upper in {"DOUBLE PRECISION", "DOUBLE"}:
        return "DOUBLE PRECISION"
    if upper == "REAL":
        return "REAL"
    if upper == "FLOAT":
        return "FLOAT"
    if upper in {"INTEGER", "INT", "INT4"}:
        return "INTEGER"
    if upper in {"BIGINT", "INT8"}:
        return "BIGINT"
    if upper in {"SMALLINT", "INT2"}:
        return "SMALLINT"
    if upper == "TINYINT":
        return "TINYINT"
    if upper == "MEDIUMINT":
        return "MEDIUMINT"
    return upper


def _quote_col(dialect: str, name: str) -> str:
    dialect = (dialect or "").lower()
    if dialect in ("mysql", "mariadb"):
        return quote_sql_identifier(name, "`")
    if dialect in ("sqlserver", "mssql"):
        safe = name.replace("]", "]]")
        return f"[{safe}]"
    return quote_sql_identifier(name, '"')


def _build_widen_ddl(
    dialect: str,
    schema: str | None,
    table_name: str,
    col: str,
    new_type: str,
    existing_type: str | None = None,
) -> str:
    """Generate a single ALTER COLUMN / MODIFY COLUMN statement."""
    dialect = (dialect or "").lower()
    table_ref = quote_table_ref(table_name, schema, dialect=dialect, sanitize=False)
    col_q = _quote_col(dialect, col)

    if dialect in (
        "postgresql",
        "postgres",
        "redshift",
        "cockroachdb",
        "yugabyte",
        "timescale",
        "supabase",
        "neon",
    ):
        # Same-family width increases do not need USING and avoid truncation risk.
        old_logical = normalize_logical_type(existing_type or "VARCHAR")
        new_logical = normalize_logical_type(new_type)
        using = ""
        if old_logical != new_logical:
            # Cross-logical casts need an explicit cast; include the length in the cast.
            using = f" USING {col_q}::{new_type}"
        return f"ALTER TABLE {table_ref} ALTER COLUMN {col_q} TYPE {new_type}{using}"

    if dialect in ("mysql", "mariadb"):
        return f"ALTER TABLE {table_ref} MODIFY COLUMN {col_q} {new_type}"

    if dialect in ("sqlserver", "mssql"):
        return f"ALTER TABLE {table_ref} ALTER COLUMN {col_q} {new_type}"

    if dialect in ("duckdb", "motherduck"):
        return f"ALTER TABLE {table_ref} ALTER COLUMN {col_q} TYPE {new_type}"

    if dialect in ("oracle", "oracle_db"):
        return f"ALTER TABLE {table_ref} MODIFY ({col_q} {new_type})"

    if dialect in ("sqlite",):
        # SQLite does not support ALTER COLUMN TYPE; caller should recreate the table.
        raise NotImplementedError("SQLite cannot ALTER COLUMN TYPE")

    raise NotImplementedError(f"Unsupported dialect for column widen: {dialect}")


def _fetch_existing_columns(
    cursor: Any,
    dialect: str,
    schema: str | None,
    table_name: str,
) -> dict[str, str]:
    """Return {column_name: type_string} from the destination catalog."""
    dialect = (dialect or "").lower()

    if dialect in (
        "postgresql",
        "postgres",
        "redshift",
        "cockroachdb",
        "yugabyte",
        "timescale",
        "supabase",
        "neon",
        "duckdb",
        "motherduck",
    ):
        # DuckDB's native driver uses ``?`` placeholders, not ``%s``.
        params = (schema or "public", table_name)
        if dialect in ("duckdb", "motherduck"):
            cursor.execute(
                """SELECT column_name, data_type, character_maximum_length,
                          numeric_precision, numeric_scale
                   FROM information_schema.columns
                   WHERE table_schema = ? AND table_name = ?""",
                params,
            )
        else:
            cursor.execute(
                """SELECT column_name, data_type, character_maximum_length,
                          numeric_precision, numeric_scale
                   FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = %s""",
                params,
            )
        out: dict[str, str] = {}
        for row in cursor.fetchall():
            # information_schema row may have 3 or 5 columns depending on the view.
            if len(row) >= 5:
                col, data_type, char_len, num_prec, num_scale = row[:5]
            else:
                col, data_type = row[0], row[1]
                char_len = num_prec = num_scale = None
            out[col] = _information_schema_type_to_str(
                data_type, char_len, num_prec, num_scale
            )
        return out

    if dialect in ("mysql", "mariadb"):
        cursor.execute(
            """SELECT column_name, data_type, character_maximum_length,
                      numeric_precision, numeric_scale
               FROM information_schema.columns
               WHERE table_schema = DATABASE() AND table_name = %s""",
            (table_name,),
        )
        out = {}
        for row in cursor.fetchall():
            if len(row) >= 5:
                col, data_type, char_len, num_prec, num_scale = row[:5]
            else:
                col, data_type = row[0], row[1]
                char_len = num_prec = num_scale = None
            out[col] = _information_schema_type_to_str(
                data_type, char_len, num_prec, num_scale
            )
        return out

    if dialect in ("sqlserver", "mssql"):
        # SQL Server table_catalog is the database; filter by schema + table.
        cursor.execute(
            """SELECT column_name, data_type, character_maximum_length,
                      numeric_precision, numeric_scale
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s""",
            (schema or "dbo", table_name),
        )
        out = {}
        for row in cursor.fetchall():
            if len(row) >= 5:
                col, data_type, char_len, num_prec, num_scale = row[:5]
            else:
                col, data_type = row[0], row[1]
                char_len = num_prec = num_scale = None
            out[col] = _information_schema_type_to_str(
                data_type, char_len, num_prec, num_scale
            )
        return out

    if dialect in ("oracle", "oracle_db"):
        cursor.execute(
            """SELECT column_name, data_type, data_length, data_precision, data_scale
               FROM all_tab_columns
               WHERE owner = UPPER(:1) AND table_name = UPPER(:2)""",
            (schema or "", table_name),
        )
        out = {}
        for row in cursor.fetchall():
            if len(row) >= 5:
                col, data_type, data_len, num_prec, num_scale = row[:5]
            else:
                col, data_type = row[0], row[1]
                data_len = num_prec = num_scale = None
            out[col] = _information_schema_type_to_str(
                data_type, data_len, num_prec, num_scale
            )
        return out

    return {}


def widen_existing_columns_native(
    cursor: Any,
    dialect: str,
    schema: str | None,
    table_name: str,
    target_cols: list[str],
    target_types: list[str],
    *,
    backfill: bool = False,
    skip_cols: list[str] | None = None,
) -> list[str]:
    """Issue ALTER COLUMN / MODIFY COLUMN to widen columns that are now too narrow.

    Returns the list of DDL statements executed.  ``backfill`` must be True for any
    DDL to be issued.  The function is idempotent: repeated calls will only emit
    ALTER statements when the target type is wider than the existing catalog type.
    """
    if not backfill or not target_cols or not target_types:
        return []

    dialect = (dialect or "").lower()
    if dialect in ("sqlite",):
        # SQLite cannot widen a column in place.  The caller must recreate the table.
        logger.debug("SQLite does not support ALTER COLUMN TYPE; skipping widen.")
        return []

    skip = set(skip_cols or [])
    existing = _fetch_existing_columns(cursor, dialect, schema, table_name)
    if not existing:
        return []

    log: list[str] = []
    for col, new_type in zip(target_cols, target_types):
        if col in skip:
            continue
        if col not in existing:
            continue
        existing_type = existing[col]
        if not is_wider_type(existing_type, new_type):
            continue
        try:
            ddl = _build_widen_ddl(
                dialect, schema, table_name, col, new_type, existing_type
            )
            # Session-level lock timeouts are now configured by each driver's
            # connection guard (e.g. apply_postgres_session_guards / apply_mysql_session_guards)
            # so ALTER COLUMN cannot hang forever on a contended table lock.
            cursor.execute(ddl)
            log.append(ddl)
            logger.debug(
                "Widened %s.%s from %s to %s", table_name, col, existing_type, new_type
            )
        except Exception as exc:
            err = str(exc).lower()
            # Ignore "already correct width" / concurrent-alter / lock-timeout
            # races and harmless syntax issues.
            if any(
                phrase in err
                for phrase in ("already", "cannot alter", "not supported", "lock timeout")
            ):
                logger.debug(
                    "Widen skipped for %s.%s: %s", table_name, col, exc, exc_info=exc
                )
                continue
            logger.warning(
                "Widen failed for %s.%s: %s", table_name, col, exc, exc_info=exc
            )
            raise
    return log


def add_missing_columns(
    engine: Any,
    table_name: str,
    schema: str | None,
    target_cols: list[str],
    sa_col_types: dict[str, Any],
    *,
    backfill: bool = False,
    connection: Any | None = None,
) -> list[str]:
    """Return DDL statements for any columns that need to be added.

    If ``backfill`` is False no changes are made.  When True, existing tables
    are inspected and ``ALTER TABLE ADD COLUMN`` statements are issued for each
    missing column.  Statements are idempotent: ``IF NOT EXISTS`` is used when the
    dialect supports it, and "already exists" errors are swallowed so concurrent
    or resume runs do not fail.  Returns the list of DDL statements executed.
    """
    if not backfill:
        return []

    import sqlalchemy as sa

    inspector = sa.inspect(engine)
    if not inspector.has_table(table_name, schema=schema):
        return []

    existing = {c["name"] for c in inspector.get_columns(table_name, schema=schema)}
    missing = [c for c in target_cols if c not in existing]
    if not missing:
        return []

    dialect = engine.dialect
    dialect_name = getattr(dialect, "name", "")
    keyword = (
        "ADD COLUMN" if dialect_name not in ("mssql", "oracle", "sybase") else "ADD"
    )
    supports_if_not_exists = dialect_name in {"postgresql", "duckdb"}
    # SQLite rejects "ADD COLUMN IF NOT EXISTS" (syntax error near EXISTS).
    if_not_exists = " IF NOT EXISTS" if supports_if_not_exists else ""
    log: list[str] = []
    quoted_schema = f'"{schema}"' if schema else None

    def _column_exists_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(
            phrase in text
            for phrase in (
                "already exists",
                "duplicate column",
                "column already exists",
                "duplicate key",
            )
        )

    def _run(conn: Any) -> None:
        for col in missing:
            sa_type = sa_col_types.get(col)
            if sa_type is None:
                continue
            col_ddl = str(
                sa.schema.CreateColumn(sa.Column(col, sa_type, quote=True)).compile(
                    dialect=dialect
                )
            )
            if quoted_schema:
                qualified = f'{quoted_schema}."{table_name}"'
            else:
                qualified = f'"{table_name}"'
            alter = f"ALTER TABLE {qualified} {keyword}{if_not_exists} {col_ddl}"
            try:
                conn.execute(sa.text(alter))
                conn.commit()
                log.append(alter)
            except Exception as exc:
                if _column_exists_error(exc):
                    try:
                        conn.rollback()
                    except Exception as rollback_exc:
                        logger.debug(
                            "add-missing-columns rollback failed: %s",
                            rollback_exc,
                            exc_info=rollback_exc,
                        )
                    logger.debug(
                        "add-missing-columns skipped existing column: %s",
                        exc,
                        exc_info=exc,
                    )
                    continue
                logger.warning(
                    "add-missing-columns failed for %s: %s", col, exc, exc_info=exc
                )
                raise

    if connection is None:
        with engine.connect() as conn:
            _run(conn)
    else:
        _run(connection)
    return log
