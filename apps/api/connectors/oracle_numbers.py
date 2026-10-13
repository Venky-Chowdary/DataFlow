"""Exact Oracle NUMBER fetch.

python-oracledb and SQLAlchemy return an unconstrained NUMBER as IEEE float64.
A 38-digit value then becomes ``3.141592653589793`` and the job completes with
nothing rejected (E1-030). The same string path calls ``int`` on the driver
text ``nan``, which aborts the read before any row can be quarantined.

NUMBER is fetched as text and converted with :func:`oracle_number_from_driver`.
That converter never calls ``int`` on a non-finite token. BINARY_FLOAT and
BINARY_DOUBLE stay native IEEE: they are not NUMBER, and they can store NaN.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

_INSTALLED = False

_NONFINITE = {
    "nan": Decimal("NaN"),
    "nann": Decimal("NaN"),
    "+nan": Decimal("NaN"),
    "-nan": Decimal("NaN"),
    "inf": Decimal("Infinity"),
    "+inf": Decimal("Infinity"),
    "infinity": Decimal("Infinity"),
    "+infinity": Decimal("Infinity"),
    "-inf": Decimal("-Infinity"),
    "-infinity": Decimal("-Infinity"),
}


def oracle_number_from_driver(value: Any) -> Any:
    """Driver NUMBER text → exact Decimal or int. ``nan`` does not raise."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # Already IEEE. Keep NaN/Infinity as Decimal so a later int() cannot
        # see the driver spelling ``nan``.
        if value != value:
            return Decimal("NaN")
        if value == float("inf"):
            return Decimal("Infinity")
        if value == float("-inf"):
            return Decimal("-Infinity")
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    token = text.lower()
    if token in _NONFINITE:
        return _NONFINITE[token]
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    try:
        dec = Decimal(text)
    except (InvalidOperation, ValueError):
        return value
    if dec.is_nan() or dec.is_infinite():
        return dec
    if dec == dec.to_integral_value():
        return int(dec)
    return dec


def _number_string_var(cursor: Any, string_type: Any, converter: Any) -> Any:
    return cursor.var(
        string_type,
        255,
        arraysize=getattr(cursor, "arraysize", None),
        outconverter=converter,
    )


def install_oracle_exact_fetch() -> None:
    """Patch SQLAlchemy's Oracle dialect so NUMBER is not fetched as float.

    Idempotent. Cursor-level handlers replace the connection handler, so both
    the connection generator and the numeric/integer type handlers are patched.

    Version/dialect resilient: the product connects via ``oracle+oracledb``,
    but the patch used to hardcode ``cx_oracle._OracleNumericCommon`` — a name
    SQLAlchemy 2.x renamed to ``_OracleNumeric``. The AttributeError then
    killed the whole Oracle driver on startup (QA C08). Every hook is resolved
    by presence, patched on each installed dialect module, and a missing
    internal name degrades to a logged fidelity gap rather than a dead driver.
    """
    global _INSTALLED
    if _INSTALLED:
        return
    import logging

    _log = logging.getLogger(__name__)

    dialect_mods: list[Any] = []
    for mod_name in ("oracledb", "cx_oracle"):
        try:
            dialect_mods.append(
                __import__(f"sqlalchemy.dialects.oracle.{mod_name}", fromlist=[mod_name])
            )
        except ImportError:
            continue
    if not dialect_mods:
        _log.warning("Oracle exact-fetch patch skipped: no oracle dialect module")
        return

    def _numeric_handler(self, dialect):  # noqa: ANN001
        dbapi = dialect.dbapi
        native = getattr(dbapi, "NATIVE_FLOAT", None)

        def handler(cursor, _name, default_type, _size, _precision, _scale):  # noqa: ANN001
            # BINARY_FLOAT / BINARY_DOUBLE are IEEE. NUMBER must not take that path.
            if native is not None and default_type is native:
                return None
            return _number_string_var(cursor, dbapi.STRING, oracle_number_from_driver)

        return handler

    def _integer_var(self, dialect, cursor, arraysize=None):  # noqa: ANN001
        dbapi = dialect.dbapi
        return cursor.var(
            dbapi.STRING,
            255,
            arraysize=arraysize if arraysize is not None else cursor.arraysize,
            outconverter=oracle_number_from_driver,
        )

    def _detect_decimal(self, value):  # noqa: ANN001
        return oracle_number_from_driver(value)

    patched: list[str] = []
    for mod in dialect_mods:
        dialect_cls = (
            getattr(mod, "OracleDialect_oracledb", None)
            or getattr(mod, "OracleDialect_cx_oracle", None)
            or getattr(mod, "_OracleDialect_cx_oracle", None)
        )
        if dialect_cls is None:
            continue
        original_generate = dialect_cls._generate_connection_outputtype_handler

        def _generate(self, _orig=original_generate):  # noqa: ANN001
            base = _orig(self)
            dbapi = self.dbapi
            native = getattr(dbapi, "NATIVE_FLOAT", None)
            number = getattr(dbapi, "NUMBER", None)

            def output_type_handler(cursor, name, default_type, size, precision, scale):  # noqa: ANN001
                if (
                    number is not None
                    and default_type == number
                    and default_type is not native
                ):
                    return _number_string_var(
                        cursor, dbapi.STRING, oracle_number_from_driver
                    )
                return base(cursor, name, default_type, size, precision, scale)

            return output_type_handler

        # The impl-level hook name is _cx_oracle_outputtypehandler on both
        # dialects; the class that carries it was renamed across releases.
        numeric_cls = (
            getattr(mod, "_OracleNumericCommon", None)
            or getattr(mod, "_OracleNumeric", None)
            or getattr(mod, "_OracleNUMBER", None)
        )
        if numeric_cls is not None:
            numeric_cls._cx_oracle_outputtypehandler = _numeric_handler
        else:
            _log.debug("oracle numeric impl class not found on %s", mod.__name__)
        integer_cls = getattr(mod, "_OracleInteger", None)
        if integer_cls is not None:
            integer_cls._cx_oracle_var = _integer_var
        dialect_cls._detect_decimal = _detect_decimal
        dialect_cls._generate_connection_outputtype_handler = _generate
        patched.append(mod.__name__.rsplit(".", 1)[-1])

    _INSTALLED = True
    if patched:
        _log.debug("Oracle exact-fetch patched dialects: %s", ", ".join(patched))


def attach_oracle_number_output(conn: Any) -> None:
    """Per-connection handler for a raw ``oracledb.connect`` (not the process default).

    The scale harness sets its own handler after connect. This must not write
    ``oracledb.defaults``.
    """
    import oracledb

    def handler(cursor: Any, metadata: Any) -> Any:
        if getattr(metadata, "type_code", None) is oracledb.DB_TYPE_NUMBER:
            return _number_string_var(
                cursor, oracledb.DB_TYPE_VARCHAR, oracle_number_from_driver
            )
        return None

    conn.outputtypehandler = handler
