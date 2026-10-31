"""A carried column DEFAULT spelled the way a PostgreSQL BOOLEAN accepts it.

MySQL and MariaDB have no boolean type: ``tinyint(1) DEFAULT '0'`` is how they
spell a flag, and SQL Server / Oracle use ``BIT`` / ``NUMBER(1)`` with ``0`` /
``1``. Mapped to a PostgreSQL ``BOOLEAN`` the column is right, but the literal
is not: ``boolean DEFAULT 0`` is refused ("column is of type boolean but
default expression is of type integer"), and that refused CREATE is what
turned MariaDB -> PG into "current transaction is aborted" (QA round 11).

The rule: a 0/1 (or yes/no, on/off, t/f) default becomes ``false`` / ``true``;
any other literal cannot be a boolean, so the default is not carried and the
fidelity report says why. NULL stays NULL.
"""

from __future__ import annotations

_PG_FAMILY = frozenset({
    "postgresql", "postgres", "pgvector", "timescaledb", "alloydb", "cockroachdb",
    "amazon_rds_postgresql", "azure_database_for_postgresql", "google_cloud_sql_postgresql",
    "aurora_postgresql", "supabase", "neon", "redshift", "amazon_redshift",
})

_FALSE = frozenset({"0", "false", "f", "n", "no", "off", "b'0'", "0::boolean"})
_TRUE = frozenset({"1", "true", "t", "y", "yes", "on", "b'1'", "1::boolean"})


def _is_boolean_type(dest_type: str) -> bool:
    head = (dest_type or "").strip().lower().split("(", 1)[0].strip()
    return head in {"boolean", "bool"}


def pg_boolean_default(default_sql: str, dest_type: str, dest_dialect: str) -> str | None:
    """The PostgreSQL BOOLEAN spelling of ``default_sql``.

    ``None``: the rule does not apply (not a PostgreSQL-family BOOLEAN column).
    ``""``: no boolean literal means this default — do not carry it.
    Otherwise ``"false"``, ``"true"`` or ``"NULL"``.
    """
    if (dest_dialect or "").strip().lower() not in _PG_FAMILY or not _is_boolean_type(dest_type):
        return None
    text = (default_sql or "").strip()
    while text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    lowered = text.lower()
    if lowered == "null":
        return "NULL"
    if len(lowered) >= 2 and lowered[0] == lowered[-1] == "'":
        lowered = lowered[1:-1].strip()
    if lowered in _FALSE:
        return "false"
    if lowered in _TRUE:
        return "true"
    return ""
