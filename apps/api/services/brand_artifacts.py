"""Dry run: which of this product's artifacts a destination holds, by name.

Lists every table, column and table comment in a destination that carries one
of the names in ``services.brand.ARTIFACTS`` (old "dataflow"/``_df_`` names and
any new "datawrap"/``_dw_`` ones). It reads the catalog only. It never renames,
drops or alters anything: renaming a live artifact would break the next run
(CDC resume points, retried chunks, ownership markers), so the report's
``action`` is always ``keep``. The operator decides what, if anything, to
migrate by hand; ``new_name`` says what the flag would call a new one.

    python scripts/brand_artifacts_dry_run.py --type postgresql \\
        --config '{"host": "...", "port": 5432, "database": "...", ...}'
"""

from __future__ import annotations

from typing import Any, Iterable

from services.brand import ARTIFACTS, Artifact, object_names_flag_on

_TABLE_SHAPES = ("table", "table_prefix", "table_suffix")
_COLUMN_SHAPES = ("column", "column_prefix")


def _table_match(name: str) -> tuple[Artifact, str] | None:
    """(artifact, which-name) for a table name, most specific shape first."""
    low = name.lower()
    for shape in _TABLE_SHAPES:
        for art in ARTIFACTS.values():
            if art.shape != shape:
                continue
            for label, spelled in (("legacy", art.legacy), ("new", art.new)):
                s = spelled.lower()
                if (
                    (shape == "table" and low == s)
                    or (shape == "table_prefix" and low.startswith(s))
                    or (shape == "table_suffix" and s in low)
                ):
                    return art, label
    return None


def _column_match(name: str) -> tuple[Artifact, str] | None:
    low = name.lower()
    for shape in _COLUMN_SHAPES:
        for art in ARTIFACTS.values():
            if art.shape != shape:
                continue
            for label, spelled in (("legacy", art.legacy), ("new", art.new)):
                s = spelled.lower()
                if (shape == "column" and low == s) or (shape == "column_prefix" and low.startswith(s)):
                    return art, label
    return None


def _comment_match(comment: Any) -> tuple[Artifact, str] | None:
    art = ARTIFACTS["managed_table_comment"]
    text = "" if comment is None else str(comment)
    if text == art.legacy:
        return art, "legacy"
    if text == art.new:
        return art, "new"
    return None


def _entry(art: Artifact, label: str, obj: str) -> dict[str, Any]:
    return {
        "kind": art.kind,
        "object": obj,
        "named": label,
        "legacy_name": art.legacy,
        "new_name": art.new,
        "where": art.where,
        "switched_by_flag": art.switchable,
        "action": "keep",
    }


def classify(
    tables: Iterable[str],
    columns: Iterable[tuple[str, str]],
    comments: Iterable[tuple[str, Any]],
) -> list[dict[str, Any]]:
    """Report entries for catalog rows (table names, (table, column), (table, comment))."""
    out: list[dict[str, Any]] = []
    for table in sorted(set(tables)):
        hit = _table_match(table)
        if hit:
            out.append(_entry(hit[0], hit[1], table))
    for table, column in sorted(set(columns)):
        hit = _column_match(column)
        if hit:
            out.append(_entry(hit[0], hit[1], f"{table}.{column}"))
    for table, comment in sorted(comments, key=lambda r: r[0]):
        hit = _comment_match(comment)
        if hit:
            out.append(_entry(hit[0], hit[1], f"{table} (table comment)"))
    return out


def _pg_catalog(cfg: dict[str, Any]) -> tuple[list, list, list]:
    from connectors.postgresql_conn import get_connection

    schema = cfg.get("schema") or "public"
    conn = get_connection(
        host=cfg.get("host", ""), port=int(cfg.get("port") or 5432),
        database=cfg.get("database", ""), username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""), ssl=bool(cfg.get("ssl", False)),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.relname, obj_description(c.oid, 'pg_class') FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'v', 'm', 'f')",
                (schema,),
            )
            rel = cur.fetchall()
            cur.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = %s",
                (schema,),
            )
            cols = cur.fetchall()
            cur.execute(
                "SELECT t.typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
                "WHERE n.nspname = %s AND t.typtype = 'e'",
                (schema,),
            )
            enums = [r[0] for r in cur.fetchall()]
        conn.rollback()
    finally:
        conn.close()
    return [r[0] for r in rel] + enums, cols, [(r[0], r[1]) for r in rel if r[1]]


def _mysql_catalog(cfg: dict[str, Any]) -> tuple[list, list, list]:
    from connectors.mysql_conn import get_connection

    conn = get_connection(
        host=cfg.get("host", ""), port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""), username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""), ssl=bool(cfg.get("ssl", False)),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME, TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE()"
            )
            rel = cur.fetchall()
            cur.execute(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE()"
            )
            cols = cur.fetchall()
    finally:
        conn.close()
    return [r[0] for r in rel], list(cols), [(r[0], r[1]) for r in rel if r[1]]


def _sqlalchemy_catalog(dest_type: str, cfg: dict[str, Any]) -> tuple[list, list, list]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine, _schema_name
    from services.engine_pool import release_engine

    engine = _engine({**cfg, "type": dest_type})
    try:
        schema = _schema_name({**cfg, "type": dest_type}) or None
        with engine.connect() as conn:
            insp = sa.inspect(conn)
            tables = list(insp.get_table_names(schema=schema))
            cols = [
                (t, str(c.get("name") or ""))
                for t in tables
                for c in insp.get_columns(t, schema=schema)
            ]
            comments = []
            for t in tables:
                try:
                    comments.append((t, (insp.get_table_comment(t, schema=schema) or {}).get("text")))
                except (NotImplementedError, sa.exc.SQLAlchemyError):
                    break
    finally:
        release_engine(engine)
    return tables, cols, [c for c in comments if c[1]]


def scan_destination(dest_type: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """Read-only inventory of this product's named artifacts in one destination."""
    kind = (dest_type or "").strip().lower()
    if kind in {"postgresql", "postgres", "redshift"}:
        tables, cols, comments = _pg_catalog(cfg)
    elif kind in {"mysql", "mariadb"}:
        tables, cols, comments = _mysql_catalog(cfg)
    else:
        tables, cols, comments = _sqlalchemy_catalog(kind, cfg)
    artifacts = classify(tables, cols, comments)
    return {
        "dry_run": True,
        "renamed": 0,
        "dest_type": kind,
        "flag_on": object_names_flag_on(),
        "legacy_count": sum(1 for a in artifacts if a["named"] == "legacy"),
        "new_count": sum(1 for a in artifacts if a["named"] == "new"),
        "artifacts": artifacts,
        "note": (
            "Read-only. Nothing was renamed. Artifacts that exist keep their names: "
            "renaming one would break the next run against it."
        ),
    }
