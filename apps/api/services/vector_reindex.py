"""Opt-in migration of vector rows to stable document/chunk identifiers."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any

from services.audit_log import append_audit_event

logger = logging.getLogger(__name__)


class VectorReindexError(RuntimeError):
    """A vector reindex request could not be completed safely."""


class UnsupportedVectorReindexError(VectorReindexError):
    """The requested vector destination does not support stable-ID reindexing."""


@dataclass(frozen=True)
class VectorReindexReport:
    scanned: int = 0
    already_stable: int = 0
    moved: int = 0
    skipped_no_source_id: int = 0
    collisions: int = 0
    dry_run: bool = True


def _identity(row: dict[str, Any]) -> tuple[str, int, str]:
    from services.vector_embedding import coerce_chunk_index

    return (
        str(row.get("source_id") or "").strip(),
        coerce_chunk_index(row.get("chunk_index")),
        str(row.get("content") or ""),
    )


def _plan(
    rows: list[dict[str, Any]],
    *,
    qdrant: bool,
) -> tuple[list[tuple[dict[str, Any], Any]], int, int, int]:
    from services.vectorization import _stable_vector_row_id

    from connectors.qdrant_writer import qdrant_point_id

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    skipped_no_source_id = 0
    for row in rows:
        source_id = str(row.get("source_id") or "").strip()
        if not source_id:
            skipped_no_source_id += 1
            continue
        groups[source_id].append(row)

    expected_by_row: dict[str, Any] = {}
    already_stable = 0
    duplicate_targets: set[str] = set()
    target_owners: dict[str, list[str]] = defaultdict(list)
    for source_id, group in groups.items():
        for row in group:
            raw_target = _stable_vector_row_id(
                source_id,
                row.get("chunk_index", 0),
                str(row.get("content") or ""),
                multi_chunk=len(group) > 1,
            )
            target = qdrant_point_id(raw_target) if qdrant else raw_target
            old_id = str(row.get("id") or "")
            expected_by_row[old_id] = target
            target_owners[str(target)].append(old_id)
            if old_id == str(target):
                already_stable += 1

    for target, owners in target_owners.items():
        if len(owners) > 1:
            duplicate_targets.update(owners)

    rows_by_id = {str(row.get("id") or ""): row for row in rows}
    all_ids = set(rows_by_id)
    collisions = set(duplicate_targets)
    moves: list[tuple[dict[str, Any], Any]] = []
    for row in rows:
        old_id = str(row.get("id") or "")
        target = expected_by_row.get(old_id)
        if target is None or str(target) == old_id or old_id in collisions:
            continue
        existing = rows_by_id.get(str(target))
        if existing is not None:
            if _identity(existing) != _identity(row):
                collisions.add(old_id)
                continue
            if (
                str(target) in expected_by_row
                and str(expected_by_row[str(target)]) != str(target)
            ):
                collisions.add(old_id)
                collisions.add(str(target))
                continue
        elif str(target) in all_ids:
            collisions.add(old_id)
            continue
        moves.append((row, target))

    if collisions:
        moves = [
            (row, target)
            for row, target in moves
            if str(row.get("id") or "") not in collisions
        ]
    return moves, already_stable, skipped_no_source_id, len(collisions)


def _pgvector_rows(cfg: dict[str, Any], schema: str, table: str) -> list[dict[str, Any]]:
    from psycopg2 import sql

    from services.vector_sync import _pgvector_connection

    conn = _pgvector_connection(cfg)
    try:
        with conn.cursor() as cur:
            relation = sql.SQL("{}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
            cur.execute(
                sql.SQL(
                    "SELECT id::text, source_id, content, chunk_index "
                    "FROM {} ORDER BY source_id NULLS FIRST, chunk_index, id::text"
                ).format(relation)
            )
            return [
                {
                    "id": row[0],
                    "source_id": row[1],
                    "content": row[2],
                    "chunk_index": row[3],
                }
                for row in cur.fetchall()
            ]
    except Exception as exc:
        raise VectorReindexError(
            f"Could not scan pgvector destination {schema}.{table}: {exc}"
        ) from exc
    finally:
        conn.close()


def _pgvector_columns(cfg: dict[str, Any], schema: str, table: str) -> list[str]:
    from services.vector_sync import _pgvector_connection

    conn = _pgvector_connection(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema, table),
            )
            columns = [str(row[0]) for row in cur.fetchall()]
    except Exception as exc:
        raise VectorReindexError(
            f"Could not inspect pgvector destination {schema}.{table}: {exc}"
        ) from exc
    finally:
        conn.close()
    required = {"id", "source_id", "content", "chunk_index"}
    if not required.issubset(columns):
        missing = ", ".join(sorted(required - set(columns)))
        raise VectorReindexError(
            f"pgvector destination {schema}.{table} lacks required columns: {missing}"
        )
    return columns


def _pgvector_move(
    cfg: dict[str, Any],
    schema: str,
    table: str,
    columns: list[str],
    moves: list[tuple[dict[str, Any], Any]],
    batch_size: int,
) -> int:
    from psycopg2 import sql

    from services.vector_sync import _pgvector_connection

    if not moves:
        return 0
    relation = sql.SQL("{}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    conn = _pgvector_connection(cfg)
    moved = 0
    try:
        for offset in range(0, len(moves), batch_size):
            batch = moves[offset : offset + batch_size]
            with conn.cursor() as cur:
                values_sql = sql.SQL(", ").join(
                    sql.SQL("(%s, %s)") for _ in batch
                )
                params: list[Any] = []
                for row, target in batch:
                    params.extend((str(row["id"]), target))
                source_cols = [name for name in columns if name != "id"]
                insert_columns = sql.SQL(", ").join(
                    [sql.Identifier("id")]
                    + [sql.Identifier(name) for name in source_cols]
                )
                select_columns = sql.SQL(", ").join(
                    [sql.SQL("moves.new_id")]
                    + [
                        sql.SQL("source.{}").format(sql.Identifier(name))
                        for name in source_cols
                    ]
                )
                source_match = sql.SQL(
                    "source.id::text = moves.old_id"
                )
                cur.execute(
                    sql.SQL(
                        "WITH moves(old_id, new_id) AS (VALUES {}), "
                        "inserted AS ("
                        "INSERT INTO {} ({}) "
                        "SELECT {} FROM moves JOIN {} AS source ON {} "
                        "ON CONFLICT (id) DO NOTHING RETURNING id::text"
                        ") "
                        "DELETE FROM {} AS old USING moves "
                        "WHERE old.id::text = moves.old_id "
                        "AND (moves.new_id IN (SELECT id FROM inserted) OR EXISTS ("
                        "SELECT 1 FROM {} AS target "
                        "WHERE target.id::text = moves.new_id "
                        "AND target.source_id IS NOT DISTINCT FROM old.source_id "
                        "AND target.chunk_index IS NOT DISTINCT FROM old.chunk_index "
                        "AND target.content IS NOT DISTINCT FROM old.content"
                        ")) RETURNING old.id::text"
                    ).format(
                        values_sql,
                        relation,
                        insert_columns,
                        select_columns,
                        relation,
                        source_match,
                        relation,
                        relation,
                    ),
                    params,
                )
                moved += len(cur.fetchall())
            conn.commit()
        return moved
    except Exception as exc:
        conn.rollback()
        raise VectorReindexError(
            f"Could not migrate pgvector destination {schema}.{table}: {exc}"
        ) from exc
    finally:
        conn.close()


def _qdrant_rows(
    session: Any, base_url: str, headers: dict[str, str], collection: str
) -> list[dict[str, Any]]:
    from services.value_serializer import json_dumps_exact_numbers

    rows: list[dict[str, Any]] = []
    offset: Any = None
    while True:
        body: dict[str, Any] = {
            "limit": 256,
            "with_payload": True,
            "with_vector": True,
        }
        if offset is not None:
            body["offset"] = offset
        resp = session.post(
            f"{base_url}/collections/{collection}/points/scroll",
            data=json_dumps_exact_numbers(body),
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            raise VectorReindexError(
                f"Qdrant reindex scroll failed: {resp.status_code} {resp.text[:300]}"
            )
        result = (resp.json() or {}).get("result") or {}
        points = result.get("points") or []
        for point in points:
            payload = point.get("payload")
            payload = payload if isinstance(payload, dict) else {}
            rows.append(
                {
                    "id": point.get("id"),
                    "source_id": payload.get("source_id"),
                    "content": payload.get("content"),
                    "chunk_index": payload.get("chunk_index", 0),
                    "payload": payload,
                    "vector": point.get("vector"),
                }
            )
        next_offset = result.get("next_page_offset")
        if not points or next_offset is None:
            break
        offset = next_offset
    return rows


def _qdrant_move(
    session: Any,
    base_url: str,
    headers: dict[str, str],
    collection: str,
    moves: list[tuple[dict[str, Any], Any]],
    batch_size: int,
) -> int:
    from services.value_serializer import json_dumps_exact_numbers

    moved = 0
    for offset in range(0, len(moves), batch_size):
        batch = moves[offset : offset + batch_size]
        points = [
            {
                "id": target,
                "vector": row.get("vector"),
                "payload": row.get("payload") or {},
            }
            for row, target in batch
        ]
        resp = session.put(
            f"{base_url}/collections/{collection}/points?wait=true",
            data=json_dumps_exact_numbers({"points": points}),
            headers=headers,
            timeout=30,
        )
        if resp.status_code not in {200, 201}:
            raise VectorReindexError(
                f"Qdrant reindex upsert failed: {resp.status_code} {resp.text[:300]}"
            )
        old_ids = [row["id"] for row, _target in batch]
        deleted = session.post(
            f"{base_url}/collections/{collection}/points/delete?wait=true",
            data=json_dumps_exact_numbers({"points": old_ids}),
            headers=headers,
            timeout=30,
        )
        if deleted.status_code not in {200, 201}:
            raise VectorReindexError(
                f"Qdrant reindex old-ID delete failed after upsert: "
                f"{deleted.status_code} {deleted.text[:300]}"
            )
        moved += len(batch)
    return moved


def _record_report(report: VectorReindexReport, db_type: str, collection: str, actor: str) -> None:
    append_audit_event(
        action="vector.reindex",
        resource=f"{db_type}:{collection}",
        actor=actor,
        details=asdict(report),
    )


def reindex_to_stable_ids(
    db_type: str,
    cfg: dict[str, Any],
    collection: str,
    *,
    schema: str | None = None,
    dry_run: bool = True,
    actor: str = "system",
    batch_size: int = 256,
) -> VectorReindexReport:
    """Migrate vector point IDs to the writer's stable source/chunk identity."""
    kind = str(db_type or "").strip().lower()
    if kind not in {"pgvector", "qdrant"}:
        raise UnsupportedVectorReindexError(
            f"Stable-ID reindex is unsupported for vector destination {kind!r}"
        )
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")
    table = str(collection or "").strip()
    if not table:
        raise ValueError("collection/table name is required")
    logger.info(
        "Starting vector stable-ID reindex for %s:%s (dry_run=%s)",
        kind,
        table,
        dry_run,
    )
    session: Any = None
    try:
        if kind == "pgvector":
            schema_name = str(schema or cfg.get("schema") or "public")
            rows = _pgvector_rows(cfg, schema_name, table)
            columns = _pgvector_columns(cfg, schema_name, table)
            moves, already_stable, skipped, collisions = _plan(rows, qdrant=False)
            moved = (
                len(moves)
                if dry_run
                else _pgvector_move(
                    cfg, schema_name, table, columns, moves, batch_size
                )
            )
        else:
            from connectors.qdrant_writer import qdrant_rest

            session, base_url, headers = qdrant_rest(cfg)
            exists = session.get(
                f"{base_url}/collections/{table}", headers=headers, timeout=10
            )
            if exists.status_code == 404:
                rows = []
            elif exists.status_code != 200:
                raise VectorReindexError(
                    f"Qdrant collection probe failed: "
                    f"{exists.status_code} {exists.text[:300]}"
                )
            else:
                rows = _qdrant_rows(session, base_url, headers, table)
            moves, already_stable, skipped, collisions = _plan(rows, qdrant=True)
            moved = (
                len(moves)
                if dry_run
                else _qdrant_move(
                    session, base_url, headers, table, moves, batch_size
                )
            )
        report = VectorReindexReport(
            scanned=len(rows),
            already_stable=already_stable,
            moved=moved,
            skipped_no_source_id=skipped,
            collisions=collisions,
            dry_run=bool(dry_run),
        )
        _record_report(report, kind, table, actor)
        logger.info(
            "Finished vector stable-ID reindex for %s:%s "
            "(scanned=%d moved=%d collisions=%d dry_run=%s)",
            kind,
            table,
            report.scanned,
            report.moved,
            report.collisions,
            report.dry_run,
        )
        return report
    except VectorReindexError:
        raise
    except Exception as exc:
        raise VectorReindexError(
            f"Stable-ID reindex failed for {kind}:{table}: {exc}"
        ) from exc
    finally:
        if session is not None:
            session.close()
