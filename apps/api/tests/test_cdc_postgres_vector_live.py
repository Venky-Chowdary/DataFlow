from __future__ import annotations

import socket
import uuid

import psycopg2
import pytest

from connectors.postgresql_conn import get_connection
from src.transfer.cdc_transfer import run_cdc_database_transfer
from src.transfer.models import EndpointConfig

PG_SOURCE = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}
PGVECTOR_PORT = 5434
QDRANT_PORT = 6335


def _ready(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _ready(5432), reason="logical PostgreSQL source not reachable on localhost:5432"
)


def _source_exec(statement: str, params: tuple = ()) -> None:
    with get_connection(**PG_SOURCE) as connection, connection.cursor() as cursor:
        cursor.execute(statement, params)
        connection.commit()


def _source_fetch(statement: str, params: tuple = ()) -> list[tuple]:
    with get_connection(**PG_SOURCE) as connection, connection.cursor() as cursor:
        cursor.execute(statement, params)
        return list(cursor.fetchall())


def _destination(kind: str, table: str) -> EndpointConfig:
    if kind == "pgvector":
        cfg = {
            "host": "localhost",
            "port": PGVECTOR_PORT,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
            "connection_string": "",
            "ssl": False,
        }
    else:
        cfg = {
            "host": "localhost",
            "port": QDRANT_PORT,
            "database": "",
            "username": "",
            "password": "",
            "schema": "",
            "connection_string": "",
            "ssl": False,
        }
    return EndpointConfig(
        kind="database",
        format=kind,
        table=table,
        **cfg,
        extra={
            "embedding_model": "hash/32",
            "text_template": "{title}\n{body}",
            "metadata_columns": ["category"],
            "chunk_size": 96,
            "chunk_overlap": 10,
            "vector_skip_unchanged": True,
        },
    )


def _count_and_chunks(kind: str, table: str, source_id: str) -> set[int]:
    if kind == "pgvector":
        connection = psycopg2.connect(
            host="localhost",
            port=PGVECTOR_PORT,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT chunk_index FROM public."{table}" WHERE source_id = %s',
                    (source_id,),
                )
                return {int(row[0]) for row in cursor.fetchall()}
        finally:
            connection.close()
    import requests

    response = requests.post(
        f"http://localhost:{QDRANT_PORT}/collections/{table}/points/scroll",
        json={
            "filter": {
                "must": [{"key": "source_id", "match": {"value": source_id}}]
            },
            "limit": 1000,
            "with_payload": ["chunk_index"],
            "with_vector": False,
        },
        timeout=30,
    )
    assert response.status_code == 200, response.text
    points = response.json()["result"]["points"]
    return {int(point["payload"]["chunk_index"]) for point in points}


def _category(kind: str, table: str, source_id: str) -> str:
    if kind == "pgvector":
        connection = psycopg2.connect(
            host="localhost",
            port=PGVECTOR_PORT,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT metadata->>\'category\' FROM public."{table}" '
                    "WHERE source_id = %s LIMIT 1",
                    (source_id,),
                )
                return str(cursor.fetchone()[0])
        finally:
            connection.close()
    import requests

    response = requests.post(
        f"http://localhost:{QDRANT_PORT}/collections/{table}/points/scroll",
        json={
            "filter": {
                "must": [{"key": "source_id", "match": {"value": source_id}}]
            },
            "limit": 1,
            "with_payload": ["category"],
            "with_vector": False,
        },
        timeout=30,
    )
    assert response.status_code == 200, response.text
    return str(response.json()["result"]["points"][0]["payload"]["category"])


def _contents(kind: str, table: str, source_id: str) -> list[str]:
    if kind == "pgvector":
        connection = psycopg2.connect(
            host="localhost",
            port=PGVECTOR_PORT,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f'SELECT content FROM public."{table}" '
                    "WHERE source_id = %s ORDER BY chunk_index",
                    (source_id,),
                )
                return [str(row[0]) for row in cursor.fetchall()]
        finally:
            connection.close()
    import requests

    response = requests.post(
        f"http://localhost:{QDRANT_PORT}/collections/{table}/points/scroll",
        json={
            "filter": {
                "must": [{"key": "source_id", "match": {"value": source_id}}]
            },
            "limit": 1000,
            "with_payload": ["content", "chunk_index"],
            "with_vector": False,
        },
        timeout=30,
    )
    assert response.status_code == 200, response.text
    points = response.json()["result"]["points"]
    points.sort(key=lambda point: point["payload"]["chunk_index"])
    return [str(point["payload"]["content"]) for point in points]


def _usage_in(value):
    if isinstance(value, dict):
        if isinstance(value.get("embedding_usage"), dict):
            return value["embedding_usage"]
        for item in value.values():
            found = _usage_in(item)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for item in value:
            found = _usage_in(item)
            if found is not None:
                return found
    return None


def _metrics_in(value, name: str) -> list[int]:
    found: list[int] = []
    if isinstance(value, dict):
        if isinstance(value.get(name), (int, float)):
            found.append(int(value[name]))
        for item in value.values():
            found.extend(_metrics_in(item, name))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_metrics_in(item, name))
    return found


@pytest.mark.parametrize(
    ("kind", "port"),
    [
        pytest.param(
            "pgvector",
            PGVECTOR_PORT,
            marks=pytest.mark.skipif(
                not _ready(PGVECTOR_PORT),
                reason="pgvector not reachable on localhost:5434",
            ),
        ),
        pytest.param(
            "qdrant",
            QDRANT_PORT,
            marks=pytest.mark.skipif(
                not _ready(QDRANT_PORT),
                reason="Qdrant not reachable on localhost:6335",
            ),
        ),
    ],
)
def test_postgres_logical_cdc_to_vector_is_idempotent_and_converges(
    kind: str, port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    import src.transfer.cdc_transfer as cdc_transfer

    source_table = "cdc_vec_" + uuid.uuid4().hex[:8]
    dest_table = "cdc_vec_" + uuid.uuid4().hex[:8]
    job_id = "cdc-vector-" + uuid.uuid4().hex[:8]
    slot_name = ""
    cursor_key = ""
    long_text = "A long replicated paragraph for vector chunks. " * 90
    rows = [
        (1, "First", "small body", "initial", "untouched"),
        (2, "Long", long_text, "initial", "untouched"),
        (3, "Third", "third body", "initial", "untouched"),
    ]
    mappings = [
        {
            "source": name,
            "target": name,
            "source_type": type_name,
            "target_type": type_name,
        }
        for name, type_name in (
            ("id", "INTEGER"),
            ("title", "TEXT"),
            ("body", "TEXT"),
            ("category", "TEXT"),
            ("notes", "TEXT"),
        )
    ]
    schema = {
        "id": "INTEGER",
        "title": "TEXT",
        "body": "TEXT",
        "category": "TEXT",
        "notes": "TEXT",
    }
    source = EndpointConfig(
        kind="database",
        format="postgresql",
        table=source_table,
        schema="public",
        **PG_SOURCE,
    )
    destination = _destination(kind, dest_table)
    stream = [
        {
            "name": source_table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
        }
    ]
    real_set_watermark = cdc_transfer.set_watermark
    checkpoint_keys: list[str] = []

    def capture_watermark(key, token, *args, **kwargs):
        checkpoint_keys.append(str(key))
        return real_set_watermark(key, token, *args, **kwargs)

    monkeypatch.setattr(cdc_transfer, "set_watermark", capture_watermark)
    captured_summaries: list[dict] = []
    real_apply = cdc_transfer._apply_change_batch

    def capture_apply(*args, **kwargs):
        result = real_apply(*args, **kwargs)
        captured_summaries.append(result[2])
        return result

    monkeypatch.setattr(cdc_transfer, "_apply_change_batch", capture_apply)

    def run():
        result = run_cdc_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=100,
            delivery_guarantee="at_least_once",
        )
        nonlocal slot_name
        slot_name = str(
            (result[2].get("cdc") or {}).get("cdc_slot_name") or slot_name
        )
        return result

    def source_id(value: int) -> str:
        return str(value)

    def assert_final(expected: dict[int, set[int]]) -> None:
        for doc_id, chunks in expected.items():
            assert _count_and_chunks(kind, dest_table, source_id(doc_id)) == chunks
        assert _count_and_chunks(kind, dest_table, source_id(2)) == set()

    try:
        _source_exec(
            f'CREATE TABLE public."{source_table}" ('
            "id INT PRIMARY KEY, title TEXT, body TEXT, category TEXT, notes TEXT)"
        )
        with get_connection(**PG_SOURCE) as connection, connection.cursor() as cursor:
            from psycopg2 import sql

            insert_sql = sql.SQL(
                "INSERT INTO {}.{} (id,title,body,category,notes) "
                "VALUES (%s,%s,%s,%s,%s)"
            ).format(sql.Identifier("public"), sql.Identifier(source_table))
            cursor.executemany(insert_sql, rows)
            connection.commit()
        _source_exec(
            f'ALTER TABLE public."{source_table}" REPLICA IDENTITY FULL'
        )
        run()
        from services.vectorization import chunk_text

        initial_long_chunks = set(
            range(len(chunk_text(f"Long\n{long_text}", 96, 10)))
        )
        assert len(initial_long_chunks) >= 5
        assert _count_and_chunks(kind, dest_table, source_id(2)) == initial_long_chunks
        assert _contents(kind, dest_table, source_id(3)) == ["Third\nthird body"]
        assert _category(kind, dest_table, source_id(3)) == "initial"

        inserted_long = "An inserted document that is intentionally long. " * 55
        _source_exec(
            f'INSERT INTO public."{source_table}" '
            "(id,title,body,category,notes) VALUES (%s,%s,%s,%s,%s)",
            (4, "Fourth", inserted_long, "initial", "untouched"),
        )
        run()

        _source_exec(
            f'UPDATE public."{source_table}" SET body=%s WHERE id=%s',
            ("short replacement", 2),
        )
        run()
        assert _count_and_chunks(kind, dest_table, source_id(2)) == {0}

        _source_exec(
            f'UPDATE public."{source_table}" SET notes=%s WHERE id=%s',
            ("not vector metadata", 3),
        )
        captured_summaries.clear()
        run()
        note_usage = _usage_in(captured_summaries)
        assert note_usage is not None
        assert (
            sum(_metrics_in(captured_summaries, "vector_docs_unchanged_skipped"))
            >= 1
        )
        assert int(note_usage["calls"]) == 0, captured_summaries
        assert _contents(kind, dest_table, source_id(3)) == ["Third\nthird body"]
        assert _category(kind, dest_table, source_id(3)) == "initial"

        _source_exec(
            f'UPDATE public."{source_table}" SET category=%s WHERE id=%s',
            ("updated metadata", 3),
        )
        captured_summaries.clear()
        run()
        assert _category(kind, dest_table, source_id(3)) == "updated metadata"
        assert sum(_metrics_in(captured_summaries, "vector_docs_embedded")) >= 1

        _source_exec(f'DELETE FROM public."{source_table}" WHERE id=%s', (2,))
        run()

        _source_exec(
            f'UPDATE public."{source_table}" SET body=%s WHERE id=%s',
            ("short inserted replacement", 4),
        )
        if kind == "qdrant":
            from services import vector_sync

            actual_cleanup = vector_sync.qdrant_delete_stale_chunks
            failed = False

            def fail_once(*args, **kwargs):
                nonlocal failed
                if not failed:
                    failed = True
                    raise RuntimeError("injected one-time stale cleanup failure")
                return actual_cleanup(*args, **kwargs)

            from services.sync_cursor import get_watermark

            cursor_key = checkpoint_keys[-1]
            watermark_before_cleanup_failure = get_watermark(cursor_key)
            monkeypatch.setattr(vector_sync, "qdrant_delete_stale_chunks", fail_once)
            with pytest.raises(Exception):
                run()
            assert failed
            assert get_watermark(cursor_key) == watermark_before_cleanup_failure
            monkeypatch.setattr(
                vector_sync, "qdrant_delete_stale_chunks", actual_cleanup
            )
            monkeypatch.setattr(cdc_transfer, "set_watermark", capture_watermark)
            run()
            assert _count_and_chunks(kind, dest_table, source_id(4)) == {0}
            assert get_watermark(cursor_key) != watermark_before_cleanup_failure
        else:
            run()
        assert _count_and_chunks(kind, dest_table, source_id(4)) == {0}

        _source_exec(
            f'UPDATE public."{source_table}" SET notes=%s WHERE id=%s',
            ("redelivery sentinel", 4),
        )
        from services.sync_cursor import get_watermark

        cursor_key = checkpoint_keys[-1]
        watermark_before_retry = get_watermark(cursor_key)
        # Learn the cursor key from the runner's own checkpoint writes.
        key_holder: list[str] = []

        def fail_checkpoint_once(key, token, *args, **kwargs):
            key_holder.append(str(key))
            if str(key) == cursor_key:
                raise RuntimeError("injected failure before CDC checkpoint")
            return real_set_watermark(key, token, *args, **kwargs)

        monkeypatch.setattr(cdc_transfer, "set_watermark", fail_checkpoint_once)
        with pytest.raises(RuntimeError, match="injected failure"):
            run()
        assert key_holder[-1] == cursor_key
        assert get_watermark(cursor_key) == watermark_before_retry

        captured_summaries.clear()
        monkeypatch.setattr(cdc_transfer, "set_watermark", capture_watermark)
        run()
        replay_usage = _usage_in(captured_summaries)
        assert replay_usage is not None
        assert int(replay_usage["calls"]) == 0
        assert sum(_metrics_in(captured_summaries, "vector_docs_unchanged_skipped")) >= 1
        assert get_watermark(cursor_key) != watermark_before_retry

        assert_final({1: {0}, 3: {0}, 4: {0}})
    finally:
        monkeypatch.setattr(cdc_transfer, "set_watermark", real_set_watermark)
        try:
            if slot_name:
                _source_exec(
                    "SELECT pg_drop_replication_slot(%s) "
                    "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=%s)",
                    (slot_name, slot_name),
                )
        except psycopg2.Error:
            pass
        _source_exec(f'DROP TABLE IF EXISTS public."{source_table}"')
        if kind == "pgvector":
            with psycopg2.connect(
                host="localhost",
                port=PGVECTOR_PORT,
                dbname="dataflow",
                user="dataflow",
                password="dataflow",
            ) as connection, connection.cursor() as cursor:
                from psycopg2 import sql

                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS public.{}").format(
                        sql.Identifier(dest_table)
                    )
                )
        else:
            import requests

            requests.delete(
                f"http://localhost:{QDRANT_PORT}/collections/{dest_table}",
                timeout=10,
            )


@pytest.mark.parametrize(
    ("kind", "port"),
    [
        pytest.param(
            "pgvector",
            PGVECTOR_PORT,
            marks=pytest.mark.skipif(
                not _ready(PGVECTOR_PORT),
                reason="pgvector not reachable on localhost:5434",
            ),
        ),
        pytest.param(
            "qdrant",
            QDRANT_PORT,
            marks=pytest.mark.skipif(
                not _ready(QDRANT_PORT),
                reason="Qdrant not reachable on localhost:6335",
            ),
        ),
    ],
)
def test_incremental_cursor_writes_insert_and_updated_vector(
    kind: str, port: int
) -> None:
    from src.transfer.stream import stream_database_transfer

    source_table = "inc_vec_" + uuid.uuid4().hex[:8]
    dest_table = "inc_vec_" + uuid.uuid4().hex[:8]
    job_id = "incremental-vector-" + uuid.uuid4().hex[:8]
    source = EndpointConfig(
        kind="database",
        format="postgresql",
        table=source_table,
        schema="public",
        **PG_SOURCE,
    )
    destination = _destination(kind, dest_table)
    mappings = [
        {
            "source": name,
            "target": name,
            "source_type": type_name,
            "target_type": type_name,
        }
        for name, type_name in (
            ("id", "INTEGER"),
            ("title", "TEXT"),
            ("body", "TEXT"),
            ("updated_at", "TIMESTAMP WITH TIME ZONE"),
        )
    ]
    schema = {
        "id": "INTEGER",
        "title": "TEXT",
        "body": "TEXT",
        "updated_at": "TIMESTAMP WITH TIME ZONE",
    }
    contract = [
        {
            "name": source_table,
            "selected": True,
            "sync_mode": "incremental_upsert",
            "cursor_field": "updated_at",
            "primary_key": "id",
        }
    ]
    try:
        _source_exec(
            f'CREATE TABLE public."{source_table}" ('
            "id INT PRIMARY KEY, title TEXT, body TEXT, "
            "updated_at TIMESTAMPTZ NOT NULL)"
        )
        _source_exec(
            f'INSERT INTO public."{source_table}" '
            "(id,title,body,updated_at) VALUES (%s,%s,%s,NOW())",
            (1, "First", "original incremental body"),
        )
        first = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="incremental_upsert",
            stream_contracts=contract,
            job_id=job_id,
            skip_preflight=True,
        )
        assert first[0] == 1

        _source_exec(
            f'INSERT INTO public."{source_table}" '
            "(id,title,body,updated_at) VALUES (%s,%s,%s,NOW()+INTERVAL '3 seconds')",
            (2, "Inserted", "new incremental document"),
        )
        _source_exec(
            f'UPDATE public."{source_table}" '
            "SET body=%s, updated_at=NOW()+INTERVAL '3 seconds' WHERE id=%s",
            ("updated incremental body", 1),
        )
        second = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="incremental_upsert",
            stream_contracts=contract,
            job_id=job_id,
            skip_preflight=True,
        )
        assert second[0] == 2
        assert any("updated incremental body" in text for text in _contents(kind, dest_table, "1"))
        assert any("new incremental document" in text for text in _contents(kind, dest_table, "2"))
    finally:
        _source_exec(f'DROP TABLE IF EXISTS public."{source_table}"')
        if kind == "pgvector":
            with psycopg2.connect(
                host="localhost",
                port=PGVECTOR_PORT,
                dbname="dataflow",
                user="dataflow",
                password="dataflow",
            ) as connection, connection.cursor() as cursor:
                from psycopg2 import sql

                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS public.{}").format(
                        sql.Identifier(dest_table)
                    )
                )
        else:
            import requests

            requests.delete(
                f"http://localhost:{QDRANT_PORT}/collections/{dest_table}",
                timeout=10,
            )
