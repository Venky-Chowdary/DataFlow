"""Owner decision 1 (QA D209): a new pgvector table is a typed table by default.

QA on 557f556c: a pgvector destination with no layout declared promised the
source's typed columns and built a vector-document table (every column only
inside ``metadata``). Round 6 made the plan honest but kept the document
layout as the default behind ``DATAFLOW_PGVECTOR_TYPED_TABLE_DEFAULT`` (off),
and its typed layout had no embedding at all.

The owner's decision: with no layout given, the job builds every source column
with its real type plus ``embedding vector(n)`` next to them, the plan and
preflight show that exact table, Gate-8 proves every source column, and the
vector-document layout is opt-in (``vector_layout=document``). An existing
document table keeps its shape on run 2.

Every test drives ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job
into a real PostgreSQL with the pgvector extension, from PostgreSQL and from
MariaDB, run 1 and run 2, with NULL / '' / whitespace / unicode cells.
"""

from __future__ import annotations

import json

import pytest

from tests.test_d209_pgvector_destination_contract_e2e import (  # noqa: F401
    _assert_document_cells,
    _assert_gate8_proved_every_value,
    _assert_typed_cells,
    _canon_type,
    _connector,
    _physical,
    _run,
    _source_rows,
    _sql,
    client,
    tables,
)

DIM = 32  # DATAFLOW_EMBEDDING_MODEL=hash/32 in the client fixture


_PG_ALIASES = {"int4": "integer", "int8": "bigint", "int2": "smallint", "varchar": "character varying"}


def _canon(declared: str) -> str:
    t = _canon_type(declared).replace("decimal(", "numeric(")
    head, sep, tail = t.partition("(")
    return _PG_ALIASES.get(head, head) + sep + tail


def _assert_plan_is_the_typed_table(plan: dict, table: str, *, embedding: bool = True) -> dict:
    contract = plan.get("destination_contract") or {}
    assert contract.get("layout") == "table", contract
    promised = [(c["name"], _canon(c["type"])) for c in contract.get("columns") or []]
    assert promised == _physical(table), {"promised": promised, "created": _physical(table)}
    names = [n for n, _t in promised]
    if embedding:
        assert ("embedding", f"vector({DIM})") in promised, promised
        assert names[-1] == "embedding"
    pre = plan.get("preflight") or {}
    assert pre.get("destination_contract") == contract
    warning = next(
        w for w in pre.get("warnings") or [] if w.get("id") == "pgvector_destination_layout"
    )
    assert "typed table" in warning["message"]
    if embedding:
        assert f"embedding vector({DIM})" in warning["message"], warning
    return contract


def _embedding_of(text: str) -> list[float]:
    from services.vectorization import embed

    return [float(x) for x in embed([text], model="hash/32")[0]]


def _assert_embeddings_follow_the_rule(dst: str, cols: tuple[str, ...]) -> None:
    """One rule for every row: the mapped row as canonical JSON."""
    rows = _sql(f"SELECT {', '.join(cols)}, embedding::text FROM {dst} ORDER BY id")
    for row in rows:
        vec = row[-1]
        assert vec is not None, row
        values = json.loads(vec)
        assert len(values) == DIM
    # Two rows with different cells have different vectors (not a constant).
    assert len({r[-1] for r in rows}) == len(rows)


def test_pg_into_pgvector_without_layout_is_the_typed_table_with_embedding(client, tables):
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination")
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest, dest_table=dst)

    plan, job = _run(client, sync_mode="full_refresh_overwrite", **args)
    contract = _assert_plan_is_the_typed_table(plan, dst)
    assert contract["layout_reason"] == "typed_default"
    assert contract["embedded_text_rule"]["kind"] == "row_json"
    assert _physical(dst)[:5] == [
        ("id", "bigint"), ("name", "text"), ("long_txt", "text"),
        ("payload", "jsonb"), ("note", "character varying(40)"),
    ]
    assert _physical(dst)[5][0] == "rev"
    _assert_typed_cells(src, dst)  # NULL is NULL, '' is '', '  ' kept, unicode exact
    _assert_embeddings_follow_the_rule(dst, ("id",))
    recon = job.get("reconciliation") or {}
    assert recon.get("passed") is True and recon.get("checksum_match") is True, recon

    before = dict(_sql(f"SELECT id, embedding::text FROM {dst}"))
    # Run 2 into the table run 1 created: NULL / '' / unicode changes.
    _sql(f"UPDATE {src} SET note = NULL, name = 'ñ2', rev = 2 WHERE id = 2")
    _sql(f"UPDATE {src} SET note = '', long_txt = NULL, rev = 2 WHERE id = 1")
    _sql(f"INSERT INTO {src} VALUES (5, 'ⓕive', NULL, '[1, \"2\"]', NULL, 2)")
    plan2, job2 = _run(
        client, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev",
        cursor_semantics="monotonic_sequence", **args,
    )
    contract2 = _assert_plan_is_the_typed_table(plan2, dst)
    assert contract2["layout_reason"] == "existing_typed_table"
    _assert_typed_cells(src, dst)
    _assert_embeddings_follow_the_rule(dst, ("id",))
    after = dict(_sql(f"SELECT id, embedding::text FROM {dst}"))
    assert after[2] != before[2] and after[1] != before[1]  # changed rows re-embedded
    assert after[3] == before[3]
    assert (job2.get("reconciliation") or {}).get("passed") is True


def test_content_column_rule_embeds_that_column_and_null_gives_no_vector(client, tables):
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination", {"content_column": "long_txt"})
    plan, _job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="full_refresh_overwrite",
    )
    contract = _assert_plan_is_the_typed_table(plan, dst)
    assert contract["embedded_text_rule"] == {
        "kind": "column", "column": "long_txt",
        "null": "SQL NULL or '' → empty content, no embedding",
    }
    _assert_typed_cells(src, dst)
    rows = dict(_sql(f"SELECT id, embedding::text FROM {dst}"))
    src_text = dict(_sql(f"SELECT id, long_txt FROM {src}"))
    for key, text in src_text.items():
        if not text:
            assert rows[key] is None, (key, text)  # NULL and '' embed nothing
        else:
            got = [float(x) for x in json.loads(rows[key])]
            assert got == pytest.approx(_embedding_of(text), rel=1e-5, abs=1e-6)


def test_document_layout_is_opt_in_and_an_existing_document_table_keeps_its_shape(client, tables):
    src, dst = tables["src"], tables["dst"]
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination")
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest, dest_table=dst)

    # Run 1: the run itself opts into the document layout.
    plan, job = _run(client, sync_mode="full_refresh_overwrite", vector_layout="document", **args)
    contract = plan["destination_contract"]
    assert (contract["layout"], contract["layout_reason"]) == ("document", "declared")
    assert [c["name"] for c in contract["columns"]][:4] == ["id", "content", "embedding", "metadata"]
    _assert_document_cells(src, dst, rule_column=None)
    _assert_gate8_proved_every_value(job, 4)
    shape = _physical(dst)

    # Run 2 with no layout given: the document table DataFlow created stays one.
    _sql(f"UPDATE {src} SET name = NULL, rev = 2 WHERE id = 4")
    plan2, job2 = _run(
        client, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev",
        cursor_semantics="monotonic_sequence", **args,
    )
    contract2 = plan2["destination_contract"]
    assert (contract2["layout"], contract2["layout_reason"]) == ("document", "existing_document_table")
    assert _physical(dst) == shape
    _assert_document_cells(src, dst, rule_column=None)
    _assert_gate8_proved_every_value(job2, 4)


def test_existing_typed_table_without_embedding_keeps_its_shape(client, tables):
    """A typed table made before the embedding column existed is not altered."""
    src, dst = tables["src"], tables["dst"]
    _sql(
        f"CREATE TABLE {dst} (id BIGINT PRIMARY KEY, name TEXT, long_txt TEXT, "
        "payload JSONB, note VARCHAR(40), rev INTEGER NOT NULL DEFAULT 1)"
    )
    shape = _physical(dst)
    source = _connector("postgresql", "source")
    dest = _connector("pgvector", "destination")
    plan, job = _run(
        client, source_connector_id=source, source_table=src, dest_connector_id=dest,
        dest_table=dst, sync_mode="incremental_upsert", primary_key="id", cursor_column="rev",
        cursor_semantics="monotonic_sequence",
    )
    contract = _assert_plan_is_the_typed_table(plan, dst, embedding=False)
    assert contract["layout_reason"] == "existing_typed_table"
    assert contract["embedding_columns"] == []
    assert _physical(dst) == shape
    _assert_typed_cells(src, dst)
    assert (job.get("reconciliation") or {}).get("passed") is True


def test_csv_file_into_pgvector_without_layout_is_typed_with_embedding(client, tables):
    """A file source takes the same default: typed columns plus the embedding."""
    import uuid as _uuid

    from tests.test_d209_pgvector_destination_contract_e2e import PG
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dst = tables["dst"]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="pgvector", host=PG["host"], port=PG["port"],
            database=PG["dbname"], username=PG["user"], password=PG["password"],
            schema="public", table=dst,
        ),
        source_filename="notes.csv",
        source_content="id,note\n1,Zoë ✓\n2,\n3,plain\n".encode("utf-8"),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    from services.mongodb_service import get_mongodb_service

    job_id = _uuid.uuid4().hex[:24]
    get_mongodb_service().update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    names = [n for n, _t in _physical(dst)]
    assert names[:2] == ["id", "note"] and names[-1] == "embedding", _physical(dst)
    assert ("embedding", f"vector({DIM})") in _physical(dst)
    rows = _sql(f"SELECT id::text, note, embedding IS NOT NULL FROM {dst} ORDER BY id")
    assert [r[2] for r in rows] == [True, True, True], rows
    assert rows[0][1] == "Zoë ✓"
