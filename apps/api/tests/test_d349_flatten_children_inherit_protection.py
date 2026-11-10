"""D349 - protecting a flattened document field protects every column derived from it.

QA round 37 (prod afd88cd3): MongoDB ``r37_m`` -> new PostgreSQL table with
``column_transforms {payload: redact}``. The reader flattens ``payload: {k,
nested: {a: [...]}, s}`` into ``payload_k`` / ``payload_nested`` /
``payload_nested_a`` / ``payload_s``; only ``payload`` was redacted, the four
children landed in clear and preflight was green.

Unit tests pin the lineage and the inheritance rule. Live tests drive the QA
route (``start_transfer`` -> confirm -> job) and the engine route (Studio /
REST mappings, schedule replay, a field that appears in run 2) from MongoDB on
127.0.0.1:27017 into PostgreSQL, and read every ``payload_*`` column back.
"""

from __future__ import annotations

import json
import socket
import uuid
from types import SimpleNamespace

import pytest

from services.flatten_protection import (
    flatten_lineage,
    flatten_preflight_warnings,
    protect_discovered_children,
    protect_flatten_children,
    protect_planned_children,
    protect_run_children,
)
from services.json_intelligence import flatten_document
from tests.test_d190_d194_d197_declared_string_types_e2e import (
    PG,
    _drop,
    _mongo_source,
    _pg,
    _pg_column_types,
    _pg_connector,
    _pg_rows,
    _run,
    transfer_client,  # noqa: F401 - fixture
)

_DOC = {
    "_id": 1,
    "qty": 60,
    "payload": {"k": 6, "nested": {"a": [1, 2, 6], "deep": {"x": 1}}, "s": 'q"uote'},
    # A real field that shares the prefix: never a child.
    "payload_id": "real",
}
_CHILDREN = {"payload_k", "payload_nested", "payload_nested_a", "payload_s"}


def _expanded_row(doc=_DOC) -> dict:
    """A row as the Mongo reader emits it: objects/arrays as JSON text."""
    flat = flatten_document(doc)
    return {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in flat.items()}


def _mappings(parent_tf: str = "redact", **child_tf) -> list[dict]:
    types = {
        "_id": "BIGINT", "qty": "BIGINT", "payload": "JSONB", "payload_id": "TEXT",
        "payload_k": "BIGINT", "payload_nested": "JSONB", "payload_nested_a": "JSONB",
        "payload_s": "TEXT",
    }
    out = []
    for col, typ in types.items():
        tf = parent_tf if col == "payload" else child_tf.get(col, "none")
        out.append({"source": col, "target": col, "transform": tf,
                    "source_type": typ, "target_type": typ})
    return out


def _by_source(rows) -> dict:
    return {m["source"]: m for m in rows}


# --------------------------------------------------------------------- lineage


def test_flattener_records_which_column_each_child_came_from():
    lineage: dict[str, str] = {}
    flat = flatten_document(_DOC, lineage=lineage)
    assert lineage == {
        "payload_k": "payload",
        "payload_nested": "payload",
        "payload_nested_a": "payload_nested",
        "payload_s": "payload",
    }
    # Beyond the depth limit stays on the ancestor blob: no column, no lineage.
    assert "payload_nested_deep" not in flat and "payload_nested_deep" not in lineage
    assert "payload_id" not in lineage


def test_lineage_from_the_readers_expanded_rows_matches_the_raw_documents():
    assert flatten_lineage([_expanded_row()]) == flatten_lineage([_DOC])
    assert set(flatten_lineage([_expanded_row()])) == _CHILDREN


# ----------------------------------------------------------------- inheritance


def test_redact_parent_redacts_every_child_and_types_them_to_hold_the_token():
    result = protect_flatten_children(_mappings("redact"), flatten_lineage([_expanded_row()]))
    assert not result.refusal
    rows = _by_source(result.mappings)
    assert rows["payload"]["transform"] == "redact"  # the parent is unchanged
    for col in _CHILDREN:
        assert rows[col]["transform"] == "redact", col
        assert rows[col]["protected_by"] == "payload"
        assert rows[col]["transform_override"] is True
    # A BIGINT cannot hold "<redacted>": the child is planned as text. A JSON
    # child keeps its carrier and holds the token as a JSON string, like the parent.
    assert rows["payload_k"]["target_type"] == "TEXT"
    assert rows["payload_nested"]["target_type"] == "JSONB"
    assert {i["column"] for i in result.inherited} == _CHILDREN
    # An unrelated real column sharing the prefix is not touched.
    assert rows["payload_id"] == _by_source(_mappings())["payload_id"]


@pytest.mark.parametrize(
    "parent_tf,scalar,structured",
    [
        ("omit", "omit", "omit"),
        ("hash_pii", "hash_pii", "hash_pii"),
        # mask_pii masks a JSON value only where a PII pattern matches, so an
        # object / array child would keep its content: it is redacted instead.
        ("mask_pii", "mask_pii", "redact"),
        ("sha256", "hash_pii", "hash_pii"),  # an alias resolves to its engine id
    ],
)
def test_omit_hash_and_mask_parents_cover_their_children(parent_tf, scalar, structured):
    result = protect_flatten_children(_mappings(parent_tf), flatten_lineage([_expanded_row()]))
    rows = _by_source(result.mappings)
    assert rows["payload_k"]["transform"] == scalar
    assert rows["payload_s"]["transform"] == scalar
    assert rows["payload_nested"]["transform"] == structured
    assert rows["payload_nested_a"]["transform"] == structured
    assert rows["payload_id"]["transform"] == "none"


def test_a_protected_parent_with_no_flatten_children_is_unchanged():
    maps = [{"source": "email", "target": "email", "transform": "redact"},
            {"source": "email_domain", "target": "email_domain", "transform": "none"}]
    rows = [{"email": "a@example.com", "email_domain": "example.com"}]
    result = protect_flatten_children(maps, flatten_lineage(rows))
    assert result.mappings == maps and not result.inherited and not result.refusal


def test_children_of_an_unprotected_parent_map_as_today():
    maps = _mappings("none")
    result = protect_flatten_children(maps, flatten_lineage([_expanded_row()]))
    assert result.mappings == maps and not result.inherited


def test_a_child_the_operator_protected_differently_keeps_its_own_protection():
    maps = _mappings("redact", payload_s="hash_pii")
    rows = _by_source(protect_flatten_children(
        maps, flatten_lineage([_expanded_row()]), explicit={"payload_s": "hash_pii"}
    ).mappings)
    assert rows["payload_s"]["transform"] == "hash_pii"
    assert rows["payload_k"]["transform"] == "redact"


def test_explicitly_unprotecting_a_child_of_a_protected_parent_is_refused():
    result = protect_flatten_children(
        _mappings("redact", payload_s="trim"),
        flatten_lineage([_expanded_row()]),
        explicit={"payload_s": "trim"},
    )
    assert "payload_s is derived from payload" in result.refusal
    assert "redact" in result.refusal


def test_execute_refuses_a_replayed_override_that_writes_a_child_in_clear():
    maps = _mappings("redact")
    _by_source(maps)["payload_k"].update(transform="trim", transform_override=True)
    request = SimpleNamespace(source=SimpleNamespace(format="mongodb", extra={}), shape_recipe={})
    with pytest.raises(ValueError, match="payload_k is derived from payload"):
        protect_run_children(request, maps, [_expanded_row()])


def test_execute_protects_studio_mappings_and_honours_a_plans_stamps():
    request = SimpleNamespace(source=SimpleNamespace(format="mongodb", extra={}), shape_recipe={})
    rows = _by_source(protect_run_children(request, _mappings("redact"), [_expanded_row()]))
    assert all(rows[c]["transform"] == "redact" for c in _CHILDREN)
    # A stored plan stamp covers a child the run's sample never shows.
    maps = _mappings("redact") + [{"source": "payload_rare", "target": "payload_rare",
                                   "transform": "redact", "protected_by": "payload"}]
    assert _by_source(protect_run_children(request, maps, []))["payload_rare"]["transform"] == "redact"


def test_execute_does_not_touch_a_relational_source_or_disabled_flattening():
    # A PostgreSQL table with a jsonb column and a real payload_k column: the
    # reader flattens nothing, so payload_k is an independent column.
    pg = SimpleNamespace(source=SimpleNamespace(format="postgresql", extra={}), shape_recipe={})
    maps = _mappings("redact")
    assert protect_run_children(pg, maps, [_expanded_row()]) == maps
    off = SimpleNamespace(
        source=SimpleNamespace(format="mongodb", extra={"flatten_nested": False}), shape_recipe={}
    )
    assert protect_run_children(off, maps, [_expanded_row()]) == maps


def test_a_pipeline_that_redacts_the_parent_protects_its_children():
    result = protect_planned_children(
        _mappings("none"),
        source_type="mongodb",
        source_cfg={},
        sample_rows=[],
        introspected_lineage=flatten_lineage([_DOC]),
        column_transforms={},
        column_pipelines={"payload": ["trim", "redact"]},
    )
    rows = _by_source(result.mappings)
    assert all(rows[c]["transform"] == "redact" for c in _CHILDREN)
    assert rows["payload"]["transform"] == "none"  # the pipeline redacts it on the read


def test_a_field_first_seen_mid_stream_under_a_protected_parent_inherits():
    from services.value_serializer import SQL_NULL_SENTINEL

    late = {**_DOC, "payload": {**_DOC["payload"], "late": "secret"}}
    row = _expanded_row(late)
    headers = list(row)
    cfg = SimpleNamespace(
        src_type="mongodb",
        mappings=_mappings("redact") + [
            {"source": "payload_late", "target": "payload_late", "create_new": True}
        ],
        schema={"payload_late": "VARCHAR"},
        column_types={"payload_late": "VARCHAR"},
    )
    batch = SimpleNamespace(headers=headers, rows=[[row[h] for h in headers], [SQL_NULL_SENTINEL] * len(headers)])
    protect_discovered_children(cfg, batch, ["payload_late"])
    rows = _by_source(cfg.mappings)
    assert rows["payload_late"]["transform"] == "redact"
    assert rows["payload_late"]["protected_by"] == "payload"
    assert cfg.schema["payload_late"] == "TEXT"
    assert rows["payload_id"]["transform"] == "none"


@pytest.mark.parametrize("protection", ["redact", "hash_pii", "mask_pii"])
@pytest.mark.parametrize("carrier", ["JSONB", "BIGINT", "BOOLEAN", "TIMESTAMPTZ"])
def test_the_write_resolver_never_swaps_a_protection_for_an_inferred_cast(protection, carrier):
    """Studio / REST rows carry no ``transform_override``: ``redact`` on a JSON
    column used to resolve to the ``json`` cast and write the clear document."""
    from services.transform_resolver import resolve_transform

    row = {"source": "payload", "target": "payload", "transform": protection}
    assert resolve_transform(row, column_types={"payload": carrier}) == protection


def test_preflight_names_the_inherited_children_and_counts_them_protected():
    from services.preflight_proof_bundle import pii_detection_warnings, protected_pii_columns
    from services.source_engine_scope import bind_source_engine

    with bind_source_engine("mongodb"):
        warnings = flatten_preflight_warnings(_mappings("redact"), [_expanded_row()])
        protected = protected_pii_columns(_mappings("hash_pii"), [_expanded_row()])
        compliance = {"sensitive_fields": ["payload_s"], "findings": []}
        pii = pii_detection_warnings(compliance, _mappings("hash_pii"), [_expanded_row()])
    assert [w["id"] for w in warnings] == ["flatten_children_protected"]
    for col in _CHILDREN:
        assert col in warnings[0]["message"]
    assert protected >= {"payload", "payload_k", "payload_s", "payload_nested"}
    assert pii[0]["details"]["protected_columns"] == ["payload_s"]
    assert "flatten_children_protected" in [w["id"] for w in pii]
    # An explicit un-protection is said out loud before Execute refuses it.
    clear = _mappings("redact", payload_s="trim")
    _by_source(clear)["payload_s"]["transform_override"] = True
    with bind_source_engine("mongodb"):
        refused = flatten_preflight_warnings(clear, [_expanded_row()])
    assert refused[0]["id"] == "flatten_child_in_clear"
    # A relational source is not a flattening source: nothing to say.
    with bind_source_engine("postgresql"):
        assert flatten_preflight_warnings(_mappings("redact"), [_expanded_row()]) == []


# ---------------------------------------------------------------- live routes


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _docs(n: int = 4, start: int = 1, **extra_payload) -> list[dict]:
    return [
        {
            "_id": i,
            "qty": 50 + i * 10,
            "name": f"n{i}",
            "payload": {"k": 5 + i, "nested": {"a": [1, 2, 5 + i]}, "s": 'q"uote', **extra_payload},
            "payload_id": f"real{i}",
        }
        for i in range(start, start + n)
    ]


@pytest.fixture
def mongo_coll():
    pymongo = pytest.importorskip("pymongo")
    if not _up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    coll = client["dataflow_test"]["d349_" + uuid.uuid4().hex[:8]]
    try:
        yield coll
    finally:
        coll.drop()
        client.close()


def _payload_cells(table: str) -> tuple[dict, list[dict]]:
    types = _pg_column_types(table)
    cols = ["_id"] + sorted(c for c in types if c.startswith("payload"))
    return types, [dict(zip(cols, r)) for r in _pg_rows(table, cols, "_id")]


def _assert_no_clear_payload(rows: list[dict], docs: list[dict], parent_tf: str) -> None:
    by_id = {d["_id"]: d for d in docs}
    for row in rows:
        src = by_id[row["_id"]]["payload"]
        clear = {json.dumps(src["k"]), src["s"], json.dumps(src["nested"]),
                 json.dumps(src["nested"]["a"]), str(src["k"])}
        for col, val in row.items():
            if col in {"_id", "payload_id"} or val is None:
                continue
            if col == "payload" and parent_tf == "mask_pii":
                continue  # mask_pii on the JSON parent itself: pre-existing semantics
            text = val if isinstance(val, str) else json.dumps(val)
            assert text not in clear and val not in (src["k"], src["nested"], src["nested"]["a"]), (
                col, val)
        assert row["payload_id"] == f"real{row['_id']}"  # the unrelated column is intact


@pytest.mark.parametrize("parent_tf", ["redact", "omit", "mask_pii", "hash_pii"])
def test_qa_route_mongo_to_postgres_writes_no_child_in_clear(
    transfer_client, mongo_coll, parent_tf  # noqa: F811
):
    _pg().close()
    client = transfer_client
    docs = _docs()
    mongo_coll.insert_many([dict(d) for d in docs])
    table = mongo_coll.name + "_pg"
    _drop(table)
    try:
        plan, _job = _run(
            client,
            source_connector_id=_mongo_source(mongo_coll),
            source_table=mongo_coll.name,
            dest_connector_id=_pg_connector("d349"),
            dest_table=table,
            sync_mode="full_refresh_overwrite",
            source_filter={"column": "qty", "op": "gt", "value": 50},
            column_transforms={"payload": parent_tf},
            pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d349"},
        )
        types, rows = _payload_cells(table)
        assert len(rows) == len(docs)
        if parent_tf == "omit":
            assert not any(c.startswith("payload") and c != "payload_id" for c in types), types
        else:
            assert set(types) >= _CHILDREN | {"payload"}
            _assert_no_clear_payload(rows, docs, parent_tf)
            if parent_tf == "redact":
                assert all(r["payload"] == "<redacted>" for r in rows)
                assert all(r[c] == "<redacted>" for r in rows for c in _CHILDREN)
            if parent_tf == "hash_pii":
                assert all(len(r["payload"]) >= 16 for r in rows)
        # The plan and its preflight name every inherited child.
        inherited = {i["column"]: i for i in plan.get("inherited_protection") or []}
        assert set(inherited) == _CHILDREN
        assert {i["parent"] for i in inherited.values()} == {"payload"}
        warnings = {w["id"]: w for w in (plan.get("preflight") or {}).get("warnings") or []}
        assert all(c in warnings["flatten_children_protected"]["message"] for c in _CHILDREN)
    finally:
        _drop(table)


def test_qa_route_refuses_a_child_explicitly_left_in_clear(
    transfer_client, mongo_coll  # noqa: F811
):
    from src.ai.copilot.transfer_tools import start_transfer

    _pg().close()
    mongo_coll.insert_many([dict(d) for d in _docs()])
    staged = start_transfer(
        source_connector_id=_mongo_source(mongo_coll),
        source_table=mongo_coll.name,
        dest_connector_id=_pg_connector("d349x"),
        dest_table=mongo_coll.name + "_x",
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        column_transforms={"payload": "redact", "payload_s": "trim"},
        pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d349"},
    )
    assert not staged.success
    assert "payload_s is derived from payload" in str(staged.error)


def _engine_request(coll, table: str, mappings: list[dict], schema_policy: str = "manual_review"):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=mappings,
        source=EndpointConfig(
            kind="database", format="mongodb", connection_string="mongodb://127.0.0.1:27017",
            database="dataflow_test", collection=coll.name,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", host=PG["host"], port=PG["port"],
            database=PG["dbname"], username=PG["user"], password=PG["password"],
            schema="public", table=table,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
        schema_policy=schema_policy,
    )


def _studio_mappings(parent_tf: str) -> list[dict]:
    """What Studio / REST sends: one row per column, children left as ``none``."""
    cols = ["_id", "qty", "name", "payload", "payload_id", *sorted(_CHILDREN)]
    return [
        {"source": c, "target": c, "confidence": 1.0,
         "transform": parent_tf if c == "payload" else "none"}
        for c in cols
    ]


def _run_engine(request, fake, *, require_success: bool = True):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d349" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    if require_success:
        assert result.success, result.error
    return result


@pytest.fixture
def engine_jobs(monkeypatch):
    """The in-process engine pattern (AGENTS.md): memory jobs, no object store."""
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            status = (self.get_job(job_id) or {}).get("status", "running")
            return self.update_job_status(job_id, status, **fields)

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    fake = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


@pytest.mark.parametrize("parent_tf", ["redact", "hash_pii"])
def test_engine_route_protects_studio_mappings_and_a_field_new_in_run_2(
    mongo_coll, engine_jobs, parent_tf
):
    """Studio / REST mappings, then a schedule-style replay of the same mappings
    after the source grew ``payload.extra`` (propagate_columns adds the new
    column, D344-style): no child lands in clear, the new one included."""
    _pg().close()
    docs = _docs()
    mongo_coll.insert_many([dict(d) for d in docs])
    table = mongo_coll.name + "_eng"
    _drop(table)
    try:
        request = _engine_request(mongo_coll, table, _studio_mappings(parent_tf))
        _run_engine(request, engine_jobs)
        _types, rows = _payload_cells(table)
        assert len(rows) == len(docs)
        _assert_no_clear_payload(rows, docs, parent_tf)

        late = _docs(2, start=10, extra="top-secret")
        mongo_coll.insert_many([dict(d) for d in late])
        _run_engine(
            _engine_request(mongo_coll, table, _studio_mappings(parent_tf), "propagate_columns"),
            engine_jobs,
        )
        types, rows = _payload_cells(table)
        assert len(rows) == len(docs) + len(late)
        _assert_no_clear_payload(rows, docs + late, parent_tf)
        values = [v for r in rows for v in r.values()]
        assert "top-secret" not in values
        # propagate_columns added the new child, and it inherited the parent's protection.
        assert "payload_extra" in types, types
        extra = [r["payload_extra"] for r in rows if r["_id"] >= 10]
        if parent_tf == "redact":
            assert extra == ["<redacted>", "<redacted>"], extra
        else:
            assert all(len(v) >= 16 for v in extra), extra
    finally:
        _drop(table)


def test_a_child_first_met_on_a_later_page_lands_protected(mongo_coll, engine_jobs, monkeypatch):
    """The streaming writer maps a field it first meets on page 3 ("no silent
    drop"); flattened out of the redacted parent, it is redacted too.

    Gate-8 refuses this run on base as well (the population digest of a column
    first met mid-stream differs, unrelated to its transform); the table is
    kept, so the cells that were written are what this checks.
    """
    import src.transfer.stream as stream_mod

    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 4)
    _pg().close()
    docs = _docs(8) + _docs(3, start=20, late="late-secret")
    mongo_coll.insert_many([dict(d) for d in docs])
    table = mongo_coll.name + "_late"
    _drop(table)
    try:
        _run_engine(
            _engine_request(mongo_coll, table, _studio_mappings("redact")),
            engine_jobs,
            require_success=False,
        )
        types, rows = _payload_cells(table)
        assert len(rows) == len(docs)
        assert "payload_late" in types, types
        assert {r["payload_late"] for r in rows if r["_id"] >= 20} == {"<redacted>"}
        _assert_no_clear_payload(rows, docs, "redact")
    finally:
        _drop(table)
