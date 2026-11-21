"""NF-1: a warehouse overwrite keeps the object's security metadata.

The overwrite of a Snowflake table, BigQuery table or Elasticsearch index used
to rename / copy the live object aside and let the writer create a new one.
Everything bound to the old object went with it: grants, masking and row
access policies, tags and streams (Snowflake), table IAM, row access policies
and column policy tags (BigQuery), aliases and settings (Elasticsearch).

Snowflake runs on fakesnow (data path; fakesnow has no roles, grants,
policies, tags or SWAP, so the governed restore runs on a scripted cursor).
BigQuery and Elasticsearch run on in-memory fakes of the client calls used.
None of this is a live proof: real Snowflake / BigQuery accounts are needed
for that (see docs/use/sync-modes.md).
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from connectors.overwrite_governance import (
    GovernanceNotPreserved,
    SnowflakeGovernance,
    snowflake_reconcile,
)
from connectors.overwrite_set_aside import (
    _Snowflake,
    discard_set_aside,
    restore_set_aside,
    retire_set_aside,
)
from connectors.table_manager import TableDropError

PRIOR = [(1, "a", 10), (2, "b", 20), (3, "c", 30)]

# ---------------------------------------------------------------- Snowflake


@pytest.fixture()
def snowflake(tmp_path, monkeypatch):
    pytest.importorskip("fakesnow")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    cfg = {
        "host": "localhost", "username": "test", "password": "test",
        "database": "dataflow", "schema": "PUBLIC", "warehouse": "",
    }
    return cfg, "NF1_" + uuid.uuid4().hex[:8].upper()


def _sf(sql: str, params: Any = None) -> list[tuple]:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params) if params is not None else cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _seed(table: str) -> None:
    # The column default stands in for what fakesnow cannot model (grants,
    # policies, tags): a property bound to the table object, which survives
    # only if the object does. A table the writer creates has no default.
    _sf(f'CREATE TABLE "{table}" ("ID" INTEGER, "NAME" VARCHAR, "QTY" INTEGER DEFAULT 5)')
    for row in PRIOR:
        _sf(f'INSERT INTO "{table}" VALUES (%s, %s, %s)', row)


def _object_marker(table: str) -> str | None:
    rows = _sf(
        "SELECT column_default FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = 'QTY'",
        (table,),
    )
    return "governed-object" if rows and rows[0][0] == "5" else None


@pytest.fixture()
def sf_statements(monkeypatch):
    """Every statement the set-aside sends to Snowflake."""
    import connectors.overwrite_set_aside as osa

    seen: list[str] = []
    real = osa._snowflake_connection

    def _spy(cfg: dict[str, Any], schema: str | None) -> Any:
        conn = real(cfg, schema)
        cursor = conn.cursor

        class _Cur:
            def __init__(self, cur: Any) -> None:
                self._cur = cur

            def execute(self, sql: str, *args: Any) -> Any:
                seen.append(sql)
                return self._cur.execute(sql, *args)

            def __getattr__(self, name: str) -> Any:
                return getattr(self._cur, name)

            def __enter__(self) -> Any:
                self._cur.__enter__()
                return self

            def __exit__(self, *exc: Any) -> Any:
                return self._cur.__exit__(*exc)

        conn.cursor = lambda *a, **k: _Cur(cursor(*a, **k))
        return conn

    monkeypatch.setattr(osa, "_snowflake_connection", _spy)
    return seen


def test_snowflake_success_path_never_replaces_the_live_table(snowflake, sf_statements):
    cfg, table = snowflake
    _seed(table)

    backup = retire_set_aside("snowflake", cfg, table, "PUBLIC")
    _sf(f"INSERT INTO \"{table}\" VALUES (9, 'new', 90)")
    discard_set_aside("snowflake", cfg, table, backup, "PUBLIC")

    assert _sf(f'SELECT "ID", "NAME", "QTY" FROM "{table}"') == [(9, "new", 90)]
    # Object identity kept: the table-bound comment is still on the live name.
    assert _object_marker(table) == "governed-object"
    live_ref = f'"PUBLIC"."{table}"'
    touching_live = [
        s for s in sf_statements
        if live_ref in s and any(w in s.upper() for w in ("RENAME", "DROP", "CREATE OR REPLACE", "SWAP"))
        and f'"{table}__DF_BAK"' not in s
    ]
    assert touching_live == []
    assert any(s.startswith(f'CREATE TABLE "PUBLIC"."{backup}" CLONE {live_ref}') for s in sf_statements)
    assert f"TRUNCATE TABLE {live_ref}" in sf_statements


def test_snowflake_failed_run_restores_rows_in_place(snowflake):
    """No policy on the table: INSERT OVERWRITE puts the rows back into the same object."""
    cfg, table = snowflake
    _seed(table)
    backup = retire_set_aside("snowflake", cfg, table, "PUBLIC")
    # The failed run added a column and wrote part of its rows.
    _sf(f'ALTER TABLE "{table}" ADD COLUMN "EXTRA" VARCHAR')
    _sf(f"INSERT INTO \"{table}\" VALUES (7, 'partial', 70, 'x')")

    restore_set_aside("snowflake", cfg, table, backup, "PUBLIC")

    assert sorted(_sf(f'SELECT * FROM "{table}"')) == sorted(PRIOR)
    assert _object_marker(table) == "governed-object"
    tables = {r[0] for r in _sf("SELECT table_name FROM information_schema.tables")}
    assert backup not in tables


def test_snowflake_truncate_refusal_drops_the_clone_and_keeps_rows(snowflake, monkeypatch):
    cfg, table = snowflake
    _seed(table)
    import connectors.overwrite_set_aside as osa

    real = osa._snowflake_connection

    def _no_truncate(cfg: dict[str, Any], schema: str | None) -> Any:
        conn = real(cfg, schema)
        cursor = conn.cursor

        def _cur(*a: Any, **k: Any) -> Any:
            cur = cursor(*a, **k)
            execute = cur.execute

            def _exec(sql: str, *args: Any) -> Any:
                if sql.startswith("TRUNCATE"):
                    raise RuntimeError("Insufficient privileges to operate on table")
                return execute(sql, *args)

            cur.execute = _exec
            return cur

        conn.cursor = _cur
        return conn

    monkeypatch.setattr(osa, "_snowflake_connection", _no_truncate)
    with pytest.raises(TableDropError, match="Insufficient privileges"):
        retire_set_aside("snowflake", cfg, table, "PUBLIC")

    assert sorted(_sf(f'SELECT * FROM "{table}"')) == sorted(PRIOR)
    tables = {r[0] for r in _sf("SELECT table_name FROM information_schema.tables")}
    assert f"{table}__DF_BAK" not in tables


class _ScriptedCursor:
    """A Snowflake cursor that answers the governance reads from a script."""

    def __init__(self, governance: dict[str, dict[str, list[dict[str, Any]]]], *, refuse: set[str] = frozenset()):
        self.governance = governance
        self.refuse = refuse
        self.executed: list[str] = []
        self._rows: list[tuple] = []
        self.description: list[tuple] | None = None

    def _answer(self, rows: list[dict[str, Any]]) -> None:
        keys = list(rows[0]) if rows else ["x"]
        self.description = [(k.upper(),) for k in keys]
        self._rows = [tuple(r[k] for k in keys) for r in rows]

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append(sql)
        for word in self.refuse:
            if word in sql:
                raise RuntimeError(f"SQL access control error: {word}")
        target = "BAK" if "__DF_BAK" in (sql + str(params)) else "LIVE"
        gov = self.governance[target]
        if sql.startswith("SHOW GRANTS"):
            self._answer(gov.get("grants", []))
        elif "POLICY_REFERENCES" in sql:
            self._answer(gov.get("policies", []))
        elif "TAG_REFERENCES_ALL_COLUMNS" in sql:
            self._answer(gov.get("column_tags", []))
        elif "TAG_REFERENCES(" in sql:
            self._answer(gov.get("table_tags", []))
        else:
            self.description, self._rows = None, []

    def fetchall(self) -> list[tuple]:
        return list(self._rows)


_GRANT = {"privilege": "SELECT", "granted_to": "ROLE", "grantee_name": "ANALYST"}
_OWNER = {"privilege": "OWNERSHIP", "granted_to": "ROLE", "grantee_name": "LOADER"}
_MASK = {
    "policy_kind": "MASKING_POLICY", "policy_db": "GOV", "policy_schema": "POL",
    "policy_name": "SSN_MASK", "ref_column_name": "SSN", "ref_arg_column_names": None, "tag_name": None,
}
_RAP = {
    "policy_kind": "ROW_ACCESS_POLICY", "policy_db": "GOV", "policy_schema": "POL",
    "policy_name": "REGION_RAP", "ref_column_name": None, "ref_arg_column_names": '[ "REGION" ]',
    "tag_name": None,
}
_TAG = {"tag_database": "GOV", "tag_schema": "TAGS", "tag_name": "PII", "tag_value": "ssn",
        "level": "COLUMN", "column_name": "SSN"}
_TTAG = {"tag_database": "GOV", "tag_schema": "TAGS", "tag_name": "DOMAIN", "tag_value": "hr", "level": "TABLE"}


def _gov(cur: Any, ref: str) -> SnowflakeGovernance:
    from connectors.overwrite_governance import snowflake_governance

    return snowflake_governance(cur, ref, database="DB")


def test_snowflake_governance_read_and_reconcile_statements():
    live = {"grants": [_GRANT, _OWNER], "policies": [_MASK, _RAP], "column_tags": [_TAG], "table_tags": [_TTAG]}
    cur = _ScriptedCursor({"LIVE": live, "BAK": {"grants": [_OWNER]}})
    source = _gov(cur, '"PUBLIC"."T"')
    target = _gov(cur, '"PUBLIC"."T__DF_BAK"')
    assert source.grants == [("SELECT", "ROLE", "ANALYST")]  # OWNERSHIP is never re-granted
    assert source.has_policies is True

    ran = snowflake_reconcile(cur, source=source, target=target, target_ref='"PUBLIC"."T__DF_BAK"')

    assert ran == [
        'GRANT SELECT ON TABLE "PUBLIC"."T__DF_BAK" TO ROLE ANALYST',
        'ALTER TABLE "PUBLIC"."T__DF_BAK" MODIFY COLUMN "SSN" SET MASKING POLICY "GOV"."POL"."SSN_MASK"',
        'ALTER TABLE "PUBLIC"."T__DF_BAK" ADD ROW ACCESS POLICY "GOV"."POL"."REGION_RAP" ON ("REGION")',
        'ALTER TABLE "PUBLIC"."T__DF_BAK" MODIFY COLUMN "SSN" SET TAG "GOV"."TAGS"."PII" = \'ssn\'',
        'ALTER TABLE "PUBLIC"."T__DF_BAK" SET TAG "GOV"."TAGS"."DOMAIN" = \'hr\'',
    ]


def test_snowflake_reconcile_skips_what_the_clone_already_carries():
    same = {"grants": [_GRANT], "policies": [_MASK], "column_tags": [_TAG]}
    cur = _ScriptedCursor({"LIVE": same, "BAK": same})
    ran = snowflake_reconcile(
        cur, source=_gov(cur, '"PUBLIC"."T"'), target=_gov(cur, '"PUBLIC"."T__DF_BAK"'),
        target_ref='"PUBLIC"."T__DF_BAK"',
    )
    assert ran == []


def _restore_with(cur: _ScriptedCursor, monkeypatch) -> None:
    import connectors.overwrite_set_aside as osa

    class _Conn:
        def cursor(self) -> Any:
            return cur

        def close(self) -> None:
            pass

    cur.__enter__ = lambda: cur  # type: ignore[attr-defined]
    monkeypatch.setattr(osa, "_snowflake_connection", lambda cfg, schema: _Conn())
    monkeypatch.setattr(_ScriptedCursor, "__enter__", lambda self: self, raising=False)
    monkeypatch.setattr(_ScriptedCursor, "__exit__", lambda self, *a: False, raising=False)
    monkeypatch.setattr(_Snowflake, "_resolve", staticmethod(lambda c, s, name: name))
    restore_set_aside("snowflake", {"database": "DB", "schema": "PUBLIC"}, "T", "T__DF_BAK", "PUBLIC")


def test_snowflake_governed_restore_swaps_a_reconciled_backup(monkeypatch):
    """A masked table is not read back through its policy: the backup is swapped in,
    after it was given every grant, policy and tag the live table has."""
    live = {"grants": [_GRANT], "policies": [_MASK]}
    cur = _ScriptedCursor({"LIVE": live, "BAK": {}})

    _restore_with(cur, monkeypatch)

    run = [s for s in cur.executed if not s.startswith(("SHOW", "SELECT"))]
    assert run == [
        'GRANT SELECT ON TABLE "PUBLIC"."T__DF_BAK" TO ROLE ANALYST',
        'ALTER TABLE "PUBLIC"."T__DF_BAK" MODIFY COLUMN "SSN" SET MASKING POLICY "GOV"."POL"."SSN_MASK"',
        'ALTER TABLE "PUBLIC"."T" SWAP WITH "PUBLIC"."T__DF_BAK"',
        'DROP TABLE IF EXISTS "PUBLIC"."T__DF_BAK"',
    ]
    assert not any("INSERT OVERWRITE" in s for s in cur.executed)


def test_snowflake_restore_with_unreadable_policies_keeps_both(monkeypatch):
    """Policies unknown: neither a read-back (masked values) nor a swap (maybe unmasked)."""
    cur = _ScriptedCursor({"LIVE": {"grants": [_GRANT]}, "BAK": {}}, refuse={"POLICY_REFERENCES"})

    with pytest.raises(TableDropError, match="unreadable masking and row access policies"):
        _restore_with(cur, monkeypatch)

    assert not any(w in s for s in cur.executed for w in ("SWAP", "DROP", "INSERT OVERWRITE"))


def test_snowflake_refused_reapply_does_not_swap(monkeypatch):
    cur = _ScriptedCursor({"LIVE": {"policies": [_MASK]}, "BAK": {}}, refuse={"SET MASKING POLICY"})

    with pytest.raises(TableDropError, match="SET MASKING POLICY"):
        _restore_with(cur, monkeypatch)

    assert not any("SWAP" in s or s.startswith("DROP") for s in cur.executed)


# ----------------------------------------------------------------- BigQuery


def _bq_fields(policy_tag: bool = True) -> list[Any]:
    from google.cloud import bigquery

    tags = bigquery.PolicyTagList(names=["projects/p/locations/us/taxonomies/1/policyTags/2"])
    return [
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("ssn", "STRING", policy_tags=tags if policy_tag else None),
    ]


class _GovernedBigQuery:
    """Tables with schema, labels, IAM bindings and row access policies."""

    def __init__(self, *, iam_readable: bool = True, buffered: bool = False) -> None:
        self.tables: dict[str, dict[str, Any]] = {}
        self.iam_readable = iam_readable
        self.buffered = buffered
        self.log: list[str] = []
        self.queries: list[str] = []

    def add(self, table_id: str, rows: list[tuple], *, governed: bool = True) -> None:
        self.tables[table_id] = {
            "rows": list(rows),
            "schema": _bq_fields(governed),
            "labels": {"owner": "finance"} if governed else {},
            "iam": [{"role": "roles/bigquery.dataViewer", "members": ["group:analysts@example.com"]}]
            if governed else [],
            "raps": [{"rowAccessPolicyReference": {"policyId": "eu_only"},
                      "filterPredicate": "region = 'EU'", "grantees": ["group:eu@example.com"]}]
            if governed else [],
            "buffered": self.buffered,
        }

    def _t(self, table_id: str) -> dict[str, Any]:
        if table_id not in self.tables:
            exc = type("NotFound", (Exception,), {})(f"Not found: Table {table_id}")
            raise exc
        return self.tables[table_id]

    def get_table(self, table_id: str, **_: Any) -> Any:
        t = self._t(table_id)
        return SimpleNamespace(
            schema=t["schema"], labels=t["labels"], description=None, time_partitioning=None,
            range_partitioning=None, clustering_fields=None,
            streaming_buffer={"estimated_rows": 1} if t["buffered"] else None,
        )

    def get_iam_policy(self, table_id: str, **_: Any) -> Any:
        if not self.iam_readable:
            raise RuntimeError("Access Denied: bigquery.tables.getIamPolicy")
        return SimpleNamespace(bindings=[dict(b) for b in self._t(table_id)["iam"]])

    def set_iam_policy(self, table_id: str, policy: Any, **_: Any) -> Any:
        self.log.append(f"iam {table_id}")
        self._t(table_id)["iam"] = [dict(b) for b in policy.bindings]
        return policy

    @property
    def _connection(self) -> Any:
        def _api(method: str, path: str) -> dict:
            parts = path.split("/")
            table_id = f"{parts[2]}.{parts[4]}.{parts[6]}"
            return {"rowAccessPolicies": list(self._t(table_id)["raps"])}

        return SimpleNamespace(api_request=_api)

    def copy_table(self, src: str, dst: str, job_config: Any = None, **_: Any) -> Any:
        # A copy job copies data and schema; not IAM, not row access policies.
        s = self._t(src)
        self.log.append(f"copy {src} -> {dst}")
        self.tables[dst] = {"rows": list(s["rows"]), "schema": list(s["schema"]), "labels": dict(s["labels"]),
                            "iam": [], "raps": [], "buffered": False}
        return SimpleNamespace(result=lambda **_: None)

    def query(self, sql: str, **_: Any) -> Any:
        import re

        self.queries.append(sql)
        m = re.match(r"CREATE OR REPLACE ROW ACCESS POLICY `([^`]+)` ON `([^`]+)` GRANT TO \((.*)\) FILTER USING \((.*)\)$", sql)
        if m:
            name, table_id, grantees, flt = m.groups()
            self.log.append(f"rap {table_id}")
            self._t(table_id)["raps"].append({"rowAccessPolicyReference": {"policyId": name},
                                              "filterPredicate": flt,
                                              "grantees": [g.strip().strip('"') for g in grantees.split(",")]})
            return SimpleNamespace(result=lambda **_: None)
        raise AssertionError(f"unexpected query: {sql}")

    def create_table(self, table: Any, **_: Any) -> Any:
        table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
        self.log.append(f"create {table_id}")
        self.tables[table_id] = {"rows": [], "schema": list(table.schema), "labels": dict(table.labels or {}),
                                 "iam": [], "raps": [], "buffered": False}
        return table

    def delete_table(self, table_id: str, not_found_ok: bool = False, **_: Any) -> None:
        self.log.append(f"delete {table_id}")
        self.tables.pop(table_id, None)

    def close(self) -> None:
        pass


_CFG = {"database": "p", "schema": "d"}


@pytest.fixture()
def bq(monkeypatch):
    import connectors.bigquery_conn as bq_conn

    fake = _GovernedBigQuery()
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: fake)
    return fake


def _policy_tagged(schema: list[Any]) -> list[str]:
    return [f.name for f in schema if f.policy_tags is not None and list(f.policy_tags.names)]


def test_bigquery_governed_overwrite_recreates_access_control_before_rows(bq):
    bq.add("p.d.orders", PRIOR)

    backup = retire_set_aside("bigquery", _CFG, "orders", "d")

    live = bq.tables["p.d.orders"]
    assert live["rows"] == []
    assert _policy_tagged(live["schema"]) == ["ssn"]
    assert live["labels"] == {"owner": "finance"}
    assert live["iam"] == [{"role": "roles/bigquery.dataViewer", "members": ["group:analysts@example.com"]}]
    assert [p["filterPredicate"] for p in live["raps"]] == ["region = 'EU'"]
    # The backup is protected while it exists: same row filter, same IAM.
    bak = bq.tables[f"p.d.{backup}"]
    assert bak["rows"] == list(PRIOR) and bak["raps"] and bak["iam"]
    # Order: row access policy before IAM, both before the writer gets the table.
    order = [e for e in bq.log if e.endswith("p.d.orders") and e.split()[0] in {"create", "rap", "iam"}]
    assert order == ["create p.d.orders", "rap p.d.orders", "iam p.d.orders"]

    bq.tables["p.d.orders"]["rows"] = [(9, "new")]
    discard_set_aside("bigquery", _CFG, "orders", backup, "d")
    assert sorted(bq.tables) == ["p.d.orders"]
    assert bq.tables["p.d.orders"]["iam"] and bq.tables["p.d.orders"]["raps"]


def test_bigquery_failed_run_restores_rows_and_access_control(bq):
    bq.add("p.d.orders", PRIOR)
    backup = retire_set_aside("bigquery", _CFG, "orders", "d")
    bq.tables["p.d.orders"]["rows"] = [(7, "partial")]

    restore_set_aside("bigquery", _CFG, "orders", backup, "d")

    live = bq.tables["p.d.orders"]
    assert sorted(bq.tables) == ["p.d.orders"]
    assert live["rows"] == list(PRIOR)
    assert live["iam"] and [p["filterPredicate"] for p in live["raps"]] == ["region = 'EU'"]
    assert _policy_tagged(live["schema"]) == ["ssn"]


def test_bigquery_unreadable_iam_refuses_before_any_change(monkeypatch):
    import connectors.bigquery_conn as bq_conn

    fake = _GovernedBigQuery(iam_readable=False)
    fake.add("p.d.orders", PRIOR)
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: fake)

    with pytest.raises(TableDropError, match="unreadable table IAM policy"):
        retire_set_aside("bigquery", _CFG, "orders", "d")

    assert sorted(fake.tables) == ["p.d.orders"] and fake.tables["p.d.orders"]["rows"] == list(PRIOR)
    assert not any(e.startswith(("copy", "delete")) for e in fake.log)


def test_bigquery_unreadable_iam_proceeds_only_with_the_approval(monkeypatch):
    import connectors.bigquery_conn as bq_conn

    fake = _GovernedBigQuery(iam_readable=False)
    fake.add("p.d.orders", PRIOR)
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: fake)

    backup = retire_set_aside("bigquery", _CFG, "orders", "d", governance_loss_approved=True)

    assert fake.tables[f"p.d.{backup}"]["rows"] == list(PRIOR)
    # What could be read is still carried (policy tags, row access policy).
    assert _policy_tagged(fake.tables["p.d.orders"]["schema"]) == ["ssn"]
    assert fake.tables["p.d.orders"]["raps"]


def test_bigquery_governed_table_is_never_copied_by_query(monkeypatch):
    """A query copy reads through row filters and masking: a governed table with a
    streaming buffer refuses rather than copy by query."""
    import connectors.bigquery_conn as bq_conn

    fake = _GovernedBigQuery(buffered=True)
    fake.add("p.d.orders", PRIOR)
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: fake)

    with pytest.raises(TableDropError, match="streaming buffer"):
        retire_set_aside("bigquery", _CFG, "orders", "d")

    assert fake.queries == [] and sorted(fake.tables) == ["p.d.orders"]


def test_bigquery_ungoverned_table_keeps_the_writer_create_path(bq):
    bq.add("p.d.orders", PRIOR, governed=False)

    backup = retire_set_aside("bigquery", _CFG, "orders", "d")

    # Nothing to carry: the live table is gone and the writer creates it as before.
    assert sorted(bq.tables) == [f"p.d.{backup}"]


# ------------------------------------------------------------ Elasticsearch


@pytest.fixture()
def es(monkeypatch):
    import connectors.elasticsearch_reader as es_reader
    from tests.test_overwrite_all_quarantined_keeps_set_aside_destination import _FakeElasticsearch

    store: dict[str, dict[str, Any]] = {
        "orders": {
            "docs": [1, 2, 3],
            "settings": {"number_of_shards": 3, "number_of_replicas": 1, "index.lifecycle.name": "keep-90d"},
            "aliases": {"orders_eu": {"filter": {"term": {"region": "EU"}}}, "orders_read": {}},
        }
    }
    client = _FakeElasticsearch(store)
    monkeypatch.setattr(es_reader, "_client", lambda cfg: client)
    return client, store


def test_elasticsearch_overwrite_keeps_the_index_its_aliases_and_settings(es):
    client, store = es
    backup = retire_set_aside("elasticsearch", {}, "orders")

    live = store["orders"]
    assert live["docs"] == []
    assert set(live["aliases"]) == {"orders_eu", "orders_read"}
    assert live["settings"] == {"number_of_shards": 3, "number_of_replicas": 1,
                                "index.lifecycle.name": "keep-90d"}
    assert store[backup]["docs"] == [1, 2, 3]
    assert store[backup]["settings"]["index.blocks.write"] is True

    live["docs"] = [4, 5]
    discard_set_aside("elasticsearch", {}, "orders", backup)
    assert sorted(store) == ["orders"] and store["orders"]["docs"] == [4, 5]
    assert set(store["orders"]["aliases"]) == {"orders_eu", "orders_read"}


def test_elasticsearch_failed_run_restores_documents_and_aliases(es):
    client, store = es
    backup = retire_set_aside("elasticsearch", {}, "orders")
    store["orders"]["docs"] = [9]

    restore_set_aside("elasticsearch", {}, "orders", backup)

    assert sorted(store) == ["orders"]
    assert store["orders"]["docs"] == [1, 2, 3]
    assert store["orders"]["aliases"]["orders_eu"] == {"filter": {"term": {"region": "EU"}}}
    assert "index.blocks.write" not in store["orders"]["settings"]
    assert store["orders"]["settings"]["index.lifecycle.name"] == "keep-90d"


def test_elasticsearch_failed_empty_puts_the_documents_back(es, monkeypatch):
    client, store = es

    def _fail(**_: Any) -> dict:
        store["orders"]["docs"] = store["orders"]["docs"][1:]  # partly deleted
        raise RuntimeError("search_phase_execution_exception")

    monkeypatch.setattr(client, "delete_by_query", _fail)
    with pytest.raises(TableDropError, match="search_phase_execution_exception"):
        retire_set_aside("elasticsearch", {}, "orders")

    assert sorted(store) == ["orders"]
    assert store["orders"]["docs"] == [1, 2, 3]
    assert set(store["orders"]["aliases"]) == {"orders_eu", "orders_read"}


# ----------------------------------------------------------- Validate gate


def _gate(db_type: str, *, acknowledged: bool = False, probe: Any = None, sync_mode: str = "full_refresh_overwrite",
          exists: bool | None = True) -> dict[str, Any] | None:
    from services.overwrite_governance_gate import build_overwrite_governance_gate

    return build_overwrite_governance_gate(
        sync_mode=sync_mode, db_type=db_type, table_exists=exists, destination_config={},
        destination_table="orders", acknowledged=acknowledged, probe=probe,
    )


def test_gate_discloses_snowflake_and_elasticsearch_without_blocking():
    for engine in ("snowflake", "elasticsearch"):
        gate = _gate(engine)
        assert gate["status"] == "pass" and gate["details"]["requires_approval"] is False
        assert gate["details"]["preserved"] and "on_failure" in gate["details"]


def test_gate_blocks_bigquery_when_access_control_cannot_be_read():
    probe = lambda cfg, table: {"ran": True, "governed": True, "unreadable": ["table IAM policy"], "message": ""}  # noqa: E731
    gate = _gate("bigquery", probe=probe)
    assert gate["status"] == "block"
    assert gate["details"]["requires_approval"] is True
    assert "table IAM policy cannot be read" in gate["message"]

    approved = _gate("bigquery", probe=probe, acknowledged=True)
    assert approved["status"] == "pass" and approved["details"]["acknowledged"] is True


def test_gate_blocks_bigquery_when_unmeasured():
    gate = _gate("bigquery", probe=lambda c, t: {"ran": False, "message": "timed out"})
    assert gate["status"] == "block"


def test_gate_passes_bigquery_when_access_control_is_carried():
    gate = _gate("bigquery", probe=lambda c, t: {"ran": True, "governed": True, "unreadable": []})
    assert gate["status"] == "pass"
    assert "time-travel" in gate["message"]


def test_gate_is_silent_for_other_modes_engines_and_new_tables():
    assert _gate("snowflake", sync_mode="full_refresh_append") is None
    assert _gate("postgresql") is None
    assert _gate("bigquery", exists=False) is None


def test_validate_approval_reaches_the_overwrite_clear():
    from services.overwrite_governance_gate import APPROVAL_KEY, stamp_overwrite_governance_approval

    request = SimpleNamespace(destination=SimpleNamespace(extra={"x": 1}), ddl_changes_acknowledged=True)
    stamp_overwrite_governance_approval(request)
    assert request.destination.extra == {"x": 1, APPROVAL_KEY: True}


def test_governance_error_names_the_loss_and_the_remedy():
    exc = GovernanceNotPreserved("p.d.t", ["unreadable row access policies"])
    assert "unreadable row access policies" in str(exc)
    assert "Approve the disclosed loss at Validate" in str(exc)
