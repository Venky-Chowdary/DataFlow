"""Security metadata of a warehouse object an overwrite replaces (NF-1).

An overwrite used to set the live Snowflake table, BigQuery table or
Elasticsearch index aside and let the writer create a new object under the
live name. Everything bound to the old object went with it: grants, masking
and row access policies, tags and streams (Snowflake), the table IAM policy,
row access policies and column policy tags (BigQuery), aliases and index
settings (Elasticsearch). :mod:`connectors.overwrite_set_aside` now keeps the
live object where it can. This module holds what that needs:

- **Snowflake**: read the governance bound to a table (``SHOW GRANTS ON
  TABLE``, ``INFORMATION_SCHEMA.POLICY_REFERENCES``, ``TAG_REFERENCES`` and
  ``TAG_REFERENCES_ALL_COLUMNS``) and copy what a backup clone lacks onto it,
  so that swapping the backup back in after a failed run publishes it with
  the same grants, policies and tags. Snowflake documents that ``CREATE ...
  CLONE`` copies no grants unless ``COPY GRANTS`` is given; whether a clone
  keeps direct policy and tag attachments could not be verified against the
  documentation from this environment, so the backup is reconciled against
  the live table instead of trusting either answer.
- **BigQuery**: read the table IAM policy, the row access policies, the
  schema (column policy tags live in it), labels, description, partitioning
  and clustering, and put them on a table the overwrite creates.
- **Elasticsearch**: read and re-add the aliases of an index.

Nothing here is a best-effort: a governance item that cannot be read or put
back raises :class:`GovernanceNotPreserved`, and the overwrite either refuses
before anything changed or (with the operator's recorded approval) proceeds
knowing what is lost. ``describe_overwrite_governance`` is what Validate
shows for each engine.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


class GovernanceNotPreserved(RuntimeError):
    """A grant, policy, tag, alias or IAM binding the overwrite cannot carry."""

    def __init__(self, table: str, items: list[str]) -> None:
        self.table = table
        self.items = list(items)
        super().__init__(
            f"{table}: the overwrite cannot keep "
            + "; ".join(self.items)
            + ". Approve the disclosed loss at Validate (destination DDL approval) "
            "or give the connector's role the privileges to read and re-apply it."
        )


# ---------------------------------------------------------------- Snowflake

#: Privileges SHOW GRANTS lists that are not re-granted: OWNERSHIP moves with
#: the object, and a clone is owned by the role that made it.
_SF_NOT_REGRANTED = frozenset({"OWNERSHIP"})
_SF_GRANTEE_KINDS = {"ROLE": "ROLE", "DATABASE_ROLE": "DATABASE ROLE", "SHARE": "SHARE"}


@dataclass
class SnowflakeGovernance:
    """What is bound to one Snowflake table. ``None`` means it could not be read."""

    grants: list[tuple[str, str, str]] | None = None  # (privilege, grantee kind, grantee)
    policies: list[dict[str, str]] | None = None  # direct masking / row access attachments
    tag_policies: list[dict[str, str]] | None = None  # policies reached through a tag
    tags: list[dict[str, str]] | None = None  # (tag, value, column or "")
    unreadable: list[str] = field(default_factory=list)

    @property
    def has_policies(self) -> bool | None:
        """True when any masking / row access policy protects the table; None when unknown."""
        if self.policies is None or self.tag_policies is None:
            return None
        return bool(self.policies or self.tag_policies)


def _sf_rows(cur: Any, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    cur.execute(sql, params) if params else cur.execute(sql)
    names = [str(d[0]).lower() for d in (cur.description or [])]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def _sf_entity_name(database: str, qualified: str) -> str:
    """``"DB"."SCHEMA"."TABLE"`` for the INFORMATION_SCHEMA table functions."""
    from connectors.sql_identifiers import quote_sql_identifier

    return f"{quote_sql_identifier(database)}.{qualified}" if database else qualified


def snowflake_governance(cur: Any, qualified: str, *, database: str = "") -> SnowflakeGovernance:
    """Read the grants, policies and tags of ``qualified`` (a quoted ``schema.table``)."""
    gov = SnowflakeGovernance()
    entity = _sf_entity_name(database, qualified)
    try:
        rows = _sf_rows(cur, f"SHOW GRANTS ON TABLE {qualified}")
        gov.grants = sorted(
            {
                (str(r.get("privilege") or "").upper(), str(r.get("granted_to") or "").upper(),
                 str(r.get("grantee_name") or ""))
                for r in rows
                if str(r.get("privilege") or "").upper() not in _SF_NOT_REGRANTED
                and str(r.get("granted_to") or "").upper() in _SF_GRANTEE_KINDS
            }
        )
    except Exception as exc:  # noqa: BLE001 - unreadable is recorded, never assumed empty
        logger.info("SHOW GRANTS ON TABLE %s refused: %s", qualified, exc)
        gov.unreadable.append("grants")
    try:
        rows = _sf_rows(
            cur,
            "SELECT POLICY_KIND, POLICY_DB, POLICY_SCHEMA, POLICY_NAME, REF_COLUMN_NAME, "
            "REF_ARG_COLUMN_NAMES, TAG_NAME FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES("
            "REF_ENTITY_NAME => %s, REF_ENTITY_DOMAIN => 'table'))",
            (entity,),
        )
        direct, via_tag = [], []
        for r in rows:
            ref = {
                "kind": str(r.get("policy_kind") or "").upper(),
                "policy": ".".join(
                    _q(str(r.get(k) or "")) for k in ("policy_db", "policy_schema", "policy_name")
                    if r.get(k)
                ),
                "column": str(r.get("ref_column_name") or ""),
                "args": str(r.get("ref_arg_column_names") or ""),
            }
            (via_tag if r.get("tag_name") else direct).append(ref)
        gov.policies, gov.tag_policies = direct, via_tag
    except Exception as exc:  # noqa: BLE001
        logger.info("POLICY_REFERENCES for %s refused: %s", entity, exc)
        gov.unreadable.append("masking and row access policies")
    try:
        tags: list[dict[str, str]] = []
        for r in _sf_rows(
            cur,
            "SELECT TAG_DATABASE, TAG_SCHEMA, TAG_NAME, TAG_VALUE, LEVEL, COLUMN_NAME "
            "FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(%s, 'table'))",
            (entity,),
        ):
            if str(r.get("level") or "").upper() == "COLUMN" and r.get("column_name"):
                tags.append(_sf_tag(r, column=str(r["column_name"])))
        for r in _sf_rows(
            cur,
            "SELECT TAG_DATABASE, TAG_SCHEMA, TAG_NAME, TAG_VALUE, LEVEL "
            "FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(%s, 'table'))",
            (entity,),
        ):
            if str(r.get("level") or "").upper() == "TABLE":
                tags.append(_sf_tag(r, column=""))
        gov.tags = tags
    except Exception as exc:  # noqa: BLE001
        logger.info("TAG_REFERENCES for %s refused: %s", entity, exc)
        gov.unreadable.append("object tags")
    return gov


def _q(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    return quote_sql_identifier(name)


def _sf_tag(row: dict[str, Any], *, column: str) -> dict[str, str]:
    tag = ".".join(_q(str(row.get(k) or "")) for k in ("tag_database", "tag_schema", "tag_name") if row.get(k))
    return {"tag": tag, "value": str(row.get("tag_value") or ""), "column": column}


def _sf_literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _sf_grantee(name: str) -> str:
    """A grantee as SHOW GRANTS names it, quoted (``DB.ROLE`` for a database role)."""
    return ".".join(_q(part) for part in str(name).split("."))


def snowflake_reconcile(
    cur: Any,
    *,
    source: SnowflakeGovernance,
    target: SnowflakeGovernance,
    target_ref: str,
    skip_unreadable: bool = False,
) -> list[str]:
    """Put on ``target_ref`` every grant, direct policy and tag ``source`` has and it lacks.

    Returns the statements run. Raises :class:`GovernanceNotPreserved` when an
    item of ``source`` is unknown (unreadable) or a statement is refused: the
    caller must then not publish ``target_ref`` under the live name.
    ``skip_unreadable`` (a loss approved at Validate) reconciles only the
    categories both sides could read.
    """
    unknown = [item for item in source.unreadable]
    unknown += [f"{item} (on the backup)" for item in target.unreadable]
    if unknown and not skip_unreadable:
        raise GovernanceNotPreserved(target_ref, [f"unreadable {u}" for u in unknown])
    statements: list[str] = []
    have = set(target.grants or [])
    for privilege, kind, grantee in (source.grants if target.grants is not None else None) or []:
        if (privilege, kind, grantee) in have:
            continue
        statements.append(
            f"GRANT {privilege} ON TABLE {target_ref} TO {_SF_GRANTEE_KINDS[kind]} {_sf_grantee(grantee)}"
        )
    have_policies = {(p["kind"], p["policy"], p["column"]) for p in target.policies or []}
    for p in (source.policies if target.policies is not None else None) or []:
        if (p["kind"], p["policy"], p["column"]) in have_policies:
            continue
        if p["kind"] == "ROW_ACCESS_POLICY":
            args = ", ".join(_q(a.strip().strip('"')) for a in _split_args(p["args"]))
            statements.append(f"ALTER TABLE {target_ref} ADD ROW ACCESS POLICY {p['policy']} ON ({args})")
        elif p["kind"] in {"MASKING_POLICY", "PROJECTION_POLICY"} and p["column"]:
            word = "MASKING" if p["kind"] == "MASKING_POLICY" else "PROJECTION"
            using = ""
            extra = [a for a in _split_args(p["args"]) if a]
            if extra:
                using = " USING (" + ", ".join([_q(p["column"])] + [_q(a.strip().strip('"')) for a in extra]) + ")"
            statements.append(
                f"ALTER TABLE {target_ref} MODIFY COLUMN {_q(p['column'])} "
                f"SET {word} POLICY {p['policy']}{using}"
            )
        else:
            raise GovernanceNotPreserved(target_ref, [f"{p['kind'].lower()} {p['policy']}"])
    have_tags = {(t["tag"], t["value"], t["column"]) for t in target.tags or []}
    for t in (source.tags if target.tags is not None else None) or []:
        if (t["tag"], t["value"], t["column"]) in have_tags:
            continue
        if t["column"]:
            statements.append(
                f"ALTER TABLE {target_ref} MODIFY COLUMN {_q(t['column'])} "
                f"SET TAG {t['tag']} = {_sf_literal(t['value'])}"
            )
        else:
            statements.append(f"ALTER TABLE {target_ref} SET TAG {t['tag']} = {_sf_literal(t['value'])}")
    for sql in statements:
        try:
            cur.execute(sql)
        except Exception as exc:  # noqa: BLE001 - one refused item fails the reconcile
            raise GovernanceNotPreserved(target_ref, [f"{sql!r} was refused: {exc}"]) from exc
    return statements


def _split_args(raw: str) -> list[str]:
    text = str(raw or "").strip().strip("[]")
    return [part.strip() for part in text.split(",") if part.strip()]


# ----------------------------------------------------------------- BigQuery


@dataclass
class BigQueryGovernance:
    """Table-bound security metadata BigQuery loses when a table is deleted."""

    iam_bindings: list[dict[str, Any]] = field(default_factory=list)
    row_access_policies: list[dict[str, Any]] = field(default_factory=list)
    policy_tag_columns: list[str] = field(default_factory=list)
    schema: list[Any] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    description: str | None = None
    time_partitioning: Any = None
    range_partitioning: Any = None
    clustering_fields: list[str] | None = None
    unreadable: list[str] = field(default_factory=list)

    @property
    def governed(self) -> bool:
        """True when deleting the table would drop access control."""
        return bool(self.iam_bindings or self.row_access_policies or self.policy_tag_columns)


def _policy_tag_columns(fields: list[Any], prefix: str = "") -> list[str]:
    out: list[str] = []
    for f in fields or []:
        name = f"{prefix}{getattr(f, 'name', '')}"
        tags = getattr(f, "policy_tags", None)
        if tags is not None and list(getattr(tags, "names", None) or []):
            out.append(name)
        out.extend(_policy_tag_columns(list(getattr(f, "fields", None) or []), f"{name}."))
    return out


def _bq_rap_path(table_id: str) -> str:
    from urllib.parse import quote

    project, dataset, table = str(table_id).split(".", 2)
    return (
        f"/projects/{quote(project, safe='')}/datasets/{quote(dataset, safe='')}"
        f"/tables/{quote(table, safe='')}/rowAccessPolicies"
    )


def bigquery_governance(client: Any, table_id: str, call: dict[str, Any]) -> BigQueryGovernance:
    """Read the governance of ``table_id`` (``project.dataset.table``)."""
    gov = BigQueryGovernance()
    table = client.get_table(table_id, **call)
    gov.schema = list(getattr(table, "schema", None) or [])
    gov.policy_tag_columns = _policy_tag_columns(gov.schema)
    gov.labels = dict(getattr(table, "labels", None) or {})
    gov.description = getattr(table, "description", None)
    gov.time_partitioning = getattr(table, "time_partitioning", None)
    gov.range_partitioning = getattr(table, "range_partitioning", None)
    gov.clustering_fields = getattr(table, "clustering_fields", None)
    try:
        policy = client.get_iam_policy(table_id, **call)
        gov.iam_bindings = [
            {"role": b.get("role"), "members": sorted(b.get("members") or [])}
            for b in (getattr(policy, "bindings", None) or [])
            if b.get("members")
        ]
    except Exception as exc:  # noqa: BLE001
        logger.info("getIamPolicy on %s refused: %s", table_id, exc)
        gov.unreadable.append("table IAM policy")
    try:
        resp = client._connection.api_request(method="GET", path=_bq_rap_path(table_id))
        for p in (resp or {}).get("rowAccessPolicies") or []:
            ref = p.get("rowAccessPolicyReference") or {}
            gov.row_access_policies.append({
                "name": ref.get("policyId") or "",
                "filter": p.get("filterPredicate") or "",
                "grantees": list(p.get("grantees") or []),
            })
        if any(not p["grantees"] for p in gov.row_access_policies):
            # Older API responses do not return grantees; without them the
            # policy cannot be recreated as it was.
            gov.unreadable.append("row access policy grantees")
    except Exception as exc:  # noqa: BLE001
        logger.info("rowAccessPolicies.list on %s refused: %s", table_id, exc)
        gov.unreadable.append("row access policies")
    return gov


def _bq_q(table_id: str) -> str:
    return "`" + table_id.replace("`", "") + "`"


def bigquery_apply(client: Any, table_id: str, gov: BigQueryGovernance, call: dict[str, Any]) -> None:
    """Re-create row access policies, then the IAM policy, on ``table_id``.

    Row access policies first: once a table has one, a reader outside every
    grantee list sees no rows, so the window before IAM lands is closed, not
    open. Raises :class:`GovernanceNotPreserved` on any refusal.
    """
    for p in gov.row_access_policies:
        grantees = ", ".join('"' + g.replace('"', "") + '"' for g in p["grantees"])
        name = "`" + str(p["name"]).replace("`", "") + "`"
        sql = (
            f"CREATE OR REPLACE ROW ACCESS POLICY {name} ON {_bq_q(table_id)} "
            f"GRANT TO ({grantees}) FILTER USING ({p['filter']})"
        )
        try:
            client.query(sql, job_retry=None).result()
        except Exception as exc:  # noqa: BLE001
            raise GovernanceNotPreserved(table_id, [f"row access policy {p['name']}: {exc}"]) from exc
    if gov.iam_bindings:
        try:
            policy = client.get_iam_policy(table_id, **call)
            policy.bindings = [dict(b) for b in gov.iam_bindings]
            client.set_iam_policy(table_id, policy, **call)
        except Exception as exc:  # noqa: BLE001
            raise GovernanceNotPreserved(table_id, [f"table IAM policy: {exc}"]) from exc


def bigquery_create_like(client: Any, table_id: str, gov: BigQueryGovernance, call: dict[str, Any]) -> None:
    """Create an empty ``table_id`` with the captured schema (policy tags kept),
    partitioning, clustering, labels and description, then its policies."""
    from google.cloud import bigquery

    table = bigquery.Table(table_id, schema=list(gov.schema))
    if gov.time_partitioning is not None:
        table.time_partitioning = gov.time_partitioning
    if gov.range_partitioning is not None:
        table.range_partitioning = gov.range_partitioning
    if gov.clustering_fields:
        table.clustering_fields = list(gov.clustering_fields)
    if gov.labels:
        table.labels = dict(gov.labels)
    if gov.description:
        table.description = gov.description
    try:
        client.create_table(table, **call)
    except Exception as exc:  # noqa: BLE001
        raise GovernanceNotPreserved(table_id, [f"create with the previous schema and policy tags: {exc}"]) from exc
    bigquery_apply(client, table_id, gov, call)


# ------------------------------------------------------------ Elasticsearch


def _es_body(raw: Any) -> dict[str, Any]:
    """The JSON body of an elasticsearch-py response.

    elasticsearch-py 8 returns ``ObjectApiResponse`` (not a ``dict``); its
    payload is ``.body``. Reading only dicts made every index look alias-less.
    """
    body = getattr(raw, "body", raw)
    if isinstance(body, Mapping):
        return dict(body)
    try:
        return dict(body or {})
    except (TypeError, ValueError):
        return {}


def elasticsearch_aliases(client: Any, index: str) -> list[dict[str, Any]]:
    """The aliases of ``index`` as ``update_aliases`` add actions."""
    raw = _es_body(client.indices.get_alias(index=index))
    entry = raw.get(index)
    if entry is None and len(raw) == 1:
        # An alias-resolved name answers under the concrete index name.
        entry = next(iter(raw.values()))
    body = _es_body(entry or {})
    actions: list[dict[str, Any]] = []
    for alias, spec in _es_body(body.get("aliases") or {}).items():
        add = {"index": index, "alias": alias}
        spec = _es_body(spec)
        for key in ("filter", "routing", "index_routing", "search_routing", "is_write_index", "is_hidden"):
            if key in spec:
                add[key] = spec[key]
        actions.append({"add": add})
    return actions


def elasticsearch_restore_aliases(client: Any, index: str, actions: list[dict[str, Any]]) -> None:
    """Re-add the aliases, then read them back: a missing alias fails the restore.

    A filtered alias is often the access boundary readers go through; a
    restore that silently dropped it would report success on an index nobody
    can read the way they did before.
    """
    if not actions:
        return
    for action in actions:
        action["add"]["index"] = index
    client.indices.update_aliases(actions=actions)
    wanted = {a["add"]["alias"]: a["add"] for a in actions}
    present = {a["add"]["alias"]: a["add"] for a in elasticsearch_aliases(client, index)}
    missing = sorted(
        name for name, spec in wanted.items()
        if name not in present or present[name].get("filter") != spec.get("filter")
    )
    if missing:
        raise GovernanceNotPreserved(index, [f"alias(es) {missing} were not re-added"])


# ----------------------------------------------------------------- Validate


def describe_overwrite_governance(db_type: str) -> dict[str, Any] | None:
    """What an overwrite of an existing object keeps, per engine (Validate).

    ``requires_approval`` is True only where a loss is possible on the
    success path; the runtime check (``retire_set_aside``) measures it on the
    live object and refuses without the approval.
    """
    dt = (db_type or "").lower().strip()
    if dt == "snowflake":
        return {
            "engine": "snowflake",
            "mechanism": (
                "The live table is kept: its grants, policies and tags are read first, a zero-copy "
                "CLONE (TRANSIENT for a transient table) is the backup and is given the same grants, "
                "policies and tags, then the live table is emptied with TRUNCATE and loaded in place."
            ),
            "preserved": [
                "grants", "masking and row access policies", "object tags", "streams on the table",
                "clustering key, comments and other table properties",
            ],
            "on_failure": (
                "A failed run puts the previous rows back with INSERT OVERWRITE when the table has no "
                "masking or row access policy. Otherwise it swaps the governed backup clone in "
                "(ALTER TABLE ... SWAP WITH); a stream on the table then follows the swapped-out "
                "object and goes stale. If that cannot finish, both tables stay and the error names "
                "the exact SWAP / DROP statements that put the previous rows back."
            ),
            "not_preserved": [],
            "notes": [
                "Grants, policies or tags the connector's role cannot read block Validate and refuse "
                "the overwrite before any change, unless the destination DDL is approved.",
                "A TEMPORARY table is refused: its backup could not outlive the session.",
                "TRUNCATE plus reload reaches stream consumers as delete-all and insert-all.",
                "Columns the table has and the run does not write stay (NULL); a type the "
                "table cannot take fails the run and restores the previous rows.",
            ],
            "requires_approval": False,
        }
    if dt == "bigquery":
        return {
            "engine": "bigquery",
            "mechanism": (
                "The live table is copied to a backup with a copy job and recreated empty with its "
                "schema (policy tags included), partitioning, clustering, labels and description; "
                "its row access policies and table IAM policy are re-created before any row is written."
            ),
            "preserved": [
                "column policy tags", "row access policies", "table IAM policy", "labels and description",
                "partitioning and clustering",
            ],
            "not_preserved": [
                "the previous table's time-travel history and change history (APPENDS/CHANGES)",
                "search and vector indexes and table constraints on the previous table",
            ],
            "on_failure": (
                "A failed run copies the backup back under the live name and re-creates the same "
                "row access policies and IAM policy on it; between the copy and the re-creation "
                "the restored table carries only dataset-level access."
            ),
            "notes": [
                "When the connector cannot read or re-create the table's IAM policy or row access "
                "policies, Execute refuses the overwrite before anything changes unless destination "
                "DDL approval was given at Validate.",
            ],
            "requires_approval": True,
        }
    if dt == "elasticsearch":
        return {
            "engine": "elasticsearch",
            "mechanism": (
                "The live index is kept: it is cloned to a write-blocked backup, emptied with "
                "delete-by-query and loaded in place."
            ),
            "preserved": ["aliases (including filtered aliases)", "index settings and lifecycle policy", "mappings"],
            "not_preserved": [],
            "on_failure": (
                "A failed run replaces the index with a clone of the backup (same settings and "
                "mappings) and re-adds the aliases it had."
            ),
            "notes": [
                "A field whose type the existing mapping cannot take fails the run and restores "
                "the previous documents.",
            ],
            "requires_approval": False,
        }
    return None
