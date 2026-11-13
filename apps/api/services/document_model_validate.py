"""The rules of a document model (``services.document_model.validate_model``).

Kept apart from the model so the dataclasses and the parser stay small and
dependency-free. Every rule reports an :class:`~services.document_model.Issue`
with a named code; nothing raises.

Checks, in the order they are reported:

* the collection name and the limits;
* the root: source known, key present, key columns exist and are unique
  (catalog proof passes, a sample only warns, nothing is an error);
* each relationship: id unique, parent known and earlier in the list, parent
  embedded (children cannot nest under a reference, a separate collection or
  a primitive), depth within ``max_depth``, the source not already on its
  ancestor chain, join present with columns on both sides and compatible
  types, ``join_origin="fk"`` backed by a foreign key, and the rule's own
  proof:

  - ``embed_array``: the parent's join columns are unique (the child is the
    many side) and ``child_key`` is unique; no ``child_key`` is count-only,
    a warning, or an error for change capture;
  - ``embed_object`` and ``reference`` by id: the child's join columns are
    unique (1:1 or N:1);
  - ``reference`` as an id array: like ``embed_array``, with a ``child_key``;
  - ``primitive``: embedded rules only, exactly one field;
  - ``merge_into_parent``: ``embed_object`` only, no path, no collision;

* paths, per document container (the root document and each array
  element): every path follows ``services.document_paths``, ``_id`` is the
  root key's, and no two paths collide.

Uniqueness proof comes from a primary key, a unique key, or a foreign key
that references the columns (a foreign key can only reference a unique key).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from services import document_paths as dp
from services.document_model import (
    ERROR,
    MAX_DEPTH_HARD_CAP,
    MONGODB_MAX_BSON_BYTES,
    ROOT_ID,
    WARNING,
    DocumentModel,
    FieldSpec,
    Issue,
    Relationship,
    SourceTable,
)

PROVEN = "proven"
SAMPLE = "sample"
NONE = "none"


@dataclass(frozen=True)
class _Fk:
    owner: str
    columns: tuple[str, ...]
    referenced: str
    referenced_columns: tuple[str, ...]


def _qualified(schema: Any, table: Any) -> str:
    schema = str(schema or "").strip()
    table = str(table or "").strip()
    return f"{schema}.{table}" if schema else table


def _same_table(a: str, b: str) -> bool:
    """Same table, tolerating a schema named on one side only."""
    a, b = a.casefold(), b.casefold()
    if a == b:
        return True
    if "." in a and "." in b:
        return False
    return a.rsplit(".", 1)[-1] == b.rsplit(".", 1)[-1]


def _attr(obj: Any, name: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name)
    return getattr(obj, name, None)


def _one_fk(owner: str, fk: Any) -> _Fk:
    return _Fk(
        owner=owner,
        columns=tuple(str(c) for c in (_attr(fk, "columns") or ())),
        referenced=_qualified(_attr(fk, "referenced_schema"), _attr(fk, "referenced_table")),
        referenced_columns=tuple(str(c) for c in (_attr(fk, "referenced_columns") or ())),
    )


def normalize_foreign_keys(fks: Any) -> list[_Fk]:
    """Foreign keys as ``(owner, columns, referenced, referenced_columns)``.

    Accepts a mapping of owning table to its keys, or an iterable of measured
    ``ForeignKeys`` sets (or their dicts) that name their own table.
    """
    if not fks:
        return []
    out: list[_Fk] = []
    if isinstance(fks, Mapping):
        for owner, items in fks.items():
            out.extend(_one_fk(str(owner), fk) for fk in (items or ()))
        return out
    for group in fks:
        owner = _qualified(_attr(group, "schema"), _attr(group, "table"))
        out.extend(_one_fk(owner, fk) for fk in (_attr(group, "items") or ()))
    return out


_NUMERIC_EXACT = frozenset({"integer", "decimal"})
_TEXT_EXACT = frozenset({"string", "text"})
_TEXT_LIKE = frozenset({"string", "text", "uuid", "objectid"})
_NUMERIC_LIKE = frozenset({"integer", "decimal", "float"})


def join_type_verdict(parent_type: str, child_type: str) -> tuple[str, str] | None:
    """``None`` when the join compares cleanly, else ``(severity, reason)``.

    Types are read through the product's own logical-type normaliser
    (``type_system.normalize_logical_type``), so ``int4``, ``INTEGER`` and
    ``NUMBER(10,0)`` are one kind. An undeclared type is not compared.
    """
    if not str(parent_type or "").strip() or not str(child_type or "").strip():
        return None
    from services.type_system import normalize_logical_type

    a = normalize_logical_type(parent_type)
    b = normalize_logical_type(child_type)
    if a == b:
        if a == "float":
            return WARNING, "floating-point values may not compare exactly"
        return None
    if {a, b} <= _NUMERIC_EXACT or {a, b} <= _TEXT_EXACT:
        return None
    if {a, b} <= _TEXT_LIKE:
        return WARNING, f"{a} and {b} values will be compared as text"
    if {a, b} <= _NUMERIC_LIKE:
        return WARNING, "floating-point values may not compare exactly"
    return ERROR, f"{a} values cannot be matched with {b} values"


@dataclass
class _Node:
    """Where a resolved model node sits: depth, ancestor sources, children."""

    depth: int
    chain: tuple[str, ...]
    source: str
    nests_children: bool
    # Container that receives this node's children, and the prefix inside it.
    container: str | None = None
    prefix: str = ""


@dataclass
class _Entry:
    path: str
    kind: str
    where: str
    merged_from: str = ""


@dataclass
class _Checker:
    model: DocumentModel
    tables: dict[str, SourceTable]
    fks: list[_Fk]
    cdc: bool
    issues: list[Issue] = field(default_factory=list)
    nodes: dict[str, _Node | None] = field(default_factory=dict)
    containers: dict[str, list[_Entry]] = field(default_factory=dict)
    container_names: dict[str, str] = field(default_factory=dict)

    # -- helpers ---------------------------------------------------------------

    def add(self, code: str, severity: str, path: str, message: str) -> None:
        self.issues.append(Issue(code=code, severity=severity, path=path, message=message))

    def table(self, name: str) -> SourceTable | None:
        if name in self.tables:
            return self.tables[name]
        for known, table in self.tables.items():
            if _same_table(known, name):
                return table
        return None

    def columns_exist(self, source: str, cols: Iterable[str], where: str) -> bool:
        """Report every missing column; True when all exist (or the table is unknown)."""
        table = self.table(source)
        if table is None:
            return False
        ok = True
        for i, col in enumerate(cols):
            if col not in table.columns:
                ok = False
                self.add(
                    "column_unknown",
                    ERROR,
                    where.format(i=i),
                    f"Column {col!r} is not in {source}.",
                )
        return ok

    def uniqueness(self, source: str, cols: Iterable[str]) -> str:
        table = self.table(source)
        wanted = set(cols)
        if table is None or not wanted:
            return NONE
        if table.primary_key and set(table.primary_key) <= wanted:
            return PROVEN
        if any(set(key) <= wanted for key in table.unique_keys):
            return PROVEN
        for fk in self.fks:
            if (
                fk.referenced_columns
                and _same_table(fk.referenced, source)
                and set(fk.referenced_columns) <= wanted
            ):
                return PROVEN
        if any(set(key) <= wanted for key in table.sampled_unique):
            return SAMPLE
        return NONE

    def fk_backs_join(self, parent: str, child: str, rel: Relationship) -> bool:
        child_refs = {(j.child, j.parent) for j in rel.join}
        parent_refs = {(j.parent, j.child) for j in rel.join}
        for fk in self.fks:
            pairs = set(zip(fk.columns, fk.referenced_columns, strict=False))
            if _same_table(fk.owner, child) and _same_table(fk.referenced, parent):
                if pairs == child_refs:
                    return True
            if _same_table(fk.owner, parent) and _same_table(fk.referenced, child):
                if pairs == parent_refs:
                    return True
        return False

    def expanded_fields(
        self, source: str, fields: tuple[FieldSpec, ...], at: str, drop: set[str]
    ) -> list[tuple[FieldSpec, str]]:
        """Listed fields with their locations; none listed means every column."""
        if fields:
            return [(f, f"{at}.fields[{j}].path") for j, f in enumerate(fields)]
        table = self.table(source)
        if table is None:
            return []
        return [
            (FieldSpec(source=c, path=c), f"{at}.fields")
            for c in table.columns
            if c not in drop
        ]

    def proof(
        self,
        verdict: str,
        *,
        code: str,
        where: str,
        failed: str,
        sampled: str,
    ) -> None:
        if verdict == PROVEN:
            return
        if verdict == SAMPLE:
            self.add(f"{code}_unique_by_sample_only", WARNING, where, sampled)
        else:
            self.add(f"{code}_not_unique", ERROR, where, failed)

    # -- model-level -----------------------------------------------------------

    def check_collection_and_limits(self) -> None:
        name = self.model.collection
        bad = (
            "$" in name
            or "\x00" in name
            or name.startswith("system.")
            or len(name.encode("utf-8")) > 255
        )
        if bad:
            self.add(
                "collection_name_invalid",
                ERROR,
                "collection",
                f"{name!r} cannot be a MongoDB collection name: it may not contain "
                "'$' or a NUL character, start with 'system.', or exceed 255 bytes.",
            )
        limits = self.model.limits
        if limits.max_depth > MAX_DEPTH_HARD_CAP:
            self.add(
                "max_depth_over_cap",
                ERROR,
                "limits.max_depth",
                f"Nesting depth {limits.max_depth} is above the limit of "
                f"{MAX_DEPTH_HARD_CAP} levels.",
            )
        if limits.max_doc_bytes > MONGODB_MAX_BSON_BYTES:
            self.add(
                "max_doc_bytes_over_limit",
                ERROR,
                "limits.max_doc_bytes",
                f"Document size limit {limits.max_doc_bytes:,} bytes is above "
                f"MongoDB's maximum of {MONGODB_MAX_BSON_BYTES:,} bytes.",
            )

    # -- root ------------------------------------------------------------------

    def check_root(self) -> None:
        root = self.model.root
        table = self.table(root.source)
        if table is None:
            self.add(
                "root_source_unknown",
                ERROR,
                "root.source",
                f"The root table {root.source} was not found in the source.",
            )
        if not root.key:
            self.add(
                "root_key_missing",
                ERROR,
                "root.key",
                f"Choose the column or columns that identify each {root.source} row; "
                "they become the document's _id.",
            )
        elif self.columns_exist(root.source, root.key, "root.key[{i}]"):
            cols = ", ".join(root.key)
            self.proof(
                self.uniqueness(root.source, root.key),
                code="root_key",
                where="root.key",
                failed=f"({cols}) is not a primary or unique key of {root.source}, so "
                "two rows could produce the same document id.",
                sampled=f"({cols}) was unique in a sample of {root.source} only; no "
                "primary or unique key guarantees it. Duplicate keys are quarantined.",
            )
        if root.fields:
            self.columns_exist(
                root.source, (f.source for f in root.fields), "root.fields[{i}].source"
            )
        self.nodes[ROOT_ID] = _Node(
            depth=0,
            chain=(root.source,),
            source=root.source,
            nests_children=True,
            container=ROOT_ID,
            prefix="",
        )
        self.containers[ROOT_ID] = [_Entry("_id", dp.SCALAR, "root.id")]
        self.container_names[ROOT_ID] = "the document"
        for f, where in self.expanded_fields(root.source, root.fields, "root", set()):
            self.place(ROOT_ID, "", f.path, dp.SCALAR, where)

    # -- relationships ---------------------------------------------------------

    def resolve_parent(self, i: int, rel: Relationship, seen: set[str]) -> _Node | None:
        at = f"relationships[{i}]"
        if rel.id == ROOT_ID:
            self.add(
                "relationship_id_reserved",
                ERROR,
                f"{at}.id",
                "'root' names the root table; give this relationship another id.",
            )
        elif rel.id in seen:
            self.add(
                "duplicate_relationship_id",
                ERROR,
                f"{at}.id",
                f"Two relationships are named {rel.id!r}; each needs its own id.",
            )
        if rel.parent in self.nodes:
            return self.nodes[rel.parent]
        later = {r.id for r in self.model.relationships[i + 1 :]}
        if rel.parent in later:
            self.add(
                "parent_not_earlier",
                ERROR,
                f"{at}.parent",
                f"{rel.id!r} nests under {rel.parent!r}, which is defined later; "
                "list a parent before its children.",
            )
        else:
            self.add(
                "parent_unknown",
                ERROR,
                f"{at}.parent",
                f"{rel.id!r} nests under {rel.parent!r}, which is not 'root' or "
                "another relationship.",
            )
        return None

    def check_relationship(self, i: int, rel: Relationship, seen: set[str]) -> None:
        at = f"relationships[{i}]"
        parent = self.resolve_parent(i, rel, seen)
        register = rel.id != ROOT_ID and rel.id not in seen
        seen.add(rel.id)
        if parent is None:
            if register:
                self.nodes[rel.id] = None
            return
        attached = parent.nests_children
        if not attached:
            self.add(
                "parent_not_embedded",
                ERROR,
                f"{at}.parent",
                f"{rel.id!r} cannot nest under {rel.parent!r}: only the root, an "
                "embedded array of objects or an embedded object can hold children.",
            )
        depth = parent.depth + 1
        max_depth = min(self.model.limits.max_depth, MAX_DEPTH_HARD_CAP)
        if depth > max_depth:
            self.add(
                "too_deep",
                ERROR,
                at,
                f"{rel.id!r} is nested {depth} levels deep; this model allows "
                f"{max_depth}.",
            )
        if any(_same_table(src, rel.source) for src in parent.chain):
            self.add(
                "recursive_source",
                ERROR,
                f"{at}.source",
                f"{rel.source} already appears above {rel.id!r} in the same branch; "
                "a table cannot be nested inside itself.",
            )
        child_table = self.table(rel.source)
        if child_table is None:
            self.add(
                "source_unknown",
                ERROR,
                f"{at}.source",
                f"Table {rel.source} (used by {rel.id!r}) was not found in the source.",
            )
        joins_known = self.check_join(i, rel, parent.source)
        if rel.fields:
            self.columns_exist(rel.source, (f.source for f in rel.fields), at + ".fields[{i}].source")
        key_known = bool(rel.child_key) and self.columns_exist(
            rel.source, rel.child_key, at + ".child_key[{i}]"
        )
        if rel.options.order_by:
            self.columns_exist(
                rel.source, (c for c, _ in rel.options.order_by), at + ".options.order_by[{i}]"
            )
        if joins_known:
            self.check_rule_proof(i, rel, parent.source, key_known)
        drop = {j.child for j in rel.join} if rel.options.drop_join_keys else set()
        fields = self.expanded_fields(rel.source, rel.fields, at, drop)
        self.check_options(i, rel, fields, child_table is not None)
        node = _Node(
            depth=depth,
            chain=parent.chain + (rel.source,),
            source=rel.source,
            nests_children=False,
        )
        if register:
            self.nodes[rel.id] = node
        if attached:
            self.place_relationship(i, rel, parent, node, fields)

    def check_join(self, i: int, rel: Relationship, parent_source: str) -> bool:
        at = f"relationships[{i}]"
        if not rel.join:
            self.add(
                "join_missing",
                ERROR,
                f"{at}.join",
                f"Say how {rel.source} rows match their parent: add at least one "
                "pair of join columns.",
            )
            return False
        parent_ok = self.columns_exist(
            parent_source, (j.parent for j in rel.join), at + ".join[{i}].parent"
        )
        child_ok = self.columns_exist(
            rel.source, (j.child for j in rel.join), at + ".join[{i}].child"
        )
        if not (parent_ok and child_ok):
            return False
        parent_table = self.table(parent_source)
        child_table = self.table(rel.source)
        if parent_table is None or child_table is None:
            return False
        for k, pair in enumerate(rel.join):
            verdict = join_type_verdict(
                parent_table.columns.get(pair.parent, ""),
                child_table.columns.get(pair.child, ""),
            )
            if verdict is None:
                continue
            severity, reason = verdict
            self.add(
                "join_type_mismatch" if severity == ERROR else "join_type_lossy",
                severity,
                f"{at}.join[{k}]",
                f"{parent_source}.{pair.parent} joins {rel.source}.{pair.child}, "
                f"but {reason}.",
            )
        if rel.join_origin == "fk" and not self.fk_backs_join(parent_source, rel.source, rel):
            self.add(
                "join_fk_missing",
                ERROR,
                f"{at}.join_origin",
                f"No foreign key links {rel.source} and {parent_source} on exactly "
                "these columns; mark the join as declared or fix the columns.",
            )
        return True

    def check_child_key(self, i: int, rel: Relationship, key_known: bool) -> None:
        at = f"relationships[{i}]"
        if not rel.child_key:
            self.add(
                "child_key_missing",
                ERROR if self.cdc else WARNING,
                f"{at}.child_key",
                (
                    f"{rel.id!r} has no child key, so change capture cannot tell "
                    "which element changed; choose the column or columns that "
                    f"identify each {rel.source} row."
                    if self.cdc
                    else f"{rel.id!r} has no child key: its elements can only be "
                    "verified by count, not one by one, and change capture is not "
                    "available. Choose the column or columns that identify each "
                    f"{rel.source} row."
                ),
            )
            return
        if not key_known:
            return
        cols = ", ".join(rel.child_key)
        self.proof(
            self.uniqueness(rel.source, rel.child_key),
            code="child_key",
            where=f"{at}.child_key",
            failed=f"Child key ({cols}) is not a primary or unique key of {rel.source}.",
            sampled=f"Child key ({cols}) was unique in a sample of {rel.source} only; "
            "no primary or unique key guarantees it.",
        )

    def check_rule_proof(
        self, i: int, rel: Relationship, parent_source: str, key_known: bool
    ) -> None:
        at = f"relationships[{i}]"
        parent_cols = [j.parent for j in rel.join]
        child_cols = [j.child for j in rel.join]
        pcols = ", ".join(parent_cols)
        ccols = ", ".join(child_cols)
        one_parent = (
            f"Each {rel.source} row must belong to one {parent_source} row, but "
            f"({pcols}) is not a primary or unique key of {parent_source}"
        )
        one_parent_sample = (
            f"({pcols}) was unique in a sample of {parent_source} only; no primary "
            "or unique key guarantees each child has one parent."
        )
        one_child = (
            f"Each {parent_source} row must match at most one {rel.source} row, but "
            f"({ccols}) is not a primary or unique key of {rel.source}"
        )
        one_child_sample = (
            f"({ccols}) was unique in a sample of {rel.source} only; no primary or "
            "unique key guarantees at most one match."
        )
        if rel.rule == "embed_array":
            self.proof(
                self.uniqueness(parent_source, parent_cols),
                code="embed_array_parent",
                where=f"{at}.join",
                failed=one_parent + "; to attach one matching row, use embed_object.",
                sampled=one_parent_sample,
            )
            self.check_child_key(i, rel, key_known)
        elif rel.rule == "embed_object":
            self.proof(
                self.uniqueness(rel.source, child_cols),
                code="embed_object",
                where=f"{at}.join",
                failed=one_child + "; to embed every match, use embed_array.",
                sampled=one_child_sample,
            )
        elif rel.rule == "reference" and rel.options.reference_shape == "ids_array":
            self.proof(
                self.uniqueness(parent_source, parent_cols),
                code="reference_parent",
                where=f"{at}.join",
                failed=one_parent + ".",
                sampled=one_parent_sample,
            )
            if not rel.child_key:
                self.add(
                    "reference_ids_need_child_key",
                    ERROR,
                    f"{at}.child_key",
                    f"A list of references needs a child key: the column or columns "
                    f"that identify each {rel.source} row.",
                )
            else:
                self.check_child_key(i, rel, key_known)
        elif rel.rule == "reference":
            self.proof(
                self.uniqueness(rel.source, child_cols),
                code="reference",
                where=f"{at}.join",
                failed=one_child + "; to reference every match, use a list of ids.",
                sampled=one_child_sample,
            )

    def check_options(
        self,
        i: int,
        rel: Relationship,
        fields: list[tuple[FieldSpec, str]],
        table_known: bool,
    ) -> None:
        at = f"relationships[{i}]"
        opts = rel.options
        embedded = rel.rule in ("embed_array", "embed_object")
        if opts.primitive:
            if not embedded:
                self.add(
                    "primitive_not_supported",
                    ERROR,
                    f"{at}.options.primitive",
                    f"{rel.id!r} is a {rel.rule}; only embedded arrays and objects can "
                    "hold a single value instead of an object.",
                )
            elif (rel.fields or table_known) and len(fields) != 1:
                self.add(
                    "primitive_needs_one_field",
                    ERROR,
                    f"{at}.fields",
                    f"{rel.id!r} stores a single value, so it needs exactly one field; "
                    f"it has {len(fields)}.",
                )
        if opts.merge_into_parent:
            if rel.rule != "embed_object" or opts.primitive:
                self.add(
                    "merge_into_parent_not_object",
                    ERROR,
                    f"{at}.options.merge_into_parent",
                    f"Only an embedded object of fields can be merged into its parent; "
                    f"{rel.id!r} is {'a single value' if opts.primitive else 'a ' + rel.rule}.",
                )
            elif rel.path:
                self.add(
                    "merge_into_parent_has_path",
                    ERROR,
                    f"{at}.path",
                    f"{rel.id!r} merges its fields into the parent, so it takes no path; "
                    "clear the path or turn off merging.",
                )
        needs_path = rel.rule in ("embed_array", "reference") or (
            rel.rule == "embed_object" and not opts.merge_into_parent
        )
        if needs_path and not rel.path:
            self.add(
                "path_missing",
                ERROR,
                f"{at}.path",
                f"Choose where {rel.id!r} goes in the parent document.",
            )
        if opts.order_by and rel.rule != "embed_array":
            self.add(
                "option_ignored",
                WARNING,
                f"{at}.options.order_by",
                f"{rel.id!r} is not an array, so its sort order is not used.",
            )

    # -- paths -----------------------------------------------------------------

    def valid_piece(self, container: str, prefix: str, path: str, where: str) -> bool:
        """Report every rule ``path`` breaks when placed at ``prefix`` in ``container``."""
        top = container == ROOT_ID and prefix == ""
        if top and path.split(".")[0] == dp.ID_FIELD:
            self.add(
                "id_path_reserved",
                ERROR,
                where,
                f"{path!r} is the document id, which is set from the root key; "
                "choose another path.",
            )
            return False
        problems = dp.path_issues(path, allow_id=top)
        for problem in problems:
            self.add(problem.code, ERROR, where, problem.message)
        return not problems

    def place(
        self,
        container: str,
        prefix: str,
        path: str,
        kind: str,
        where: str,
        *,
        merged_from: str = "",
    ) -> bool:
        """Add ``prefix + path`` to a container when ``path`` is valid there."""
        if not self.valid_piece(container, prefix, path, where):
            return False
        self.containers[container].append(
            _Entry(prefix + path, kind, where, merged_from=merged_from)
        )
        return True

    def place_relationship(
        self,
        i: int,
        rel: Relationship,
        parent: _Node,
        node: _Node,
        fields: list[tuple[FieldSpec, str]],
    ) -> None:
        at = f"relationships[{i}]"
        container, prefix = parent.container, parent.prefix
        if container is None:
            return
        opts = rel.options
        if rel.rule == "collection":
            return
        if rel.rule == "reference":
            kind = dp.ARRAY if opts.reference_shape == "ids_array" else dp.SCALAR
            if rel.path:
                self.place(container, prefix, rel.path, kind, f"{at}.path")
            return
        if rel.rule == "embed_array":
            if rel.path:
                self.place(container, prefix, rel.path, dp.ARRAY, f"{at}.path")
            if opts.primitive or opts.merge_into_parent:
                return
            # Keyed by position, not id: a duplicate id must not merge two
            # arrays' element namespaces (the duplicate is reported already).
            elements = at
            self.containers[elements] = []
            self.container_names[elements] = f"each {rel.id!r} element"
            node.nests_children = True
            node.container, node.prefix = elements, ""
            for f, where in fields:
                self.place(elements, "", f.path, dp.SCALAR, where)
            return
        # embed_object
        if opts.primitive:
            if rel.path and not opts.merge_into_parent:
                self.place(container, prefix, rel.path, dp.SCALAR, f"{at}.path")
            return
        if opts.merge_into_parent:
            node.nests_children = True
            node.container, node.prefix = container, prefix
            for f, where in fields:
                self.place(container, prefix, f.path, dp.SCALAR, where, merged_from=rel.id)
            return
        if not rel.path or not self.valid_piece(container, prefix, rel.path, f"{at}.path"):
            return
        inner = f"{prefix}{rel.path}."
        node.nests_children = True
        node.container, node.prefix = container, inner
        if not fields:
            self.containers[container].append(
                _Entry(prefix + rel.path, dp.SCALAR, f"{at}.path")
            )
        for f, where in fields:
            self.place(container, inner, f.path, dp.SCALAR, where)

    def check_collisions(self) -> None:
        for container, entries in self.containers.items():
            where_by_path: dict[str, list[_Entry]] = {}
            for e in entries:
                where_by_path.setdefault(e.path, []).append(e)
            dup_seen: dict[str, int] = {}
            label = self.container_names[container]
            for problem in dp.collision_issues((e.path, e.kind) for e in entries):
                if problem.code == "path_duplicate":
                    n = dup_seen.get(problem.path, 0) + 1
                    dup_seen[problem.path] = n
                    involved = [where_by_path[problem.path][n], where_by_path[problem.path][0]]
                else:
                    involved = [where_by_path[problem.path][0], where_by_path[problem.other][0]]
                merged = next((e for e in involved if e.merged_from), None)
                if merged is not None:
                    self.add(
                        "merge_into_parent_collision",
                        ERROR,
                        merged.where,
                        f"Merging {merged.merged_from!r} into its parent clashes in "
                        f"{label}: {problem.message}",
                    )
                else:
                    self.add(
                        problem.code,
                        ERROR,
                        involved[0].where,
                        f"In {label}: {problem.message}",
                    )


def run_checks(
    model: DocumentModel,
    source_schemas: Mapping[str, SourceTable | Mapping[str, Any]],
    fks: Any,
    *,
    cdc: bool = False,
) -> list:
    tables = {
        str(name): SourceTable.from_dict(raw) for name, raw in (source_schemas or {}).items()
    }
    checker = _Checker(model=model, tables=tables, fks=normalize_foreign_keys(fks), cdc=cdc)
    checker.check_collection_and_limits()
    checker.check_root()
    seen: set[str] = set()
    for i, rel in enumerate(model.relationships):
        checker.check_relationship(i, rel, seen)
    checker.check_collisions()
    return checker.issues


__all__ = ["join_type_verdict", "normalize_foreign_keys", "run_checks"]
