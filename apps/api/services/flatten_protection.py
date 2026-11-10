"""A protected parent protects every column the flattener derived from it (D349).

A MongoDB (or DynamoDB) document field ``payload: {k, nested: {a: [...]}, s}``
is read as ``payload`` plus the columns ``payload_k``, ``payload_nested``,
``payload_nested_a`` and ``payload_s``, each holding part of its content.
``payload: redact`` used to stamp the parent alone, so the destination held
``"<redacted>"`` beside every value it was meant to hide.

A child is a column the flattener itself named under the parent — recorded by
``flatten_document(lineage=...)`` on the parent's own value — never a column
that merely shares the prefix: a real ``payload_id`` field is left alone.

What a child inherits (``child_protection``):

* ``redact`` → ``redact``; ``omit`` → ``omit``; ``hash_pii`` → ``hash_pii``
  (an HMAC of the child's text, defined for every type);
* ``mask_pii`` → ``mask_pii`` on a scalar child, ``redact`` on an object or
  array child: ``mask_pii`` masks a structured value only where a PII pattern
  matches, so ``[1,2,6]`` would be written in clear.

A redacted, masked or hashed child is text, so a numeric / boolean / temporal
child is planned onto a text carrier (a JSON child keeps its JSON carrier and
holds the token as a JSON string, exactly as the parent does).

An operator who explicitly gives a child of a protected parent a transform that
writes it in clear (``column_transforms {"payload_k": "trim"}``, a replay
``transform_overrides``) is refused, never silently overruled either way.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

#: Transforms that keep a column's content out of the destination: the write
#: path's governance ids (``governance_ops.WRITE_PATH_IDS``) plus ``omit``.
PROTECTING_TRANSFORMS = frozenset({"redact", "omit", "mask_pii", "hash_pii"})

#: Source engines whose reader expands document fields into ``parent_child``
#: columns (``expand_mongo_documents`` / ``expand_dynamo_documents``).
FLATTENING_SOURCES = frozenset({"mongodb", "dynamodb"})


@dataclass
class FlattenProtection:
    mappings: list[dict[str, Any]]
    #: ``[{"column", "parent", "transform"}]`` — every child this stamped.
    inherited: list[dict[str, str]] = field(default_factory=list)
    refusal: str = ""


def protecting_transform(mapping: Mapping[str, Any] | None) -> str:
    """The engine id of the protection a mapping applies, or ``""``."""
    if not isinstance(mapping, Mapping):
        return ""
    from services.mapping_constraints import is_intentional_omit

    if is_intentional_omit(mapping):
        return "omit"
    return protecting_spec(mapping.get("transform"))


def protecting_spec(spec: Any) -> str:
    """The protecting engine id a transform spec resolves to, or ``""``.

    A multi-step pipeline protects when one of its steps does: every step
    after a one-way step only sees its token / digest / mask.
    """
    from services.column_transform_chain import chain_steps
    from services.transform_engine import normalize_engine_transform

    steps = chain_steps(spec)
    for step in steps if steps is not None else [spec]:
        resolved = normalize_engine_transform(str(step or "")) or ""
        if resolved in PROTECTING_TRANSFORMS:
            return resolved
    return ""


def source_flattens(source_type: str, cfg: Mapping[str, Any] | None = None) -> bool:
    """True when this source's reader expands document fields into columns."""
    from services.json_intelligence import mongo_flatten_enabled
    from src.transfer.connector_capabilities import resolve_driver_type

    kind = str(source_type or "").strip().lower()
    if not kind or resolve_driver_type(kind) not in FLATTENING_SOURCES:
        return False
    return mongo_flatten_enabled(dict(cfg) if isinstance(cfg, Mapping) else None)


def flatten_lineage(rows: Iterable[Any] | None) -> dict[str, str]:
    """``{child column: the column it was derived from}`` over sampled rows.

    Works on raw documents and on the reader's expanded rows (where a parent
    is its JSON text): each object-valued field is re-flattened on its own
    value, so the names are the flattener's, not a prefix guess. In an
    expanded row a nested object (``payload_nested``) is itself a child; its
    children are taken from the walk of its root so the depth limit holds.
    """
    from services.json_intelligence import flatten_document, parse_json_object

    lineage: dict[str, str] = {}
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        walks: dict[str, dict[str, str]] = {}
        for key, value in row.items():
            obj = value if isinstance(value, dict) else parse_json_object(value)
            if not obj:
                continue
            local: dict[str, str] = {}
            flatten_document({str(key): obj}, lineage=local)
            walks[str(key)] = local
        derived = {name for local in walks.values() for name in local}
        for key, local in walks.items():
            if key in derived:
                continue
            for child, parent in local.items():
                lineage.setdefault(child, parent)
    return lineage


def _descendants(lineage: Mapping[str, str]) -> dict[str, list[str]]:
    """``{column (lower): every column derived from it, at any depth}``."""
    children: dict[str, list[str]] = {}
    for child, parent in lineage.items():
        if child and parent and child.lower() != parent.lower():
            children.setdefault(parent.lower(), []).append(child)
    out: dict[str, list[str]] = {}
    for parent, direct in children.items():
        seen: dict[str, str] = {}
        stack = list(direct)
        while stack:
            col = stack.pop()
            if col.lower() not in seen:
                seen[col.lower()] = col
                stack.extend(children.get(col.lower(), []))
        out[parent] = list(seen.values())
    return out


def _is_structured(
    mapping: Mapping[str, Any], column_types: Mapping[str, str] | None = None
) -> bool:
    """An object / array child (by its mapping's types or the source schema)."""
    from services.type_system import is_structural_type

    declared = [str(mapping.get(k) or "") for k in ("source_type", "target_type")]
    declared.append(str((column_types or {}).get(str(mapping.get("source") or "")) or ""))
    return any(is_structural_type(t) for t in declared if t.strip())


def child_protection(
    parent_transform: str,
    child: Mapping[str, Any],
    column_types: Mapping[str, str] | None = None,
) -> str:
    """The protection a child of a ``parent_transform`` parent is written with."""
    if parent_transform == "mask_pii" and _is_structured(child, column_types):
        return "redact"
    return parent_transform


def _text_carrier(row: dict[str, Any], column_types: Mapping[str, str] | None) -> None:
    """A redacted / masked / hashed child is text: never a numeric carrier."""
    from services.type_system import LOGICAL_TEXT, normalize_logical_type

    target = str(row.get("target_type") or "").strip()
    if _is_structured(row, column_types) or (
        target and normalize_logical_type(target) == LOGICAL_TEXT
    ):
        return
    row["target_type"] = "TEXT"


def explicit_unprotect_refusal(column: str, spec: Any, parent: str, parent_tf: str) -> str:
    return (
        f"{column} is derived from {parent} (the reader flattens {parent} into "
        f"{column}), and {parent} is {parent_tf}: transform {str(spec or 'none')!r} "
        f"on {column} would write part of {parent} in clear. Drop the transform "
        f"on {column} (it inherits {parent_tf}) or protect it with "
        "redact / omit / mask_pii / hash_pii."
    )


def protect_flatten_children(
    mappings: list[dict[str, Any]] | None,
    lineage: Mapping[str, str] | None,
    *,
    explicit: Mapping[str, Any] | None = None,
    parent_transforms: Mapping[str, str] | None = None,
    refuse_overrides: bool = False,
    column_types: Mapping[str, str] | None = None,
) -> FlattenProtection:
    """Give every flatten child of a protected parent the parent's protection.

    ``explicit``: the operator's own per-column transforms (``column_transforms``
    keys, single or pipeline); a child named there with a transform that does
    not protect it is refused, one that does keeps it. ``parent_transforms``: protected parents whose protection is not
    on their mapping (a multi-step pipeline that redacts on the read).
    ``refuse_overrides``: a child row whose ``transform_override`` names a
    non-protecting transform is refused (Execute: a replayed override).
    """
    rows = [dict(m) for m in mappings or [] if isinstance(m, Mapping)]
    result = FlattenProtection(mappings=rows)
    tree = _descendants(lineage or {})
    if not tree:
        return result
    parents: dict[str, tuple[str, str]] = {}
    for name, spec in (parent_transforms or {}).items():
        tf = protecting_spec(spec)
        if tf:
            parents[str(name).lower()] = (str(name), tf)
    for row in rows:
        source = str(row.get("source") or "")
        tf = protecting_transform(row)
        # An inherited stamp is not a parent's own choice: its children are
        # its root's descendants and inherit from the root.
        if tf and source and not row.get("protected_by"):
            parents.setdefault(source.lower(), (source, tf))
    inherit: dict[str, tuple[str, str]] = {}
    for parent_lower, (parent, tf) in parents.items():
        for child in tree.get(parent_lower, []):
            inherit.setdefault(child.lower(), (parent, tf))
    explicit_lower = {str(k).strip().lower(): (str(k), v) for k, v in (explicit or {}).items()}
    for row in rows:
        source = str(row.get("source") or "")
        if source.lower() not in inherit:
            continue
        parent, tf = inherit[source.lower()]
        named = explicit_lower.get(source.lower())
        if named is not None:
            if not protecting_spec(named[1]):
                result.refusal = explicit_unprotect_refusal(named[0], named[1], parent, tf)
                return result
            continue  # the operator protected it (a Map transform or a pipeline)
        if protecting_transform(row) or source.lower() in parents:
            continue  # its own protection (Map, pipeline, earlier stamp) stands
        if refuse_overrides and row.get("transform_override"):
            result.refusal = explicit_unprotect_refusal(source, row.get("transform"), parent, tf)
            return result
        inherited = child_protection(tf, row, column_types)
        row["transform"] = inherited
        row["transform_override"] = True
        row["protected_by"] = parent
        if inherited != "omit":
            _text_carrier(row, column_types)
        result.inherited.append({"column": source, "parent": parent, "transform": inherited})
    return result


def stamped_lineage(mappings: Iterable[Any] | None) -> dict[str, str]:
    """Lineage a plan already proved, carried on the stored mapping rows."""
    return {
        str(m.get("source")): str(m.get("protected_by"))
        for m in mappings or []
        if isinstance(m, Mapping) and m.get("source") and m.get("protected_by")
    }


def inherited_protection_warning(mappings: Iterable[Any] | None) -> dict[str, Any] | None:
    """Preflight names the columns that inherit a parent's protection."""
    inherited = [
        {
            "column": str(m.get("source") or ""),
            "parent": str(m.get("protected_by") or ""),
            "transform": protecting_transform(m),
        }
        for m in mappings or []
        if isinstance(m, Mapping) and m.get("protected_by")
    ]
    if not inherited:
        return None
    by_parent: dict[str, list[str]] = {}
    for item in inherited:
        by_parent.setdefault(item["parent"], []).append(
            f"{item['column']} ({item['transform']})"
        )
    lines = [f"{p} → {', '.join(cols)}" for p, cols in by_parent.items()]
    return {
        "id": "flatten_children_protected",
        "message": (
            "Columns flattened from a protected field carry its content, so they "
            "inherit its protection: " + "; ".join(lines) + "."
        ),
        "details": {"inherited_protection": inherited},
    }


def _recipe_protections(recipe: Mapping[str, Any] | None) -> dict[str, str]:
    """Columns a shaping recipe protects on the read (a pipeline's redact)."""
    out: dict[str, str] = {}
    steps = recipe.get("steps") if isinstance(recipe, Mapping) else None
    for step in steps if isinstance(steps, list) else []:
        if not isinstance(step, Mapping) or step.get("enabled") is False:
            continue
        tf = protecting_spec(step.get("op"))
        column = str(step.get("column") or "")
        if tf and tf != "omit" and column:
            out.setdefault(column, tf)
    return out


def protect_planned_children(
    mappings: list[dict[str, Any]],
    *,
    source_type: str,
    source_cfg: Mapping[str, Any] | None,
    sample_rows: Iterable[Any] | None,
    introspected_lineage: Mapping[str, str] | None,
    column_transforms: Mapping[str, Any] | None,
    column_pipelines: Mapping[str, Any] | None,
) -> FlattenProtection:
    """Plan (MCP plan / start / create_schedule): stamp and name the children.

    Lineage is the introspection's (raw documents, exact) plus the reader
    sample's. The operator's own ``column_transforms`` decide a child they
    name; a pipeline that protects a parent protects its children too.
    """
    if not source_flattens(source_type, source_cfg):
        return FlattenProtection(mappings=list(mappings or []))
    lineage = dict(introspected_lineage or {})
    for child, parent in flatten_lineage(sample_rows).items():
        lineage.setdefault(child, parent)
    pipelines = dict(column_pipelines or {})
    return protect_flatten_children(
        mappings,
        lineage,
        explicit={**dict(column_transforms or {}), **pipelines},
        parent_transforms={k: v for k, v in pipelines.items() if protecting_spec(v)},
    )


def protect_run_children(
    request: Any,
    mappings: list[dict[str, Any]],
    sample_rows: Iterable[Any] | None,
    column_types: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Execute (every route: MCP, REST / Studio, schedule replay, run 2).

    Runs after Map and schema propagation, so a field that appeared since the
    plan (a new ``payload_x``) inherits too. A replayed override that would
    write a child in clear stops the run before anything is written.
    """
    source = getattr(request, "source", None)
    if not mappings or not source_flattens(
        str(getattr(source, "format", "") or ""), getattr(source, "extra", None)
    ):
        return mappings
    lineage = stamped_lineage(mappings)
    for child, parent in flatten_lineage(sample_rows).items():
        lineage.setdefault(child, parent)
    result = protect_flatten_children(
        mappings,
        lineage,
        parent_transforms=_recipe_protections(getattr(request, "shape_recipe", None)),
        refuse_overrides=True,
        column_types=column_types,
    )
    if result.refusal:
        raise ValueError(result.refusal)
    return result.mappings


def protect_discovered_children(cfg: Any, batch: Any, new_headers: list[str]) -> None:
    """Streaming: a field first seen mid-run under a protected parent inherits.

    The streaming writer maps a document field it first meets on a later page
    (``create_new``, "no silent drop"). A ``payload_late`` flattened from a
    redacted ``payload`` is that parent's content, so before this page is
    written its mapping is protected and its carrier is text. Lineage comes
    from this page's own parent values.
    """
    if not new_headers or not source_flattens(str(getattr(cfg, "src_type", "") or "")):
        return
    parents = {
        str(m.get("source") or "")
        for m in cfg.mappings or []
        if protecting_transform(m) and not m.get("protected_by")
    }
    if not parents or not any(
        h.startswith(f"{p}_") for h in new_headers for p in parents
    ):
        return
    idx = {p: batch.headers.index(p) for p in parents if p in batch.headers}
    rows = [
        {p: row[i] for p, i in idx.items() if i < len(row)}
        for row in batch.rows or []
        if isinstance(row, (list, tuple))
    ]
    result = protect_flatten_children(
        cfg.mappings, {**stamped_lineage(cfg.mappings), **flatten_lineage(rows)},
        refuse_overrides=True,
        column_types=cfg.schema,
    )
    if result.refusal:
        raise ValueError(result.refusal)
    cfg.mappings[:] = result.mappings
    from services.type_system import is_structural_type
    from src.transfer.type_mapper import ddl_carrier_type

    fresh = set(new_headers)
    for item in result.inherited:
        column = item["column"]
        if column not in fresh or item["transform"] == "omit":
            continue
        if not is_structural_type(str(cfg.schema.get(column) or "")):
            cfg.schema[column] = "TEXT"
            cfg.column_types[column] = ddl_carrier_type("TEXT")


def preview_inherited(
    mappings: list[dict[str, Any]] | None, sample_rows: Iterable[Any] | None
) -> FlattenProtection:
    """What Execute will stamp on these mappings — for Validate to name (D349).

    The source engine is the one ``run_file_preflight`` binds; a plan's own
    stamps (``protected_by``) are honoured whatever the engine.
    """
    from services.source_engine_scope import active_source_engine

    rows = list(mappings or [])
    lineage = stamped_lineage(rows)
    if source_flattens(active_source_engine()):
        for child, parent in flatten_lineage(sample_rows).items():
            lineage.setdefault(child, parent)
    if not lineage:
        return FlattenProtection(mappings=rows)
    return protect_flatten_children(rows, lineage, refuse_overrides=True)


def flatten_preflight_warnings(
    mappings: list[dict[str, Any]] | None, sample_rows: Iterable[Any] | None
) -> list[dict[str, Any]]:
    """The inherited-protection warning, or why Execute will refuse the map."""
    preview = preview_inherited(mappings, sample_rows)
    if preview.refusal:
        return [{
            "id": "flatten_child_in_clear",
            "message": preview.refusal + " Execute refuses this mapping.",
            "details": {"refusal": preview.refusal},
        }]
    warning = inherited_protection_warning(preview.mappings)
    return [warning] if warning else []
