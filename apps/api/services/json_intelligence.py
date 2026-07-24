"""JSON / nested document intelligence — flattening and type recommendations."""

from __future__ import annotations

import json
import os
import re
from typing import Any

DOT_PATH = re.compile(r"^[a-zA-Z_][\w.]*\.\w+")
JSON_PREFIX = re.compile(r"^[\[{]")

# Cap expanded nested keys so sparse Mongo docs cannot explode column counts.
MAX_FLATTENED_KEYS = 128
DEFAULT_FLATTEN_DEPTH = 2

# Explicit Map-step STRUCT / JSON / ARRAY policy (operator choice — rematch + write agree).
STRUCT_POLICY_STORE_AS_JSON = "store_as_json"
STRUCT_POLICY_FLATTEN_TOP_LEVEL = "flatten_top_level_keys"
STRUCT_POLICY_FLATTEN_DEEP = "flatten_deep"
ARRAY_POLICY_EXPLODE = "explode_rows"
VALID_STRUCT_POLICIES = frozenset({
    STRUCT_POLICY_STORE_AS_JSON,
    STRUCT_POLICY_FLATTEN_TOP_LEVEL,
    STRUCT_POLICY_FLATTEN_DEEP,
    ARRAY_POLICY_EXPLODE,
})
# Top-level flatten depth=1; deep uses DEFAULT_FLATTEN_DEPTH (capped keys).
STRUCT_FLATTEN_DEPTH = 1
STRUCT_MAX_TOP_LEVEL_KEYS = 32
# Cap row explosion so a huge array cannot OOM the transfer.
ARRAY_EXPLODE_MAX = 256


def _looks_like_json(value: Any) -> bool:
    if isinstance(value, (dict, list)):
        return True
    if not isinstance(value, str):
        return False
    s = value.strip()
    return bool(JSON_PREFIX.match(s))


def normalize_struct_policy(value: Any) -> str | None:
    """Return a valid struct/array policy id or None when unset/invalid."""
    if value is None:
        return None
    s = str(value).strip().lower()
    if s in VALID_STRUCT_POLICIES:
        return s
    if s in {"json", "blob", "variant", "json_blob", "serialize"}:
        return STRUCT_POLICY_STORE_AS_JSON
    if s in {"flatten", "flatten_keys", "top_level", "expand"}:
        return STRUCT_POLICY_FLATTEN_TOP_LEVEL
    if s in {"flatten_deep", "deep", "deep_flatten"}:
        return STRUCT_POLICY_FLATTEN_DEEP
    if s in {"explode", "explode_rows", "unnest", "array_explode"}:
        return ARRAY_POLICY_EXPLODE
    return None


def _parse_object_sample(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        parsed = json.loads(s)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def top_level_keys_from_samples(
    samples: list[Any],
    *,
    max_keys: int = STRUCT_MAX_TOP_LEVEL_KEYS,
    promotable_only: bool = True,
) -> list[str]:
    """Union of top-level object keys across JSON object samples (stable order).

    When ``promotable_only`` (default), skip nested-object values — they stay on
    the parent JSON blob under ``flatten_top_level_keys`` (max_depth=1).
    """
    seen: dict[str, None] = {}
    for raw in samples:
        obj = _parse_object_sample(raw)
        if not obj:
            continue
        for key, value in obj.items():
            name = str(key).strip()
            if not name or name in seen:
                continue
            if promotable_only and isinstance(value, dict):
                continue
            seen[name] = None
            if len(seen) >= max_keys:
                return list(seen.keys())
    return list(seen.keys())


def flatten_struct_field(
    value: Any,
    *,
    parent_key: str,
    sep: str = "_",
    max_depth: int = STRUCT_FLATTEN_DEPTH,
) -> dict[str, Any]:
    """Flatten one STRUCT/JSON object field.

    Parent is kept (serialized later) so the blob is never silently dropped.
    Arrays stay on the parent key as JSON — no row explosion here (see explode).
    """
    obj = _parse_object_sample(value)
    if obj is None:
        return {}
    return flatten_document(
        {parent_key: obj},
        sep=sep,
        max_depth=max_depth,
        keep_parent=True,
    )


def _parse_array_sample(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not (s.startswith("[") and s.endswith("]")):
        return None
    try:
        parsed = json.loads(s)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, list) else None


def apply_struct_policies_to_row(
    row: dict[str, Any],
    policies: dict[str, str],
) -> dict[str, Any]:
    """Apply per-column STRUCT flatten policies onto a row dict (not explode)."""
    if not row or not policies:
        return dict(row) if row else {}
    out = dict(row)
    for col, policy in policies.items():
        norm = normalize_struct_policy(policy)
        if norm not in {STRUCT_POLICY_FLATTEN_TOP_LEVEL, STRUCT_POLICY_FLATTEN_DEEP}:
            continue
        if col not in out or out[col] is None:
            continue
        depth = STRUCT_FLATTEN_DEPTH if norm == STRUCT_POLICY_FLATTEN_TOP_LEVEL else DEFAULT_FLATTEN_DEPTH
        flat = flatten_struct_field(out[col], parent_key=col, max_depth=depth)
        for k, v in flat.items():
            if k == col:
                continue
            if k not in out or out.get(k) is None:
                # Serialize nested leftovers for the string matrix.
                if isinstance(v, (dict, list)):
                    out[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"), default=str)
                else:
                    out[k] = v
    return out


def struct_policies_from_mappings(mappings: list[dict[str, Any]] | None) -> dict[str, str]:
    """Extract ``source → policy`` for flatten / explode columns."""
    out: dict[str, str] = {}
    for m in mappings or []:
        src = str(m.get("source") or "").strip()
        if not src:
            continue
        policy = normalize_struct_policy(m.get("struct_policy") or m.get("structPolicy"))
        if policy and policy != STRUCT_POLICY_STORE_AS_JSON:
            out[src] = policy
    return out


def materialize_struct_policies(
    headers: list[str],
    data_rows: list[list[Any]],
    mappings: list[dict[str, Any]] | None,
) -> tuple[list[str], list[list[Any]]]:
    """Expand tabular headers/rows for flatten + array explode Map choices.

    - ``flatten_top_level_keys`` / ``flatten_deep``: promote nested keys.
    - ``explode_rows``: duplicate parent row per array element (capped).
    Parent JSON blob is always kept on flatten so nothing is silently dropped.
    """
    policies = struct_policies_from_mappings(mappings)
    if not policies or not headers:
        return headers, data_rows

    flatten_policies = {
        k: v for k, v in policies.items()
        if v in {STRUCT_POLICY_FLATTEN_TOP_LEVEL, STRUCT_POLICY_FLATTEN_DEEP}
    }
    explode_cols = [k for k, v in policies.items() if v == ARRAY_POLICY_EXPLODE]

    header_list = list(headers)
    header_set = set(header_list)
    working_rows = list(data_rows)

    if flatten_policies:
        sample_cap = min(len(working_rows), 50)
        for row in working_rows[:sample_cap]:
            as_dict = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
            flat = apply_struct_policies_to_row(as_dict, flatten_policies)
            for key in flat:
                if key not in header_set:
                    header_set.add(key)
                    header_list.append(key)
        new_rows: list[list[Any]] = []
        for row in working_rows:
            as_dict = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
            flat = apply_struct_policies_to_row(as_dict, flatten_policies)
            new_rows.append([flat.get(h) for h in header_list])
        working_rows = new_rows
        headers = header_list

    if explode_cols:
        exploded: list[list[Any]] = []
        elem_col = f"{explode_cols[0]}_elem" if len(explode_cols) == 1 else "_df_array_elem"
        if elem_col not in header_set:
            header_list.append(elem_col)
            header_set.add(elem_col)
        for row in working_rows:
            as_dict = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
            # Explode the first configured array column (multi-array explode is out of scope).
            col = explode_cols[0]
            arr = _parse_array_sample(as_dict.get(col))
            if not arr:
                # Keep the row — never drop because array empty/unparseable.
                base = [as_dict.get(h) for h in header_list]
                if elem_col in header_list:
                    base[header_list.index(elem_col)] = None
                exploded.append(base)
                continue
            for idx, elem in enumerate(arr[:ARRAY_EXPLODE_MAX]):
                clone = dict(as_dict)
                if isinstance(elem, (dict, list)):
                    clone[elem_col] = json.dumps(elem, ensure_ascii=False, separators=(",", ":"), default=str)
                else:
                    clone[elem_col] = elem
                # Preserve original array blob on parent; index for reconcile honesty.
                clone[f"{col}_idx"] = idx
                if f"{col}_idx" not in header_set:
                    header_list.append(f"{col}_idx")
                    header_set.add(f"{col}_idx")
                exploded.append([clone.get(h) for h in header_list])
            if len(arr) > ARRAY_EXPLODE_MAX:
                # Remainder stays as a quarantine-friendly note on a synthetic row? Prefer
                # keep overflow elements serialized on parent — already retained in col.
                pass
        working_rows = exploded

    return header_list, working_rows


def expand_dynamo_documents(
    docs: list[dict[str, Any]],
    *,
    cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Flatten DynamoDB map leaves like Mongo — parent kept; arrays not exploded."""
    if not docs or not mongo_flatten_enabled(cfg):
        return docs
    # Skip sentinel / envelope values that are not plain dicts.
    from connectors.dynamodb_reader import DDB_EXPLICIT_NULL, SET_KIND_KEY

    out: list[dict[str, Any]] = []
    for doc in docs:
        clean: dict[str, Any] = {}
        for k, v in doc.items():
            if v is DDB_EXPLICIT_NULL:
                clean[k] = DDB_EXPLICIT_NULL
            elif isinstance(v, dict) and SET_KIND_KEY in v:
                clean[k] = v.get("v", [])
            else:
                clean[k] = v
        out.append(flatten_document(clean))
    return out


def flatten_column_recommendations(
    columns: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    """Suggest flatten targets for nested JSON Compass would store as BSON."""
    out: list[dict[str, str]] = []
    rows = sample_rows or []

    for col in columns:
        if DOT_PATH.match(col):
            out.append({
                "column": col,
                "kind": "dot_notation",
                "flatten_target": col.replace(".", "_"),
                "detail": "Dot-path field — map to typed warehouse column",
            })
            continue

        sample_val: Any = None
        for row in rows[:20]:
            if col in row and row[col] is not None:
                sample_val = row[col]
                break

        if not _looks_like_json(sample_val):
            continue

        if isinstance(sample_val, list):
            kind = "array"
            detail = "Array — explode rows or JSON-serialize per destination"
        elif isinstance(sample_val, dict):
            kind = "nested_object"
            detail = "Nested object — Map chooses JSON blob or flatten top-level keys"
        else:
            try:
                parsed = json.loads(str(sample_val))
                kind = "array" if isinstance(parsed, list) else "nested_object"
                detail = (
                    "JSON array — serialize per destination DDL"
                    if kind == "array"
                    else "JSON object — Map chooses store-as-JSON or flatten top-level keys"
                )
            except json.JSONDecodeError:
                continue

        out.append({
            "column": col,
            "kind": kind,
            "flatten_target": f"{col}_json",
            "detail": detail,
            "default_struct_policy": STRUCT_POLICY_STORE_AS_JSON,
        })

    return out[:12]


def flatten_document(
    doc: dict[str, Any],
    *,
    sep: str = "_",
    max_depth: int = DEFAULT_FLATTEN_DEPTH,
    keep_parent: bool = True,
) -> dict[str, Any]:
    """Expand nested dicts into ``parent_child`` columns for SQL / warehouse maps.

    Parent objects are kept (serialized later to VARIANT/JSON) so nothing is
    lost; leaf scalars are promoted so Map can bind ``address_city`` etc.
    Arrays stay on the parent key as JSON — no row explosion.
    """
    if not isinstance(doc, dict):
        return {}
    out: dict[str, Any] = dict(doc) if keep_parent else {}
    added = 0

    def _walk(obj: dict[str, Any], prefix: str, depth: int) -> None:
        nonlocal added
        for key, value in obj.items():
            if added >= MAX_FLATTENED_KEYS:
                return
            name = f"{prefix}{sep}{key}" if prefix else str(key)
            if isinstance(value, dict):
                if depth < max_depth:
                    if keep_parent and name not in out:
                        out[name] = value
                    _walk(value, name, depth + 1)
                # Nested beyond max_depth stays on the ancestor parent blob only —
                # never promote a partial nested object as its own column.
            elif isinstance(value, list):
                if name not in out:
                    out[name] = value
                    added += 1
            else:
                if name not in out or out.get(name) is None:
                    out[name] = value
                    added += 1

    for key, value in doc.items():
        if isinstance(value, dict):
            _walk(value, str(key), 1)
        elif added >= MAX_FLATTENED_KEYS:
            break

    return out


def mongo_flatten_enabled(cfg: dict[str, Any] | None = None) -> bool:
    """Operator / env switch — default on so Mongo→SQL maps nested leaves."""
    if cfg is not None and "flatten_nested" in cfg:
        return bool(cfg.get("flatten_nested"))
    env = (os.environ.get("DATAFLOW_MONGO_FLATTEN_NESTED") or "1").strip().lower()
    return env not in {"0", "false", "no", "off"}


def expand_mongo_documents(
    docs: list[dict[str, Any]],
    *,
    cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Apply nested expansion when enabled; otherwise return docs unchanged."""
    if not docs or not mongo_flatten_enabled(cfg):
        return docs
    return [flatten_document(doc) for doc in docs]
