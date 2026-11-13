"""Mapping-as-code: a column mapping as a versioned, checksummed YAML/JSON document.

Schema (``apiVersion: datawrap.io/mapping/v1``, ``kind: ColumnMapping``)::

    apiVersion: datawrap.io/mapping/v1
    kind: ColumnMapping
    metadata:
      name: orders → customers
      source: {table: orders, type: mysql}
      destination: {table: customers, type: postgresql}
      exportedAt: 2026-11-13T10:00:00+00:00     # not covered by the checksum
    spec:
      mappings:                                 # one entry per source column
        - {source: order_id, target: legacy_ref, target_type: BIGINT}
        - {source: first_name, target: "", transform: omit, intentional_omit: true,
           omit_reason: dropped_redundant, omit_reason_text: Carried in full_name}
      computed:                                 # Transform steps that produce sources
        - {op: derive_column, options: {to: full_name, expression: "concat(first_name, ' ', last_name)"}}
    checksum: sha256:…                          # over apiVersion, kind, metadata (minus exportedAt), spec

Rules:

* The checksum detects a damaged or accidentally altered export: a match means
  "unchanged since export", not "authentic" (anyone can recompute a sha256, so
  it is not a signature). A document whose checksum does not match is refused;
  a hand-authored document may omit ``checksum`` and is accepted as
  hand-edited (the import says so).
* ``computed`` steps are Transform (shape) steps — the one expression engine.
  Each is parsed by :class:`services.shape_models.ShapeRecipe` on export and on
  import, so a document can never carry an expression the engine would refuse.
* YAML is read with a ``SafeLoader`` that also refuses anchors and aliases
  (an alias bomb expands exponentially in memory), and both YAML and JSON are
  bounded in nesting depth and node count *before* anything is expanded or
  converted, so a 600-byte document cannot exhaust memory or the stack.
* Only known mapping fields travel; anything else is dropped, never executed.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

DOCUMENT_API_VERSION = "datawrap.io/mapping/v1"
DOCUMENT_KIND = "ColumnMapping"
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
MAX_MAPPINGS = 5000
MAX_COMPUTED = 200
#: Structure bounds: a real document is ~10 shallow fields per mapping.
MAX_DEPTH = 24
MAX_NODES = 120_000
#: The stored document (a template) may not exceed this when serialised.
MAX_STORED_BYTES = 512 * 1024
_NAME_LIMIT = 256
_SCALARS = (str, int, float, bool, type(None))

#: Mapping fields a document carries (everything else is UI or run state).
_FIELDS = (
    "source", "target", "source_type", "target_type", "transform",
    "intentional_omit", "omit_reason", "omit_reason_text", "create_new",
    "assignment_strategy",
)
#: Transform ops that produce a column a mapping can read.
_COMPUTED_OPS = frozenset({"derive_column", "constant_column", "concat_columns", "run_timestamp"})


class MappingDocumentError(ValueError):
    """The document cannot be exported or imported; the message names why."""


def _text(value: Any, limit: int = _NAME_LIMIT) -> str:
    return str(value if value is not None else "").strip()[:limit]


def _check_structure(value: Any, what: str) -> None:
    """Refuse a structure deeper than MAX_DEPTH or larger than MAX_NODES (iterative)."""
    stack: list[tuple[Any, int]] = [(value, 1)]
    nodes = 0
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_NODES:
            raise MappingDocumentError(f"{what} has too many values (more than {MAX_NODES})")
        if depth > MAX_DEPTH:
            raise MappingDocumentError(f"{what} is nested deeper than {MAX_DEPTH} levels")
        if isinstance(item, dict):
            stack.extend((v, depth + 1) for v in item.values())
            nodes += len(item)
        elif isinstance(item, (list, tuple)):
            stack.extend((v, depth + 1) for v in item)


def _json_depth_ok(text: str) -> None:
    """Bound JSON nesting before the recursive decoder sees it."""
    depth = 0
    in_string = False
    escaped = False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch in "[{":
            depth += 1
            if depth > MAX_DEPTH:
                raise MappingDocumentError(f"the document is nested deeper than {MAX_DEPTH} levels")
        elif ch in "]}":
            depth -= 1


def _clean_mapping(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise MappingDocumentError(f"mappings[{index}] is not an object")
    for key in ("source", "target"):
        value = raw.get(key)
        if value is None and key == "target":
            continue  # no target: an omitted column
        if value is None and key in raw:
            raise MappingDocumentError(
                f"mappings[{index}].{key} is empty: YAML reads a bare null or ~ as no value; "
                "quote it if that is the column's name"
            )
        if value is not None and not isinstance(value, str):
            # YAML 1.1 reads bare yes/no/on/off, numbers and .inf as other
            # types; a column named "yes" must not arrive as True.
            raise MappingDocumentError(
                f"mappings[{index}].{key} must be text: quote it ('yes', '123'); YAML read a bare "
                f"{type(value).__name__} value there"
            )
    source = _text(raw.get("source"))
    if not source:
        raise MappingDocumentError(f"mappings[{index}] has no source column")
    out: dict[str, Any] = {"source": source, "target": _text(raw.get("target"))}
    for key in _FIELDS:
        if not isinstance(raw.get(key), _SCALARS):
            raise MappingDocumentError(f"mappings[{index}].{key} must be text, a number or true/false")
    for key in _FIELDS[2:]:
        value = raw.get(key)
        if value in (None, "", False):
            continue
        out[key] = bool(value) if isinstance(value, bool) else _text(value, 1000)
    return out


def _clean_computed(raw: Any) -> list[dict[str, Any]]:
    from services.shape_models import ShapeError, ShapeRecipe

    steps = list(raw or [])
    if len(steps) > MAX_COMPUTED:
        raise MappingDocumentError(f"computed has {len(steps)} steps; at most {MAX_COMPUTED}")
    clean: list[dict[str, Any]] = []
    for i, step in enumerate(steps):
        if not isinstance(step, dict) or step.get("op") not in _COMPUTED_OPS:
            raise MappingDocumentError(
                f"computed[{i}] must be one of {', '.join(sorted(_COMPUTED_OPS))}"
            )
        clean.append({"op": str(step["op"]), "options": dict(step.get("options") or {})})
    if clean:
        try:
            ShapeRecipe.parse({"steps": clean})
        except ShapeError as exc:
            raise MappingDocumentError(f"computed step refused by the Transform engine: {exc}") from exc
    return clean


def _checksum(doc: dict[str, Any]) -> str:
    meta = {k: v for k, v in dict(doc.get("metadata") or {}).items() if k != "exportedAt"}
    body = {
        "apiVersion": doc.get("apiVersion"),
        "kind": doc.get("kind"),
        "metadata": meta,
        "spec": doc.get("spec"),
    }
    canon = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return "sha256:" + hashlib.sha256(canon.encode("utf-8")).hexdigest()


def _endpoint_meta(raw: Any) -> dict[str, str]:
    raw = raw if isinstance(raw, dict) else {}
    return {k: _text(raw.get(k)) for k in ("table", "type") if _text(raw.get(k))}


def build_mapping_document(
    *,
    mappings: list[dict[str, Any]],
    computed: list[dict[str, Any]] | None = None,
    name: str = "",
    source: dict[str, Any] | None = None,
    destination: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalise, validate and checksum a mapping into the v1 document."""
    if len(mappings or []) > MAX_MAPPINGS:
        raise MappingDocumentError(f"{len(mappings)} mappings; a document holds at most {MAX_MAPPINGS}")
    _check_structure(mappings or [], "mappings")
    _check_structure(computed or [], "computed")
    clean = [_clean_mapping(m, i) for i, m in enumerate(mappings or [])]
    try:
        from services.type_stamp_grammar import validate_mapping_type_stamps

        validate_mapping_type_stamps(clean, context="mapping document")
    except ValueError as exc:
        raise MappingDocumentError(str(exc)) from exc
    doc: dict[str, Any] = {
        "apiVersion": DOCUMENT_API_VERSION,
        "kind": DOCUMENT_KIND,
        "metadata": {
            "name": _text(name, 120),
            "source": _endpoint_meta(source),
            "destination": _endpoint_meta(destination),
            "exportedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        },
        "spec": {"mappings": clean, "computed": _clean_computed(computed)},
    }
    doc["checksum"] = _checksum(doc)
    size = len(json.dumps(doc, ensure_ascii=False, default=str).encode("utf-8"))
    if size > MAX_STORED_BYTES:
        raise MappingDocumentError(
            f"the mapping is {size // 1024} KB; a document may be at most {MAX_STORED_BYTES // 1024} KB "
            "(computed step options count)"
        )
    return doc


def dump_mapping_document(doc: dict[str, Any], *, fmt: str = "yaml") -> str:
    if fmt == "json":
        return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
    if fmt != "yaml":
        raise MappingDocumentError("format must be yaml or json")
    import yaml

    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120)


class _Pairs(dict):
    """A mapping as parsed, remembering keys it repeated (YAML/JSON keep the last)."""

    duplicates: list[str]
    line: int = 0


def _json_pairs(pairs: list[tuple[Any, Any]]) -> _Pairs:
    out = _Pairs()
    out.duplicates = []
    for key, value in pairs:
        if key in out and key not in out.duplicates:
            out.duplicates.append(key)
        out[key] = value
    return out


def _refuse_repeated_keys(value: Any) -> None:
    """Refuse any mapping that repeated a key, naming the key and where (iterative)."""
    stack: list[tuple[Any, str]] = [(value, "")]
    while stack:
        node, path = stack.pop()
        if isinstance(node, dict):
            repeated = getattr(node, "duplicates", None)
            if repeated:
                where = path or "the document"
                line = f" (line {node.line})" if getattr(node, "line", 0) else ""
                raise MappingDocumentError(
                    f"{where} repeats the key '{repeated[0]}'{line}: each key may appear once in an entry"
                )
            for key, child in node.items():
                if isinstance(child, (dict, list)):
                    stack.append((child, f"{path}.{key}" if path else str(key)))
        elif isinstance(node, list):
            for i, child in enumerate(node):
                if isinstance(child, (dict, list)):
                    stack.append((child, f"{path}[{i}]"))


def _no_alias_loader():
    import yaml

    class NoAliasSafeLoader(yaml.SafeLoader):
        """SafeLoader that refuses anchors/aliases and bounds depth and size."""

        _depth = 0
        _nodes = 0

        def compose_node(self, parent, index):  # noqa: ANN001 - PyYAML signature
            event = self.peek_event()
            if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None):
                raise MappingDocumentError("YAML anchors and aliases are not allowed in a mapping document")
            self._nodes += 1
            if self._nodes > MAX_NODES:
                raise MappingDocumentError(f"the document has too many values (more than {MAX_NODES})")
            self._depth += 1
            try:
                if self._depth > MAX_DEPTH:
                    raise MappingDocumentError(f"the document is nested deeper than {MAX_DEPTH} levels")
                return super().compose_node(parent, index)
            finally:
                self._depth -= 1

        def construct_mapping(self, node, deep=False):  # noqa: ANN001 - PyYAML signature
            # PyYAML keeps the last of a repeated key; remember the repeat so
            # the import refuses it by name instead of guessing which was meant.
            mapping = super().construct_mapping(node, deep=deep)
            out = _Pairs(mapping)
            out.duplicates = []
            out.line = node.start_mark.line + 1
            seen: set[Any] = set()
            for key_node, _value in node.value:
                key = self.construct_object(key_node, deep=deep)
                try:
                    hashable = hash(key) is not None
                except TypeError:
                    hashable = False
                if hashable and key in seen and key not in out.duplicates:
                    out.duplicates.append(key)
                if hashable:
                    seen.add(key)
            return out

    NoAliasSafeLoader.add_constructor(
        "tag:yaml.org,2002:map", NoAliasSafeLoader.construct_mapping
    )
    return NoAliasSafeLoader


def _parse(text: str) -> Any:
    stripped = text.lstrip()
    try:
        if stripped.startswith("{"):
            _json_depth_ok(text)
            try:
                parsed = json.loads(text, object_pairs_hook=_json_pairs)
            except ValueError as exc:
                raise MappingDocumentError(f"not valid JSON: {exc.msg} (line {exc.lineno})") from exc
        else:
            import yaml

            try:
                parsed = yaml.load(text, Loader=_no_alias_loader())  # noqa: S506 - SafeLoader subclass
            except MappingDocumentError:
                raise
            except yaml.YAMLError as exc:
                raise MappingDocumentError(f"not valid YAML: {str(exc).splitlines()[0]}") from exc
            except ValueError as exc:
                # A constructor refusing a scalar (an integer over Python's
                # 4,300-digit limit): the document's fault, not the server's.
                raise MappingDocumentError(
                    "not valid YAML: a value could not be read (too large or malformed)"
                ) from exc
    except RecursionError as exc:
        raise MappingDocumentError(f"the document is nested deeper than {MAX_DEPTH} levels") from exc
    _check_structure(parsed, "the document")
    _refuse_repeated_keys(parsed)
    return parsed


def load_mapping_document(text: str) -> dict[str, Any]:
    """Parse, check version/schema/checksum, and return the normalised document."""
    return load_mapping_document_with_status(text)[0]


def load_mapping_document_with_status(text: str) -> tuple[dict[str, Any], bool]:
    """:func:`load_mapping_document` plus whether a checksum was verified
    (False for a hand-authored document that carries none)."""
    if not isinstance(text, str) or not text.strip():
        raise MappingDocumentError("the document is empty")
    if len(text.encode("utf-8")) > MAX_DOCUMENT_BYTES:
        raise MappingDocumentError(f"the document is larger than {MAX_DOCUMENT_BYTES // 1024} KB")
    raw = _parse(text)
    if not isinstance(raw, dict):
        raise MappingDocumentError("the document must be a mapping object with apiVersion, kind and spec")
    if raw.get("apiVersion") != DOCUMENT_API_VERSION:
        raise MappingDocumentError(
            f"unsupported apiVersion {str(raw.get('apiVersion'))[:60]!r}; this release reads "
            f"{DOCUMENT_API_VERSION}"
        )
    if raw.get("kind") != DOCUMENT_KIND:
        raise MappingDocumentError(f"kind must be {DOCUMENT_KIND}")
    spec = raw.get("spec")
    if not isinstance(spec, dict) or not isinstance(spec.get("mappings"), list):
        raise MappingDocumentError("spec.mappings must be a list")
    meta = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    stated = raw.get("checksum")
    if stated is not None and stated != _checksum(raw):
        raise MappingDocumentError(
            "checksum mismatch: the document changed after it was exported. Re-export it, "
            "or remove the checksum line to import it as a hand-edited document."
        )
    rebuilt = build_mapping_document(
        mappings=spec["mappings"],
        computed=spec.get("computed") or [],
        name=meta.get("name") or "",
        source=meta.get("source"),
        destination=meta.get("destination"),
    )
    if meta.get("exportedAt"):
        rebuilt["metadata"]["exportedAt"] = str(meta["exportedAt"])
    if stated is not None and rebuilt["checksum"] != stated:
        # Valid checksum over fields this release drops on read: still the
        # sender's content, but say what was normalised away.
        raise MappingDocumentError(
            "the document carries fields this release does not read; re-export it from Datawrap"
        )
    return rebuilt, stated is not None
