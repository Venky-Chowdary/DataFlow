"""Document model: how several source tables become one MongoDB document.

A model names a root table (one document per root row), a root key (which
becomes ``_id``), and a tree of relationships that place child rows inside
the root document: as an array (``embed_array``), as a sub-document
(``embed_object``), as a key that points elsewhere (``reference``), or in a
collection of their own (``collection``).

This module holds the model only: the dataclasses, a strict parser
(:func:`parse_model`), the identity pinned at Validate (:func:`model_hash`)
and the entry point for the rules (:func:`validate_model`, implemented in
``services.document_model_validate``). Reading rows, building documents and
writing them are later steps; nothing here touches a database.

``_id`` (product decision 1): the root key becomes ``_id``, the value itself
for a one-column key and an ordered sub-document for a composite key, so a
re-run, overwrite or upsert lands on the same document. ``id.mode =
"objectid"`` lets MongoDB generate ObjectIds instead (with a unique index on
the key). ``"composite"`` is accepted as a spelling of ``"key"``: a
composite id is what the key mode does with several columns.

Defaults (product decision 3): at most 10,000 elements per array, and an
oversize document or array quarantines its root with a reason, never
truncated.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

MODEL_VERSION = 1
DEFAULT_MAX_ELEMENTS = 10_000
DEFAULT_MAX_DEPTH = 4
MAX_DEPTH_HARD_CAP = 10
#: 15.5 MiB: headroom under MongoDB's 16 MiB BSON limit for the envelope.
DEFAULT_MAX_DOC_BYTES = 16_252_928
MONGODB_MAX_BSON_BYTES = 16 * 1024 * 1024

ERROR = "error"
WARNING = "warning"

RULES = ("embed_array", "embed_object", "reference", "collection")
JOIN_ORIGINS = ("fk", "declared")
ID_MODES = ("key", "objectid")
_ID_MODE_ALIASES = {"composite": "key"}
REFERENCE_SHAPES = ("id", "ids_array")
ON_ORPHAN = ("quarantine", "ignore")
ON_OVERSIZE = ("quarantine", "fail")
ORDER_DIRECTIONS = ("asc", "desc")
ROOT_ID = "root"


class DocumentModelError(ValueError):
    """A model that cannot be read. ``where`` names the place, ``code`` the fault."""

    def __init__(self, code: str, where: str, message: str):
        super().__init__(message)
        self.code = code
        self.where = where


@dataclass(frozen=True)
class Issue:
    """One finding of :func:`validate_model`.

    ``path`` is where in the model the finding points (``root.key[0]``,
    ``relationships[2].fields[1].path``), so an editor can highlight it.
    """

    code: str
    severity: str
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "severity": self.severity,
            "path": self.path,
            "message": self.message,
        }


def has_errors(issues: Sequence[Issue]) -> bool:
    """True when any issue blocks the model (warnings do not)."""
    return any(i.severity == ERROR for i in issues)


@dataclass(frozen=True)
class FieldSpec:
    """One source column placed at ``path`` (relative to its container)."""

    source: str
    path: str
    label: str = ""

    def to_dict(self, *, labels: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"source": self.source, "path": self.path}
        if labels and self.label:
            out["label"] = self.label
        return out


@dataclass(frozen=True)
class IdSpec:
    mode: str = "key"

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode}


@dataclass(frozen=True)
class RootSpec:
    source: str
    key: tuple[str, ...]
    id: IdSpec = field(default_factory=IdSpec)
    #: Empty means every column of the source, each at its own name.
    fields: tuple[FieldSpec, ...] = ()

    def to_dict(self, *, labels: bool = True) -> dict[str, Any]:
        return {
            "source": self.source,
            "key": list(self.key),
            "id": self.id.to_dict(),
            "fields": [f.to_dict(labels=labels) for f in self.fields],
        }


@dataclass(frozen=True)
class JoinPair:
    parent: str
    child: str

    def to_dict(self) -> dict[str, Any]:
        return {"parent": self.parent, "child": self.child}


@dataclass(frozen=True)
class RelationshipOptions:
    #: The element (or sub-document) is the single field's value, not an object.
    primitive: bool = False
    #: embed_object only: the fields go beside the parent's, not under ``path``.
    merge_into_parent: bool = False
    order_by: tuple[tuple[str, str], ...] = ()
    max_elements: int = DEFAULT_MAX_ELEMENTS
    #: With no ``fields`` listed, leave the child's join columns out.
    drop_join_keys: bool = True
    reference_shape: str = "id"
    on_orphan: str = "quarantine"

    def to_dict(self) -> dict[str, Any]:
        return {
            "primitive": self.primitive,
            "merge_into_parent": self.merge_into_parent,
            "order_by": [[col, direction] for col, direction in self.order_by],
            "max_elements": self.max_elements,
            "drop_join_keys": self.drop_join_keys,
            "reference_shape": self.reference_shape,
            "on_orphan": self.on_orphan,
        }


@dataclass(frozen=True)
class Relationship:
    id: str
    parent: str
    source: str
    rule: str
    join: tuple[JoinPair, ...] = ()
    join_origin: str = "declared"
    path: str = ""
    child_key: tuple[str, ...] = ()
    #: Empty means every column of the source (minus join keys when
    #: ``options.drop_join_keys``), each at its own name.
    fields: tuple[FieldSpec, ...] = ()
    options: RelationshipOptions = field(default_factory=RelationshipOptions)
    label: str = ""

    def to_dict(self, *, labels: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "parent": self.parent,
            "source": self.source,
            "join": [j.to_dict() for j in self.join],
            "join_origin": self.join_origin,
            "rule": self.rule,
            "path": self.path,
            "child_key": list(self.child_key),
            "fields": [f.to_dict(labels=labels) for f in self.fields],
            "options": self.options.to_dict(),
        }
        if labels and self.label:
            out["label"] = self.label
        return out


@dataclass(frozen=True)
class Limits:
    max_depth: int = DEFAULT_MAX_DEPTH
    max_doc_bytes: int = DEFAULT_MAX_DOC_BYTES
    on_oversize: str = "quarantine"

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_depth": self.max_depth,
            "max_doc_bytes": self.max_doc_bytes,
            "on_oversize": self.on_oversize,
        }


@dataclass(frozen=True)
class DocumentModel:
    collection: str
    root: RootSpec
    relationships: tuple[Relationship, ...] = ()
    limits: Limits = field(default_factory=Limits)
    version: int = MODEL_VERSION
    label: str = ""

    def to_dict(self, *, labels: bool = True) -> dict[str, Any]:
        """Every setting spelled out (defaults included); labels optional."""
        out: dict[str, Any] = {
            "version": self.version,
            "collection": self.collection,
            "root": self.root.to_dict(labels=labels),
            "relationships": [r.to_dict(labels=labels) for r in self.relationships],
            "limits": self.limits.to_dict(),
        }
        if labels and self.label:
            out["label"] = self.label
        return out

    def relationship(self, rel_id: str) -> Relationship | None:
        for rel in self.relationships:
            if rel.id == rel_id:
                return rel
        return None


# --- parsing -----------------------------------------------------------------

_MISSING = object()


def _obj(raw: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise DocumentModelError(
            "wrong_type", where, f"{where} must be an object with named settings."
        )
    return raw


def _only(raw: Mapping[str, Any], allowed: Sequence[str], where: str) -> None:
    for key in raw:
        if key not in allowed:
            raise DocumentModelError(
                "unknown_key",
                where,
                f"{key!r} is not a setting of {where}; "
                f"the settings are: {', '.join(allowed)}.",
            )


def _get(raw: Mapping[str, Any], key: str, where: str, *, required: bool = False) -> Any:
    value = raw.get(key, _MISSING)
    if value is _MISSING and required:
        raise DocumentModelError(
            "missing_key", f"{where}.{key}", f"{where} needs {key!r}."
        )
    return value


def _utf8(value: str, where: str) -> str:
    """``value`` when it is valid UTF-8 text, else ``bad_value`` at ``where``.

    A lone surrogate (``"\\ud800"``) is a Python string but not UTF-8: it
    cannot be stored as BSON, hashed, or shown in a JSON response, so it is
    refused here rather than crashing whatever touches the model next.
    """
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise DocumentModelError(
            "bad_value",
            where,
            f"{where} is not valid UTF-8 text (it holds a lone surrogate); "
            "MongoDB stores text as UTF-8.",
        ) from None
    return value


def _text(value: Any, where: str, *, default: str | None = None) -> str:
    if value is _MISSING or value is None:
        if default is not None:
            return default
        raise DocumentModelError("missing_key", where, f"{where} is required.")
    if not isinstance(value, str) or (default is None and not value.strip()):
        raise DocumentModelError("wrong_type", where, f"{where} must be non-empty text.")
    _utf8(value, where)
    return value.strip() if default is None else value


def _choice(
    value: Any, choices: Sequence[str], where: str, default: str | None = None
) -> str:
    """One of ``choices``; missing or null gives ``default``, or is refused
    as ``missing_key`` when the setting has no default."""
    if value is _MISSING or value is None:
        if default is None:
            raise DocumentModelError("missing_key", where, f"{where} is required.")
        return default
    if not isinstance(value, str) or value not in choices:
        raise DocumentModelError(
            "bad_value",
            where,
            f"{where} is {value!r}; it must be one of: {', '.join(choices)}.",
        )
    return value


def _flag(value: Any, where: str, default: bool) -> bool:
    if value is _MISSING or value is None:
        return default
    if not isinstance(value, bool):
        raise DocumentModelError("wrong_type", where, f"{where} must be true or false.")
    return value


def _positive_int(value: Any, where: str, default: int) -> int:
    if value is _MISSING or value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise DocumentModelError("wrong_type", where, f"{where} must be a whole number.")
    if value < 1:
        raise DocumentModelError("bad_value", where, f"{where} must be at least 1.")
    return value


def _list(value: Any, where: str) -> list[Any]:
    if value is _MISSING or value is None:
        return []
    if not isinstance(value, list | tuple):
        raise DocumentModelError("wrong_type", where, f"{where} must be a list.")
    return list(value)


def _columns(value: Any, where: str) -> tuple[str, ...]:
    """A key's columns: text, each named once."""
    out: list[str] = []
    for i, v in enumerate(_list(value, where)):
        col = _text(v, f"{where}[{i}]")
        if col in out:
            raise DocumentModelError(
                "duplicate_key_column",
                f"{where}[{i}]",
                f"{where} names column {col!r} twice; list each column once.",
            )
        out.append(col)
    return tuple(out)


def _label(raw: Mapping[str, Any], where: str) -> str:
    return _text(raw.get("label", _MISSING), f"{where}.label", default="")


_FIELD_KEYS = ("source", "path", "label")


def _parse_fields(value: Any, where: str) -> tuple[FieldSpec, ...]:
    out: list[FieldSpec] = []
    for i, item in enumerate(_list(value, where)):
        at = f"{where}[{i}]"
        raw = _obj(item, at)
        _only(raw, _FIELD_KEYS, at)
        source = _text(_get(raw, "source", at, required=True), f"{at}.source")
        # The path is checked by the validator against the path rules; here
        # it only has to be text. Omitted, the column keeps its own name.
        path = raw.get("path", _MISSING)
        if path is _MISSING or path is None:
            path = source
        elif not isinstance(path, str):
            raise DocumentModelError("wrong_type", f"{at}.path", f"{at}.path must be text.")
        else:
            _utf8(path, f"{at}.path")
        out.append(FieldSpec(source=source, path=path, label=_label(raw, at)))
    return tuple(out)


_ROOT_KEYS = ("source", "key", "id", "fields")
_ID_KEYS = ("mode",)


def _parse_root(value: Any) -> RootSpec:
    where = "root"
    raw = _obj(value, where)
    _only(raw, _ROOT_KEYS, where)
    id_raw = raw.get("id", _MISSING)
    mode = "key"
    if id_raw is not _MISSING and id_raw is not None:
        id_obj = _obj(id_raw, "root.id")
        _only(id_obj, _ID_KEYS, "root.id")
        spelled = id_obj.get("mode", _MISSING)
        spelled = _ID_MODE_ALIASES.get(spelled, spelled) if isinstance(spelled, str) else spelled
        mode = _choice(spelled, ID_MODES + tuple(_ID_MODE_ALIASES), "root.id.mode", "key")
    return RootSpec(
        source=_text(_get(raw, "source", where, required=True), "root.source"),
        key=_columns(raw.get("key", _MISSING), "root.key"),
        id=IdSpec(mode=mode),
        fields=_parse_fields(raw.get("fields", _MISSING), "root.fields"),
    )


_JOIN_KEYS = ("parent", "child")


def _parse_join(value: Any, where: str) -> tuple[JoinPair, ...]:
    out: list[JoinPair] = []
    for i, item in enumerate(_list(value, where)):
        at = f"{where}[{i}]"
        raw = _obj(item, at)
        _only(raw, _JOIN_KEYS, at)
        out.append(
            JoinPair(
                parent=_text(_get(raw, "parent", at, required=True), f"{at}.parent"),
                child=_text(_get(raw, "child", at, required=True), f"{at}.child"),
            )
        )
    return tuple(out)


_OPTION_KEYS = (
    "primitive",
    "merge_into_parent",
    "order_by",
    "max_elements",
    "drop_join_keys",
    "reference_shape",
    "on_orphan",
)


def _parse_order_by(value: Any, where: str) -> tuple[tuple[str, str], ...]:
    out: list[tuple[str, str]] = []
    for i, item in enumerate(_list(value, where)):
        at = f"{where}[{i}]"
        if not isinstance(item, list | tuple) or len(item) != 2:
            raise DocumentModelError(
                "wrong_type", at, f"{at} must be a pair: [column, \"asc\" or \"desc\"]."
            )
        col = _text(item[0], at)
        direction = item[1].lower() if isinstance(item[1], str) else item[1]
        out.append((col, _choice(direction, ORDER_DIRECTIONS, at, "asc")))
    return tuple(out)


def _parse_options(value: Any, where: str) -> RelationshipOptions:
    if value is _MISSING or value is None:
        return RelationshipOptions()
    raw = _obj(value, where)
    _only(raw, _OPTION_KEYS, where)
    g = raw.get
    return RelationshipOptions(
        primitive=_flag(g("primitive", _MISSING), f"{where}.primitive", False),
        merge_into_parent=_flag(
            g("merge_into_parent", _MISSING), f"{where}.merge_into_parent", False
        ),
        order_by=_parse_order_by(g("order_by", _MISSING), f"{where}.order_by"),
        max_elements=_positive_int(
            g("max_elements", _MISSING), f"{where}.max_elements", DEFAULT_MAX_ELEMENTS
        ),
        drop_join_keys=_flag(g("drop_join_keys", _MISSING), f"{where}.drop_join_keys", True),
        reference_shape=_choice(
            g("reference_shape", _MISSING), REFERENCE_SHAPES, f"{where}.reference_shape", "id"
        ),
        on_orphan=_choice(g("on_orphan", _MISSING), ON_ORPHAN, f"{where}.on_orphan", "quarantine"),
    )


_REL_KEYS = (
    "id",
    "parent",
    "source",
    "join",
    "join_origin",
    "rule",
    "path",
    "child_key",
    "fields",
    "options",
    "label",
)


def _parse_relationship(value: Any, where: str) -> Relationship:
    raw = _obj(value, where)
    _only(raw, _REL_KEYS, where)
    path = raw.get("path", _MISSING)
    if path is _MISSING or path is None:
        path = ""
    elif not isinstance(path, str):
        raise DocumentModelError("wrong_type", f"{where}.path", f"{where}.path must be text.")
    else:
        _utf8(path, f"{where}.path")
    # Required: a null rule is refused like a missing one (no default rule).
    rule = _get(raw, "rule", where, required=True)
    return Relationship(
        id=_text(_get(raw, "id", where, required=True), f"{where}.id"),
        parent=_text(_get(raw, "parent", where, required=True), f"{where}.parent"),
        source=_text(_get(raw, "source", where, required=True), f"{where}.source"),
        rule=_choice(rule, RULES, f"{where}.rule"),
        join=_parse_join(raw.get("join", _MISSING), f"{where}.join"),
        join_origin=_choice(
            raw.get("join_origin", _MISSING), JOIN_ORIGINS, f"{where}.join_origin", "declared"
        ),
        path=path,
        child_key=_columns(raw.get("child_key", _MISSING), f"{where}.child_key"),
        fields=_parse_fields(raw.get("fields", _MISSING), f"{where}.fields"),
        options=_parse_options(raw.get("options", _MISSING), f"{where}.options"),
        label=_label(raw, where),
    )


_LIMIT_KEYS = ("max_depth", "max_doc_bytes", "on_oversize")


def _parse_limits(value: Any) -> Limits:
    if value is _MISSING or value is None:
        return Limits()
    raw = _obj(value, "limits")
    _only(raw, _LIMIT_KEYS, "limits")
    return Limits(
        # The hard cap is a validation finding, so an editor can show it.
        max_depth=_positive_int(raw.get("max_depth", _MISSING), "limits.max_depth", DEFAULT_MAX_DEPTH),
        max_doc_bytes=_positive_int(
            raw.get("max_doc_bytes", _MISSING), "limits.max_doc_bytes", DEFAULT_MAX_DOC_BYTES
        ),
        on_oversize=_choice(
            raw.get("on_oversize", _MISSING), ON_OVERSIZE, "limits.on_oversize", "quarantine"
        ),
    )


_MODEL_KEYS = ("version", "collection", "root", "relationships", "limits", "label")


def parse_model(raw: Any) -> DocumentModel:
    """Read a model payload strictly.

    Every unknown setting is refused by name (a misspelt ``max_elemnts``
    must not silently fall back to the default), every missing required
    setting is named, and every default is filled in. Rules that need the
    source tables (columns, keys, paths, the tree) are :func:`validate_model`'s.

    A :class:`DocumentModel` built in code is read again from its
    ``to_dict()``, so it is held to the same rules as a payload.
    """
    if isinstance(raw, DocumentModel):
        raw = raw.to_dict()
    if not isinstance(raw, Mapping):
        raise DocumentModelError(
            "model_not_object", "model", "A document model must be an object."
        )
    _only(raw, _MODEL_KEYS, "model")
    version = raw.get("version", _MISSING)
    if version is not _MISSING and (
        isinstance(version, bool) or version != MODEL_VERSION
    ):
        raise DocumentModelError(
            "unsupported_version",
            "model.version",
            f"Document model version {version!r} is not supported; "
            f"this release reads version {MODEL_VERSION}.",
        )
    root = _get(raw, "root", "model", required=True)
    relationships = raw.get("relationships", _MISSING)
    if relationships is not _MISSING and relationships is not None and not isinstance(
        relationships, list | tuple
    ):
        raise DocumentModelError(
            "wrong_type", "model.relationships", "model.relationships must be a list."
        )
    return DocumentModel(
        version=MODEL_VERSION,
        collection=_text(_get(raw, "collection", "model", required=True), "model.collection"),
        root=_parse_root(root),
        relationships=tuple(
            _parse_relationship(item, f"relationships[{i}]")
            for i, item in enumerate(_list(relationships, "model.relationships"))
        ),
        limits=_parse_limits(raw.get("limits", _MISSING)),
        label=_label(raw, "model"),
    )


def model_hash(model: DocumentModel | Mapping[str, Any] | None) -> str:
    """Identity pinned at Validate and re-checked before Execute.

    Canonical JSON of the parsed model (sorted keys, every default spelled
    out, labels stripped), so key order, labels and omitted-versus-explicit
    defaults never change it, and any change to what is built does. No
    model (``None`` or ``{}``) has no identity: ``""``. A malformed payload
    raises :class:`DocumentModelError`, the same as :func:`parse_model`.
    The digest is 16 hex characters, like ``ShapeRecipe.recipe_hash``.
    """
    if model is None or (isinstance(model, Mapping) and not model):
        return ""
    parsed = parse_model(model)
    payload = json.dumps(
        parsed.to_dict(labels=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class SourceTable:
    """What validation knows about one source table.

    ``columns`` maps each column to its declared type (``""`` when unknown).
    ``primary_key`` and ``unique_keys`` come from the catalog and prove
    uniqueness; ``sampled_unique`` held in a sample only, which earns a
    warning, never a pass.
    """

    columns: Mapping[str, str]
    primary_key: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    sampled_unique: tuple[tuple[str, ...], ...] = ()

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any] | SourceTable) -> SourceTable:
        """Read a table description strictly.

        ``columns`` is ``{name: type}`` (a type may be ``None`` or ``""``
        when unknown) or a list of names; ``primary_key`` is a list of
        column names; ``unique_keys`` and ``sampled_unique`` are lists of
        such lists. Anything else raises :class:`DocumentModelError` with
        code ``bad_source_schema``: a loosely read shape (a string read as
        its characters) could prove a key that the catalog never declared.
        """
        if isinstance(raw, SourceTable):
            return raw
        if not isinstance(raw, Mapping):
            raise _schema_error(
                "",
                "a source table must be an object with its columns and keys, "
                f"not {type(raw).__name__}.",
            )
        return cls(
            columns=_schema_columns(raw.get("columns")),
            primary_key=_schema_key(raw.get("primary_key"), "primary_key", allow_empty=True),
            unique_keys=_schema_keys(raw.get("unique_keys"), "unique_keys"),
            sampled_unique=_schema_keys(raw.get("sampled_unique"), "sampled_unique"),
        )


def _schema_error(where: str, message: str) -> DocumentModelError:
    return DocumentModelError("bad_source_schema", where, message[0].upper() + message[1:])


def _schema_columns(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        if all(isinstance(k, str) and (v is None or isinstance(v, str)) for k, v in value.items()):
            return {k: v or "" for k, v in value.items()}
    elif isinstance(value, list | tuple) and all(isinstance(c, str) for c in value):
        return {c: "" for c in value}
    raise _schema_error(
        "columns",
        "columns must map each column name to its type (text), or list the names.",
    )


def _schema_key(value: Any, where: str, *, allow_empty: bool) -> tuple[str, ...]:
    if value is None and allow_empty:
        return ()
    if (
        isinstance(value, list | tuple)
        and (value or allow_empty)
        and all(isinstance(c, str) and c for c in value)
    ):
        return tuple(value)
    raise _schema_error(
        where,
        f"{where} must be a list of column names, such as [\"customer_id\"].",
    )


def _schema_keys(value: Any, where: str) -> tuple[tuple[str, ...], ...]:
    if value is None:
        return ()
    if isinstance(value, list | tuple) and all(isinstance(k, list | tuple) for k in value):
        return tuple(_schema_key(k, where, allow_empty=False) for k in value)
    raise _schema_error(
        where,
        f"{where} must be a list of keys, each a list of column names, such as "
        '[["email"], ["region", "code"]].',
    )


def validate_model(
    model: DocumentModel | Mapping[str, Any],
    source_schemas: Mapping[str, SourceTable | Mapping[str, Any]],
    fks: Any = None,
    *,
    cdc: bool = False,
    database: str = "",
) -> list[Issue]:
    """Every rule the model breaks, given the source tables and foreign keys.

    ``source_schemas`` maps each source name used in the model to a
    :class:`SourceTable` (or its dict form). ``fks`` is either a mapping of
    owning table to its foreign keys (``ForeignKey`` objects or their dicts)
    or an iterable of measured ``ForeignKeys`` sets. ``cdc=True`` turns the
    count-only warning for arrays without a ``child_key`` into an error,
    because change capture cannot place a changed element without its key.
    ``database`` is the target database when known: MongoDB's 255-byte
    namespace limit counts ``<database>.<collection>``, so with it the limit
    is checked exactly; without it a collection name long enough that a
    database name could push it over the limit is a warning.

    A payload that does not parse comes back as one error issue.
    """
    try:
        parsed = parse_model(model)
    except DocumentModelError as exc:
        return [Issue(code=exc.code, severity=ERROR, path=exc.where, message=str(exc))]
    from services.document_model_validate import run_checks

    return run_checks(parsed, source_schemas, fks, cdc=cdc, database=database)


__all__ = [
    "DEFAULT_MAX_DEPTH",
    "DEFAULT_MAX_DOC_BYTES",
    "DEFAULT_MAX_ELEMENTS",
    "ERROR",
    "MAX_DEPTH_HARD_CAP",
    "WARNING",
    "DocumentModel",
    "DocumentModelError",
    "FieldSpec",
    "IdSpec",
    "Issue",
    "JoinPair",
    "Limits",
    "Relationship",
    "RelationshipOptions",
    "RootSpec",
    "SourceTable",
    "has_errors",
    "model_hash",
    "parse_model",
    "validate_model",
]
