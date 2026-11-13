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

* The checksum detects a damaged or altered export. A document whose checksum
  does not match is refused; a hand-authored document may omit ``checksum``
  and is accepted as *unverified* (the import says so).
* ``computed`` steps are Transform (shape) steps — the one expression engine.
  Each is parsed by :class:`services.shape_models.ShapeRecipe` on export and on
  import, so a document can never carry an expression the engine would refuse.
* YAML is read with ``yaml.safe_load``: tags that construct objects are refused.
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
_NAME_LIMIT = 256

#: Mapping fields a document carries (everything else is UI or run state).
_FIELDS = (
    "source", "target", "source_type", "target_type", "transform",
    "intentional_omit", "omit_reason", "omit_reason_text", "create_new",
    "assignment_strategy",
)
#: Transform ops that produce a column a mapping can read.
_COMPUTED_OPS = frozenset({"derive_column", "constant_column", "concat_columns"})


class MappingDocumentError(ValueError):
    """The document cannot be exported or imported; the message names why."""


def _text(value: Any, limit: int = _NAME_LIMIT) -> str:
    return str(value if value is not None else "").strip()[:limit]


def _clean_mapping(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise MappingDocumentError(f"mappings[{index}] is not an object")
    source = _text(raw.get("source"))
    if not source:
        raise MappingDocumentError(f"mappings[{index}] has no source column")
    out: dict[str, Any] = {"source": source, "target": _text(raw.get("target"))}
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
    return doc


def dump_mapping_document(doc: dict[str, Any], *, fmt: str = "yaml") -> str:
    if fmt == "json":
        return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"
    if fmt != "yaml":
        raise MappingDocumentError("format must be yaml or json")
    import yaml

    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120)


def _parse(text: str) -> Any:
    stripped = text.lstrip()
    if stripped.startswith("{"):
        try:
            return json.loads(text)
        except ValueError as exc:
            raise MappingDocumentError(f"not valid JSON: {exc.msg} (line {exc.lineno})") from exc
    import yaml

    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise MappingDocumentError(f"not valid YAML: {str(exc).splitlines()[0]}") from exc


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
            "or remove the checksum line to import a hand-edited document as unverified."
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
