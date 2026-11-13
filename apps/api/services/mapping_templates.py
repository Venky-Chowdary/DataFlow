"""Workspace mapping templates: save a mapping, apply it to another table.

A template is a mapping-as-code document (:mod:`services.mapping_document`)
saved in one workspace. Applying it to a route whose columns are named a little
differently (``OrderID`` instead of ``order_id``, ``contact_email`` instead of
``email_address``) re-binds each template side onto the live columns with the
auto-map tiers (:mod:`services.mapping_name_match`), then proposes the pairs as
suggestions for the operator to preview — never applied silently. Pairs that
cannot be re-bound are listed by name, with the side that is missing.

Records live in the shared control-plane record set, one per template,
partitioned by workspace: another workspace's template answers like one that
does not exist.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from services.mapping_name_match import suggest_assignments
from services.platform_config import data_dir
from services.shared_doc_store import tracked
from services.shared_record_store import SharedRecordSet
from services.workspace_filter import current_workspace_stamp

MAX_TEMPLATES_PER_WORKSPACE = 200


def _store_path():
    return data_dir() / "mapping_templates.json"


def _file_load() -> dict[str, Any]:
    path = _store_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _file_save(data: dict[str, Any]) -> None:
    from services.atomic_file import write_json_atomic

    write_json_atomic(_store_path(), dict(data))


_DOC = SharedRecordSet(
    "mapping_templates",
    file_load=_file_load,
    file_save=_file_save,
    file_path=_store_path,
    partition_key="workspace_id",
)


def _owner(workspace_id: str | None) -> str:
    explicit = str(workspace_id or "").strip()
    return explicit or current_workspace_stamp()


def _summary(record: dict[str, Any]) -> dict[str, Any]:
    spec = (record.get("document") or {}).get("spec") or {}
    return {
        "id": record.get("id"),
        "name": record.get("name"),
        "description": record.get("description") or "",
        "workspace_id": record.get("workspace_id") or "",
        "created_at": record.get("created_at"),
        "created_by": record.get("created_by") or "",
        "mapping_count": len(spec.get("mappings") or []),
        "computed_count": len(spec.get("computed") or []),
        "checksum": (record.get("document") or {}).get("checksum"),
    }


#: One create at a time in this process; across replicas the record set's
#: revision check (save of the partition read) makes a racing create retry.
_CREATE_LOCK = threading.Lock()


class TemplateLimitReached(ValueError):
    """The workspace already holds the maximum number of templates."""


@_DOC.retrying
def create_template(
    *,
    name: str,
    description: str,
    document: dict[str, Any],
    workspace_id: str | None = None,
    actor: str = "",
) -> dict[str, Any]:
    owner = _owner(workspace_id)
    with _CREATE_LOCK:
        # Count and insert against ONE read of the partition, saved with its
        # revision: two creates cannot both see room for the last slot.
        data = _DOC.load(part=owner)
        existing = [
            r for r in data.values()
            if isinstance(r, dict) and str(r.get("workspace_id") or "") == owner
        ]
        if len(existing) >= MAX_TEMPLATES_PER_WORKSPACE:
            raise TemplateLimitReached(
                f"This workspace already has {MAX_TEMPLATES_PER_WORKSPACE} mapping templates; delete one first."
            )
        template_id = f"mtpl_{uuid.uuid4().hex[:16]}"
        record = {
            "id": template_id,
            "workspace_id": owner,
            "name": str(name or "").strip()[:120],
            "description": str(description or "").strip()[:1000],
            "document": document,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "created_by": str(actor or "")[:256],
        }
        data[template_id] = record
        _DOC.save(tracked(data, getattr(data, "rev", None)))
    return record


def list_templates(*, workspace_id: str) -> list[dict[str, Any]]:
    """Summaries of one workspace's templates (one partition read), newest first."""
    rows = [
        r for r in _DOC.load(part=workspace_id).values()
        if isinstance(r, dict) and str(r.get("workspace_id") or "") == workspace_id
    ]
    rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    return [_summary(r) for r in rows]


def get_template(template_id: str) -> dict[str, Any] | None:
    found = _DOC.load(keys=[str(template_id)]).get(str(template_id))
    return found if isinstance(found, dict) else None


@_DOC.retrying
def delete_template(template_id: str) -> bool:
    data = _DOC.load(keys=[str(template_id)])
    if str(template_id) not in data:
        return False
    data.pop(str(template_id), None)
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return True


def _bind(names: list[str], live: list[str]) -> dict[str, tuple[str, float, str]]:
    """Template name → (live name, confidence, kind), one-to-one."""
    exact = {n: (n, 1.0, "exact") for n in names if n in set(live)}
    rest = [n for n in names if n not in exact]
    open_live = [c for c in live if c not in {v[0] for v in exact.values()}]
    for s in suggest_assignments(rest, open_live):
        exact[s["source"]] = (s["target"], float(s["confidence"]), s["kind"])
    return exact


def rebind_template_pairs(
    pairs: Iterable[tuple[str, str]],
    *,
    source_columns: list[str],
    destination_columns: list[str],
) -> dict[str, Any]:
    """Re-bind template ``(source, target)`` pairs onto the live columns."""
    pairs = [(str(s), str(t)) for s, t in pairs if str(s) and str(t)]
    src = _bind([s for s, _ in pairs], list(dict.fromkeys(source_columns)))
    dst = _bind(list(dict.fromkeys(t for _, t in pairs)), list(dict.fromkeys(destination_columns)))
    suggestions: list[dict[str, Any]] = []
    unbound: list[dict[str, str]] = []
    used_targets: set[str] = set()
    for s, t in pairs:
        hit_s, hit_t = src.get(s), dst.get(t)
        if not hit_s or not hit_t or hit_t[0] in used_targets:
            missing = "both" if not hit_s and not hit_t else ("source" if not hit_s else "target")
            unbound.append({"source": s, "target": t, "missing": missing})
            continue
        used_targets.add(hit_t[0])
        confidence = round(min(hit_s[1], hit_t[1]), 3)
        notes = [f"Template pair {s} → {t}"]
        if hit_s[0] != s:
            notes.append(f"{s} re-bound to {hit_s[0]} ({hit_s[2]})")
        if hit_t[0] != t:
            notes.append(f"{t} re-bound to {hit_t[0]} ({hit_t[2]})")
        suggestions.append({
            "source": hit_s[0],
            "target": hit_t[0],
            "confidence": confidence,
            "kind": "template",
            "reason": "; ".join(notes) + ".",
            "selected": confidence >= 0.7,
            "alternatives": [],
        })
    return {"suggestions": suggestions, "unbound": unbound}
