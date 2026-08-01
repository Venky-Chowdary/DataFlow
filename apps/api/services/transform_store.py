"""Persistence for transformation projects.

Follows the contract-store pattern already in the codebase: an abstract store,
a file-backed implementation with atomic writes under a lock, and a Mongo
implementation that mirrors every write to the file first so a Mongo outage
degrades to the file rather than losing the project.

A *project* is the unit an operator manages: a destination, a schema, and an
ordered set of models. Binding models to a destination at the project level
rather than per-model is what lets the post-load hook decide, from a finished
transfer alone, which models to run.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.transform_models import TransformModel, build_plan

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_newer(candidate: "TransformProject", incumbent: "TransformProject") -> bool:
    """True when ``candidate`` is strictly newer than ``incumbent``.

    Version is the primary signal because it increments on every save. When
    versions tie (e.g. a copy restored from backup), ``updated_at`` breaks the
    tie. Equality returns False so a stale Mongo document never replaces an
    equally versioned file copy that was written first.
    """
    if candidate.version != incumbent.version:
        return candidate.version > incumbent.version
    return (candidate.updated_at or "") > (incumbent.updated_at or "")


@dataclass
class TransformProject:
    """A destination-bound set of models."""

    id: str = ""
    name: str = ""
    #: Saved connector id for the destination the models run against.
    destination_connector_id: str = ""
    #: Schema/dataset the models are materialized into.
    schema: str = ""
    models: list[TransformModel] = field(default_factory=list)
    enabled: bool = True
    #: Run automatically when a transfer into this destination completes.
    run_after_transfer: bool = True
    #: Only auto-run when the landed table is in this list. Empty = any table.
    trigger_tables: list[str] = field(default_factory=list)
    workspace_id: str = ""
    description: str = ""
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    version: int = 0

    def __post_init__(self) -> None:
        if not self.id:
            self.id = uuid.uuid4().hex[:24]

    def validate(self) -> None:
        """Reject a project that could not run, at save time rather than run time."""
        if not (self.name or "").strip():
            raise ValueError("Transformation project needs a name.")
        if not (self.destination_connector_id or "").strip():
            raise ValueError(
                "Transformation project needs a destination connector — models "
                "run as SQL against a warehouse, not in the application."
            )
        # build_plan raises on duplicate names and on dependency cycles. Doing
        # it here means a cycle is caught when it is authored, not when a
        # transfer finishes at 3am and the DAG will not resolve.
        build_plan(self.models)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "destination_connector_id": self.destination_connector_id,
            "schema": self.schema,
            "models": [m.to_dict() for m in self.models],
            "enabled": self.enabled,
            "run_after_transfer": self.run_after_transfer,
            "trigger_tables": list(self.trigger_tables),
            "workspace_id": self.workspace_id,
            "description": self.description,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransformProject":
        return cls(
            id=str(data.get("id") or ""),
            name=str(data.get("name") or ""),
            destination_connector_id=str(data.get("destination_connector_id") or ""),
            schema=str(data.get("schema") or ""),
            models=[TransformModel.from_dict(m) for m in (data.get("models") or [])],
            enabled=bool(data.get("enabled", True)),
            run_after_transfer=bool(data.get("run_after_transfer", True)),
            trigger_tables=[str(t) for t in (data.get("trigger_tables") or [])],
            workspace_id=str(data.get("workspace_id") or ""),
            description=str(data.get("description") or ""),
            created_at=str(data.get("created_at") or _now()),
            updated_at=str(data.get("updated_at") or _now()),
            version=int(data.get("version") or 0),
        )

    def triggered_by(self, table: str) -> bool:
        """Whether a transfer that landed ``table`` should run this project."""
        if not (self.enabled and self.run_after_transfer):
            return False
        if not self.trigger_tables:
            return True
        landed = (table or "").strip().lower()
        return any(landed == t.strip().lower() for t in self.trigger_tables)


class TransformProjectStore(ABC):
    @abstractmethod
    def list(self, workspace_id: str = "") -> list[TransformProject]: ...

    @abstractmethod
    def get(self, project_id: str) -> TransformProject | None: ...

    @abstractmethod
    def save(self, project: TransformProject) -> TransformProject: ...

    @abstractmethod
    def delete(self, project_id: str) -> bool: ...


class FileTransformProjectStore(TransformProjectStore):
    """Atomic, lock-guarded JSON file store.

    The temp file carries pid and thread id so two writers cannot steal each
    other's source before ``os.replace`` — the same guard the contract file
    store uses, and the reason schedules.json's plain ``write_text`` is not the
    pattern to copy.
    """

    def __init__(self, path: Path | None = None) -> None:
        override = os.environ.get("DATAFLOW_TRANSFORMS_PATH")
        if path is not None:
            self.path = path
        elif override:
            self.path = Path(override)
        else:
            self.path = data_dir() / "transform_projects.json"
        self._lock = threading.RLock()

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8")) or {}
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "Transformation project store at %s is unreadable (%s). Refusing "
                "to treat it as empty — that would silently drop every project.",
                self.path,
                exc,
            )
            raise

    def _write(self, payload: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.path)

    def list(self, workspace_id: str = "") -> list[TransformProject]:
        with self._lock:
            raw = self._read()
        projects = [TransformProject.from_dict(v) for v in raw.values()]
        if workspace_id:
            projects = [p for p in projects if p.workspace_id in ("", workspace_id)]
        return sorted(projects, key=lambda p: p.updated_at, reverse=True)

    def get(self, project_id: str) -> TransformProject | None:
        with self._lock:
            raw = self._read()
        entry = raw.get(project_id)
        return TransformProject.from_dict(entry) if entry else None

    def save(self, project: TransformProject) -> TransformProject:
        project.validate()
        with self._lock:
            raw = self._read()
            existing = raw.get(project.id) or {}
            project.created_at = str(existing.get("created_at") or project.created_at)
            project.version = int(existing.get("version") or 0) + 1
            project.updated_at = _now()
            raw[project.id] = project.to_dict()
            self._write(raw)
        return project

    def delete(self, project_id: str) -> bool:
        with self._lock:
            raw = self._read()
            if project_id not in raw:
                return False
            raw.pop(project_id)
            self._write(raw)
        return True


class MongoTransformProjectStore(TransformProjectStore):
    """Mongo-primary with a file mirror.

    Writes hit the file first so a Mongo failure cannot lose a project the
    operator believes was saved. Reads merge both with Mongo winning.
    """

    COLLECTION = "transform_projects"

    def __init__(self, mongo_service: Any | None = None) -> None:
        self._mongo = mongo_service
        self._file = FileTransformProjectStore()

    def _db(self) -> Any | None:
        service = self._mongo
        if service is None:
            try:
                from services.mongodb_service import get_mongodb_service

                service = get_mongodb_service()
                self._mongo = service
            except Exception as exc:
                logger.debug("mongo unavailable for transforms: %s", exc)
                return None
        if type(service).__name__ == "MemoryMongoDBService":
            return None
        try:
            return service.get_database()
        except Exception as exc:
            logger.debug("mongo database unavailable for transforms: %s", exc)
            return None

    def list(self, workspace_id: str = "") -> list[TransformProject]:
        merged: dict[str, TransformProject] = {
            p.id: p for p in self._file.list(workspace_id)
        }
        db = self._db()
        if db is not None:
            try:
                query = {"workspace_id": {"$in": ["", workspace_id]}} if workspace_id else {}
                for doc in db[self.COLLECTION].find(query):
                    doc.pop("_id", None)
                    project = TransformProject.from_dict(doc)
                    existing = merged.get(project.id)
                    # File is written first. A failed Mongo replace leaves the
                    # file newer — preferring Mongo unconditionally would then
                    # silently roll the operator's last edit back.
                    if existing is None or _is_newer(project, existing):
                        merged[project.id] = project
            except Exception as exc:
                logger.warning("transform project read from Mongo failed: %s", exc)
        return sorted(merged.values(), key=lambda p: p.updated_at, reverse=True)

    def get(self, project_id: str) -> TransformProject | None:
        file_copy = self._file.get(project_id)
        db = self._db()
        if db is not None:
            try:
                doc = db[self.COLLECTION].find_one({"id": project_id})
                if doc:
                    doc.pop("_id", None)
                    mongo_copy = TransformProject.from_dict(doc)
                    if file_copy is None or _is_newer(mongo_copy, file_copy):
                        return mongo_copy
            except Exception as exc:
                logger.warning("transform project fetch from Mongo failed: %s", exc)
        return file_copy

    def save(self, project: TransformProject) -> TransformProject:
        saved = self._file.save(project)
        db = self._db()
        if db is not None:
            try:
                db[self.COLLECTION].replace_one(
                    {"id": saved.id}, saved.to_dict(), upsert=True
                )
            except Exception as exc:
                logger.warning(
                    "transform project %s saved to file but not Mongo: %s",
                    saved.id,
                    exc,
                )
        return saved

    def delete(self, project_id: str) -> bool:
        removed = self._file.delete(project_id)
        db = self._db()
        if db is not None:
            try:
                result = db[self.COLLECTION].delete_one({"id": project_id})
                removed = removed or bool(getattr(result, "deleted_count", 0))
            except Exception as exc:
                logger.warning("transform project delete from Mongo failed: %s", exc)
        return removed


_store: TransformProjectStore | None = None
_store_lock = threading.Lock()


def get_transform_store(mongo_service: Any | None = None) -> TransformProjectStore:
    global _store
    with _store_lock:
        if _store is None:
            _store = MongoTransformProjectStore(mongo_service)
        return _store


def reset_transform_store() -> None:
    """Test hook — drop the cached singleton."""
    global _store
    with _store_lock:
        _store = None
