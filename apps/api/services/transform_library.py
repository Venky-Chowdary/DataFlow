"""Persistence for transform recipes — the Transform Library store.

Mirrors :mod:`services.transform_store`: abstract store, atomic file-backed
implementation, and a Mongo implementation that writes the file first so a
Mongo outage degrades to the file rather than losing a recipe.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.transform_recipe import TransformRecipe, _now

logger = logging.getLogger(__name__)

#: Prior versions kept inside the stored record under this key — version pin
#: needs the actual definition a transfer bound, not just a counter.
_HISTORY_KEY = "history"
_HISTORY_CAP = 100


def _recipe_at_version(
    entry: dict[str, Any], version: int | None
) -> TransformRecipe | None:
    """Resolve a stored record to the recipe at ``version`` (None → latest)."""
    if not entry:
        return None
    current_version = int(entry.get("version") or 0)
    if version is None or int(version) == current_version:
        body = {k: v for k, v in entry.items() if k != _HISTORY_KEY}
        return TransformRecipe.from_dict(body)
    for prior in entry.get(_HISTORY_KEY) or []:
        if int(prior.get("version") or 0) == int(version):
            return TransformRecipe.from_dict(prior)
    return None


def _is_newer(candidate: TransformRecipe, incumbent: TransformRecipe) -> bool:
    if candidate.version != incumbent.version:
        return candidate.version > incumbent.version
    return (candidate.updated_at or "") > (incumbent.updated_at or "")


def recipe_step_diff(
    from_steps: list[dict[str, Any]],
    to_steps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Per-position step diff between two recipe versions.

    Ordered, positional: a recipe is a pipeline, so step N of version A is
    compared to step N of version B — a rename or re-order shows honestly
    as removed+added/modified entries rather than a silent set diff.
    """
    out: list[dict[str, Any]] = []
    for i in range(max(len(from_steps), len(to_steps))):
        before = from_steps[i] if i < len(from_steps) else None
        after = to_steps[i] if i < len(to_steps) else None
        if before == after:
            continue
        if before is None:
            out.append({"index": i, "change": "added", "after": after})
        elif after is None:
            out.append({"index": i, "change": "removed", "before": before})
        else:
            changed_keys = sorted(
                k
                for k in set(before) | set(after)
                if before.get(k) != after.get(k)
            )
            out.append(
                {
                    "index": i,
                    "change": "modified",
                    "changed_keys": changed_keys,
                    "before": before,
                    "after": after,
                }
            )
    return out


def recipe_diff_hash(
    recipe_id: str,
    from_version: int,
    to_version: int,
    step_diff: list[dict[str, Any]],
) -> str:
    """Content hash binding an upgrade preview to a later re-pin.

    ``upgrade-preview`` returns this so ``transform-recipe/upgrade`` can
    require the exact diff the operator reviewed — a recipe edited again
    after previewing produces a different hash and the upgrade refuses
    with 409 instead of silently applying an unreviewed change.
    """
    canon = json.dumps(
        {
            "recipe_id": recipe_id,
            "from_version": int(from_version),
            "to_version": int(to_version),
            "step_diff": step_diff,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


class TransformLibraryStore(ABC):
    @abstractmethod
    def list(self, workspace_id: str = "") -> list[TransformRecipe]: ...

    @abstractmethod
    def get(
        self,
        recipe_id: str,
        workspace_id: str | None = None,
        version: int | None = None,
    ) -> TransformRecipe | None: ...

    @abstractmethod
    def save(self, recipe: TransformRecipe) -> TransformRecipe: ...

    @abstractmethod
    def delete(self, recipe_id: str) -> bool: ...


class FileTransformLibraryStore(TransformLibraryStore):
    """Atomic, lock-guarded JSON file store (same pattern as transform_projects)."""

    def __init__(self, path: Path | None = None) -> None:
        override = getenv_brand("TRANSFORM_LIBRARY_PATH")
        if path is not None:
            self.path = path
        elif override:
            self.path = Path(override)
        else:
            self.path = data_dir() / "transform_recipes.json"
        self._lock = threading.RLock()

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8")) or {}
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "Transform library store at %s is unreadable (%s). Refusing "
                "to treat it as empty — that would silently drop every recipe.",
                self.path,
                exc,
            )
            raise

    def _write(self, payload: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.path)

    def list(self, workspace_id: str = "") -> list[TransformRecipe]:
        with self._lock:
            raw = self._read()
        recipes = [TransformRecipe.from_dict(v) for v in raw.values()]
        if workspace_id:
            recipes = [r for r in recipes if r.workspace_id == workspace_id]
        return sorted(recipes, key=lambda r: r.updated_at, reverse=True)

    def _entry(self, recipe_id: str) -> dict[str, Any]:
        """The raw stored record (with history) — version resolution needs it."""
        with self._lock:
            raw = self._read()
        return raw.get(recipe_id) or {}

    def get(
        self,
        recipe_id: str,
        workspace_id: str | None = None,
        version: int | None = None,
    ) -> TransformRecipe | None:
        recipe = _recipe_at_version(self._entry(recipe_id), version)
        if recipe is None:
            return None
        if workspace_id is not None:
            if workspace_id == "":
                if recipe.workspace_id != "":
                    return None
            elif recipe.workspace_id != workspace_id:
                return None
        return recipe

    def save(self, recipe: TransformRecipe) -> TransformRecipe:
        recipe.refresh_status()
        with self._lock:
            raw = self._read()
            existing = raw.get(recipe.id) or {}
            # A version is a runnable identity, not a save counter: a
            # metadata-only edit (name/description) keeps the version so a
            # pinned binding cannot be invalidated by a comment change.
            runnable_changed = True
            if existing:
                runnable = (
                    existing.get("steps") != [s.to_dict() for s in recipe.steps]
                    or existing.get("parameters")
                    != {k: dict(v or {}) for k, v in recipe.parameters.items()}
                    or existing.get("input_columns") != list(recipe.input_columns)
                    or existing.get("output_columns") != list(recipe.output_columns)
                    or bool(existing.get("enabled", True)) != bool(recipe.enabled)
                )
                runnable_changed = runnable
            history = list(existing.get(_HISTORY_KEY) or [])
            if existing and runnable_changed:
                prior = {
                    k: v for k, v in existing.items() if k != _HISTORY_KEY
                }
                history.append(prior)
                history = history[-_HISTORY_CAP:]
            recipe.created_at = str(existing.get("created_at") or recipe.created_at)
            recipe.version = (
                int(existing.get("version") or 0) + 1
                if (not existing or runnable_changed)
                else int(existing.get("version") or 1)
            )
            recipe.updated_at = _now()
            stored = recipe.to_dict()
            stored[_HISTORY_KEY] = history
            raw[recipe.id] = stored
            self._write(raw)
        return recipe

    def delete(self, recipe_id: str) -> bool:
        with self._lock:
            raw = self._read()
            if recipe_id not in raw:
                return False
            raw.pop(recipe_id)
            self._write(raw)
        return True


class MongoTransformLibraryStore(TransformLibraryStore):
    """Mongo-primary with a file mirror; file is written first."""

    COLLECTION = "transform_recipes"

    def __init__(self, mongo_service: Any | None = None) -> None:
        self._mongo = mongo_service
        self._file = FileTransformLibraryStore()

    def _db(self) -> Any | None:
        service = self._mongo
        if service is None:
            try:
                from services.mongodb_service import get_mongodb_service

                service = get_mongodb_service()
                self._mongo = service
            except Exception as exc:
                logger.debug("mongo unavailable for transform library: %s", exc)
                return None
        if type(service).__name__ == "MemoryMongoDBService":
            return None
        try:
            return service.get_database()
        except Exception as exc:
            logger.debug("mongo database unavailable for transform library: %s", exc)
            return None

    def list(self, workspace_id: str = "") -> list[TransformRecipe]:
        merged: dict[str, TransformRecipe] = {
            r.id: r for r in self._file.list(workspace_id)
        }
        db = self._db()
        if db is not None:
            try:
                query = {"workspace_id": workspace_id} if workspace_id else {}
                for doc in db[self.COLLECTION].find(query):
                    doc.pop("_id", None)
                    recipe = TransformRecipe.from_dict(doc)
                    existing = merged.get(recipe.id)
                    if existing is None or _is_newer(recipe, existing):
                        merged[recipe.id] = recipe
            except Exception as exc:
                logger.warning("transform library read from Mongo failed: %s", exc)
        return sorted(merged.values(), key=lambda r: r.updated_at, reverse=True)

    def get(
        self,
        recipe_id: str,
        workspace_id: str | None = None,
        version: int | None = None,
    ) -> TransformRecipe | None:
        file_entry = self._file._entry(recipe_id)
        best_entry = file_entry
        db = self._db()
        if db is not None:
            try:
                query = {"id": recipe_id}
                doc = db[self.COLLECTION].find_one(query)
                if doc:
                    doc.pop("_id", None)
                    if not best_entry or int(doc.get("version") or 0) > int(
                        best_entry.get("version") or 0
                    ):
                        best_entry = doc
            except Exception as exc:
                logger.warning(
                    "transform library fetch from Mongo failed: %s", exc
                )
        recipe = _recipe_at_version(best_entry, version)
        if recipe is None:
            return None
        if workspace_id is not None:
            if workspace_id == "":
                if recipe.workspace_id != "":
                    return None
            elif recipe.workspace_id != workspace_id:
                return None
        return recipe

    def save(self, recipe: TransformRecipe) -> TransformRecipe:
        saved = self._file.save(recipe)
        db = self._db()
        if db is not None:
            try:
                # Persist exactly what the file stored — history included — so
                # version pin resolves identically from either backend.
                doc = dict(self._file._entry(saved.id) or saved.to_dict())
                db[self.COLLECTION].replace_one(
                    {"id": saved.id}, doc, upsert=True
                )
            except Exception as exc:
                logger.warning(
                    "transform recipe %s saved to file but not Mongo: %s",
                    saved.id,
                    exc,
                )
        return saved

    def delete(self, recipe_id: str) -> bool:
        """Remove the recipe from BOTH stores; a surviving copy ghosts the list.

        The durable store goes first: when the Mongo delete fails we raise
        instead of removing only the file mirror — otherwise ``list()`` merges
        the orphaned Mongo doc back in and the recipe keeps showing in the
        Library after a "successful" delete.  A recipe that exists in only one
        store still reports success when that copy is removed.
        """
        db = self._db()
        mongo_deleted = False
        if db is not None:
            try:
                mongo_deleted = (
                    db[self.COLLECTION].delete_one({"id": recipe_id}).deleted_count
                    > 0
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Transform recipe {recipe_id} could not be deleted from the "
                    "primary store; no copy was removed"
                ) from exc
        return self._file.delete(recipe_id) or mongo_deleted


_store: TransformLibraryStore | None = None


def get_transform_library(mongo_service: Any | None = None) -> TransformLibraryStore:
    global _store
    if _store is None:
        _store = MongoTransformLibraryStore(mongo_service)
    return _store


def reset_transform_library() -> None:
    global _store
    _store = None
