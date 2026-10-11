"""Persistent SCIM identity mappings and group synchronization state."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from pymongo.errors import DuplicateKeyError, PyMongoError

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir

USERS_COLLECTION = "scim_users"
GROUPS_COLLECTION = "scim_groups"
STORE_PATH = data_dir() / "scim.json"
_STATE_ID = "__scim_state__"
_EMPTY_FILE: dict[str, Any] = {
    "users": [],
    "groups": [],
    "group_mappings": {},
    "managed_memberships": [],
}
_LOCK = threading.RLock()
_PREPARED_DATABASES: set[str] = set()


class ScimStoreUnavailable(RuntimeError):
    """SCIM persistence could not complete an operation."""


def _path() -> Path:
    return STORE_PATH


def _database():
    try:
        db = mongo_database()
        if db is not None and db.name not in _PREPARED_DATABASES:
            db[USERS_COLLECTION].create_index("userName_lower", unique=True)
            db[GROUPS_COLLECTION].create_index("displayName_lower", unique=True)
            _PREPARED_DATABASES.add(db.name)
        return db
    except PyMongoError as exc:
        raise ScimStoreUnavailable("SCIM persistence is unavailable") from exc


def _public(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    if doc is None:
        return None
    return {key: value for key, value in doc.items() if key not in {"_id", "userName_lower", "displayName_lower"}}


def _read_users() -> list[dict[str, Any]]:
    db = _database()
    if db is not None:
        try:
            return [
                _public(dict(row)) or {}
                for row in db[USERS_COLLECTION].find({}, {"_id": False})
            ]
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM user storage is unavailable") from exc
    raw = load_json_doc(_path(), _EMPTY_FILE)
    return [dict(row) for row in raw.get("users", []) if isinstance(row, dict)]


def _read_groups() -> list[dict[str, Any]]:
    db = _database()
    if db is not None:
        try:
            return [
                _public(dict(row)) or {}
                for row in db[GROUPS_COLLECTION].find(
                    {"_id": {"$ne": _STATE_ID}}, {"_id": False}
                )
            ]
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM group storage is unavailable") from exc
    raw = load_json_doc(_path(), _EMPTY_FILE)
    return [dict(row) for row in raw.get("groups", []) if isinstance(row, dict)]


def _read_state() -> dict[str, Any]:
    db = _database()
    if db is not None:
        try:
            row = db[GROUPS_COLLECTION].find_one({"_id": _STATE_ID}, {"_id": False})
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM synchronization state is unavailable") from exc
        return {
            "group_mappings": dict((row or {}).get("group_mappings", {})),
            "managed_memberships": list((row or {}).get("managed_memberships", [])),
        }
    raw = load_json_doc(_path(), _EMPTY_FILE)
    return {
        "group_mappings": dict(raw.get("group_mappings", {})),
        "managed_memberships": list(raw.get("managed_memberships", [])),
    }


def _write_state(state: dict[str, Any]) -> None:
    db = _database()
    if db is not None:
        try:
            db[GROUPS_COLLECTION].replace_one(
                {"_id": _STATE_ID},
                {"_id": _STATE_ID, **state},
                upsert=True,
            )
            return
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM synchronization state is unavailable") from exc
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        data["group_mappings"] = dict(state.get("group_mappings", {}))
        data["managed_memberships"] = list(state.get("managed_memberships", []))


def get_user(user_id: str) -> dict[str, Any] | None:
    return next((row for row in _read_users() if row.get("id") == user_id), None)


def get_user_by_username(user_name: str) -> dict[str, Any] | None:
    normalized = user_name.strip().lower()
    return next(
        (row for row in _read_users() if row.get("userName", "").lower() == normalized),
        None,
    )


def list_users() -> list[dict[str, Any]]:
    return _read_users()


def create_user(user: dict[str, Any]) -> dict[str, Any]:
    db = _database()
    if db is not None:
        try:
            db[USERS_COLLECTION].insert_one(
                {
                    "_id": user["id"],
                    **user,
                    "userName_lower": user["userName"].lower(),
                }
            )
            return dict(user)
        except DuplicateKeyError as exc:
            raise ScimStoreUnavailable("SCIM user identity already exists") from exc
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM user storage is unavailable") from exc
    try:
        with json_doc_transaction(_path(), _EMPTY_FILE) as data:
            users = list(data.get("users", []))
            if any(
                str(row.get("userName", "")).lower() == user["userName"].lower()
                for row in users
            ):
                raise ScimStoreUnavailable("SCIM user identity already exists")
            data["users"] = [*users, dict(user)]
    except OSError as exc:
        raise ScimStoreUnavailable("SCIM user storage is unavailable") from exc
    return dict(user)


def update_user(user: dict[str, Any]) -> dict[str, Any]:
    db = _database()
    if db is not None:
        try:
            db[USERS_COLLECTION].replace_one(
                {"_id": user["id"]},
                {
                    "_id": user["id"],
                    **user,
                    "userName_lower": user["userName"].lower(),
                },
                upsert=True,
            )
            return dict(user)
        except DuplicateKeyError as exc:
            raise ScimStoreUnavailable("SCIM user identity already exists") from exc
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM user storage is unavailable") from exc
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        data["users"] = [
            dict(user) if row.get("id") == user["id"] else row
            for row in data.get("users", [])
            if isinstance(row, dict)
        ]
    return dict(user)


def delete_user(user_id: str) -> bool:
    db = _database()
    if db is not None:
        try:
            return db[USERS_COLLECTION].delete_one({"_id": user_id}).deleted_count > 0
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM user storage is unavailable") from exc
    removed = False
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        users = [row for row in data.get("users", []) if isinstance(row, dict)]
        kept = [row for row in users if row.get("id") != user_id]
        data["users"] = kept
        removed = len(kept) != len(users)
    return removed


def get_group(group_id: str) -> dict[str, Any] | None:
    return next((row for row in _read_groups() if row.get("id") == group_id), None)


def get_group_by_name(display_name: str) -> dict[str, Any] | None:
    normalized = display_name.strip().lower()
    return next(
        (row for row in _read_groups() if row.get("displayName", "").lower() == normalized),
        None,
    )


def list_groups() -> list[dict[str, Any]]:
    return _read_groups()


def create_group(group: dict[str, Any]) -> dict[str, Any]:
    db = _database()
    if db is not None:
        try:
            db[GROUPS_COLLECTION].insert_one(
                {
                    "_id": group["id"],
                    **group,
                    "displayName_lower": group["displayName"].lower(),
                }
            )
            return dict(group)
        except DuplicateKeyError as exc:
            raise ScimStoreUnavailable("SCIM group name already exists") from exc
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM group storage is unavailable") from exc
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        groups = list(data.get("groups", []))
        if any(
            str(row.get("displayName", "")).lower() == group["displayName"].lower()
            for row in groups
        ):
            raise ScimStoreUnavailable("SCIM group name already exists")
        data["groups"] = [*groups, dict(group)]
    return dict(group)


def update_group(group: dict[str, Any]) -> dict[str, Any]:
    db = _database()
    if db is not None:
        try:
            db[GROUPS_COLLECTION].replace_one(
                {"_id": group["id"]},
                {
                    "_id": group["id"],
                    **group,
                    "displayName_lower": group["displayName"].lower(),
                },
                upsert=True,
            )
            return dict(group)
        except DuplicateKeyError as exc:
            raise ScimStoreUnavailable("SCIM group name already exists") from exc
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM group storage is unavailable") from exc
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        data["groups"] = [
            dict(group) if row.get("id") == group["id"] else row
            for row in data.get("groups", [])
            if isinstance(row, dict)
        ]
    return dict(group)


def delete_group(group_id: str) -> bool:
    db = _database()
    if db is not None:
        try:
            return db[GROUPS_COLLECTION].delete_one({"_id": group_id}).deleted_count > 0
        except PyMongoError as exc:
            raise ScimStoreUnavailable("SCIM group storage is unavailable") from exc
    removed = False
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        groups = [row for row in data.get("groups", []) if isinstance(row, dict)]
        kept = [row for row in groups if row.get("id") != group_id]
        data["groups"] = kept
        removed = len(kept) != len(groups)
    return removed


def get_group_mappings() -> dict[str, dict[str, str]]:
    with _LOCK:
        return dict(_read_state()["group_mappings"])


def set_group_mappings(mappings: dict[str, dict[str, str]]) -> None:
    with _LOCK:
        state = _read_state()
        state["group_mappings"] = dict(mappings)
        _write_state(state)


def get_managed_memberships() -> dict[tuple[str, str], str]:
    with _LOCK:
        rows = _read_state()["managed_memberships"]
        result = {}
        for row in rows:
            if isinstance(row, dict) and row.get("workspace_id") and row.get("email"):
                result[(row["workspace_id"], row["email"].strip().lower())] = row.get("role", "")
        return result


def set_managed_memberships(memberships: dict[tuple[str, str], str]) -> None:
    with _LOCK:
        state = _read_state()
        state["managed_memberships"] = [
            {"workspace_id": workspace_id, "email": email, "role": role}
            for (workspace_id, email), role in sorted(memberships.items())
        ]
        _write_state(state)
