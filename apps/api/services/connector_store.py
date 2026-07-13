"""Saved database connector configurations — reusable source/destination profiles."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.secret_vault import decrypt_secret, encrypt_secret

STORE_PATH = data_dir() / "connectors.json"


@dataclass
class SavedConnector:
    id: str
    name: str
    type: str
    role: str  # source | destination | both
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    schema: str = "public"
    connection_string: str = ""
    ssl: bool = True
    warehouse: str = ""
    auth_mode: str = ""
    auth_role: str = ""
    api_key: str = ""
    service_account: str = ""
    last_tested_at: str | None = None
    last_test_ok: bool = False
    created_at: str = field(default_factory=lambda: _now())

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Never expose raw password in list responses — masked handled at API layer
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SavedConnector:
        password = decrypt_secret(data.get("password", "") or "")
        conn_str = decrypt_secret(data.get("connection_string", "") or "")
        return cls(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            role=data.get("role", "both"),
            host=data.get("host", ""),
            port=int(data.get("port", 5432)),
            database=data.get("database", ""),
            username=data.get("username", ""),
            password=password,
            schema=data.get("schema", "public"),
            connection_string=conn_str,
            ssl=bool(data.get("ssl", True)),
            warehouse=data.get("warehouse", ""),
            auth_mode=data.get("auth_mode", ""),
            auth_role=data.get("auth_role", ""),
            api_key=data.get("api_key", ""),
            service_account=data.get("service_account", ""),
            last_tested_at=data.get("last_tested_at"),
            last_test_ok=bool(data.get("last_test_ok", False)),
            created_at=data.get("created_at", _now()),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_all() -> list[SavedConnector]:
    if not STORE_PATH.exists():
        return _seed_defaults() if _seed_enabled() else []
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        items = [SavedConnector.from_dict(c) for c in raw.get("connectors", [])]
    except Exception:
        return _seed_defaults() if _seed_enabled() else []
    if not _seed_enabled():
        items = [c for c in items if not c.id.startswith("demo-")]
    return items


def _seed_enabled() -> bool:
    import os
    return os.getenv("DATAFLOW_SEED_DEMO", "").lower() in ("1", "true", "yes")


def _save_all(connectors: list[SavedConnector]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = []
    for c in connectors:
        d = c.to_dict()
        if d.get("password"):
            d["password"] = encrypt_secret(d["password"])
        if d.get("connection_string"):
            d["connection_string"] = encrypt_secret(d["connection_string"])
        payload.append(d)
    STORE_PATH.write_text(
        json.dumps({"connectors": payload}, indent=2),
        encoding="utf-8",
    )


def _seed_defaults() -> list[SavedConnector]:
    """Example profiles — user replaces with real connection strings."""
    defaults = [
        SavedConnector(
            id="demo-pg-source",
            name="PostgreSQL · Source (demo)",
            type="postgresql",
            role="source",
            connection_string="postgresql://readonly:pass@localhost:5432/source_db",
        ),
        SavedConnector(
            id="demo-snowflake-dest",
            name="Snowflake · Warehouse (demo)",
            type="snowflake",
            role="destination",
            connection_string="snowflake://user:pass@account.snowflakecomputing.com/warehouse_db",
            warehouse="COMPUTE_WH",
        ),
        SavedConnector(
            id="demo-mongo-dest",
            name="MongoDB · Analytics (demo)",
            type="mongodb",
            role="destination",
            connection_string="mongodb://user:pass@localhost:27017/analytics",
        ),
    ]
    _save_all(defaults)
    return defaults


def list_connectors(role: str | None = None) -> list[SavedConnector]:
    items = _load_all()
    if role:
        items = [c for c in items if c.role == role or c.role == "both"]
    return items


def get_connector(connector_id: str) -> SavedConnector | None:
    for c in _load_all():
        if c.id == connector_id:
            return c
    return None


def create_connector(data: dict[str, Any]) -> SavedConnector:
    connectors = _load_all()
    conn = SavedConnector(
        id=str(uuid.uuid4()),
        name=data["name"],
        type=data["type"],
        role=data.get("role", "both"),
        host=data.get("host", ""),
        port=int(data.get("port", 5432)),
        database=data.get("database", ""),
        username=data.get("username", ""),
        password=data.get("password", ""),
        schema=data.get("schema", "public"),
        connection_string=data.get("connection_string", ""),
        ssl=bool(data.get("ssl", True)),
        warehouse=data.get("warehouse", ""),
        auth_mode=data.get("auth_mode", ""),
        auth_role=data.get("auth_role", ""),
        api_key=data.get("api_key", ""),
        service_account=data.get("service_account", ""),
    )
    connectors.append(conn)
    _save_all(connectors)
    return conn


def update_connector(connector_id: str, data: dict[str, Any]) -> SavedConnector | None:
    connectors = _load_all()
    for i, c in enumerate(connectors):
        if c.id != connector_id:
            continue
        updated = SavedConnector.from_dict({**c.to_dict(), **data, "id": connector_id})
        connectors[i] = updated
        _save_all(connectors)
        return updated
    return None


def delete_connector(connector_id: str) -> bool:
    connectors = _load_all()
    filtered = [c for c in connectors if c.id != connector_id]
    if len(filtered) == len(connectors):
        return False
    _save_all(filtered)
    return True


def mark_tested(connector_id: str, ok: bool) -> None:
    connectors = _load_all()
    for i, c in enumerate(connectors):
        if c.id == connector_id:
            connectors[i] = SavedConnector.from_dict(
                {**c.to_dict(), "last_tested_at": _now(), "last_test_ok": ok}
            )
            _save_all(connectors)
            return


def mask_connector(c: SavedConnector) -> dict[str, Any]:
    d = c.to_dict()
    if d.get("password"):
        d["password"] = "****"
    if d.get("connection_string"):
        d["connection_string"] = d["connection_string"].replace(
            d.get("password", "xxxx"), "****"
        ) if "password" in c.to_dict() and c.password else _mask_conn_str(d["connection_string"])
    if d.get("api_key"):
        d["api_key"] = "****"
    if d.get("service_account"):
        d["service_account"] = "****"
    return d


def _mask_conn_str(s: str) -> str:
    import re

    return re.sub(r":([^:@/]+)@", ":****@", s)
