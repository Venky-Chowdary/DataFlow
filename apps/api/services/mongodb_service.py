"""
DataTransfer.space — MongoDB Service
Handles all MongoDB operations for persistence and data transfer
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

from pymongo import MongoClient


def _job_name_key(name: str) -> str:
    """Canonical uniqueness key for job display names (case-insensitive)."""
    return (name or "").strip().casefold()


def _as_object_id(job_id: str):
    """Return a valid ``ObjectId`` or ``None`` for malformed ids.

    Callers must check ``None`` and degrade instead of crashing on an
    arbitrary/external job identifier.
    """
    from bson import ObjectId, errors

    if not job_id:
        return None
    try:
        return ObjectId(job_id)
    except (errors.InvalidId, TypeError, ValueError):
        return None


def _fresh_object_id_hex() -> str:
    """Return a fresh 24-character hex string that looks like an ObjectId.

    Used as a fallback job id when MongoDB is unavailable so the transfer
    engine can keep running and report its own result.
    """
    import os

    return os.urandom(12).hex()


class MongoDBService:
    """MongoDB service for DataTransfer platform"""

    def __init__(self, connection_string: str | None = None):
        if connection_string:
            self.connection_string = connection_string
        else:
            try:
                from services.platform_config import mongodb_uri
                self.connection_string = mongodb_uri()
            except ImportError:
                self.connection_string = os.environ.get(
                    "MONGODB_URI", "mongodb://localhost:27017/"
                )
        self.client: Optional[MongoClient] = None
        self.db_name = "datatransfer"

    def connect(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            # Fail hung sockets instead of freezing the asyncio event loop
            # when sync pymongo is called from request handlers.
            self.client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,
                socketTimeoutMS=20000,
                connectTimeoutMS=5000,
                waitQueueTimeoutMS=10000,
            )
            self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"[ERROR] MongoDB connection failed: {e}")
            self.client = None
            return False

    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.client = None

    def get_database(self, db_name: Optional[str] = None):
        """Get database instance.

        Raises ``ConnectionError`` (instead of an opaque ``NoneType`` error)
        when the server is unreachable, so callers can degrade cleanly.
        """
        if not self.client:
            self.connect()
        if not self.client:
            raise ConnectionError(
                f"MongoDB unavailable at {self.connection_string}"
            )
        return self.client[db_name or self.db_name]

    def test_connection(self) -> dict:
        """Test connection and return server info"""
        try:
            if not self.client:
                self.connect()
            info = self.client.server_info()
            return {
                "connected": True,
                "version": info.get("version"),
                "host": self.connection_string,
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
                "host": self.connection_string,
            }

    # ═══════════════════════════════════════════════════════════════════════
    # CONNECTOR CONFIGURATION STORAGE
    # ═══════════════════════════════════════════════════════════════════════

    def save_connector(self, connector_data: dict) -> str:
        """Save a connector configuration"""
        db = self.get_database()
        collection = db["connectors"]

        connector_data["created_at"] = datetime.now(timezone.utc)
        connector_data["updated_at"] = datetime.now(timezone.utc)

        result = collection.insert_one(connector_data)
        return str(result.inserted_id)

    def get_connector(self, connector_id: str) -> Optional[dict]:
        """Get a connector by ID"""
        db = self.get_database()
        collection = db["connectors"]

        oid = _as_object_id(connector_id)
        if not oid:
            return None

        result = collection.find_one({"_id": oid})
        if result:
            result["_id"] = str(result["_id"])
        return result

    def list_connectors(self) -> list[dict]:
        """List all saved connectors"""
        db = self.get_database()
        collection = db["connectors"]

        connectors = []
        for doc in collection.find().sort("created_at", -1):
            doc["_id"] = str(doc["_id"])
            connectors.append(doc)
        return connectors

    def update_connector(self, connector_id: str, updates: dict) -> bool:
        """Update a connector configuration"""
        db = self.get_database()
        collection = db["connectors"]

        oid = _as_object_id(connector_id)
        if not oid:
            return False

        updates["updated_at"] = datetime.now(timezone.utc)
        result = collection.update_one(
            {"_id": oid},
            {"$set": updates}
        )
        return result.modified_count > 0

    def delete_connector(self, connector_id: str) -> bool:
        """Delete a connector"""
        db = self.get_database()
        collection = db["connectors"]

        oid = _as_object_id(connector_id)
        if not oid:
            return False

        result = collection.delete_one({"_id": oid})
        return result.deleted_count > 0

    # ═══════════════════════════════════════════════════════════════════════
    # DATA TRANSFER OPERATIONS
    # ═══════════════════════════════════════════════════════════════════════

    def insert_data(self, database: str, collection: str, data: list[dict], client: Optional["MongoClient"] = None) -> dict:
        """Insert data into a MongoDB collection"""
        try:
            db_client = client or self.client
            if not db_client:
                self.connect()
                db_client = self.client
            db = db_client[database]
            coll = db[collection]

            if not data:
                return {"success": False, "error": "No data to insert"}

            result = coll.insert_many(data)
            return {
                "success": True,
                "inserted_count": len(result.inserted_ids),
                "database": database,
                "collection": collection,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_client_for_connector(self, connector_id: str):
        """Build MongoClient from saved connector config (file store or platform MongoDB)."""
        from pymongo import MongoClient

        from src.transfer.adapters import (
            _lookup_saved_connector,
            mongodb_connection_string,
        )

        connector = _lookup_saved_connector(connector_id) or self.get_connector(connector_id)
        if not connector:
            return None, None
        conn_str = mongodb_connection_string(connector)
        return MongoClient(conn_str, serverSelectionTimeoutMS=10000), connector

    def create_collection_from_schema(self, database: str, collection: str, schema: dict, client=None) -> dict:
        """Create a collection with optional schema validation"""
        try:
            db_client = client or self.client
            if not db_client:
                self.connect()
                db_client = self.client
            db = db_client[database]

            if collection in db.list_collection_names():
                return {"success": True, "message": "Collection already exists"}

            db.create_collection(collection)
            return {
                "success": True,
                "message": f"Collection '{collection}' created in database '{database}'",
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_collection_stats(self, database: str, collection: str) -> dict:
        """Get statistics for a collection"""
        try:
            db = self.client[database]
            coll = db[collection]

            count = coll.count_documents({})
            sample = list(coll.find().limit(5))

            for doc in sample:
                doc["_id"] = str(doc["_id"])

            return {
                "success": True,
                "document_count": count,
                "sample_documents": sample,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ═══════════════════════════════════════════════════════════════════════
    # TRANSFER JOB TRACKING
    # ═══════════════════════════════════════════════════════════════════════

    def create_transfer_job(self, job_data: dict) -> str:
        """Create a new transfer job record.

        If MongoDB is unavailable, return a generated ObjectId-compatible id
        so the transfer engine can continue and report its result.
        """
        try:
            db = self.get_database()
        except ConnectionError:
            return _fresh_object_id_hex()

        collection = db["transfer_jobs"]

        job_data["status"] = "pending"
        job_data["created_at"] = datetime.now(timezone.utc)
        job_data["started_at"] = None
        job_data["completed_at"] = None
        job_data["records_processed"] = 0
        job_data["errors"] = []
        if job_data.get("name") and not job_data.get("name_key"):
            job_data["name_key"] = _job_name_key(str(job_data["name"]))
        try:
            from services.job_phases import initial_phases
            job_data["phases"] = initial_phases()
        except Exception:
            pass

        result = collection.insert_one(job_data)
        return str(result.inserted_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        """Update transfer job status.

        Degrades gracefully when MongoDB is unavailable so a transient
        persistence outage does not kill an otherwise-successful transfer.
        """
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        collection = db["transfer_jobs"]

        oid = _as_object_id(job_id)
        if not oid:
            return False

        updates = {"status": status, "updated_at": datetime.now(timezone.utc)}
        updates.update(kwargs)

        prev_doc = None
        try:
            prev_doc = collection.find_one(
                {"_id": oid},
                {
                    "status": 1,
                    "phases": 1,
                    "records_processed": 1,
                    "rejected_rows": 1,
                    "coerced_null_rows": 1,
                    "reconciliation": 1,
                    "cdc_lag_seconds": 1,
                    "cdc_lease_conflict": 1,
                    "destination_summary": 1,
                    "reconcile": 1,
                    "event_log": 1,
                    "message": 1,
                    "phase": 1,
                },
            )
        except Exception:
            prev_doc = None
        previous_status = (prev_doc or {}).get("status")

        try:
            from services.job_trust import attach_trust_to_updates

            attach_trust_to_updates(status, updates, previous=prev_doc)
        except Exception:
            pass

        if status == "running":
            updates.setdefault("started_at", datetime.now(timezone.utc))
        elif status in ("completed", "completed_with_quarantine", "failed", "cancelled"):
            updates["completed_at"] = datetime.now(timezone.utc)

        phase_label = kwargs.get("phase")
        message = kwargs.get("message", "")

        # Durable operator event log (Jobs Log tab). Cap to last 200 lines.
        try:
            if "event_log" not in updates:
                prev_log = list((prev_doc or {}).get("event_log") or [])
                line_parts: list[str] = []
                if phase_label and str(phase_label) != str((prev_doc or {}).get("phase") or ""):
                    line_parts.append(f"Entered {phase_label} phase")
                msg_s = str(message or "").strip()
                if msg_s and msg_s != str((prev_doc or {}).get("message") or "").strip():
                    line_parts.append(msg_s[:300])
                err_s = str(kwargs.get("error") or "").strip()
                if err_s:
                    line_parts.append(f"Error: {err_s[:300]}")
                rows = kwargs.get("records_processed")
                if rows is not None:
                    try:
                        rows_i = int(rows)
                        prev_rows = int((prev_doc or {}).get("records_processed") or 0)
                        if rows_i > 0 and rows_i - prev_rows >= 10_000:
                            line_parts.append(f"{rows_i:,} rows processed")
                    except Exception:
                        pass
                if line_parts:
                    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    for part in line_parts:
                        prev_log.append(f"{stamp} — {part}")
                    updates["event_log"] = prev_log[-200:]
        except Exception:
            pass

        if phase_label:
            try:
                from services.job_phases import (
                    advance_phase,
                    complete_phases,
                    initial_phases,
                    phase_from_engine_label,
                )

                phases = (prev_doc or {}).get("phases") or initial_phases()
                mapped = phase_from_engine_label(str(phase_label))
                if status in ("completed", "completed_with_quarantine"):
                    phases = complete_phases(phases, success=True, message=message or "")
                elif status in ("failed", "cancelled"):
                    phases = complete_phases(phases, success=False, message=kwargs.get("error") or message or "")
                else:
                    phases = advance_phase(phases, mapped, status="active", message=message or "")
                updates["phases"] = phases
            except Exception:
                pass

        # Fencing: reject stale worker progress when lease_fence is provided.
        fence = updates.pop("lease_fence", None)
        if fence is None:
            try:
                from services.worker_leases import active_fence

                fence = active_fence(job_id)
            except Exception:
                fence = None
        filt: dict = {"_id": oid}
        if fence is not None:
            updates["lease_fence"] = fence
            # Allow first write (no fence yet) or matching fence only.
            filt = {
                "_id": oid,
                "$or": [
                    {"lease_fence": {"$exists": False}},
                    {"lease_fence": None},
                    {"lease_fence": fence},
                ],
            }

        result = collection.update_one(filt, {"$set": updates})
        ok = result.modified_count > 0 or result.matched_count > 0
        if ok:
            try:
                from services.ops_metrics import record_terminal_job_transition

                reconcile = updates.get("reconcile") or (prev_doc or {}).get("reconcile") or {}
                reconcile_ok = None
                if isinstance(reconcile, dict) and "ok" in reconcile:
                    reconcile_ok = bool(reconcile.get("ok"))
                record_terminal_job_transition(
                    previous_status=previous_status,
                    status=status,
                    records=int(updates.get("records_processed") or (prev_doc or {}).get("records_processed") or 0),
                    quarantined=int(updates.get("rejected_rows") or (prev_doc or {}).get("rejected_rows") or 0),
                    reconcile_ok=reconcile_ok,
                )
            except Exception:
                pass
        return ok

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        """Patch job metadata without changing status (e.g. rename)."""
        if not fields:
            return False
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        collection = db["transfer_jobs"]
        oid = _as_object_id(job_id)
        if not oid:
            return False
        updates = {**fields, "updated_at": datetime.now(timezone.utc)}
        result = collection.update_one({"_id": oid}, {"$set": updates})
        return result.matched_count > 0

    def is_job_name_taken(
        self,
        name: str,
        *,
        workspace_id: str | None = None,
        exclude_job_id: str | None = None,
    ) -> bool:
        """Case-insensitive name collision check within a workspace."""
        needle = _job_name_key(name)
        if not needle:
            return False
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        collection = db["transfer_jobs"]
        query: dict[str, Any] = {"name_key": needle}
        ws = (workspace_id or "").strip()
        if ws:
            query["workspace_id"] = ws
        else:
            query["$or"] = [
                {"workspace_id": ""},
                {"workspace_id": None},
                {"workspace_id": {"$exists": False}},
            ]
        doc = collection.find_one(query, {"_id": 1})
        if doc is None:
            # Legacy rows may lack name_key — fall back to casefold match on name.
            legacy: dict[str, Any] = {
                "name": {"$regex": f"^{re.escape(name.strip())}$", "$options": "i"},
            }
            if ws:
                legacy["workspace_id"] = ws
            else:
                legacy["$or"] = [
                    {"workspace_id": ""},
                    {"workspace_id": None},
                    {"workspace_id": {"$exists": False}},
                ]
            doc = collection.find_one(legacy, {"_id": 1})
        if not doc:
            return False
        if exclude_job_id and str(doc.get("_id")) == str(exclude_job_id):
            return False
        return True

    def get_job(self, job_id: str) -> Optional[dict]:
        """Get a transfer job by ID"""
        try:
            db = self.get_database()
        except ConnectionError:
            return None
        collection = db["transfer_jobs"]

        result = None
        oid = _as_object_id(job_id)
        if oid is not None:
            result = collection.find_one({"_id": oid})
        # Fallback: some stores persist string ids / job_id field (memory→mongo, retries).
        if result is None and job_id:
            result = collection.find_one({"_id": job_id})
        if result is None and job_id:
            result = collection.find_one({"job_id": job_id})
        if result:
            result["_id"] = str(result["_id"])
        return result

    def list_jobs(self, limit: int = 50, workspace_id: str | None = None) -> list[dict]:
        """List recent transfer jobs, optionally filtered to a workspace."""
        db = self.get_database()
        collection = db["transfer_jobs"]

        query: dict[str, Any] = {}
        if workspace_id is not None:
            # An empty workspace id shows only global jobs; a non-empty id shows
            # that workspace plus global shared jobs.
            allowed = [workspace_id, "", None]
            if workspace_id == "":
                allowed = ["", None]
            query["$or"] = [{"workspace_id": w} for w in allowed if w is not None] + [{"workspace_id": {"$exists": False}}]
        jobs = []
        for doc in collection.find(query).sort("created_at", -1).limit(limit):
            doc["_id"] = str(doc["_id"])
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if doc.get(key) and hasattr(doc[key], "isoformat"):
                    doc[key] = doc[key].isoformat()
            jobs.append(doc)
        return jobs


class MemoryMongoDBService:
    """In-memory fallback for tests and DATAFLOW_JOB_STORE=memory.

    Mirrors the small subset of MongoDBService used by routers and the
    transfer engine without requiring a running MongoDB server.
    """

    def __init__(self):
        self._connectors: dict[str, dict] = {}
        self._jobs: dict[str, dict] = {}
        self.client: Any = None
        self.connection_string = "memory://"
        self.db_name = "datatransfer"

    def connect(self) -> bool:
        self.client = True
        return True

    def disconnect(self) -> None:
        self.client = None

    def get_database(self, db_name: Optional[str] = None) -> dict:
        return {}

    def test_connection(self) -> dict:
        return {"connected": True, "version": "memory", "host": self.connection_string}

    @staticmethod
    def _new_id() -> str:
        from bson import ObjectId

        return str(ObjectId())

    def save_connector(self, connector_data: dict) -> str:
        oid = self._new_id()
        rec = dict(connector_data)
        rec["_id"] = oid
        rec.setdefault("created_at", datetime.now(timezone.utc))
        rec.setdefault("updated_at", datetime.now(timezone.utc))
        self._connectors[oid] = rec
        return oid

    def get_connector(self, connector_id: str) -> Optional[dict]:
        rec = self._connectors.get(connector_id)
        if rec:
            rec = dict(rec)
            rec["_id"] = str(rec["_id"])
            return rec
        return None

    def list_connectors(self) -> list[dict]:
        items = sorted(
            self._connectors.values(),
            key=lambda c: c.get("created_at") or "",
            reverse=True,
        )
        return [dict(c, _id=str(c["_id"])) for c in items]

    def update_connector(self, connector_id: str, updates: dict) -> bool:
        rec = self._connectors.get(connector_id)
        if not rec:
            return False
        rec.update(updates)
        rec["updated_at"] = datetime.now(timezone.utc)
        return True

    def delete_connector(self, connector_id: str) -> bool:
        return self._connectors.pop(connector_id, None) is not None

    def insert_data(
        self,
        database: str,
        collection: str,
        data: list[dict],
        client: Optional[Any] = None,
    ) -> dict:
        return {
            "success": True,
            "inserted_count": len(data),
            "database": database,
            "collection": collection,
        }

    def get_client_for_connector(self, connector_id: str):
        return None, None

    def create_collection_from_schema(
        self,
        database: str,
        collection: str,
        schema: dict,
        client: Optional[Any] = None,
    ) -> dict:
        return {
            "success": True,
            "message": f"Collection '{collection}' created in database '{database}'",
        }

    def get_collection_stats(self, database: str, collection: str) -> dict:
        return {"success": True, "document_count": 0, "sample_documents": []}

    def create_transfer_job(self, job_data: dict) -> str:
        oid = self._new_id()
        job = dict(job_data)
        job["_id"] = oid
        job.setdefault("status", "pending")
        job.setdefault("created_at", datetime.now(timezone.utc))
        job.setdefault("started_at", None)
        job.setdefault("completed_at", None)
        job.setdefault("records_processed", 0)
        job.setdefault("errors", [])
        job.setdefault("phases", [])
        if job.get("name") and not job.get("name_key"):
            job["name_key"] = _job_name_key(str(job["name"]))
        self._jobs[oid] = job
        return oid

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        rec = self._jobs.get(job_id)
        if not rec:
            return False
        previous_status = rec.get("status")
        fence = kwargs.pop("lease_fence", None)
        if fence is None:
            try:
                from services.worker_leases import active_fence

                fence = active_fence(job_id)
            except Exception:
                fence = None
        if fence is not None:
            existing_fence = rec.get("lease_fence")
            if existing_fence is not None and existing_fence != fence:
                return False
            kwargs["lease_fence"] = fence

        try:
            from services.job_trust import attach_trust_to_updates

            attach_trust_to_updates(status, kwargs, previous=rec)
        except Exception:
            pass

        prev_phase = rec.get("phase")
        prev_message = str(rec.get("message") or "").strip()
        prev_rows = int(rec.get("records_processed") or 0)
        prev_log = list(rec.get("event_log") or [])

        rec.update(kwargs)
        rec["status"] = status
        rec["updated_at"] = datetime.now(timezone.utc)
        if status == "running":
            rec.setdefault("started_at", datetime.now(timezone.utc))
        elif status in ("completed", "completed_with_quarantine", "failed", "cancelled"):
            rec["completed_at"] = datetime.now(timezone.utc)

        if "event_log" not in kwargs:
            try:
                line_parts: list[str] = []
                phase_label = kwargs.get("phase")
                message = str(kwargs.get("message") or "").strip()
                err_s = str(kwargs.get("error") or "").strip()
                if phase_label and str(phase_label) != str(prev_phase or ""):
                    line_parts.append(f"Entered {phase_label} phase")
                if message and message != prev_message:
                    line_parts.append(message[:300])
                if err_s:
                    line_parts.append(f"Error: {err_s[:300]}")
                if "records_processed" in kwargs:
                    try:
                        rows_i = int(kwargs["records_processed"])
                        if rows_i > 0 and rows_i - prev_rows >= 10_000:
                            line_parts.append(f"{rows_i:,} rows processed")
                    except Exception:
                        pass
                if line_parts:
                    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    for part in line_parts:
                        prev_log.append(f"{stamp} — {part}")
                    rec["event_log"] = prev_log[-200:]
            except Exception:
                pass

        phase_label = kwargs.get("phase")
        if phase_label:
            try:
                from services.job_phases import (
                    advance_phase,
                    complete_phases,
                    initial_phases,
                    phase_from_engine_label,
                )

                phases = rec.get("phases") or initial_phases()
                mapped = phase_from_engine_label(str(phase_label))
                if status in ("completed", "completed_with_quarantine"):
                    phases = complete_phases(phases, success=True, message=kwargs.get("message", ""))
                elif status in ("failed", "cancelled"):
                    phases = complete_phases(
                        phases,
                        success=False,
                        message=kwargs.get("error") or kwargs.get("message", ""),
                    )
                else:
                    phases = advance_phase(
                        phases,
                        mapped,
                        status="active",
                        message=kwargs.get("message", ""),
                    )
                rec["phases"] = phases
            except Exception:
                pass
        try:
            from services.ops_metrics import record_terminal_job_transition

            reconcile = rec.get("reconcile") or {}
            reconcile_ok = None
            if isinstance(reconcile, dict) and "ok" in reconcile:
                reconcile_ok = bool(reconcile.get("ok"))
            record_terminal_job_transition(
                previous_status=previous_status,
                status=status,
                records=int(rec.get("records_processed") or 0),
                quarantined=int(rec.get("rejected_rows") or 0),
                reconcile_ok=reconcile_ok,
            )
        except Exception:
            pass
        return True

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        """Patch job metadata without changing status (e.g. rename)."""
        if not fields:
            return False
        rec = self._jobs.get(job_id)
        if not rec:
            return False
        rec.update(fields)
        rec["updated_at"] = datetime.now(timezone.utc)
        return True

    def is_job_name_taken(
        self,
        name: str,
        *,
        workspace_id: str | None = None,
        exclude_job_id: str | None = None,
    ) -> bool:
        needle = _job_name_key(name)
        if not needle:
            return False
        ws = (workspace_id or "").strip()
        for jid, rec in self._jobs.items():
            if exclude_job_id and str(jid) == str(exclude_job_id):
                continue
            rec_ws = (rec.get("workspace_id") or "").strip()
            if ws and rec_ws not in (ws, ""):
                continue
            if not ws and rec_ws:
                continue
            key = _job_name_key(str(rec.get("name_key") or rec.get("name") or ""))
            if key == needle:
                return True
        return False

    def get_job(self, job_id: str) -> Optional[dict]:
        rec = self._jobs.get(job_id)
        if rec:
            rec = dict(rec)
            rec["_id"] = str(rec["_id"])
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if rec.get(key) and hasattr(rec[key], "isoformat"):
                    rec[key] = rec[key].isoformat()
            return rec
        return None

    def list_jobs(self, limit: int = 50, workspace_id: str | None = None) -> list[dict]:
        items = sorted(
            self._jobs.values(),
            key=lambda j: j.get("created_at") or "",
            reverse=True,
        )
        if workspace_id is not None:
            allowed = {workspace_id, "", None}
            if workspace_id == "":
                allowed = {"", None}
            items = [j for j in items if j.get("workspace_id") in allowed]
        items = items[:limit]
        out = []
        for rec in items:
            job = dict(rec)
            job["_id"] = str(job["_id"])
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if job.get(key) and hasattr(job[key], "isoformat"):
                    job[key] = job[key].isoformat()
            out.append(job)
        return out


# Global instance
_mongodb_service: Optional[MongoDBService] = None


def get_mongodb_service() -> MongoDBService:
    """Get or create MongoDB service instance."""
    global _mongodb_service
    if _mongodb_service is None:
        if os.environ.get("DATAFLOW_JOB_STORE", "").lower() == "memory":
            _mongodb_service = MemoryMongoDBService()
            _mongodb_service.connect()
        else:
            _mongodb_service = MongoDBService()
            _mongodb_service.connect()
    return _mongodb_service
