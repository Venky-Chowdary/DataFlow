"""
Datawrap — MongoDB Service
Handles all MongoDB operations for persistence and data transfer
"""

from __future__ import annotations

import logging
import os
from services.brand_env import getenv_brand
import re
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from pymongo import MongoClient

from services.runtime_estimate import append_throughput_mark

#: Statuses a job never leaves on its own. Progress writes that arrive after a
#: job reaches one of these are stale by definition and must be dropped, not
#: applied — otherwise a late write resurrects a cancelled or failed job.
TERMINAL_JOB_STATUSES = frozenset(
    {"completed", "completed_with_quarantine", "failed", "cancelled"}
)


logger = logging.getLogger(__name__)

_CONNECTOR_SECRET_KEYS = (
    "password",
    "connection_string",
    "api_key",
    "private_key",
    "service_account",
    "secret_access_key",
    "access_key_secret",
    "token",
    "refresh_token",
    "client_secret",
)


def _encrypt_connector_secrets(data: dict) -> dict:
    """Encrypt secret fields before persisting a connector document."""
    from services.secret_vault import encrypt_secret

    out = dict(data)
    for key in _CONNECTOR_SECRET_KEYS:
        val = out.get(key)
        if isinstance(val, str) and val and val != "****" and not val.startswith("["):
            out[key] = encrypt_secret(val, label=f"connector-{key}")
    return out


def _decrypt_connector_secrets(data: dict | None) -> dict | None:
    """Decrypt secret fields after loading a connector for internal use."""
    if not data:
        return data
    from services.secret_vault import decrypt_secret

    out = dict(data)
    for key in _CONNECTOR_SECRET_KEYS:
        val = out.get(key)
        if isinstance(val, str) and val and val != "****":
            try:
                out[key] = decrypt_secret(val)
            except Exception as exc:
                logger.warning("Failed to decrypt connector field %s: %s", key, exc)
    return out


def redact_connector_secrets(data: dict | None) -> dict | None:
    """Mask secrets for API responses — never return live credentials to clients."""
    if not data:
        return data
    import re

    out = dict(data)
    for key in _CONNECTOR_SECRET_KEYS:
        val = out.get(key)
        if not val:
            continue
        if key == "connection_string" and isinstance(val, str):
            out[key] = re.sub(r":([^:@/]+)@", ":****@", val)
        else:
            out[key] = "****"
    return out


def _job_name_key(name: str) -> str:
    """Canonical uniqueness key for job display names (case-insensitive)."""
    return (name or "").strip().casefold()


def _is_duplicate_key(exc: BaseException) -> bool:
    """Whether an insert failed because the key was already taken.

    Matched by type first so the check does not depend on driver message
    wording, with a text fallback for wrapped or mocked errors.
    """
    try:
        from pymongo.errors import DuplicateKeyError

        if isinstance(exc, DuplicateKeyError):
            return True
    except Exception:
        pass
    if getattr(exc, "code", None) == 11000:
        return True
    return "duplicate key" in str(exc).lower() or "e11000" in str(exc).lower()


def _as_utc(value: datetime) -> datetime:
    """Treat a naive timestamp as UTC so comparisons never raise.

    MongoDB returns naive datetimes by default; comparing one against an aware
    ``now()`` raises, which would turn a claim check into a request failure.
    """
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


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


def job_key_filter(job_id: str) -> dict | None:
    """Mongo filter matching a job by ObjectId **or** by its string id.

    ``create_transfer_job`` honours a caller-supplied ``_id`` of any shape
    (fleet claim, scheduler, CLI, ``execute_tracked(job_id=...)``), while the
    write paths used to accept ObjectId-shaped ids only. A job created with an
    external id was therefore readable but permanently un-updatable: its very
    first checkpoint save returned False and the transfer aborted mid-write,
    leaving committed rows at the destination with no resume token. One
    resolver now serves every read and write so an id that is accepted at
    creation stays addressable for the life of the job.
    """
    if not job_id:
        return None
    oid = _as_object_id(job_id)
    clauses: list[dict] = []
    if oid is not None:
        clauses.append({"_id": oid})
    clauses.append({"_id": job_id})
    clauses.append({"job_id": job_id})
    return clauses[0] if len(clauses) == 1 else {"$or": clauses}


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

        connector_data = _encrypt_connector_secrets(connector_data)
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
            return _decrypt_connector_secrets(result)
        return result

    def list_connectors(self) -> list[dict]:
        """List all saved connectors"""
        db = self.get_database()
        collection = db["connectors"]

        connectors = []
        for doc in collection.find().sort("created_at", -1):
            doc["_id"] = str(doc["_id"])
            connectors.append(_decrypt_connector_secrets(doc) or doc)
        return connectors

    def update_connector(self, connector_id: str, updates: dict) -> bool:
        """Update a connector configuration"""
        db = self.get_database()
        collection = db["connectors"]

        oid = _as_object_id(connector_id)
        if not oid:
            return False

        updates = _encrypt_connector_secrets(updates)
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

        # Honor caller-supplied job id (execute_tracked / resume / fleet claim).
        explicit = job_data.get("_id") or job_data.get("job_id")
        if explicit and "_id" not in job_data:
            job_data = dict(job_data)
            job_data["_id"] = str(explicit)

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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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

        key = job_key_filter(job_id)
        if not key:
            return False

        updates = {"status": status, "updated_at": datetime.now(timezone.utc)}
        updates.update(kwargs)

        prev_doc = None
        try:
            prev_doc = collection.find_one(
                key,
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
                    "throughput_marks": 1,
                },
            )
        except Exception:
            prev_doc = None
        previous_status = (prev_doc or {}).get("status")

        # Terminal statuses are final. A worker's next per-chunk progress write
        # used to happily reset `status` from "cancelled" back to "running",
        # so a cancel that landed between the worker's status read and its
        # status write was erased — the UI flipped back to Running and the
        # operator had to race the loop. `allow_terminal_exit=True` is the one
        # documented way out, used by resume.
        allow_terminal_exit = bool(kwargs.pop("allow_terminal_exit", False))
        if (
            previous_status in TERMINAL_JOB_STATUSES
            and status not in TERMINAL_JOB_STATUSES
            and not allow_terminal_exit
        ):
            logging.getLogger(__name__).info(
                "Ignoring %s update for job %s: already terminal (%s)",
                status,
                job_id,
                previous_status,
            )
            return False

        try:
            from services.job_trust import attach_trust_to_updates

            attach_trust_to_updates(status, updates, previous=prev_doc)
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

        if status == "running":
            updates.setdefault("started_at", datetime.now(timezone.utc))
        elif status in ("completed", "completed_with_quarantine", "failed", "cancelled"):
            updates["completed_at"] = datetime.now(timezone.utc)

        # Throughput evidence for the cutover-window estimate. Bounded to the
        # trailing window the estimator reads, so the job doc cannot grow.
        if "records_processed" in kwargs and "throughput_marks" not in updates:
            marks = append_throughput_mark(
                (prev_doc or {}).get("throughput_marks"),
                kwargs.get("records_processed"),
            )
            if marks is not None:
                updates["throughput_marks"] = marks

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
                    except Exception as exc:
                        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                if line_parts:
                    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    for part in line_parts:
                        prev_log.append(f"{stamp} — {part}")
                    updates["event_log"] = prev_log[-200:]
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

        # Fencing: reject stale worker progress when lease_fence is provided.
        fence = updates.pop("lease_fence", None)
        if fence is None:
            try:
                from services.worker_leases import active_fence

                fence = active_fence(job_id)
            except Exception:
                fence = None
        filt: dict = dict(key)
        if fence is not None:
            updates["lease_fence"] = fence
            # Allow first write (no fence yet) or matching fence only. The key
            # itself may already be an `$or`, so both go under `$and` rather
            # than one silently replacing the other.
            filt = {
                "$and": [
                    key,
                    {
                        "$or": [
                            {"lease_fence": {"$exists": False}},
                            {"lease_fence": None},
                            {"lease_fence": fence},
                        ]
                    },
                ]
            }

        # Control-plane BSON budget — never let quarantine/checkpoint previews
        # blow MongoDB's 16 MiB update command (Excel→SQL high-reject cliff).
        try:
            from services.job_document_budget import apply_job_update_with_budget

            result = apply_job_update_with_budget(collection, filt, updates)
        except Exception as budget_exc:
            logging.getLogger(__name__).error(
                "transfer_jobs update failed for %s: %s",
                job_id,
                budget_exc,
                exc_info=budget_exc,
            )
            # Last ditch: status/error only so Theater still shows failure.
            try:
                from services.job_document_budget import emergency_strip_job_update

                result = collection.update_one(
                    filt, {"$set": emergency_strip_job_update(updates)}
                )
            except Exception:
                return False
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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        return ok

    def request_job_cancel(self, job_id: str) -> bool:
        """Record a durable cancellation request for a running job.

        Separate from ``status`` on purpose. ``status`` is rewritten by the
        worker on every chunk, so expressing cancellation only as
        ``status="cancelled"`` meant a cancel landing between the worker's read
        and its write was silently overwritten. Nothing on the progress path
        touches ``cancel_requested``, so once set it stays set until an explicit
        resume clears it, and the worker is guaranteed to observe it at its next
        checkpoint.
        """
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        key = job_key_filter(job_id)
        if not key:
            return False
        result = db["transfer_jobs"].update_one(
            key,
            {
                "$set": {
                    "cancel_requested": True,
                    "cancel_requested_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return result.matched_count > 0

    def clear_job_cancel(self, job_id: str) -> bool:
        """Clear a cancellation request so a resumed job can run again."""
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        key = job_key_filter(job_id)
        if not key:
            return False
        result = db["transfer_jobs"].update_one(
            key,
            {
                "$unset": {"cancel_requested": "", "cancel_requested_at": ""},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
        )
        return result.matched_count > 0

    def is_cancel_requested(self, job_id: str) -> bool:
        """Whether an operator has asked this job to stop.

        Checks the durable flag *and* the status, so a cancel written by an
        older build (status-only) is still honoured.
        """
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        key = job_key_filter(job_id)
        if not key:
            return False
        doc = db["transfer_jobs"].find_one(
            key, {"cancel_requested": 1, "status": 1}
        )
        if not doc:
            return False
        return bool(doc.get("cancel_requested")) or doc.get("status") == "cancelled"

    def claim_job_idempotency(
        self,
        *,
        key: str,
        job_id: str,
        ttl_seconds: int | None = None,
    ) -> tuple[bool, str, str]:
        """Try to claim the exclusive right to run one transfer.

        Returns ``(acquired, existing_job_id, existing_status)``.

        The claim is an ``insert_one`` keyed on ``_id``, so the storage engine
        decides the winner. A read-then-write check would leave a window where
        two callers both see "no claim" and both proceed, which is exactly the
        double-submit case this exists to prevent.

        A claim whose job already finished is not a conflict — re-running the
        same transfer later is normal — so it is taken over rather than
        rejected.
        """
        from services.job_idempotency import claim_expiry

        if not key or not job_id:
            return True, "", ""
        try:
            db = self.get_database()
        except ConnectionError:
            # Without a shared store there is nothing to coordinate through.
            # Allow the run rather than blocking all transfers on Mongo.
            return True, "", ""

        claims = db["job_claims"]
        self._ensure_claim_indexes(claims)
        now = datetime.now(timezone.utc)
        doc = {
            "_id": key,
            "job_id": job_id,
            "claimed_at": now,
            "expires_at": claim_expiry(ttl_seconds),
        }
        try:
            claims.insert_one(doc)
            return True, "", ""
        except Exception as exc:
            if not _is_duplicate_key(exc):
                # A claim store problem must not block data movement; log it and
                # let the transfer proceed rather than failing closed on infra.
                logger.warning("idempotency claim failed for %s: %s", key, exc)
                return True, "", ""

        existing = claims.find_one({"_id": key}) or {}
        holder_id = str(existing.get("job_id") or "")
        holder_status = self._job_status(holder_id) if holder_id else ""
        expires_at = existing.get("expires_at")

        stale = (
            not holder_id
            or holder_status in TERMINAL_JOB_STATUSES
            or (isinstance(expires_at, datetime) and _as_utc(expires_at) <= now)
        )
        if stale:
            # Take over: either the previous run finished, or it died without
            # releasing and its claim has aged out.
            result = claims.update_one(
                {"_id": key, "job_id": existing.get("job_id")},
                {"$set": {**doc, "superseded_job_id": holder_id}},
            )
            if result.matched_count:
                return True, "", ""
            # Lost a race to another taker; fall through and report the winner.
            existing = claims.find_one({"_id": key}) or {}
            holder_id = str(existing.get("job_id") or "")
            holder_status = self._job_status(holder_id) if holder_id else ""

        return False, holder_id, holder_status

    def bind_job_idempotency(
        self, key: str, from_job_id: str, to_job_id: str
    ) -> bool:
        """Atomically replace the holder id on an existing claim.

        Used after the job document is created so the claim that was reserved
        with a placeholder id points at the real job, without releasing the
        slot in between.
        """
        if not key or not from_job_id or not to_job_id:
            return False
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        try:
            result = db["job_claims"].update_one(
                {"_id": key, "job_id": from_job_id},
                {
                    "$set": {
                        "job_id": to_job_id,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            return result.matched_count > 0
        except Exception as exc:
            logger.warning("idempotency bind failed for %s: %s", key, exc)
            return False

    def release_job_idempotency(self, key: str, job_id: str = "") -> bool:
        """Release a claim once its job reaches a terminal state.

        Scoped to ``job_id`` when supplied so a late release from a superseded
        run cannot free the claim held by the job that took over from it.
        """
        if not key:
            return False
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        query: dict[str, Any] = {"_id": key}
        if job_id:
            query["job_id"] = job_id
        try:
            return db["job_claims"].delete_one(query).deleted_count > 0
        except Exception as exc:
            logger.warning("idempotency release failed for %s: %s", key, exc)
            return False

    def _ensure_claim_indexes(self, claims: Any) -> None:
        """Add the TTL index that reclaims abandoned claims.

        Attempted once per process. A missing TTL index only means expired
        claims linger until the explicit staleness check above notices them, so
        failure here is logged rather than raised.
        """
        if getattr(self, "_claim_indexes_ready", False):
            return
        try:
            claims.create_index("expires_at", expireAfterSeconds=0)
            claims.create_index("job_id")
        except Exception as exc:
            logger.debug("job_claims index setup skipped: %s", exc)
        self._claim_indexes_ready = True

    def _job_status(self, job_id: str) -> str:
        """Current status of a job, or '' when it cannot be read."""
        key = job_key_filter(job_id)
        if not key:
            return ""
        try:
            doc = self.get_database()["transfer_jobs"].find_one(key, {"status": 1})
        except Exception:
            return ""
        return str((doc or {}).get("status") or "")

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        """Patch job metadata without changing status (e.g. rename)."""
        if not fields:
            return False
        try:
            db = self.get_database()
        except ConnectionError:
            return False
        collection = db["transfer_jobs"]
        key = job_key_filter(job_id)
        if not key:
            return False
        updates = {**fields, "updated_at": datetime.now(timezone.utc)}
        result = collection.update_one(key, {"$set": updates})
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

        key = job_key_filter(job_id)
        result = collection.find_one(key) if key else None
        if result:
            result["_id"] = str(result["_id"])
        return result

    def list_jobs(self, limit: int = 50, workspace_id: str | None = None) -> list[dict]:
        """List recent transfer jobs, optionally filtered to a workspace."""
        from services.job_list_view import slim_job_for_list

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
        # Projection drops the heaviest arrays at the Mongo layer when possible.
        projection = {
            "rejected_details": 0,
            "logs": 0,
            "log_lines": 0,
            "events": 0,
            "chunks": 0,
            "mapping_proof": 0,
            "sample_rows": 0,
            "preview_rows": 0,
            "quarantine_rows": 0,
            "quarantine_samples": 0,
            "destination_summary.rejected_details": 0,
            "destination_summary.sample_rows": 0,
            "destination_summary.preview_rows": 0,
            "source_summary.sample_rows": 0,
            "reconciliation.mismatches": 0,
            "reconciliation.sample_mismatches": 0,
        }
        for doc in collection.find(query, projection).sort("created_at", -1).limit(limit):
            doc["_id"] = str(doc["_id"])
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if doc.get(key) and hasattr(doc[key], "isoformat"):
                    doc[key] = doc[key].isoformat()
            jobs.append(slim_job_for_list(doc))
        return jobs


class MemoryMongoDBService:
    """In-memory fallback for tests and DATAFLOW_JOB_STORE=memory.

    Mirrors the small subset of MongoDBService used by routers and the
    transfer engine without requiring a running MongoDB server.
    """

    def __init__(self):
        self._connectors: dict[str, dict] = {}
        self._jobs: dict[str, dict] = {}
        self._claims: dict[str, dict] = {}
        self._claim_lock = threading.Lock()
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

    def claim_job_idempotency(
        self,
        *,
        key: str,
        job_id: str,
        ttl_seconds: int | None = None,
    ) -> tuple[bool, str, str]:
        """In-process mirror of the Mongo claim.

        Held under a lock so two threads submitting the same transfer race the
        same way they would against the database, which is what makes the
        single-process test for this defect meaningful.
        """
        from services.job_idempotency import claim_expiry

        if not key or not job_id:
            return True, "", ""
        now = datetime.now(timezone.utc)
        with self._claim_lock:
            existing = self._claims.get(key)
            if existing:
                holder_id = str(existing.get("job_id") or "")
                holder_status = str(
                    (self._jobs.get(holder_id) or {}).get("status") or ""
                )
                expires_at = existing.get("expires_at")
                stale = (
                    not holder_id
                    or holder_status in TERMINAL_JOB_STATUSES
                    or (isinstance(expires_at, datetime) and _as_utc(expires_at) <= now)
                )
                if not stale:
                    return False, holder_id, holder_status
            self._claims[key] = {
                "job_id": job_id,
                "claimed_at": now,
                "expires_at": claim_expiry(ttl_seconds),
            }
            return True, "", ""

    def bind_job_idempotency(
        self, key: str, from_job_id: str, to_job_id: str
    ) -> bool:
        if not key or not from_job_id or not to_job_id:
            return False
        with self._claim_lock:
            existing = self._claims.get(key)
            if not existing:
                return False
            if str(existing.get("job_id") or "") != from_job_id:
                return False
            existing["job_id"] = to_job_id
            return True

    def release_job_idempotency(self, key: str, job_id: str = "") -> bool:
        if not key:
            return False
        with self._claim_lock:
            existing = self._claims.get(key)
            if not existing:
                return False
            if job_id and str(existing.get("job_id") or "") != job_id:
                return False
            del self._claims[key]
            return True

    def save_connector(self, connector_data: dict) -> str:
        oid = self._new_id()
        rec = _encrypt_connector_secrets(dict(connector_data))
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
            return _decrypt_connector_secrets(rec)
        return None

    def list_connectors(self) -> list[dict]:
        items = sorted(
            self._connectors.values(),
            key=lambda c: c.get("created_at") or "",
            reverse=True,
        )
        out = []
        for c in items:
            row = dict(c, _id=str(c["_id"]))
            out.append(_decrypt_connector_secrets(row) or row)
        return out

    def update_connector(self, connector_id: str, updates: dict) -> bool:
        rec = self._connectors.get(connector_id)
        if not rec:
            return False
        rec.update(_encrypt_connector_secrets(updates))
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
        job = dict(job_data)
        oid = str(job.get("_id") or job.get("job_id") or self._new_id())
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
            # Fail-closed resume requires a job shell — mint one for programmatic
            # execute_tracked(job_id=…) callers (memory store / tests / CLI).
            self.create_transfer_job({"_id": job_id, "status": status or "pending"})
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

        prev_phase = rec.get("phase")
        prev_message = str(rec.get("message") or "").strip()
        prev_rows = int(rec.get("records_processed") or 0)
        prev_log = list(rec.get("event_log") or [])

        prev_marks = rec.get("throughput_marks")

        rec.update(kwargs)
        rec["status"] = status
        rec["updated_at"] = datetime.now(timezone.utc)
        if "records_processed" in kwargs and "throughput_marks" not in kwargs:
            marks = append_throughput_mark(prev_marks, kwargs.get("records_processed"))
            if marks is not None:
                rec["throughput_marks"] = marks
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
                    except Exception as exc:
                        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                if line_parts:
                    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    for part in line_parts:
                        prev_log.append(f"{stamp} — {part}")
                    rec["event_log"] = prev_log[-200:]
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
        from services.job_list_view import slim_job_for_list

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
            out.append(slim_job_for_list(job))
        return out


# Global instance
_mongodb_service: Optional[MongoDBService] = None


def get_mongodb_service() -> MongoDBService:
    """Get or create MongoDB service instance."""
    global _mongodb_service
    if _mongodb_service is None:
        if getenv_brand("JOB_STORE", "").lower() == "memory":
            _mongodb_service = MemoryMongoDBService()
            _mongodb_service.connect()
        else:
            _mongodb_service = MongoDBService()
            _mongodb_service.connect()
    return _mongodb_service
