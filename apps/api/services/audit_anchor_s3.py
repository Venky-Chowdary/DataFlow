"""S3 Object Lock (compliance mode) anchor provider for :mod:`services.audit_anchor`.

An anchor is a small JSON object naming the digest it seals, written with
``ObjectLockMode=COMPLIANCE`` and a retain-until date: until then nobody, the
bucket owner and the root account included, can overwrite or delete that
version. The bucket must have been created with Object Lock enabled.

Every write is read back before it counts: the bucket's Object Lock
configuration, the version's retention (mode and retain-until) and the body's
digest. Anything that cannot be confirmed answers ``failed`` with the reason;
nothing here falls back to the local stub, and nothing reports ``anchored``
on the strength of a PUT alone. :func:`verify_location` repeats the read-back
for a stored receipt (verify-on-read).

Settings (``getenv_brand``: ``DATAWRAP_*`` or ``DATAFLOW_*``):
``AUDIT_ANCHOR_S3_BUCKET`` (required), ``AUDIT_ANCHOR_S3_PREFIX`` (default
``audit-anchors``), ``AUDIT_ANCHOR_RETENTION_DAYS`` (default 2555, 1..36500),
and the client settings ``AUDIT_ANCHOR_S3_ENDPOINT`` / ``_REGION`` /
``_PATH_STYLE`` / ``_ACCESS_KEY`` / ``_SECRET_KEY`` with the export store's
fallbacks (:func:`services.export_object_store.s3_client_for`).
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from services.brand_env import getenv_brand

PROVIDER = "s3"
MODE = "COMPLIANCE"
DEFAULT_PREFIX = "audit-anchors"
RETENTION_DAYS_DEFAULT, RETENTION_DAYS_MIN, RETENTION_DAYS_MAX = 2555, 1, 36500
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


class AnchorUnconfirmed(RuntimeError):
    """The anchor could not be written or confirmed; the message says why."""


def bucket() -> str:
    return str(getenv_brand("AUDIT_ANCHOR_S3_BUCKET", "") or "").strip()


def prefix() -> str:
    raw = str(getenv_brand("AUDIT_ANCHOR_S3_PREFIX", "") or "").strip().strip("/")
    parts = [_SAFE.sub("_", p) for p in raw.split("/") if p not in ("", ".", "..")]
    return "/".join(parts) or DEFAULT_PREFIX


def retention_days() -> int:
    raw = str(getenv_brand("AUDIT_ANCHOR_RETENTION_DAYS", "") or "").strip()
    try:
        days = int(float(raw)) if raw else RETENTION_DAYS_DEFAULT
    except ValueError:
        days = RETENTION_DAYS_DEFAULT
    return max(RETENTION_DAYS_MIN, min(RETENTION_DAYS_MAX, days))


def client() -> Any:
    from services.export_object_store import s3_client_for

    return s3_client_for("AUDIT_ANCHOR_")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _aware(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _error(exc: Exception) -> str:
    """The S3 error code and class only (never the request or credentials)."""
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str((response.get("Error") or {}).get("Code") or "")
    return f"{type(exc).__name__}{(' ' + code) if code else ''}"


def object_key(kind: str, digest: str, at: datetime) -> str:
    return "/".join((
        prefix(), _SAFE.sub("_", kind) or "anchor", at.strftime("%Y/%m/%d"),
        f"{_SAFE.sub('_', digest)[:128]}-{uuid.uuid4().hex[:8]}.json",
    ))


def _body(digest: str, kind: str, subject: str, anchored_at: str, event_time: str) -> bytes:
    payload = {"digest": digest, "kind": kind, "subject": subject, "anchored_at": anchored_at,
               "event_time": event_time or None, "provider": PROVIDER}
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _require_object_lock(s3: Any, name: str) -> None:
    try:
        config = s3.get_object_lock_configuration(Bucket=name).get("ObjectLockConfiguration") or {}
    except Exception as exc:  # noqa: BLE001 - no configuration is no write-once guarantee
        raise AnchorUnconfirmed(
            f"the bucket's Object Lock configuration could not be read ({_error(exc)})"
        ) from exc
    if str(config.get("ObjectLockEnabled") or "") != "Enabled":
        raise AnchorUnconfirmed("the anchor bucket does not have Object Lock enabled")


def verify_location(location: dict[str, Any], digest: str, *, s3: Any = None,
                    now: datetime | None = None) -> dict[str, Any]:
    """Read an anchor back: retention mode, retain-until and the body's digest.

    Returns ``{status: anchored|failed, retention, reason}``.
    """
    name, key = str(location.get("bucket") or ""), str(location.get("key") or "")
    version = location.get("version_id") or None
    if not name or not key:
        return {"status": "failed", "retention": None, "reason": "the receipt names no anchor object"}
    s3 = s3 or client()
    extra = {"VersionId": version} if version else {}
    try:
        _require_object_lock(s3, name)
        # HeadObject carries the version's lock (x-amz-object-lock-mode / -retain-until-date).
        head = s3.head_object(Bucket=name, Key=key, **extra)
        retention = {"Mode": head.get("ObjectLockMode"), "RetainUntilDate": head.get("ObjectLockRetainUntilDate")}
        body = s3.get_object(Bucket=name, Key=key, **extra)["Body"].read()
    except AnchorUnconfirmed as exc:
        return {"status": "failed", "retention": None, "reason": str(exc)}
    except Exception as exc:  # noqa: BLE001 - an anchor that cannot be read back is not confirmed
        return {"status": "failed", "retention": None,
                "reason": f"the anchor object could not be read back ({_error(exc)})"}
    mode = str(retention.get("Mode") or "")
    until = _aware(retention.get("RetainUntilDate"))
    held = {"mode": mode or None, "retain_until": _iso(until) if until else None}
    if mode != MODE:
        return {"status": "failed", "retention": held,
                "reason": f"the anchor object is not under compliance-mode retention (mode {mode or 'none'})"}
    if until is None or until <= (now or datetime.now(timezone.utc)):
        return {"status": "failed", "retention": held,
                "reason": "the anchor object's retention period has ended; it is no longer write-once"}
    try:
        stored = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        stored = {}
    if str(stored.get("digest") or "") != digest:
        return {"status": "failed", "retention": held,
                "reason": "the anchor object holds a different digest than the receipt"}
    return {"status": "anchored", "retention": held, "reason": None}


def find_location(digest: str, *, kind: str, around: Any = None, s3: Any = None) -> dict[str, Any] | None:
    """The bucket location of ``digest``'s anchor, looked up in the bucket (no local index).

    Keys are ``<prefix>/<kind>/YYYY/MM/DD/<digest>-<n>.json``; the day of
    ``around`` and the day after are listed (an anchor is written right after
    signing). Every version under the key is a candidate; the first one that
    reads back ``anchored`` wins, else the last one found (it then reads back
    as the failure it is).
    """
    name = bucket()
    when = _aware(around) if around is not None else None
    if not name or when is None:
        return None
    s3 = s3 or client()
    found: dict[str, Any] | None = None
    for day in (when, when + timedelta(days=1)):
        stem = object_key(kind, digest, day).rsplit("-", 1)[0] + "-"
        try:
            listed = s3.list_object_versions(Bucket=name, Prefix=stem)
        except Exception:  # noqa: BLE001 - an unreadable bucket finds nothing; verify says not anchored
            return found
        for version in listed.get("Versions") or []:
            location = {"bucket": name, "key": version["Key"], "version_id": version.get("VersionId")}
            if verify_location(location, digest, s3=s3)["status"] == "anchored":
                return location
            found = location
    return found


def put_anchor(digest: str, *, kind: str, subject: str, event_time: str = "", s3: Any = None,
               now: datetime | None = None) -> dict[str, Any]:
    """Write one compliance-mode anchor and confirm it.

    Returns ``{status, location, retention, reason}``; ``status`` is
    ``anchored`` only after the read-back confirmed it.
    """
    name = bucket()
    if not name:
        return {"status": "failed", "location": None, "retention": None,
                "reason": "DATAFLOW_AUDIT_ANCHOR_S3_BUCKET is not set, so nothing was anchored"}
    at = now or datetime.now(timezone.utc)
    until = at + timedelta(days=retention_days())
    key = object_key(kind, digest, at)
    s3 = s3 or client()
    body = _body(digest, kind, subject, _iso(at), event_time)
    try:
        _require_object_lock(s3, name)
        resp = s3.put_object(
            Bucket=name, Key=key, Body=body, ContentType="application/json",
            ContentMD5=_md5_b64(body), ObjectLockMode=MODE, ObjectLockRetainUntilDate=until,
        )
    except AnchorUnconfirmed as exc:
        return {"status": "failed", "location": None, "retention": None, "reason": str(exc)}
    except Exception as exc:  # noqa: BLE001 - the write did not happen: say so, never pretend
        return {"status": "failed", "location": None, "retention": None,
                "reason": f"the anchor could not be written ({_error(exc)})"}
    location = {"bucket": name, "key": key, "version_id": resp.get("VersionId")}
    checked = verify_location(location, digest, s3=s3, now=at)
    return {"status": checked["status"], "location": location,
            "retention": checked["retention"], "reason": checked["reason"]}


def _md5_b64(body: bytes) -> str:
    import base64

    # Object Lock PUTs must carry an integrity header; MD5 is what S3 requires for it here.
    return base64.b64encode(hashlib.md5(body).digest()).decode("ascii")  # noqa: S324
