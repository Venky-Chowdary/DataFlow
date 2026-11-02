"""The request body both proof-pack verify routes accept (PP3).

``POST /transfer/proof-pack/verify`` took ``{"pack": {...}}`` with the pack
defaulting to ``{}``, so an empty or unwrapped body "verified" an empty pack
and answered 200 "content_sha256 mismatch". ``POST /audit/verify-pack`` took
the pack itself. A pack that verified on one route failed on the other.

Both now accept the same two shapes — ``{"pack": <pack>}`` and ``<pack>`` —
and refuse with 422, naming the missing keys, a body that is not a pack. A
real pack whose content was altered is still a 200 verdict (``ok: false``):
that is the verifier's answer, not a malformed request.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

#: Keys every exported signed proof pack carries.
REQUIRED_PACK_KEYS: tuple[str, ...] = ("content_sha256", "signature", "job_id")


def _missing(pack: dict[str, Any]) -> list[str]:
    missing = [key for key in REQUIRED_PACK_KEYS if key not in pack]
    signature = pack.get("signature")
    if "signature" in pack and not (isinstance(signature, dict) and signature.get("value")):
        missing.append("signature.value")
    return missing


def pack_from_body(body: Any) -> dict[str, Any]:
    """The pack in ``body`` (wrapped or not), or a 422 naming what is missing."""
    if isinstance(body, dict) and "pack" in body and "content_sha256" not in body:
        candidate = body.get("pack")
    else:
        candidate = body
    if not isinstance(candidate, dict):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Body must be an exported proof pack, or {\"pack\": <pack>}.",
                "missing": list(REQUIRED_PACK_KEYS),
            },
        )
    missing = _missing(candidate)
    if missing:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Not a signed proof pack: missing " + ", ".join(missing) + ".",
                "missing": missing,
            },
        )
    return candidate
