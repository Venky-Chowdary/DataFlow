"""Parity at Scale: every row of a table of any size compared inside the engines.

Status: **experimental, behind ``DATAWRAP_PARITY_SCALE=1``** (``DATAFLOW_`` also
read). Phases 0–2 of ``PARITY_AT_SCALE_DESIGN.md``: the CE1 canonical encoding
with its Python reference, the scan driver, and the dialects of the first
certification wave (SQLite, PostgreSQL, MySQL / MariaDB, SQL Server) with the
CE matrix that certifies them (:mod:`.certification`). Nothing here feeds a
verdict, Gate-8, a proof pack or the Trust Report yet (phase 3).

Shape: each engine encodes every row with CE1 (:mod:`.ce1`) from a column plan
both sides share (:mod:`.plan`), hashes it with SHA-256 and sums two 60-bit
words of the hash with a row count per comparison unit — a key range (mode R)
or a hash bucket (mode H). Units whose ``(count, Σa, Σb)`` differ are narrowed
recursively; small units return ``(key, row hash)`` pairs, the driver merges
them into exact missing / extra / changed keys, and only those rows are
fetched and handed to the existing cell judge (:mod:`.judge`), which rules with
Gate-8's own fingerprint and the Parity Studio classifier. Keyless tables are
compared as a multiset of row contents ("Content match (no key)"): a
difference is counted as missing and extra content, never attributed to a key.

Soundness invariant: ``CE1(a) == CE1(b)`` ⇒ ``fingerprint_v2(a) ==
fingerprint_v2(b)``. CE1 may be stricter than the Gate-8 fingerprint, never
looser (see :mod:`.ce1`).

Not a tamper seal: an additive hash protects against accidental change, not an
adversary who controls the data. Tamper evidence stays with the signed proof
pack and the audit chain.
"""

from __future__ import annotations

from typing import Final

FLAG: Final[str] = "PARITY_SCALE"


class ParityScaleDisabled(RuntimeError):
    """Raised when a scan is started while the feature flag is off."""


def parity_scale_enabled() -> bool:
    """``DATAWRAP_PARITY_SCALE`` (or ``DATAFLOW_PARITY_SCALE``) is on. Default off."""
    from services.brand_env import getenv_brand

    return (getenv_brand("PARITY_SCALE", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}


def require_enabled() -> None:
    if not parity_scale_enabled():
        raise ParityScaleDisabled(
            "Parity at Scale is experimental and off: set DATAWRAP_PARITY_SCALE=1 to run a scan."
        )
