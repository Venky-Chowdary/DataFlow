"""One connector resolver: name, display name and id, case- and spacing-blind.

QA asked Pilot about "orders postgres", "ORDERS  POSTGRES", "orders-postgres" and
"OrdersPostgres" for a connector saved as "Orders Postgres" and was told
"Connector not found". A person names a connector the way they read it; the
resolver compares what identifies it (id, name, display name) after folding
case, whitespace and punctuation, and asks which one when more than one fits.

Two entry points share the same matching:

* :func:`reference_matches` — a typed reference (a tool's ``connector_name`` or
  ``connector_id``) against the saved connectors.
* :func:`find_connector_mention` — the span of a free-text question that names
  a saved connector, so the question's other words are not taken for it.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

# Words that may sit around a connector mention and belong to it.
_LEAD = re.compile(r"(?:\b(?:on|in|from|using|via|at|of)\s+)?(?:\bthe\s+)?$", re.I)
_TRAIL = re.compile(r"^\s+(?:connector|connection|database|db|warehouse|instance)\b", re.I)
_TOKEN = re.compile(r"[^\s\"'`“”‘’?!,;()]+")
_MAX_WINDOW = 8


def _fold(text: str) -> str:
    return unicodedata.normalize("NFKC", str(text or "")).casefold()


def normalize_label(text: str) -> str:
    """Case-blind, whitespace-collapsed: "ORDERS  POSTGRES" == "orders postgres"."""
    return " ".join(_fold(text).split())


def compact_label(text: str) -> str:
    """Letters and digits only: "orders-postgres" == "OrdersPostgres"."""
    return "".join(ch for ch in _fold(text) if ch.isalnum())


def connector_labels(conn: dict[str, Any]) -> list[str]:
    """The name and any display name a connector is shown under."""
    extra = conn.get("extra") if isinstance(conn.get("extra"), dict) else {}
    labels = [
        conn.get("name"),
        conn.get("display_name"),
        conn.get("displayName"),
        (extra or {}).get("display_name"),
        (extra or {}).get("label"),
    ]
    out: list[str] = []
    for label in labels:
        text = str(label or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _conn_id(conn: dict[str, Any]) -> str:
    return str(conn.get("id") or conn.get("_id") or "").strip()


def reference_matches(needle: str, pool: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Connectors a typed reference names, from the strictest tier that matches.

    Tiers: id; name/display name ignoring case and spacing; name/display name
    ignoring punctuation too. A looser tier is consulted only when a stricter
    one found nothing, so "Orders Postgres" never ties with "Orders-Postgres".
    """
    raw = str(needle or "").strip()
    if not raw:
        return []
    want_id = _fold(raw)
    tiers: list[list[dict[str, Any]]] = [[], [], []]
    want_norm = normalize_label(raw)
    want_compact = compact_label(raw)
    for conn in pool:
        cid = _conn_id(conn)
        if cid and _fold(cid) == want_id:
            tiers[0].append(conn)
            continue
        labels = connector_labels(conn)
        if any(normalize_label(label) == want_norm for label in labels):
            tiers[1].append(conn)
        elif len(want_compact) >= 3 and any(compact_label(label) == want_compact for label in labels):
            tiers[2].append(conn)
    for tier in tiers:
        if tier:
            return tier
    return []


def describe(conn: dict[str, Any]) -> str:
    label = str(conn.get("name") or "").strip() or "(unnamed)"
    cid = _conn_id(conn)
    return f"{label} ({cid})" if cid else label


def ambiguity_message(needle: str, matches: list[dict[str, Any]]) -> str:
    listed = ", ".join(f"**{describe(c)}**" for c in matches[:6])
    return (
        f"“{needle}” matches {len(matches)} saved connectors: {listed}. "
        "Which one did you mean? Name it exactly or pass its id."
    )


@dataclass
class ConnectorMention:
    start: int
    end: int
    text: str
    matches: list[dict[str, Any]] = field(default_factory=list)

    @property
    def unique(self) -> dict[str, Any] | None:
        return self.matches[0] if len(self.matches) == 1 else None


def _distinctive_single_token(token: str, conn_matches: list[dict[str, Any]]) -> bool:
    """A one-word mention is a connector only when it cannot be an ordinary word.

    "orders" may be a table; "OrdersPostgres", "orders-postgres", "pg_prod2" or a
    single token spelling a multi-word name ("orderspostgres") is not.
    """
    if re.search(r"[A-Z]", token[1:]) or re.search(r"[\d_\-.]", token):
        return True
    for conn in conn_matches:
        if any(len(label.split()) > 1 for label in connector_labels(conn)):
            return True
    return False


def find_connector_mention(message: str, pool: list[dict[str, Any]]) -> ConnectorMention | None:
    """The longest span of ``message`` that names a saved connector, or None.

    The span grows to take a leading "on/in/from the" and a trailing
    "connector/database" so the rest of the sentence reads without it.
    """
    text = message or ""
    if not text.strip() or not pool:
        return None
    tokens = [(m.start(), m.end(), m.group(0)) for m in _TOKEN.finditer(text)]
    best: ConnectorMention | None = None
    for i in range(len(tokens)):
        for j in range(min(len(tokens), i + _MAX_WINDOW), i, -1):
            start, end = tokens[i][0], tokens[j - 1][1]
            span = text[start:end].rstrip(".:")
            if not span:
                continue
            matches = reference_matches(span, pool)
            if not matches:
                continue
            if j - i == 1 and not any(_conn_id(c) and _fold(_conn_id(c)) == _fold(span) for c in matches):
                if not (_distinctive_single_token(span, matches) or _LEAD_STRONG.search(text[:start])):
                    continue
            if best is None or (end - start) > (best.end - best.start):
                best = ConnectorMention(start, start + len(span), span, matches)
            break
    if best is None:
        return None
    lead = _LEAD.search(text[: best.start])
    trail = _TRAIL.match(text[best.end :])
    start = lead.start() if lead else best.start
    end = best.end + (trail.end() if trail else 0)
    return ConnectorMention(start, end, best.text, best.matches)


# "using X" / "via X" / "on X" before a single plain word means a connector.
_LEAD_STRONG = re.compile(r"\b(?:on|using|via)\s+(?:the\s+)?$", re.I)


def strip_mention(message: str, mention: ConnectorMention) -> str:
    """The question without the connector mention, spaces tidied."""
    rest = (message[: mention.start] + " " + message[mention.end :]).strip()
    return re.sub(r"\s{2,}", " ", rest)
