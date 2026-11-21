"""Assignment and suggestion policy around the semantic mapper's pair scores.

Kept apart from :mod:`services.semantic_mapper` (Phase F8 size freeze). Every
rule here is generic (no column names from any benchmark) and was tuned only on
the dev split of the held-out set (tests/fixtures/mapping_holdout_v1.json).

* :func:`pin_decisive_pairs`: a source and a destination that are each other's
  clear best match are bound before the bipartite solver runs. Maximising the
  *sum* of uncalibrated scores let two weak pairs (0.70 + 0.70) outvote one
  decisive pair (0.99 + a leftover), so ``dept_cd`` lost ``department_code``
  to a column it shares no token with.
* :func:`rescore_pairs`: candidate evidence the lexical scorer lacks.
  Abbreviations it has no dictionary entry for (``dsch_dt`` -> ``discharge_date``,
  ``los`` -> ``length_of_stay``) and value patterns in the source samples
  (an email-shaped column onto the only email column). Inferred pairs always require review.
* :func:`suggestion_policy`: one confidence line (0.70) between "suggest this
  existing column" and "no match, plan a new column", whichever path produced
  the row.
"""

from __future__ import annotations

import re
from typing import Any, Callable

# A pair is decisive when it is the best for its source and for its
# destination by at least this margin, and scores at least this much.
DECISIVE_MIN_SCORE = 0.75
DECISIVE_MARGIN = 0.05
# Review suggestions below this confidence are not shown as the mapping: the
# row becomes "no match" with the candidate kept first in its alternatives.
# Dev split: proposals under 0.70 were right 1 time in 6; 0.70-0.72, 6 in 9.
SUGGESTION_FLOOR = 0.70
INFERRED_ABBREVIATION_SCORE = 0.80
VALUE_PATTERN_MATCH_SCORE = 0.80
REVIEW_TAG = "review required"


# --------------------------------------------------------------------------
# Decisive pairs
# --------------------------------------------------------------------------


def pin_decisive_pairs(
    pinned: dict[str, str],
    source_columns: list[str],
    target_columns: list[str],
    pair_scores: dict[tuple[str, str], tuple[float, str]],
) -> dict[str, str]:
    """Extend ``pinned`` with mutual-best pairs; returns a new dict."""
    out = dict(pinned)
    changed = True
    while changed:
        changed = False
        free_src = [s for s in source_columns if s not in out]
        taken = set(out.values())
        free_tgt = [t for t in target_columns if t not in taken]
        if not free_src or not free_tgt:
            break
        for src in free_src:
            row = sorted(((pair_scores[(src, t)][0], t) for t in free_tgt), reverse=True)
            best, tgt = row[0]
            if best < DECISIVE_MIN_SCORE:
                continue
            if len(row) > 1 and best - row[1][0] < DECISIVE_MARGIN:
                continue
            col = sorted((pair_scores[(s, tgt)][0] for s in free_src if s != src), reverse=True)
            if col and best - col[0] < DECISIVE_MARGIN:
                continue
            out[src] = tgt
            changed = True
            break
    return out


# --------------------------------------------------------------------------
# Name tokens
# --------------------------------------------------------------------------

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SPLIT = re.compile(r"[^a-z0-9]+")
_LEAF_FILLERS = frozenset({"code", "id", "number", "name", "value", "type", "flag"})
_STOP = frozenset({"of", "the", "and", "to", "for", "at", "by", "in", "on"})


_DIGITS = re.compile(r"(?<=[a-z])(?=[0-9])|(?<=[0-9])(?=[a-z])")
_VOWELS = frozenset("aeiou")


def name_tokens(name: str) -> list[str]:
    raw = _CAMEL.sub("_", str(name or "").split(".")[-1].replace("[]", ""))
    raw = _DIGITS.sub("_", raw.lower())
    return [t for t in _SPLIT.split(raw) if t]


def _subseq(short: str, long: str) -> bool:
    it = iter(long)
    return all(ch in it for ch in short)


def _skeleton(piece: str) -> bool:
    """Letters after the first are consonants: how words get shortened."""
    return not (set(piece[1:]) & _VOWELS)


def _abbreviates(tok: str, word: str) -> bool:
    """``tok`` is a plausible abbreviation of ``word``.

    Same first letter, shorter, and either a truncation (``phys`` ->
    ``physician``) or a consonant skeleton taken in order (``dsch`` ->
    ``discharge``, ``bldg`` -> ``building``). A real word with inner vowels
    (``rate``) is not read as a skeleton of another word (``registration``).
    """
    if tok == word:
        return True
    if len(tok) < 3 or len(tok) >= len(word) or tok[0] != word[0] or tok.isdigit():
        return False
    if word.startswith(tok):
        return True
    return _skeleton(tok) and _subseq(tok, word)


def _covers(tok: str, words: list[str], expand: Callable[[str], list[str]]) -> bool:
    """``tok`` stands for the consecutive target ``words``."""
    exp = expand(tok)
    if exp and exp == words:
        return True
    if len(words) == 1:
        return _abbreviates(tok, words[0])
    # Acronym / initial-letter skeleton: each word contributes a piece that
    # starts with its first letter and is otherwise consonants
    # (``los`` -> length_of_stay, ``fnm`` -> first_name; not ``paid`` -> payment_id).
    def rec(i: int, k: int) -> bool:
        if k == len(words):
            return i == len(tok)
        if i >= len(tok) or tok[i] != words[k][0]:
            return False
        for j in range(i + 1, len(tok) + 1):
            piece = tok[i:j]
            if _skeleton(piece) and _subseq(piece, words[k]) and rec(j, k + 1):
                return True
        return False

    return len(tok) >= 2 and not tok.isdigit() and rec(0, 0)


def inferred_abbreviation(
    source: str,
    target: str,
    *,
    entity_prefixes: frozenset[str] = frozenset(),
    expand: Callable[[str], list[str]] = lambda t: [t],
) -> bool:
    """True when every source token abbreviates consecutive target words in order.

    Target words left over must be generic leaves (``drg`` -> ``drg_code``);
    a leading source token shared by several source columns is the table's
    entity prefix (``emp_`` in ``emp_fnm``) and may be absent from the target.
    """
    src = name_tokens(source)
    tgt = [t for t in name_tokens(target)]
    if not src or not tgt or src == tgt:
        return False
    if len(src) > 1 and src[0] in entity_prefixes and src[0] not in tgt:
        src = src[1:]
    # Monotone alignment by DP over (source index, target index).
    from functools import lru_cache

    @lru_cache(maxsize=None)
    def go(i: int, k: int) -> bool:
        if i == len(src):
            return all(w in _LEAF_FILLERS or w in _STOP for w in tgt[k:])
        if k == len(tgt):
            return False
        # A target stopword or filler may be skipped.
        if (tgt[k] in _STOP) and go(i, k + 1):
            return True
        for j in range(k + 1, min(len(tgt), k + 4) + 1):
            if _covers(src[i], tgt[k:j], expand) and go(i + 1, j):
                return True
        return False

    if not go(0, 0):
        return False
    # At least one source token must be a real abbreviation (not every token
    # equal): otherwise the lexical scorer already saw it.
    return any(t not in tgt for t in src)


# --------------------------------------------------------------------------
# Value patterns
# --------------------------------------------------------------------------

_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
_URL = re.compile(r"^(https?://|www\.)\S+$", re.I)
# Stripe-style object ids (cus_9s6, ch_3Mx, pm_1): a short lowercase prefix,
# then a token with a digit or a capital, so snake_case words never match.
_PREFIXED_ID = re.compile(r"^[a-z]{2,6}_(?=[A-Za-z0-9]*[0-9A-Z])[A-Za-z0-9]+$")
_NAME_KIND = {
    "email": frozenset({"email", "mail"}),
    "url": frozenset({"url", "website", "homepage", "link", "uri"}),
}


def sample_kind(samples: list[str] | None) -> str:
    vals = [str(s).strip() for s in (samples or []) if str(s or "").strip()]
    if not vals:
        return ""
    for kind, rx in (("email", _EMAIL), ("url", _URL), ("prefixed_id", _PREFIXED_ID)):
        if all(rx.match(v) for v in vals):
            return kind
    return "other"


def name_kind(name: str) -> str:
    toks = set(name_tokens(name))
    for kind, words in _NAME_KIND.items():
        if toks & words:
            return kind
    return ""


# --------------------------------------------------------------------------
# Rescoring
# --------------------------------------------------------------------------


def _entity_prefixes(source_columns: list[str]) -> frozenset[str]:
    firsts: dict[str, int] = {}
    for s in source_columns:
        toks = name_tokens(s)
        if len(toks) > 1:
            firsts[toks[0]] = firsts.get(toks[0], 0) + 1
    return frozenset(t for t, n in firsts.items() if n >= 2)


def rescore_pairs(
    pair_scores: dict[tuple[str, str], tuple[float, str]],
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_samples: dict[str, list[str]] | None = None,
    expand: Callable[[str], list[str]] = lambda t: [t],
    blocked: Callable[[str, str], bool] = lambda s, t: False,
) -> None:
    """Adjust ``pair_scores`` in place with abbreviation and value-pattern evidence."""
    samples = source_samples or {}
    prefixes = _entity_prefixes(source_columns)
    tgt_kinds = {t: name_kind(t) for t in target_columns}
    for src in source_columns:
        skind = sample_kind(samples.get(src))
        for tgt in target_columns:
            if blocked(src, tgt):
                continue
            score, reason = pair_scores[(src, tgt)]
            tkind = tgt_kinds[tgt]
            if score >= INFERRED_ABBREVIATION_SCORE:
                continue
            if tkind and skind == tkind and sum(1 for k in tgt_kinds.values() if k == tkind) == 1:
                pair_scores[(src, tgt)] = (
                    max(score, VALUE_PATTERN_MATCH_SCORE),
                    f"Samples are {tkind} values and this is the only {tkind} column — {REVIEW_TAG}",
                )
                continue
            if inferred_abbreviation(src, tgt, entity_prefixes=prefixes, expand=expand):
                pair_scores[(src, tgt)] = (
                    INFERRED_ABBREVIATION_SCORE,
                    f"Inferred abbreviation of {tgt} — {REVIEW_TAG}",
                )


# --------------------------------------------------------------------------
# Suggestion policy
# --------------------------------------------------------------------------

_KEEP_STRATEGIES = frozenset({
    "same_name_existing", "identity_passthrough", "pending_dest_schema", "create_compatible_new",
})


def _no_match_row(row: dict[str, Any], ctx: dict[str, Any], taken: set[str]) -> dict[str, Any]:
    """The row the mapper emits when a source has no destination column."""
    from services.decision_kernel import create_new_mapping_target_type, ddl_type
    from services.semantic_mapper import _semantic_form, create_new_target_name

    source = row["source"]
    src_type = row.get("source_type") or "VARCHAR"
    dest_db = ctx.get("dest_db") or ""
    candidate = source.strip() or _semantic_form(source)
    if candidate.lower() in taken:
        sem = _semantic_form(source)
        candidate = sem if sem.lower() not in taken else candidate
    if candidate.lower() in taken:
        base = re.sub(r"[^A-Za-z0-9_]+", "_", candidate).strip("_") or "field"
        candidate = f"{base}_text" if f"{base}_text".lower() not in taken else f"src_{base}"
    why = (
        f"Best existing column {row['target']} scored {row['confidence']:.2f}, below the "
        f"{SUGGESTION_FLOOR:.2f} suggestion floor — kept as the first alternative, not proposed"
    )
    base_row = {
        "source": source,
        "semantic_name": _semantic_form(source),
        "confidence": SUGGESTION_FLOOR,
        "user_override": False,
        "source_type": src_type,
        "alternatives": row.get("alternatives") or [],
        "score_gap": 0.0,
        "requires_review": True,
        "create_new": False,
    }
    if ctx.get("names_only_existing"):
        return {
            **base_row,
            "target": create_new_target_name(source),
            "confidence": 0.55,
            "reasoning": f"{why}. Destination column types were not loaded — retry introspect.",
            "target_type": "",
            "assignment_strategy": "pending_dest_schema",
        }
    native = ddl_type(dest_db, src_type) if dest_db else src_type
    return {
        **base_row,
        "target": candidate,
        "reasoning": (
            f"{why}. No confident destination column — map to a new field "
            f"(create/ADD as {native}); do not coerce into incompatible DDL"
        ),
        "target_type": create_new_mapping_target_type(
            src_type, dest_db, samples=(ctx.get("source_samples") or {}).get(source),
            source_db=ctx.get("src_db") or "",
        ),
        "assignment_strategy": "create_compatible_new",
        "create_new": True,
    }


def suggestion_policy(
    mappings: list[dict[str, Any]],
    target_columns: list[str],
    ctx: dict[str, Any],
) -> list[dict[str, Any]]:
    """Apply the suggestion floor, then let freed columns back-fill "no match" rows."""
    tset = set(target_columns)
    return _backfill_freed_alternatives(hide_low_suggestions(mappings, target_columns, ctx), tset, ctx)


def hide_low_suggestions(
    mappings: list[dict[str, Any]],
    target_columns: list[str],
    ctx: dict[str, Any],
) -> list[dict[str, Any]]:
    """Hide review suggestions under :data:`SUGGESTION_FLOOR`.

    A proposal onto an existing column that the mapper itself rates below the
    floor is noise in the review queue (Hungarian leftovers such as
    ``etl_batch_id`` -> ``first_name``). It becomes the same "no match" row the
    mapper emits when nothing fits, with the candidate first in its
    alternatives, so an operator can still pick it.
    """
    tset = set(target_columns)
    taken = {t.lower() for t in target_columns} | {str(m.get("target") or "").lower() for m in mappings}
    out = []
    for row in mappings:
        tgt = row.get("target")
        if (
            tgt in tset
            and not row.get("create_new")
            and not row.get("user_override")
            and row.get("assignment_strategy") not in _KEEP_STRATEGIES
            and float(row.get("confidence") or 0.0) < SUGGESTION_FLOOR
            and not str(row.get("reasoning") or "").startswith("Exact")
        ):
            new = _no_match_row(row, ctx, taken)
            taken.add(str(new["target"]).lower())
            out.append(new)
        else:
            out.append(row)
    return out


def _backfill_freed_alternatives(
    rows: list[dict[str, Any]], tset: set[str], ctx: dict[str, Any]
) -> list[dict[str, Any]]:
    """A "no match" row whose first alternative is a free existing column at or
    above the floor proposes that column for review instead of a new one."""
    from services.semantic_mapper import _calibrated_confidence, _scored_dest_type

    used = {r.get("target") for r in rows if r.get("target") in tset}
    order = sorted(
        (i for i, r in enumerate(rows) if r.get("create_new") and r.get("alternatives")),
        key=lambda i: -float(rows[i]["alternatives"][0].get("confidence") or 0.0),
    )
    for i in order:
        row = rows[i]
        alts = row["alternatives"]
        best = alts[0]
        score = float(best.get("confidence") or 0.0)
        tgt = best.get("target")
        if tgt not in tset or tgt in used or score < SUGGESTION_FLOOR:
            continue
        if row.get("assignment_strategy") == "identity_passthrough":
            continue
        gap = round(max(score - (float(alts[1].get("confidence") or 0.0) if len(alts) > 1 else 0.0), 0.0), 3)
        used.add(tgt)
        rows[i] = {
            **{k: v for k, v in row.items() if k not in ("create_new", "semantic_name", "create_new_risks")},
            "target": tgt,
            "confidence": _calibrated_confidence(score, score_gap=gap, requires_review=True),
            "reasoning": f"{best.get('reasoning')} · freed existing column preferred over a new one — {REVIEW_TAG}",
            "assignment_strategy": "freed_alternative_review",
            "target_type": _scored_dest_type(
                row.get("source_type") or "VARCHAR",
                (ctx.get("target_types") or {}).get(tgt, "VARCHAR"),
                ctx.get("dest_db") or "",
            ),
            "score_gap": gap,
            "requires_review": True,
        }
    return rows
