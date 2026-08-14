"""Choosing which rows a read-back sample should look at.

Split out of ``services.reconciliation`` (a module at its size budget). Picking
the sample is a separate question from comparing it: a uniform head sample
misses the rare category that is usually where a mapping is wrong, so a skewed
low-cardinality column is stratified over instead.

None of this makes a sample into population proof, and the seed recorded
alongside it is what lets an auditor draw the same rows again.
"""

from __future__ import annotations

from typing import Any


def normalize_cell(value: Any, *, ddl_type: str = "", engine: str = "") -> str:
    """Deferred to avoid a cycle: ``reconciliation`` re-exports this module."""
    from services.reconciliation import normalize_cell as _impl

    return _impl(value, ddl_type=ddl_type, engine=engine)


def _bucket_member_order(
    idxs: list[int], *, seed: str, bucket_name: str
) -> list[int]:
    """Deterministic intra-bucket order (stable across process restarts)."""
    import hashlib

    return sorted(
        idxs,
        key=lambda i: hashlib.sha256(
            f"{seed}:{bucket_name}:{i}".encode()
        ).hexdigest(),
    )


def _stratified_sample_indices(
    records: list[dict[str, Any]],
    *,
    stratify_col: str,
    sample_size: int,
    seed: str = "",
) -> list[int]:
    """Deterministic per-bucket quota sampling for skewed categoricals.

    Rare classes get a guaranteed slot when buckets fit in ``sample_size``.
    When bucket count exceeds ``sample_size``, prefer the *smallest* buckets
    (rare classes) — never hash-trim across buckets (that reintroduces the
    first-N trap stratification exists to prevent).

    Still a **sample** plan — never population proof.
    """
    import hashlib

    if sample_size <= 0 or not records or not stratify_col:
        return list(range(min(max(sample_size, 0), len(records))))
    buckets: dict[str, list[int]] = {}
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            continue
        raw = rec.get(stratify_col)
        key = normalize_cell(raw) if raw is not None else ""
        buckets.setdefault(key or "<null>", []).append(i)
    names = sorted(buckets.keys())
    if not names:
        return list(range(min(sample_size, len(records))))

    n_buckets = len(names)

    # More strata than slots: keep rare (smallest) buckets — 1 row each.
    if n_buckets > sample_size:
        ranked = sorted(
            names,
            key=lambda name: (
                len(buckets[name]),
                hashlib.sha256(f"{seed}:bucket:{name}".encode()).hexdigest(),
                name,
            ),
        )
        picked: list[int] = []
        for name in ranked[:sample_size]:
            scored = _bucket_member_order(
                buckets[name], seed=seed, bucket_name=name
            )
            if scored:
                picked.append(scored[0])
        return picked[:sample_size]

    # Proportional quota with floor 1 (always possible when n_buckets <= sample_size).
    base = sample_size // n_buckets
    rem = sample_size % n_buckets
    picked = []
    for bi, name in enumerate(names):
        scored = _bucket_member_order(buckets[name], seed=seed, bucket_name=name)
        take = base + (1 if bi < rem else 0)
        take = min(max(take, 1), len(scored))
        picked.extend(scored[:take])

    # Shrink from largest buckets only — never drop a bucket entirely.
    while len(picked) > sample_size:
        # Count current picks per bucket
        membership: dict[str, list[int]] = {n: [] for n in names}
        for i in picked:
            rec = records[i] if i < len(records) else {}
            raw = rec.get(stratify_col) if isinstance(rec, dict) else None
            key = normalize_cell(raw) if raw is not None else ""
            membership.setdefault(key or "<null>", []).append(i)
        # Drop one row from the largest bucket that still has >1
        candidates = [
            (len(idxs), name)
            for name, idxs in membership.items()
            if len(idxs) > 1
        ]
        if not candidates:
            # Should not happen when n_buckets <= sample_size; fail closed trim.
            picked = picked[:sample_size]
            break
        _, drop_name = max(
            candidates,
            key=lambda t: (
                t[0],
                hashlib.sha256(f"{seed}:drop:{t[1]}".encode()).hexdigest(),
            ),
        )
        drop_idxs = membership[drop_name]
        # Drop the last in deterministic bucket order
        ordered = _bucket_member_order(drop_idxs, seed=seed, bucket_name=drop_name)
        drop_i = ordered[-1]
        picked = [i for i in picked if i != drop_i]

    if len(picked) < sample_size:
        used = set(picked)
        for i in range(len(records)):
            if i not in used and isinstance(records[i], dict):
                picked.append(i)
                used.add(i)
            if len(picked) >= sample_size:
                break
    return picked[:sample_size]


def _auto_stratify_source_column(
    source_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    *,
    sort_key: str | None,
    source_sort_key: str | None,
) -> str | None:
    """Heuristic stratum: skewed low-cardinality mapped column (not the PK).

    Returns ``None`` when no safe candidate exists — caller falls back to
    keyed/positional sample. Never claims population coverage.
    """
    from collections import Counter

    exclude = {
        str(sort_key or "").strip().lower(),
        str(source_sort_key or "").strip().lower(),
        "id",
        "_id",
        "pk",
        "uuid",
        "guid",
    }
    exclude.discard("")
    best_col: str | None = None
    best_key: tuple[float, int, str] | None = None
    for m in mappings:
        src_col = str(m.get("source") or "").strip()
        tgt_col = str(m.get("target") or "").strip()
        if not src_col:
            continue
        if src_col.lower() in exclude or tgt_col.lower() in exclude:
            continue
        vals: list[str] = []
        for r in source_records:
            raw = r.get(src_col)
            cell = normalize_cell(raw) if raw is not None else ""
            vals.append(cell or "<null>")
        if not vals:
            continue
        n_classes = len(set(vals))
        if not (2 <= n_classes <= 20):
            continue
        counts = Counter(vals)
        skew = max(counts.values()) / len(vals)
        # Require imbalance so uniform enums don't pretend to stratify.
        if skew < 0.55:
            continue
        key = (skew, n_classes, src_col.lower())
        if best_key is None or key > best_key:
            best_key = key
            best_col = src_col
    return best_col
