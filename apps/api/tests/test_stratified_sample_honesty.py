"""Stratified Gate-8 sample — fail-closed honesty and rare-class coverage."""

from __future__ import annotations

from services.reconciliation import sample_compare_rows


def test_stratified_preserves_rare_class_when_buckets_exceed_sample_size():
    """Hash-trimming must not drop rare buckets (first-N / trim trap)."""
    rows: list[dict] = []
    # 12 common statuses + 1 rare — sample_size 8 cannot hold all buckets.
    for s in range(12):
        for i in range(5):
            rows.append({"id": s * 10 + i, "status": f"s{s}", "amt": i})
    rows.append({"id": 999, "status": "rare_bomb", "amt": -1})
    target = [dict(r) for r in rows]
    target[-1] = {"id": 999, "status": "rare_bomb", "amt": 9999}
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "status", "target": "status"},
        {"source": "amt", "target": "amt"},
    ]
    result = sample_compare_rows(
        rows,
        target,
        mappings,
        sample_size=8,
        sort_key="id",
        stratify_by="status",
    )
    assert result["sample_seed"]["method"] == "stratified"
    assert result["sample_seed"]["population_proof"] is False
    assert result["sample_seed"]["coverage"] == "sample"
    pk_vals = set(result["sample_seed"].get("pk_values") or [])
    assert "999" in pk_vals
    assert result["passed"] is False
    assert any(m.get("source") == "amt" for m in result["mismatches"])


def test_stratified_sample_includes_rare_category_under_budget():
    rows = [{"id": i, "status": "ok", "amt": i} for i in range(40)]
    rows.append({"id": 100, "status": "rare_fail", "amt": -1})
    rows.append({"id": 101, "status": "rare_fail", "amt": -2})
    target = list(rows)
    target[-1] = {"id": 101, "status": "rare_fail", "amt": 999}
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "status", "target": "status"},
        {"source": "amt", "target": "amt"},
    ]
    result = sample_compare_rows(
        rows,
        target,
        mappings,
        sample_size=10,
        sort_key="id",
        stratify_by="status",
    )
    assert result["sample_seed"]["method"] == "stratified"
    assert result["sample_seed"]["stratify_by"] == "status"
    assert result["sample_seed"]["population_proof"] is False
    assert result["passed"] is False
    assert any(m.get("source") == "amt" for m in result["mismatches"])


def test_auto_stratify_skips_identity_sort_key():
    """Auto-stratify must not pick the PK / sort key as the stratum."""
    rows = [{"id": i, "status": "ok" if i < 90 else "rare", "v": i} for i in range(100)]
    # Corrupt a rare row
    target = [dict(r) for r in rows]
    target[95] = {"id": 95, "status": "rare", "v": 0}
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "status", "target": "status"},
        {"source": "v", "target": "v"},
    ]
    result = sample_compare_rows(
        rows,
        target,
        mappings,
        sample_size=20,
        sort_key="id",
    )
    assert result["sample_seed"]["method"] == "stratified"
    assert result["sample_seed"]["stratify_by"] == "status"
    assert result["sample_seed"]["auto_selected"] is True
    assert result["sample_seed"]["population_proof"] is False


def test_stratified_indices_align_to_dict_working_set():
    """Non-dict rows must not shift stratified indices into the wrong records."""
    rows: list = [
        {"id": 1, "status": "a", "amt": 1},
        "skip-me",
        {"id": 2, "status": "b", "amt": 2},
        {"id": 3, "status": "a", "amt": 3},
    ]
    target = [
        {"id": 1, "status": "a", "amt": 1},
        {"id": 2, "status": "b", "amt": 99},
        {"id": 3, "status": "a", "amt": 3},
    ]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "status", "target": "status"},
        {"source": "amt", "target": "amt"},
    ]
    result = sample_compare_rows(
        rows,
        target,
        mappings,
        sample_size=2,
        sort_key="id",
        stratify_by="status",
    )
    assert result["sample_seed"]["method"] == "stratified"
    assert all(isinstance(v, str) for v in (result["sample_seed"].get("pk_values") or []))
    # Must not crash; compared rows are dicts only
    assert result["compared"] >= 1
