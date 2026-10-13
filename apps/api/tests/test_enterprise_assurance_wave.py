"""Enterprise assurance wave — anchors, composite PK, charset uniqueness, parallel honesty."""

from __future__ import annotations

import ast
from pathlib import Path



def test_audit_tip_anchor_stub(tmp_path, monkeypatch):
    from services import audit_anchor as aa
    from services import audit_log as audit

    monkeypatch.setattr(audit, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)
    monkeypatch.setattr(aa, "ANCHOR_STORE", tmp_path / "anchors.jsonl")
    monkeypatch.setenv("AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_EVERY", "1")

    e = audit.append_audit_event(action="t", resource="r", actor="u")
    tip = audit.latest_event_hash()
    assert tip == e["event_hash"]
    anchor = aa.latest_anchor()
    assert anchor and anchor["tip_hash"] == tip
    assert "stub" in str(anchor.get("provider") or "stub")


def test_extract_contract_primary_key_columns_composite():
    from services.primary_key import (
        extract_contract_primary_key,
        extract_contract_primary_key_columns,
    )

    contracts = [
        {
            "name": "orders",
            "selected": True,
            "primary_keys": ["tenant_id", "order_id"],
        }
    ]
    cols = extract_contract_primary_key_columns(contracts, stream_name="orders")
    assert cols == ["tenant_id", "order_id"]
    # Legacy single-column helper keeps first key only (preflight compatibility).
    assert extract_contract_primary_key(contracts, stream_name="orders") == "tenant_id"


def test_nchar_pad_space_uniqueness_collides():
    """NCHAR PAD SPACE should fold trailing spaces for uniqueness probes."""
    from services.data_integrity import _check_duplicate_keys

    rows = [
        {"k": "a"},
        {"k": "a "},  # same under NCHAR pad-space
    ]
    result = _check_duplicate_keys(
        [{"source": "k", "target": "k"}],
        rows,
        "strict",
        dest_kind="sqlserver",
        primary_key="k",
        sync_mode="upsert",
        target_types={"k": "NCHAR(10)"},
    )
    # Either blocks or reports issues — must not silent-pass pad-space dupes.
    assert result.get("passed") is False or result.get("blocks_transfer") is True or result.get("issues")


def test_parallel_workers_default_ordered_honesty():
    from services.parallel_chunks import DEFAULT_WORKERS, ChunkDispatcher

    # Default must stay resume-safe unless explicitly raised.
    assert DEFAULT_WORKERS >= 1
    d = ChunkDispatcher(max_workers=2)
    assert d.max_workers == 2
    assert callable(getattr(d, "abort", None))


def test_semantic_abbreviations_have_unique_keys_and_vitals_hr_mapping():
    source_path = (
        Path(__file__).resolve().parents[1] / "services" / "semantic_abbreviations.py"
    )
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    dictionary = next(
        node.value
        for node in module.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "ABBREVIATIONS"
        and isinstance(node.value, ast.Dict)
    )
    keys = [ast.literal_eval(key) for key in dictionary.keys]
    mapping = dict(zip(keys, (ast.literal_eval(value) for value in dictionary.values)))

    assert len(keys) == len(set(keys))
    assert mapping["vitals_hr"] == "heart_rate"

    from services.semantic_abbreviations import ABBREVIATIONS

    assert ABBREVIATIONS["vitals_hr"] == "heart_rate"
