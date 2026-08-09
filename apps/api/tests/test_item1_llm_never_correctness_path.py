"""ITEM 1 — LLM must never be in the data-fidelity correctness path.

Proof: Map→Validate→Execute with every LLM provider forcibly disabled must
produce byte-identical mapping decisions, DDL invent, and population checksum
versus a run where LLM is "enabled" but returns adversarial remaps.

If any decision field differs, the LLM is still deciding — fail closed.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.decision_kernel import materialize_dest_ddl
from services.mapping_pipeline import run_mapping_pipeline
from services.llm_mapping import refine_mappings_with_llm
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _decision_fingerprint(mappings: list[dict]) -> str:
    """Canonical Execute-authority fields only (not suggestion / reasoning)."""
    rows = []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        rows.append(
            {
                "source": m.get("source"),
                "target": m.get("target"),
                "target_type": m.get("target_type") or m.get("dest_type"),
                "transform": m.get("transform") or m.get("transformation") or "none",
                "create_new": bool(m.get("create_new")),
                "assignment_strategy": m.get("assignment_strategy") or "",
            }
        )
    rows.sort(key=lambda r: (str(r["source"]), str(r["target"])))
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _adversarial_llm_response() -> MagicMock:
    """LLM that would remap every column to a wrong target if it could decide."""
    mock_response = MagicMock()
    mock_response.success = True
    mock_response.content = json.dumps(
        {
            "mappings": [
                {
                    "source": "cust_id",
                    "target": "notes",
                    "confidence": 0.99,
                    "reason": "adversarial remap",
                    "transformation": "upper",
                },
                {
                    "source": "amt",
                    "target": "cust_id",
                    "confidence": 0.99,
                    "reason": "adversarial remap",
                },
                {
                    "source": "notes",
                    "target": "amt",
                    "confidence": 0.99,
                    "reason": "adversarial remap",
                },
            ]
        }
    )
    mock_response.provider = "adversarial_mock"
    return mock_response


def test_refine_adversarial_llm_cannot_change_decision_targets():
    baseline = [
        {"source": "cust_id", "target": "cust_id", "confidence": 0.95, "transform": None},
        {"source": "amt", "target": "amt", "confidence": 0.9, "transform": None},
        {"source": "notes", "target": "notes", "confidence": 0.88, "transform": None},
    ]
    targets = ["cust_id", "amt", "notes"]
    sources = ["cust_id", "amt", "notes"]

    off, meta_off = refine_mappings_with_llm(
        baseline, sources, targets, enabled=False
    )
    mock_chain = MagicMock()
    mock_chain.generate.return_value = _adversarial_llm_response()
    with (
        patch("services.llm_mapping.llm_provider_available", return_value=True),
        patch("services.llm_mapping.is_llm_enabled", return_value=True),
        patch("services.llm_mapping.is_pii_masking_enabled", return_value=True),
        patch("src.ai.llm.fallback.DataTransferFallbackChain", return_value=mock_chain),
    ):
        on, meta_on = refine_mappings_with_llm(
            baseline, sources, targets, enabled=True
        )

    assert meta_off["llm_used"] is False
    assert meta_on["llm_used"] is True
    assert meta_on.get("llm_decides") is False
    assert _decision_fingerprint(off) == _decision_fingerprint(on)
    for m in on:
        assert m["target"] == m["source"]
        # Adversarial remap must not become the decision target.
        assert m["target"] != "notes" or m["source"] == "notes"


def test_pipeline_llm_on_vs_off_identical_decisions():
    """Full mapping pipeline: adversarial LLM must not change Execute stamps."""
    source_cols = ["cust_id", "amt", "notes"]
    target_cols = ["cust_id", "amt", "notes"]
    schemas = [
        {"name": "cust_id", "inferred_type": "BIGINT", "samples": ["1", "2"]},
        {"name": "amt", "inferred_type": "DECIMAL(12,2)", "samples": ["10.50", "20.00"]},
        {"name": "notes", "inferred_type": "TEXT", "samples": ["a", "b"]},
    ]
    kwargs = dict(
        source_columns=source_cols,
        target_columns=target_cols,
        source_schemas=schemas,
        target_schemas=[
            {"name": c, "inferred_type": t, "samples": []}
            for c, t in (
                ("cust_id", "BIGINT"),
                ("amt", "DECIMAL(12,2)"),
                ("notes", "TEXT"),
            )
        ],
        confidence_threshold=0.5,
        validation_mode="warn",
        destination_db_type="sqlite",
        destination_table_exists=True,
    )

    off = run_mapping_pipeline(**kwargs, use_llm=False)
    mock_chain = MagicMock()
    mock_chain.generate.return_value = _adversarial_llm_response()
    with (
        patch("services.llm_mapping.llm_provider_available", return_value=True),
        patch("services.llm_mapping.is_llm_enabled", return_value=True),
        patch("services.llm_mapping.is_pii_masking_enabled", return_value=True),
        patch("src.ai.llm.fallback.DataTransferFallbackChain", return_value=mock_chain),
    ):
        on = run_mapping_pipeline(**kwargs, use_llm=True)

    assert off["llm"]["llm_used"] is False
    # LLM may be consulted, but decisions must match.
    assert _decision_fingerprint(off["mappings"]) == _decision_fingerprint(on["mappings"])
    for m in on["mappings"]:
        ddl_off = materialize_dest_ddl("sqlite", m.get("target_type") or "TEXT")
        ddl_on = materialize_dest_ddl(
            "sqlite",
            next(
                x.get("target_type")
                for x in off["mappings"]
                if x.get("source") == m.get("source")
            )
            or "TEXT",
        )
        assert ddl_off == ddl_on


def test_execute_llm_disabled_vs_adversarial_identical_checksum(tmp_path: Path):
    """End-to-end SQLite transfer: checksum identical with LLM off vs adversarial on."""
    src = tmp_path / "item1_src.sqlite"
    dst_off = tmp_path / "item1_dst_off.sqlite"
    dst_on = tmp_path / "item1_dst_on.sqlite"
    eng = create_engine(f"sqlite:///{src}")
    try:
        with eng.begin() as c:
            c.execute(
                text(
                    "CREATE TABLE t (cust_id INTEGER PRIMARY KEY, amt REAL, notes TEXT)"
                )
            )
            c.execute(
                text(
                    "INSERT INTO t VALUES (1, 10.5, 'a'), (2, 20.0, 'b'), (3, 30.25, 'c')"
                )
            )
    finally:
        eng.dispose()

    maps = [
        {
            "source": "cust_id",
            "target": "cust_id",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "amt",
            "target": "amt",
            "target_type": "DOUBLE",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "notes",
            "target": "notes",
            "target_type": "TEXT",
            "approved": True,
            "confidence": 0.99,
        },
    ]

    def _xfer(dst: Path) -> object:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database", format="sqlite", database=str(src), table="t"
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dst), table="t"
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="warn",
            skip_preflight=True,
            mappings=maps,
        )
        return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])

    # Engine auto-map already forces use_llm=False; still patch providers away.
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "DATAFLOW_LLM_ENABLED": "false",
            "LLM_ENABLED": "false",
        },
        clear=False,
    ):
        r_off = _xfer(dst_off)
    assert r_off.success, r_off.error

    mock_chain = MagicMock()
    mock_chain.generate.return_value = _adversarial_llm_response()
    with (
        patch.dict(
            os.environ,
            {
                "DATAFLOW_LLM_ENABLED": "true",
                "LLM_ENABLED": "true",
            },
            clear=False,
        ),
        patch("services.llm_mapping.llm_provider_available", return_value=True),
        patch("src.ai.llm.fallback.DataTransferFallbackChain", return_value=mock_chain),
    ):
        r_on = _xfer(dst_on)
    assert r_on.success, r_on.error
    assert r_off.records_transferred == r_on.records_transferred == 3

    def _table_checksum(db: Path) -> str:
        e = create_engine(f"sqlite:///{db}")
        try:
            with e.connect() as c:
                rows = c.execute(
                    text("SELECT cust_id, amt, notes FROM t ORDER BY cust_id")
                ).fetchall()
        finally:
            e.dispose()
        blob = json.dumps([list(r) for r in rows], sort_keys=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    assert _table_checksum(dst_off) == _table_checksum(dst_on)


def test_offline_env_forces_llm_policy_off(monkeypatch):
    """No API keys + LLM_ENABLED=false → refine is deterministic-only."""
    monkeypatch.setenv("LLM_ENABLED", "false")
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from services.llm_policy import is_llm_enabled

    assert is_llm_enabled() is False
    baseline = [{"source": "id", "target": "id", "confidence": 0.99}]
    merged, meta = refine_mappings_with_llm(
        baseline, ["id"], ["id"], enabled=True
    )
    assert merged == baseline
    assert meta["llm_used"] is False
