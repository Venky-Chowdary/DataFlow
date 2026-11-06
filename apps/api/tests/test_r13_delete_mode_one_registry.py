"""F3 — one ``delete_mode`` registry, every consumer routed through it.

Four vocabularies had grown: the MCP contract helper accepted soft|hard and
said "mirror only", the tool schemas enumerated soft|hard, SCD2/mirror checked
{hard, soft}, the sweep registered off. Now ``transfer_policy_values`` owns
the values, the per-sync subsets (mirror: hard|soft, default soft; cdc:
hard|soft|off, default hard-when-safe) and the error text; every site asks it.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from services.transfer_policy_values import (
    DELETE_MODE_CONSUMERS,
    DELETE_MODE_DEFAULTS,
    DELETE_MODES,
    POLICY_FIELDS,
    DeleteModeNotAllowed,
    PolicyValueError,
    canonical_policy_value,
    canonicalize_policy_fields,
    delete_mode_for,
)


def test_registry_shape_and_documented_default_divergence():
    assert DELETE_MODES == {"hard", "soft", "off"}
    assert DELETE_MODE_CONSUMERS["mirror"] == {"hard", "soft"} and DELETE_MODE_CONSUMERS["cdc"] == DELETE_MODES
    assert DELETE_MODE_DEFAULTS == {"mirror": "soft", "cdc": "hard"}
    assert "delete_mode" in POLICY_FIELDS
    assert canonicalize_policy_fields({"delete_mode": "OFF", "validation_mode": "strict"})["delete_mode"] == "off"
    with pytest.raises(PolicyValueError, match="hard, off, soft"):
        canonical_policy_value("delete_mode", "purge")


def test_delete_mode_for_applies_the_consumer_subset():
    assert delete_mode_for("", "mirror") == "" and delete_mode_for(None, "cdc") == ""
    assert delete_mode_for("HARD", "mirror") == "hard"
    assert delete_mode_for("off", "cdc") == "off"
    with pytest.raises(DeleteModeNotAllowed, match="not available for mirror sync. Allowed: hard, soft"):
        delete_mode_for("off", "mirror")
    with pytest.raises(PolicyValueError):
        delete_mode_for("purge", "cdc")


def test_mcp_contract_helper_accepts_cdc_off_and_refuses_mirror_off():
    from services.mirror_delete_notice import declare_contract_delete_mode

    contracts = [{"name": "t", "primary_key": ["id"]}]
    out, err = declare_contract_delete_mode(contracts, "cdc", "off")
    assert err == "" and out[0]["delete_mode"] == "off"
    out, err = declare_contract_delete_mode(contracts, "mirror", "off")
    assert "not available for mirror sync" in err and "delete_mode" not in out[0]
    _, err = declare_contract_delete_mode(contracts, "incremental_upsert", "hard")
    assert "mirror and cdc sync only" in err
    _, err = declare_contract_delete_mode(contracts, "cdc", "purge")
    assert "hard, off, soft" in err


def test_mcp_tool_schemas_enumerate_the_registry():
    src = (Path(__file__).resolve().parents[1] / "src" / "ai" / "copilot" / "tools.py").read_text(encoding="utf-8")
    enums = re.findall(r'"delete_mode": \{\s*"type": "string",\s*"enum": (\[[^\]]*\])', src)
    assert enums and all(set(eval(e)) == DELETE_MODES for e in enums), enums  # noqa: S307 — literal list from source


def test_mirror_resolution_refuses_off_with_the_allowed_list():
    from services.mirror_engine import resolve_mirror_delete_mode

    assert resolve_mirror_delete_mode(SimpleNamespace(delete_mode="")) == "soft"
    assert resolve_mirror_delete_mode(SimpleNamespace(delete_mode="hard")) == "hard"
    with pytest.raises(DeleteModeNotAllowed, match="Allowed: hard, soft"):
        resolve_mirror_delete_mode(SimpleNamespace(delete_mode="off"))
    with pytest.raises(PolicyValueError):
        resolve_mirror_delete_mode(None, {"delete_mode": "purge"})


def test_sweep_plan_uses_the_registry():
    from services.cdc_snapshot_sweep import plan_snapshot_sweep

    plan = {"run_snapshot": True, "reason": "watermark_missing"}
    kw = dict(dest_type="postgresql", dest_absent_before=False, source_cfg={"type": "mysql"},
              mappings=[], pk_sources=["id"])
    assert plan_snapshot_sweep(plan, delete_mode="Off", **kw).mode == "off"
    assert plan_snapshot_sweep(plan, delete_mode="", **kw).mode == DELETE_MODE_DEFAULTS["cdc"]
