"""Q1 (train review): a stored decision stamp keeps a schedule running.

Q1: a schedule stamped on feature/Cleanup (70cb4669c) must keep running after
this release unless its plan really changed. This release reclassified
PostgreSQL uuid -> Snowflake STRING/TEXT/VARCHAR (lossy -> lossless) and
replanned bit(1) -> MySQL (BOOLEAN -> BIT(1)) and unbounded varbit -> MySQL
(BIT(64) -> LONGTEXT). The fixture freezes the stamps the merge-base code
minted for those Maps (tests/fixtures/decision_stamps_feature_cleanup_70cb4669c.json,
made by running build_artifact_from_mappings / approved_mapping_ddl_fingerprint
at 70cb4669c); every one is enforced on the new code here.

"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from services.conversion_contract import assert_ddl_identity
from services.decision_kernel.execute_gate import (
    build_artifact_from_mappings,
    enforce_decision_artifact,
)
from services.decision_kernel.models import (
    compute_content_hash,
    decision_stamps_match,
    scored_structural_hash,
)

_FIX = json.loads(
    (Path(__file__).parent / "fixtures" / "decision_stamps_feature_cleanup_70cb4669c.json").read_text()
)
_CASES = sorted(_FIX["cases"])
#: How the previous release's DDL reads back from each destination catalog.
_LIVE_OLD = {"BOOLEAN": "tinyint(1)", "BIT(64)": "bit(64)", "VARCHAR": "VARCHAR(16777216)",
             "TEXT": "VARCHAR(16777216)", "varbit(100)": "varbit(100)"}


def _enforce(case: dict, maps: list[dict], *, live: dict | None, exists: bool):
    return enforce_decision_artifact(
        mappings=maps,
        dest_db=case["dest_db"],
        source_db=case["source_db"],
        approved_content_hash=case["content_hash"],
        source_fingerprint=_FIX["source_fingerprint"],
        dest_fingerprint=_FIX["dest_fingerprint"] if exists else "",
        destination_table_exists=exists,
        dest_column_names=["id", "c"] if exists else None,
        source_column_names=["id", "c"],
        live_column_types=live,
        sync_mode="incremental_append",
        error_policy="quarantine",
    )


@pytest.mark.parametrize("name", _CASES)
def test_a_feature_cleanup_stamp_of_an_unchanged_map_still_runs(name):
    case = _FIX["cases"][name]
    exists = name.endswith("|existing")
    live = None
    if exists:
        live = {"id": "bigint" if case["dest_db"] == "mysql" else "NUMBER(38,0)",
                "c": _LIVE_OLD.get(case["column_ddl"]["c"], case["column_ddl"]["c"])}
    err, art = _enforce(case, case["mappings"], live=live, exists=exists)
    assert err is None, (name, err)
    assert art is not None
    # The Map -> DDL identity stamp the schedule carries is still accepted.
    assert assert_ddl_identity(case["ddl_identity_hash"], case["mappings"], dest_db=case["dest_db"],
                               allow_previous_release=True)


@pytest.mark.parametrize("name", [n for n in _CASES if n.startswith(("bit1", "varbit_"))
                                  and n.endswith("|existing")])
def test_an_existing_table_may_hold_either_releases_type_but_not_a_third(name):
    case = _FIX["cases"][name]
    new_type = "bit(1)" if name.startswith("bit1") else "longtext"
    err, _ = _enforce(case, case["mappings"], live={"id": "bigint", "c": new_type}, exists=True)
    assert err is None, err
    err, art = _enforce(case, case["mappings"], live={"id": "bigint", "c": "varchar(5)"}, exists=True)
    assert err and art is None


@pytest.mark.parametrize("name", _CASES)
def test_a_real_plan_change_is_still_refused(name):
    case = _FIX["cases"][name]
    changed = [dict(m) for m in case["mappings"]]
    changed[0] = {**changed[0], "target_type": "VARCHAR(12)"}
    exists = name.endswith("|existing")
    err, art = _enforce(case, changed, live=None, exists=exists)
    assert err and art is None, name


@pytest.mark.parametrize("name", [n for n in _CASES if n.endswith("|create_new")])
def test_the_schedule_side_checks_keep_the_old_stamp(name, monkeypatch, tmp_path):
    from services import stamp_key_records
    from services.schedule_approvals import create_new_stamp_matches_schedule

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(stamp_key_records, "_collection", lambda: None)

    monkeypatch.setattr("services.connector_store.get_connector",
                        lambda *a, **k: SimpleNamespace(to_dict=lambda: {"type": "postgresql"}))

    case = _FIX["cases"][name]
    sched = SimpleNamespace(
        id="s-" + name.replace("|", "-"),
        mappings=case["mappings"], approved_decision_artifact_hash=case["content_hash"],
        sync_mode="incremental_append", stream_contracts=[], source_connector_id="",
        workspace_id="", source_schema_fingerprint=_FIX["source_fingerprint"],
    )
    assert create_new_stamp_matches_schedule(sched, dest_db=case["dest_db"])


def _art(maps: list[dict]):
    return build_artifact_from_mappings(
        maps, dest_db="postgresql", source_db="postgresql", source_fingerprint="fp",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:postgresql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )


def test_classification_is_not_plan_content_but_a_risk_contract_is():
    art = _art([{"source": "a", "target": "a", "source_type": "uuid", "target_type": "text",
                 "confidence": 0.9, "create_new": True}])
    payload = art.to_dict()
    regraded = json.loads(json.dumps(payload))
    conv = regraded["mappings"][0]["conversion"]
    conv.update(reason="graded differently", risk_level="safe", recommended_action="proceed")
    assert compute_content_hash(regraded) == compute_content_hash(payload)
    contracted = json.loads(json.dumps(payload))
    contracted["mappings"][0]["conversion"]["risk_contract_id"] = "rc_1"
    assert compute_content_hash(contracted) != compute_content_hash(payload)
    # A stamp minted with the classification inside (fix/qa-batch1) is accepted.
    assert decision_stamps_match(scored_structural_hash(payload), art)


@pytest.mark.parametrize("name", [n for n in _CASES if n.endswith("|create_new")])
def test_saving_the_schedule_keeps_its_old_stamp(name, monkeypatch):
    """The save path (align_schedule_decision_hash) keeps a stamp it can replay."""
    from services.schedule_decision_stamp import replay_decision_hashes

    case = _FIX["cases"][name]
    kinds = {"src": "postgresql", "dst": case["dest_db"]}
    monkeypatch.setattr(
        "services.connector_store.get_connector",
        lambda cid, *a, **k: SimpleNamespace(type=kinds.get(cid, ""),
                                             to_dict=lambda: {"type": kinds.get(cid, "")}),
    )
    hashes = replay_decision_hashes({
        "mappings": case["mappings"], "dest_connector_id": "dst", "source_connector_id": "src",
        "sync_mode": "incremental_append", "source_schema_fingerprint": _FIX["source_fingerprint"],
    })
    assert case["content_hash"] in hashes, name
